"""Per-request capture of feed-pipeline debugging information.

A ``ContextVar`` holds the current request's :class:`FeedDebugRecorder` so the
candidate, ranking, and diversification stages can record what they did without
threading a recorder argument through every layer.  This mirrors the
``request_cache_scope`` pattern in :mod:`app.lib.request_cache`: the scope is
per-task, so concurrent requests get independent recorders and child tasks
spawned via ``asyncio.gather`` inherit the parent's recorder automatically.

When no recorder is installed (the default), the ``current_recorder()`` accessor
returns ``None`` and every record helper at the call site is skipped — so the
feature has zero cost unless a debug-enabled user triggers it.

The recorder holds the *real* pipeline objects (``CandidateGenerateRequest``,
``CandidateResult``, ``RankPredictResult``, ``CandidatePost``); the per-item
"why this item?" view is assembled at display time by the CLI.  ``build_document``
strips embeddings and truncates post content before storage.
"""

from __future__ import annotations

import contextlib
from contextvars import ContextVar
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..documents import FeedDebugDocument
    from ..models import (
        CandidateGenerateRequest,
        CandidatePost,
        RankPredictResult,
    )
    from .candidates.base import CandidateResult

# Maximum number of content characters stored per candidate (a snippet, not the
# full post) so debug documents stay well under Firestore's 1 MB limit.
CONTENT_SNIPPET_MAX = 300

_recorder: ContextVar["FeedDebugRecorder | None"] = ContextVar(
    "ge_feed_debug_recorder", default=None
)


class FeedDebugRecorder:
    """Accumulates pipeline stage outputs for one feed load.

    All record methods are cheap appends/assignments.  Stage instrumentation
    only calls them when ``current_recorder()`` is non-``None``.
    """

    def __init__(self, *, feed_name: str, regenerated: bool) -> None:
        self.feed_name = feed_name
        self.regenerated = regenerated
        self.ranker_model: str | None = None
        self.diversify: bool = False
        self.generate_request: "CandidateGenerateRequest | None" = None
        self.generator_outputs: list["CandidateResult"] = []
        self.final_candidates: list["CandidatePost"] = []
        self.user_features: list[tuple[str, list[str], int]] = []
        self.ranking: "RankPredictResult | None" = None
        self.order_after_rank: list[str] = []
        self.final_order: list[str] = []
        # (at_uri, relevance, score, author_penalty, content_penalty) in final
        # selection order; populated only when diversification runs.
        self.diversification: list[tuple[str, float, float, float, float]] = []

    # -- recording -------------------------------------------------------

    def set_generate_request(self, request: "CandidateGenerateRequest") -> None:
        self.generate_request = request

    def record_generator_output(self, result: "CandidateResult") -> None:
        self.generator_outputs.append(result)

    def record_final_candidates(self, candidates: list["CandidatePost"]) -> None:
        self.final_candidates = list(candidates)

    def record_user_features(
        self, source: str, liked_post_uris: list[str], num_embeddings: int
    ) -> None:
        self.user_features.append((source, list(liked_post_uris), num_embeddings))

    def record_ranking(self, ranking: "RankPredictResult") -> None:
        self.ranking = ranking

    def record_order_after_rank(self, uris: list[str]) -> None:
        self.order_after_rank = list(uris)

    def record_final_order(self, uris: list[str]) -> None:
        self.final_order = list(uris)

    def record_diversification(self, entries: list[tuple[str, float, float, float, float]]) -> None:
        """Record per-item diversification breakdown: (at_uri, relevance, score,
        author_penalty, content_penalty) in final selection order."""
        self.diversification = list(entries)

    # -- assembly --------------------------------------------------------

    def build_document(
        self,
        *,
        request_id: str,
        username: str | None,
        generated_at: datetime,
        expires_at: datetime,
        author_usernames: dict[str, str] | None = None,
    ) -> "FeedDebugDocument":
        """Assemble a :class:`FeedDebugDocument`, stripping embeddings, truncating
        content, and stamping resolved author handles onto stored candidates.
        """
        # Imported here (not at module top) to avoid an import cycle:
        # documents -> candidates.base -> ... -> feed_debug -> documents.
        from ..documents import (
            FeedDebugDiversificationEntry,
            FeedDebugDocument,
            FeedDebugUserFeatures,
        )
        from .candidates.base import CandidateResult

        if self.generate_request is None:
            raise ValueError("FeedDebugRecorder has no generate_request to build from")

        authors = author_usernames or {}

        def sanitize(c: "CandidatePost") -> "CandidatePost":
            content = c.content
            if content is not None and len(content) > CONTENT_SNIPPET_MAX:
                content = content[:CONTENT_SNIPPET_MAX]
            username_for_author = c.author_username
            if c.author_did and c.author_did in authors:
                username_for_author = authors[c.author_did]
            return c.model_copy(
                update={
                    "minilm_l12_embedding": None,
                    "content": content,
                    "author_username": username_for_author,
                }
            )

        generator_outputs = [
            CandidateResult(
                generator_name=r.generator_name,
                candidates=[sanitize(c) for c in r.candidates],
            )
            for r in self.generator_outputs
        ]
        final_candidates = [sanitize(c) for c in self.final_candidates]
        user_features = [
            FeedDebugUserFeatures(source=source, liked_post_uris=uris, num_embeddings=n)
            for source, uris, n in self.user_features
        ]
        diversification = [
            FeedDebugDiversificationEntry(
                at_uri=at_uri,
                relevance=relevance,
                score=score,
                author_penalty=author_penalty,
                content_penalty=content_penalty,
            )
            for at_uri, relevance, score, author_penalty, content_penalty in self.diversification
        ]

        return FeedDebugDocument(
            request_id=request_id,
            user_did=self.generate_request.user_did,
            username=username,
            feed_name=self.feed_name,
            regenerated=self.regenerated,
            generate_request=self.generate_request,
            ranker_model=self.ranker_model,
            diversify=self.diversify,
            user_features=user_features,
            generator_outputs=generator_outputs,
            final_candidates=final_candidates,
            ranking=self.ranking,
            order_after_rank=self.order_after_rank,
            final_order=self.final_order,
            diversification=diversification,
            generated_at=generated_at,
            expires_at=expires_at,
        )

    def author_dids(self) -> set[str]:
        """All distinct author DIDs across stored candidates (for handle resolution)."""
        dids: set[str] = set()
        for c in self.final_candidates:
            if c.author_did:
                dids.add(c.author_did)
        for r in self.generator_outputs:
            for c in r.candidates:
                if c.author_did:
                    dids.add(c.author_did)
        return dids


def current_recorder() -> "FeedDebugRecorder | None":
    """Return the recorder for the current request, or ``None`` if not debugging."""
    return _recorder.get()


@contextlib.contextmanager
def feed_debug_scope(recorder: "FeedDebugRecorder"):
    """Install *recorder* as the current feed-debug recorder for the block."""
    token = _recorder.set(recorder)
    try:
        yield recorder
    finally:
        _recorder.reset(token)
