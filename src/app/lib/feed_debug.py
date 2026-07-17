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
import math
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..documents import FeedDebugDocument, FeedSnapshotDocument
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
        # (model_name, weight, {at_uri: normalized_score}) per configured rank
        # model, in the order they were run; populated only when ranking runs.
        self.model_scores: list[tuple[str, float, dict[str, float]]] = []
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

    def record_model_scores(self, model_name: str, weight: float, scores: dict[str, float]) -> None:
        """Record one rank model's normalized per-candidate scores and weight.

        Captures the score *after* normalization to [-1, 1] (the form the
        scores are in when combined), not the model's raw output — and not
        the final combined score, which is already captured via `ranking`.
        """
        self.model_scores.append((model_name, weight, dict(scores)))

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
            FeedDebugModelScoreEntry,
            FeedDebugScoreEntry,
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
                status=r.status,
                reason=r.reason,
                mode=r.mode,
            )
            for r in self.generator_outputs
        ]
        final_candidates = [sanitize(c) for c in self.final_candidates]
        user_features = [
            FeedDebugUserFeatures(source=source, liked_post_uris=uris, num_embeddings=n)
            for source, uris, n in self.user_features
        ]
        model_scores = [
            FeedDebugModelScoreEntry(
                model_name=model_name,
                weight=weight,
                scores=[
                    FeedDebugScoreEntry(at_uri=at_uri, score=score)
                    for at_uri, score in scores.items()
                ],
            )
            for model_name, weight, scores in self.model_scores
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
            model_scores=model_scores,
            order_after_rank=self.order_after_rank,
            final_order=self.final_order,
            diversification=diversification,
            generated_at=generated_at,
            expires_at=expires_at,
        )

    def build_pipeline_metadata(
        self,
        *,
        request_id: str,
        generated_at: datetime,
        expires_at: datetime,
        applied_social_radius: int | None = None,
    ) -> "FeedSnapshotDocument":
        """Assemble a lightweight :class:`FeedSnapshotDocument` with only the
        per-URI pipeline metadata needed by the transparency API.

        No post content, author info, or user features — just enough to
        render the generator badges, rank chart, and diversification
        penalties alongside hydrated Bluesky post data.
        """
        from ..documents import (
            DiversificationMeta,
            FeedSnapshotDocument,
            GeneratorDiagnostic,
            GeneratorMeta,
            ModelScoreMeta,
            PipelineItemMeta,
        )

        # Generator legend (weights only, no scores).
        generator_legend = [
            GeneratorMeta(name=g.name, weight=g.weight)
            for g in (self.generate_request.generators if self.generate_request else [])
        ]

        # Per-URI generator contributions. Scores from different retrieval
        # systems are not comparable, so expose percentile-like strength only
        # within each generator result.
        gens_by_uri: dict[str, list[GeneratorMeta]] = {}
        for result in self.generator_outputs:
            finite_scores = [
                c.score for c in result.candidates
                if c.score is not None and math.isfinite(c.score)
            ]
            lo = min(finite_scores) if finite_scores else None
            hi = max(finite_scores) if finite_scores else None
            for c in result.candidates:
                if c.at_uri:
                    normalized = None
                    if c.score is not None and math.isfinite(c.score):
                        assert lo is not None and hi is not None
                        normalized = 1.0 if lo == hi else (c.score - lo) / (hi - lo)
                    gens_by_uri.setdefault(c.at_uri, []).append(
                        GeneratorMeta(name=result.generator_name, score=normalized)
                    )

        requested_by_name: dict[str, int] = {}
        if self.generate_request:
            from .candidates.generate import allocate_counts
            counts = allocate_counts(
                self.generate_request.generators,
                self.generate_request.num_candidates,
            )
            requested_by_name = {
                spec.name: count
                for spec, count in zip(self.generate_request.generators, counts)
            }

        diagnostics: list[GeneratorDiagnostic] = []
        specs = self.generate_request.generators if self.generate_request else []
        for spec in specs:
            staged = [
                output for output in self.generator_outputs
                if output.generator_name == spec.name and output.mode != "primary"
            ]
            if staged:
                remaining = requested_by_name.get(spec.name, 0)
                for output in staged:
                    requested = remaining
                    returned_uris = {
                        candidate.at_uri for candidate in output.candidates if candidate.at_uri
                    }
                    contributed = sum(uri in returned_uris for uri in self.final_order)
                    diagnostics.append(
                        GeneratorDiagnostic(
                            name=spec.name,
                            weight=spec.weight,
                            requested_count=requested,
                            returned_count=len(returned_uris),
                            contributed_count=contributed,
                            status=output.status,
                            reason=output.reason,
                            mode=output.mode,
                        )
                    )
                    remaining = max(0, remaining - len(returned_uris))
                continue
            matching = [
                output for output in self.generator_outputs
                if output.generator_name == spec.name and output.mode == "primary"
            ]
            returned_uris = {
                candidate.at_uri
                for output in matching
                for candidate in output.candidates
                if candidate.at_uri
            }
            output = matching[-1] if matching else None
            contributed = sum(
                1 for uri in self.final_order
                if any(g.name == spec.name for g in gens_by_uri.get(uri, []))
            )
            diagnostics.append(
                GeneratorDiagnostic(
                    name=spec.name,
                    weight=spec.weight,
                    requested_count=requested_by_name.get(spec.name, 0),
                    returned_count=len(returned_uris),
                    contributed_count=contributed,
                    status=output.status if output else "error",
                    reason=output.reason if output else "missing_generator_result",
                )
            )

        # Per-URI rank.
        rank_by_uri: dict[str, tuple[int | None, float | None]] = {}
        if self.ranking:
            for r in self.ranking.rankings:
                rank_by_uri[r.at_uri] = (r.rank, r.rank_score)

        # Per-URI model scores.
        model_scores_by_uri: dict[str, list[ModelScoreMeta]] = {}
        for model_name, weight, scores in self.model_scores:
            for at_uri, score in scores.items():
                model_scores_by_uri.setdefault(at_uri, []).append(
                    ModelScoreMeta(name=model_name, weight=weight, score=score)
                )

        # Per-URI position after ranking.
        after_rank_pos = {uri: i for i, uri in enumerate(self.order_after_rank, start=1)}

        # Per-URI diversification.
        div_by_uri: dict[str, DiversificationMeta] = {}
        for at_uri, relevance, score, author_penalty, content_penalty in self.diversification:
            div_by_uri[at_uri] = DiversificationMeta(
                relevance=relevance,
                score=score,
                author_penalty=author_penalty,
                content_penalty=content_penalty,
            )

        items_meta = []
        for pos, at_uri in enumerate(self.final_order):
            rank, rank_score = rank_by_uri.get(at_uri, (pos + 1, None))
            items_meta.append(
                PipelineItemMeta(
                    at_uri=at_uri,
                    rank=rank,
                    rank_score=rank_score,
                    after_rank_position=after_rank_pos.get(at_uri, pos + 1),
                    generators=gens_by_uri.get(at_uri, []),
                    model_scores=model_scores_by_uri.get(at_uri, []),
                    diversification=div_by_uri.get(at_uri),
                )
            )

        return FeedSnapshotDocument(
            request_id=request_id,
            items=self.final_order,
            feed_name=self.feed_name,
            generated_at=generated_at,
            expires_at=expires_at,
            ranker_model=self.ranker_model,
            diversify=self.diversify,
            generator_legend=generator_legend,
            generator_diagnostics=diagnostics,
            applied_social_radius=applied_social_radius,
            items_meta=items_meta,
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
