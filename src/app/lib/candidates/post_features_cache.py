"""Process-wide cache of the per-post features the ranker needs.

``hydrate_posts`` used to fetch every candidate's MiniLM embedding and
politics score from Elasticsearch on every feed render. Both are fixed once a
post is indexed: the embedding is computed at ingest from the post's text, and
the politics score from its stored topic scores. Popularity candidates come
from a pool shared by every user, and two-tower candidates overlap heavily
between similar users, so most of them have been fetched by an earlier render
on the same instance.

The cache is per instance and in memory, bounded by entry count. An entry
records whether the post had an embedding at all, so a post known to have none
is not looked up again until the entry expires. Entries expire after an hour,
which bounds how long a later topic-score correction can take to reach the
ranker.
"""

from __future__ import annotations

import os
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ...models import CandidatePost


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


# ~2 KB per entry (a base64 float32 384-dim vector), so the default holds about
# 16 MB: several popularity pools plus a few hundred users' worth of
# personalized candidates.
MAX_ENTRIES = _int_env("GE_POST_FEATURES_CACHE_MAX_ENTRIES", 8000)
TTL_SECONDS = _int_env("GE_POST_FEATURES_CACHE_TTL_SEC", 3600)


@dataclass(frozen=True)
class PostFeatures:
    """What hydration knows about one post.

    ``embedding`` is the base64 float32 MiniLM vector, or ``None`` when the post
    has no embedding (it is then dropped before ranking, as before).
    """

    embedding: str | None
    politics_score: float | None


class PostFeaturesCache:
    """A bounded, expiring LRU from post AT URI to :class:`PostFeatures`."""

    def __init__(self, max_entries: int = MAX_ENTRIES, ttl_seconds: float = TTL_SECONDS) -> None:
        self._max_entries = max_entries
        self._ttl = ttl_seconds
        self._entries: OrderedDict[str, tuple[PostFeatures, float]] = OrderedDict()

    def get_many(self, at_uris: list[str]) -> dict[str, PostFeatures]:
        """The cached, unexpired features for each of *at_uris* that has them."""
        now = time.monotonic()
        found: dict[str, PostFeatures] = {}
        for uri in at_uris:
            entry = self._entries.get(uri)
            if entry is None:
                continue
            features, stored_at = entry
            if now - stored_at >= self._ttl:
                del self._entries[uri]
                continue
            self._entries.move_to_end(uri)
            found[uri] = features
        return found

    def put(self, at_uri: str, features: PostFeatures) -> None:
        self._entries[at_uri] = (features, time.monotonic())
        self._entries.move_to_end(at_uri)
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)

    def clear(self) -> None:
        self._entries.clear()

    def __len__(self) -> int:
        return len(self._entries)


_cache = PostFeaturesCache()


def get_post_features_cache() -> PostFeaturesCache:
    return _cache


def remember_post_features(candidates: list["CandidatePost"]) -> None:
    """Record candidates fetched from ``posts_recent`` with their embeddings.

    For generators whose own query already returned the embedding and topic
    scores, so a later ``hydrate_posts`` treats them as resolved, including a
    post that genuinely has no topic scores, rather than looking them up again.
    Only for candidates read from an index that carries ``topic_scores``.
    """
    cache = get_post_features_cache()
    for candidate in candidates:
        if candidate.at_uri:
            cache.put(
                candidate.at_uri,
                PostFeatures(candidate.minilm_l12_embedding, candidate.politics_score),
            )
