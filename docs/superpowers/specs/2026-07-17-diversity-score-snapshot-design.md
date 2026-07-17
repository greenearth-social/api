# Design: Move Diversity Score Metric onto FeedSnapshotDocument (revising #206)

## Goal

Revise the diversity-score-per-batch metric (issue #206) to stop storing `diversity_score` in the cursor-pagination cache (`FeedCacheDocument`) and instead source it from PR #246's `FeedSnapshotDocument`/`PipelineItemMeta`/`DiversificationMeta`, which already captures per-item pipeline metadata for every feed load. This keeps `feed_cache.py` single-purpose (pure `list[str]` for pagination) and avoids a second, redundant "scores" concept living alongside PR #246's richer per-item metadata.

## Dependency

This branch is rebased onto `gautham-raju/frontend-endpoints` (PR #246, open/unmerged) and our PR's base is retargeted there. `FeedSnapshotDocument`, `PipelineItemMeta`, and `DiversificationMeta` only exist on that branch. If PR #246 changes materially before merging, this branch needs to re-sync.

## Data flow

1. **`diversify.py`**: the `diag` tuple in `mmr_rerank()` grows from `(at_uri, relevance, score, author_penalty, content_penalty)` to `(at_uri, relevance, score, author_penalty, content_penalty, diversity_score)`. Single source of truth for the value.
2. **`documents.py`**: both `FeedDebugDiversificationEntry` (feeds the full, debug-only `FeedDebugDocument`) and `DiversificationMeta` (feeds the always-on `FeedSnapshotDocument`) gain `diversity_score: float`.
3. **`feed_debug.py`**: `FeedDebugRecorder.record_diversification` and both consumers (`to_document()` for `FeedDebugDocument`, `build_pipeline_metadata()` for `FeedSnapshotDocument`) unpack the 6-tuple and populate the new field on each entry type.
4. **Pipeline return type**: `_run_ranking_pipeline`/`_run_pipeline_capturing` in `xrpc.py` return `tuple[list[str], dict[str, float]]` — URIs plus a `{at_uri: diversity_score}` dict (empty when `feed_cfg.diversify` is `False`). Replaces the old `list[float] | None` positional list; a dict lookup naturally handles missing entries (e.g. items with no score) without length-mismatch guards.
5. **Two paths to a page's scores**:
   - **Pipeline just ran** (initial batch or cursor regeneration): scores come from the in-memory dict returned by step 4 — no extra I/O.
   - **Page served purely from `FeedCache`** (the common cursor case, no pipeline call): fetch `get_feed_snapshot(db, user_did, parsed.id)` and build the same `{at_uri: diversity_score}` shape from `snapshot.items_meta` (skipping items with no `diversification`). Fail-soft: on any exception, log and skip the metric for that page, matching the existing `debug_enabled` lookup pattern in `get_feed_skeleton`.
6. **`_record_diversity_metric(page_uris, scores_by_uri, feed_name, batch, exclude_uri=None)`** replaces the old `_log_diversity_metric`. Averages whatever scores are found for `page_uris` (skipping any URI not in the dict); when `exclude_uri` is passed (the pinned post), it is always excluded from the mean regardless of whether it happens to have a score.
7. **`feed_cache.py`/`FeedCacheDocument`** revert to their pre-#206 shape: `FeedCache.store/retrieve/append` operate on plain `list[str]`, no `CachedFeed` dataclass, no `diversity_scores` field.
8. **`_prepend_pinned`** reverts to `(pinned_uri, uris, limit) -> list[str]` — no scores parameter. Pinned-post exclusion is handled by `_record_diversity_metric`'s `exclude_uri`, not by pre-filtering a parallel score list.

## Error handling

- `get_feed_snapshot` failures (missing doc, malformed data, Firestore error) are caught and logged; the metric is skipped for that page rather than raising. This mirrors how `get_user`/`debug_enabled` lookups are already handled fail-soft in `get_feed_skeleton`.
- `FEED_SNAPSHOT_RETENTION_SECONDS` (900s) exceeds `FeedCache.DEFAULT_TTL_SECONDS` (600s), so a snapshot should still exist whenever the corresponding cache entry does. No new TTL-mismatch handling is needed beyond the existing fail-soft path.
- When `feed_cfg.diversify` is `False`, the scores dict is empty and `_record_diversity_metric` naturally emits nothing (empty page_uris intersection with an empty dict).

## Testing

- `diversify_test.py`: update `TestMmrRerankDiversityScore` and the diag-tuple-based tests (`test_author_penalty_decays_after_intervening_selection`, `test_content_penalty_decays_after_intervening_selection`) to unpack 6 elements and assert `diversity_score` is present and correct.
- `feed_debug_test.py`: extend existing diversification tests to assert `diversity_score` is populated on both `FeedDebugDiversificationEntry` and `DiversificationMeta`.
- `feed_cache_test.py`: revert to pre-#206 shape — remove all score round-trip tests, confirm `store`/`retrieve`/`append` operate on plain `list[str]`.
- `xrpc_test.py`: replace the `FeedCacheDocument`-based diversity tests with tests that (a) verify the metric on a freshly-generated page uses the in-memory dict, (b) verify the metric on a cache-served cursor page reads from `get_feed_snapshot`, (c) verify the pinned post is excluded from the mean via `exclude_uri`, (d) verify a `get_feed_snapshot` failure skips the metric without raising.

## Scope note

This spec assumes PR #246's schema (`FeedSnapshotDocument`, `PipelineItemMeta`, `DiversificationMeta`, `get_feed_snapshot`, `merge_feed_snapshot`) is available via rebase and does not change PR #246's write path (`_write_feed_snapshot_background`, `merge_feed_snapshot`) beyond adding the `diversity_score` field to `DiversificationMeta`.
