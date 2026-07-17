# Design: Log Mean Diversity Score Per Batch (Issue #206)

## Goal

For each cursor response returned to the client from `GET /xrpc/app.bsky.feed.getFeedSkeleton`, compute the mean diversity score of the posts in that page and emit it as an OTel metric. Tag with feed name and batch number (cursor position). This enables detecting insufficiently diverse result sets.

## Diversity Score Definition

`mmr_rerank()` selects candidates greedily. For each selected item `i`, `max_sim[i]` is the max similarity to any previously-selected item. The per-item diversity score is `max(0.0, 1.0 - max_sim[i])`, clamped to `[0, 1]`. The first item always gets `1.0` (no prior comparison). Higher = more diverse.

## Key Design Decision

Add `diversity_score: float | None = None` to `CandidatePost`. This avoids tuple returns or parallel lists and is forward-compatible with issue #223 (surfacing scores on public API responses). Endpoints that shouldn't expose the field omit it via their response schemas.

## Data Flow

1. `mmr_rerank()` stamps `diversity_score` on each returned `CandidatePost` before returning.
2. `_run_ranking_pipeline()` reads `[c.diversity_score for c in final]` to build a `list[float | None]`, alongside the URI list.
3. `FeedCacheDocument` gains `diversity_scores: list[float] | None = None`.
4. `FeedCache.store()` / `append()` accept optional `scores`; `retrieve()` returns `(uris, scores | None)` (or a small dataclass).
5. At every serve point in `getFeedSkeleton`, slice `scores[offset:offset+limit]`, compute mean, and emit metric.

## Metric

- **Name**: `feed.mean_diversity_score` (→ Float64Histogram, no `_count`/`_rate` suffix)
- **Attributes**: `feed_name`, `batch` (str, 0-indexed count of batches returned so far)
- **Batch number**: `0` for initial request (no cursor); `parsed.offset // limit` for cursor requests
- **Skip when**: `diversity_scores` is `None` (diversification disabled or legacy cache entry)

## Error Handling

- If `len(diversity_scores) != len(cached_uris)`, log a warning and skip the metric.
- Old cache entries without `diversity_scores` return `None`; callers handle gracefully.

## Files Changed

| File | Change |
|---|---|
| `src/app/models.py` | Add `diversity_score: float \| None = None` to `CandidatePost` |
| `src/app/lib/diversify.py` | Stamp `diversity_score` on each selected candidate in `mmr_rerank()` |
| `src/app/documents.py` | Add `diversity_scores: list[float] \| None = None` to `FeedCacheDocument` |
| `src/app/lib/feed_cache.py` | Thread scores through `store` / `retrieve` / `append` |
| `src/app/routers/xrpc.py` | Thread scores from pipeline → cache; log metric at each serve point |

## Tests

- `models_test.py` or `diversify_test.py`: `mmr_rerank` stamps correct `diversity_score` values (first=1.0, others in [0,1])
- `feed_cache_test.py`: scores round-trip through store/retrieve/append; missing scores return None
- `xrpc_test.py`: metric emitted with correct `feed_name` and `batch` for initial + cursor requests; skipped when scores absent
