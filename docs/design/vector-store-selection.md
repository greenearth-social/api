# Choosing Our Next Data Store: Memorystore or Qdrant

**Status:** Decision requested · 2026-08-10
**Companion to:** [#338](https://github.com/greenearth-social/api/pull/338), which sets out the longer-term architecture. This document covers one decision inside it: *which* store we stand up alongside Elasticsearch, and when.
**Audience:** 30-minute discussion. Technical detail is in the appendix.

---

## 1. Where we are

Elasticsearch is our source of truth for post data, and it also does the personalised search that picks candidate posts for a feed. That second job was the thing breaking our feeds. In early August we shipped a fix, and it worked:

| Measure (real user traffic) | Before (Aug 1–4) | After (Aug 6–9) |
|---|---|---|
| Feed loads served degraded | **6.3%** | **0.6%** |
| Personalised candidate step timing out | 10.9% | 2.5% |
| Slowest 5% of feed loads | 4.2s | 2.8s |

Our stated target was under 1% degraded. We are there.

The fix was to build a second, small Elasticsearch index holding only the roughly 5% of recent posts with meaningful engagement, and point the personalised search at that instead of the full corpus. Searching hundreds of thousands of posts instead of tens of millions is simply cheaper.

Two honest caveats. Several changes shipped the same week, so the improvement is not attributable to one of them with certainty. And the bottleneck moved rather than disappeared: one candidate step that was previously clean now times out at 3.9%, and a newly added one runs at 5.4% — both still against the full corpus. The direction is not in doubt; the precision is.

**There is no fire.** This decision is about moving while things are calm, not about fixing an outage.

---

## 2. Why the current fix is a bridge, not a destination

Three reasons, roughly in the order they will bite.

**It works because we only filter on one thing.** The small index is fast precisely because membership in it *is* the filter we apply — everything in it qualifies, so nothing gets excluded mid-search, so the fast path holds. Add a second filter that excludes most of the index and we are straight back to the slow path. Video-only, language, topic, politics score, social radius are all on or near the roadmap; the video-only filter is already written and switched off. Each one means either another purpose-built index or the return of the problem. **Filters combine; indexes do not.** This is the reason the approach does not scale, and it is a design property, not a tuning problem.

**Embeddings that change are Elasticsearch's worst case.** Changing one field of a document means rewriting the whole document. Our roadmap is largely about vectors that change: re-embedding the corpus every time we ship a new model, refreshing embeddings as posts accumulate engagement, and per-user and per-author embedding tables for the ranker. Elasticsearch will do all of this. It will just cost us far more than it should, and the cost lands on the same cluster that serves every feed.

**The seams are sharp.** In August we discovered that a storage optimisation had been silently deleting every embedding on a post the first time anyone liked it. Nothing failed loudly — search results just quietly got worse. We found it only because a backfill scanned 67,089 eligible posts and indexed zero of them. That class of bug comes from storing vectors in a document database and relying on document-update semantics they were not designed for.

So the question is not whether we are hurting today. It is whether we want to be in this position when the embedding roadmap arrives — and whether we would rather do the migration now, with slack, or later, under pressure.

---

## 3. The choice

Both candidates hold a compact, purpose-built copy of the vectors and the few fields we filter on. Elasticsearch stays the source of truth for everything else; if the new store is lost, we rebuild it from Elasticsearch. That single property — **derived, disposable, rebuildable** — is what keeps this decision low-risk, and it is true of either option.

| | **Memorystore** (Google-managed Redis) | **Qdrant** (purpose-built vector database) |
|---|---|---|
| Who operates it | Google. We configure a size and connect. | Us. It is a distributed database with shards, replicas, backups, upgrades and rebalancing to own. |
| Fit to today's need | Strong. One vector, one filter, small corpus. | Strong, and over-specified for it. |
| Fit to complex filtering | Adequate. Copes by choosing between two strategies per query. Degrades in the awkward middle ground. | Best available. Filtering is built into the index structure itself. |
| Fit to the rest of the roadmap | Also serves the per-user and per-author embedding tables the ranker needs — same instance, no extra infrastructure. | Vectors only. We would still need something else for the lookup tables. |
| Running several embedding versions at once | Workable — a separate index per version. | Native — several vectors per record in one collection. |
| Cost shape | Small managed instance; the index is ~100MB against a ~5TB Elasticsearch cluster. | Comparable hardware, plus our engineering time to operate it. |
| Main risk | We outgrow its filtering model and migrate again. | We take on a second stateful system to run, having just spent weeks hardening the first. |

The honest summary: **Qdrant is the better vector database. Memorystore is the better fit for what we are actually about to do.**

The near-term roadmap is dominated by two things — key-value lookups of user and author embeddings for the ranker, and cheap overwriting of vectors as models change. Neither is a hard vector-search problem. Qdrant is excellent at the hard vector-search problem we do not yet have, and does not help with the lookup tables at all, which we would then have to put somewhere else anyway.

Set against that, the operational cost is real and immediate. We spent much of the last month on Elasticsearch reliability work — adding master nodes, anti-affinity rules and disruption budgets. Taking on a second stateful cluster now would be spending the same kind of effort again, to buy a capability we cannot yet use.

---

## 4. Recommendation

**Adopt Memorystore now. Keep Qdrant as a named escalation with an explicit trigger.**

Concretely:

1. **Prove it before committing.** A time-boxed spike loading the real corpus into both, measuring speed, accuracy and behaviour under our actual filters. This is the gate; everything below assumes it passes.
2. **Move the personalised search first**, behind a flag, running alongside the current path until the numbers agree.
3. **Then use the same instance** for the user and author embedding tables the ranker roadmap needs — the part that pays for the move twice.
4. **Revisit Qdrant when we need filtered retrieval across several dimensions at once** — topic *and* language *and* social radius, for instance. That is where Memorystore's approach degrades and Qdrant's design earns its operational cost. We should recognise that moment rather than drift past it.

Elasticsearch remains the source of truth throughout, and every step falls back to it.

**What we are not recommending:** doing nothing. The current fix is stable and buys us a quarter, not a year, and the migration is not a two-week job. Starting when the roadmap forces us means being late.

---

## 5. What we need from this discussion

- Agreement that the small-index fix is a bridge, and that we start the next move now rather than when it becomes urgent.
- Approval for the time-boxed spike, which is what converts the expected numbers in this document into measured ones.
- Agreement on the escalation trigger, so choosing Memorystore today is a reversible decision rather than a permanent one.

---
---

# Appendix — Technical Comparison

Detail deliberately kept out of the body. Aimed at engineering review rather than the 30-minute discussion.

## A.1 Operational model: what we would actually own

The body calls Memorystore "managed" and Qdrant "ours to run." The distinction is worth being precise about, because it is the single largest cost difference and it is easy to overstate.

**Both stores are stateful.** Memorystore holds our index in memory on machines that can fail, just as Qdrant does. The difference is not statefulness but *who carries the operational burden of the state*, and how much that burden matters given what the state is.

**Qdrant is a distributed database we would operate.** A collection is split into shards; each shard can be replicated across nodes with a configurable replication factor, and a write-consistency factor controls how many replicas must acknowledge a write. In a self-hosted deployment we create and drop replicas and move shards between nodes ourselves as we scale. Qdrant's Kubernetes operator automates shard rebalancing and rolling upgrades, but that operator is part of the paid Private Cloud offering — the plain Helm chart explicitly does not provide the same zero-downtime upgrade behaviour. Backups are our schedule to define and test.

That is a genuine on-call surface: shard placement, replica health, disk pressure, version upgrades, restore drills. It is the same category of work as the Elasticsearch reliability effort we have just been through — three master nodes, anti-affinity rules, pod disruption budgets — and it would not be amortised, because a second cluster does not reuse the first one's operational muscle memory as much as it looks like it should.

**Memorystore removes most, not all, of that.** Google handles node replacement, patching and failover. We choose an instance size and a replica count. What remains ours is capacity planning and the consequences of eviction. That is a much smaller surface, but it is not zero — and Memorystore's vector support is a feature of a managed key-value service, so we are also accepting whatever pace Google sets for it.

**The property that makes this decision low-stakes is not the operating model — it is that the store is derived.** Elasticsearch remains the source of truth. The vector index is rebuilt from it, holds nothing original, and can be discarded and repopulated. So durability guarantees, backup strategy, replication factor and consistency semantics — the things that make operating a stateful database genuinely hard — mostly do not apply to us. We need availability, not durability.

This cuts both ways, and it is worth saying plainly. It substantially lowers the risk of running Qdrant ourselves, because the worst case is a rebuild rather than data loss. But it also removes most of the reason to pay for Qdrant's durability and consistency machinery, which is a large part of what we would be taking on. **A derived store is an argument for the simpler option, not merely an argument that the complex one is survivable.**

## A.2 Search algorithms

Both stores implement the same two approaches, and both are the same two Elasticsearch already uses.

**Exhaustive search** compares the query against every candidate vector. Cost is linear in the number of vectors, but the memory access is sequential, which modern CPUs handle extremely well. For our corpus — roughly 773k vectors of 128 dimensions, about 100MB once compressed to 8-bit integers — a full sweep is on the order of 10ms if the data is in memory. Memorystore calls this `FLAT`; Qdrant reaches it via a full-scan threshold.

**Graph search (HNSW)** builds a navigable graph over the vectors and walks it, examining a few thousand candidates instead of all of them. Cost grows with the logarithm of corpus size rather than linearly. Both stores implement it with the standard tunables (`M`, `ef_construction`, and a runtime `EF_RUNTIME` / `ef` for the speed-versus-accuracy trade).

Two consequences worth holding onto:

- **Neither store beats Elasticsearch on algorithm.** Elasticsearch runs the same graph search, with the same 8-bit compression, and applies it automatically. Our problem was never that Elasticsearch had the wrong algorithm — it was that our filter forced it to abandon the algorithm (§A.3), and that the vectors were competing for memory with ~5TB of documents. The gain from moving is *residency and predictability*, not a better index.
- **Graph search is memory-access-hostile.** Each hop jumps to an unrelated memory address, so it degrades badly when data is not resident — which is exactly the effect we measured on Elasticsearch, where the identical query cost 1188ms or 242ms depending purely on cache state. A dedicated in-memory store removes that variance by construction. Exhaustive search, being sequential, is far more forgiving — which is why at our corpus size the simpler strategy is competitive and the choice of store matters more than the choice of algorithm.

**Compression.** Both support reducing vectors from 32-bit floats to 8-bit integers, roughly a 4× memory saving for negligible accuracy loss. Qdrant additionally offers more aggressive schemes with a re-ranking stage. At 773k vectors we do not need them; they matter at 100M+.

## A.3 Filtered search — the part that actually differs

This is where the two designs diverge, and it is the crux of the recommendation.

The difficulty: a vector graph encodes *similarity* only. It knows nothing about like counts, languages or topics. Given a filter, an engine must either walk the graph while skipping non-matching entries — which stalls, because a node's nearest neighbours are mostly filtered out, so it must explore far more of the graph — or filter first and exhaustively scan what remains. Work on the first approach scales roughly with the inverse of how selective the filter is.

**Elasticsearch (today):** picks between the two per segment, and the fallback is punitive. If the matching set is smaller than the candidate pool it skips the graph outright; otherwise it walks the graph under a work budget equal to the number of matching documents, and if it exceeds that budget it *discards the traversal and exhaustively scans anyway*. With our engagement filter matching ~4.6% of the corpus, we paid for a failed graph walk and then a scan of ~460,000 vectors per query. Restricting the corpus fixed this by making the filter match ~100% of the index — which is why it works, and why it stops working the moment a second filter is applied.

**Memorystore:** the same two strategies, chosen up front rather than discovered by exhausting a budget. Pre-filtering finds matches via secondary tag and numeric indexes and then brute-forces them; inline filtering runs the graph search and skips non-matching hits; the service picks automatically based on the filter. Filters are tag-based (exact or prefix match) and numeric ranges, combinable with AND, OR and negation. This is meaningfully better than Elasticsearch's behaviour because no work is thrown away — but it is still a *policy chooser over a filter-blind index*, not filter-aware indexing. In the awkward middle ground, where a filter matches say 20–60% of the corpus, both strategies are poor and there is no third option.

**Qdrant:** the only one of the three that changes the index itself. It estimates the matching set size from payload index statistics and picks full scan or graph traversal against a configurable threshold — so far, the same idea as Memorystore, with better statistics. The real difference is that for payload values common enough to matter, Qdrant builds *additional graph links restricted to the subset sharing that value*, so the graph stays traversable under the filter and does not stall. It also supports several named vectors per record, so A/B model variants live in one collection rather than parallel indexes.

The cost of that capability is that it must be declared and paid for at index time: each filterable field adds index size and build time, and it must be chosen in advance rather than applied arbitrarily at query time.

## A.4 Fit against our query patterns

**Today's pattern:** one vector; filters on engagement (matches ~100% of the small index by construction), recency (broad), and model version (broad except during a rollout). Selectivity is high across the board.

At this shape, Qdrant's structural advantage is inert — its own estimator would choose a full scan for our corpus anyway, exactly as Memorystore's would. We would be paying the operational cost of a distributed database for a capability the query pattern never invokes. **Memorystore fits today's pattern and Qdrant does not fight it, but neither does it help.**

**The roadmap's pattern:** several filters at once, at moderate selectivity — video-only, language, topic, politics score, social radius. This is precisely the middle ground where a policy chooser has no good option and filter-aware indexing pays. **If that pattern materialises, Memorystore fights it and Qdrant is built for it.** That is the escalation trigger in §4, stated in terms of query shape rather than a date.

**One pattern neither handles by being a vector store:** the per-user and per-author embedding tables in the ranker roadmap are key-value lookups, not searches. Memorystore serves them natively on the same instance. Qdrant does not, so choosing it means running a key-value store as well — which quietly makes the "one new system" comparison a two-system one.

**One trap to design around either way.** Our model-version filter matches essentially the whole corpus in steady state, but during a model rollout the corpus splits by version and the matching set climbs from near zero as re-embedding progresses. A filter that selective produces *thin or empty results rather than slow ones* — a silent quality failure. Whichever store we choose should partition by model version rather than filter on it, and alarm on retrieved-candidate counts.

---

**Sources for the vendor claims above:** [Memorystore vector search](https://docs.cloud.google.com/memorystore/docs/cluster/about-vector-search) · [Memorystore query syntax](https://docs.cloud.google.com/memorystore/docs/cluster/query-syntax) · [Qdrant distributed deployment](https://qdrant.tech/documentation/guides/distributed_deployment/) · [Qdrant filterable HNSW](https://qdrant.tech/course/essentials/day-2/filterable-hnsw/) · [Elasticsearch kNN search](https://www.elastic.co/docs/solutions/search/vector/knn)
