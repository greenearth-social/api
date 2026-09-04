"""Fit a MiniLM query vector to a free-text prompt (ingex#482).

Pipeline, one recipe, no tunable axes:

    expand  -> one Sonnet call turns the prompt into a 24-term keyword bag
    pool    -> BM25 over posts_recent with each distinct token once
    sample  -> 80 keyword posts (lexical MMR) + 120 random posts
    score   -> Sonnet rates the 80 keyword posts 1-10; random posts are forced to 1
    fit     -> ridge regression on L2-normalised embeddings -> query vector

Numbers quoted in the comments (judged means, completion curves, token
counts) are measurements from those experiments, 2026-07/08, against the
production posts index.
The prompt text and the numeric choices come from the keyword-expansion and
query-vector-fitting experiments (llm-approximation-experiments-jplk, Juan's
repo). Comments here give the reasoning; the measurements live there.

This module is pure library code: no FastAPI, no Firestore. The router
(routers/llm_query_vectors.py) calls `fit_query_vector` and stores the result.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass, field

import numpy as np
from anthropic import AsyncAnthropic

from .elasticsearch import POSTS_KNN_INDEX, fetch_post_embeddings, unwrap_es_response

logger = logging.getLogger(__name__)


class FitError(RuntimeError):
    """Base class for the pipeline's own failures (LLM or corpus), as opposed
    to transport errors from the Elasticsearch or Anthropic clients, which
    propagate unchanged."""


@dataclass
class Usage:
    """Token counter shared by every model call in one fit; feeds the cost
    figure in the response and the cost breaker in step 4.

    `input_tokens`/`output_tokens` are what the API reported. A scoring call
    cancelled at the deadline reports nothing but is still billed for its
    input and for whatever it generated before the disconnect, so step 4 adds
    an estimate for those under `est_*` and cost_usd() charges both. The
    estimate leans high (see score_posts), so the figure is a ceiling on real
    spend rather than an undercount; on measured runs with 2-6 of 80 calls
    cancelled it sits 3-8% above the reported tokens' cost.
    """

    input_tokens: int = 0
    output_tokens: int = 0
    calls: int = 0
    max_output_tokens: int = 0
    est_input_tokens: int = 0
    est_output_tokens: int = 0

    def add(self, usage) -> None:
        self.input_tokens += usage.input_tokens
        self.output_tokens += usage.output_tokens
        self.max_output_tokens = max(self.max_output_tokens, usage.output_tokens)
        self.calls += 1

    def add_estimate(self, input_tokens: int, output_tokens: int) -> None:
        self.est_input_tokens += input_tokens
        self.est_output_tokens += output_tokens

    def merge(self, other: Usage) -> None:
        self.input_tokens += other.input_tokens
        self.output_tokens += other.output_tokens
        self.max_output_tokens = max(self.max_output_tokens, other.max_output_tokens)
        self.calls += other.calls
        self.est_input_tokens += other.est_input_tokens
        self.est_output_tokens += other.est_output_tokens


# --------------------------------------------------------------------------- #
# 0. Anthropic client                                                           #
#    Same shape as lib/perspective.py: key read from the environment when the  #
#    singleton is first built (GE_ANTHROPIC_API_KEY, Secret Manager             #
#    anthropic-api-key-{stage,prod} via scripts/deploy.sh), closed from the    #
#    FastAPI lifespan in main.py.                                               #
# --------------------------------------------------------------------------- #

# The SDK's own retry (429/408/409/5xx + connection errors, exponential
# backoff, honours retry-after). Default is 2; the scoring step fires 80
# calls at once, so give a rate-limit blip more room. Measured 2026-08-30:
# 200 calls at once on the org's Scale tier produced zero 429s.
_ANTHROPIC_MAX_RETRIES = 4
# Per-request HTTP timeout. Sonnet returns a 1-10 verdict in ~2s (p50) and
# the expansion in ~5-9s; 30s means one hung connection cannot hold the
# whole request for the SDK default of 10 minutes.
_ANTHROPIC_TIMEOUT_S = 30.0

_client: AsyncAnthropic | None = None


def _get_anthropic_client() -> AsyncAnthropic:
    global _client
    if _client is None:
        key = os.environ.get("GE_ANTHROPIC_API_KEY")
        if not key:
            raise RuntimeError("GE_ANTHROPIC_API_KEY environment variable is not set")
        _client = AsyncAnthropic(
            api_key=key, max_retries=_ANTHROPIC_MAX_RETRIES, timeout=_ANTHROPIC_TIMEOUT_S
        )
    return _client


async def close_anthropic_client() -> None:
    """Close the singleton's HTTP pool, if one was ever created (lifespan shutdown)."""
    global _client
    if _client is not None:
        await _client.close()
        _client = None


# --------------------------------------------------------------------------- #
# 1. Keyword expansion                                                          #
#    One Sonnet call turns the prompt into a bag of search terms. Single-pass,  #
#    24 terms: in the expansion experiments (2026-07, 16 variants x 12 prompts) #
#    plain 16/24/32-term bags were indistinguishable (4.70/4.79/4.78 judged     #
#    mean) and within noise of the best two-pass variant; 24 is the middle of   #
#    that band and one call instead of two.                                     #
# --------------------------------------------------------------------------- #

EXPANSION_MODEL = "claude-sonnet-5"
N_KEYWORDS = 24
# Cap on the reply length. A 24-term {"keywords": [...]} is ~150-400 output
# tokens, so this is headroom, not a budget: a cap that is too low truncates
# the JSON and the parse below fails loudly.
_EXPANSION_MAX_TOKENS = 1500
# The schema below cannot enforce the count, and the model sometimes returns
# fewer than the N_KEYWORDS asked for. One ask, no re-ask; the reply is
# handled in three bands: 24 terms is normal, 16-23 is accepted with a log
# line, and below 16 (or an empty reply) raises ExpansionError. 16 is not a
# degraded bag - a 16-term variant scored within noise of the 24-term one in
# the experiments.
MIN_KEYWORDS = 16

# System prompt for the expansion call. Text from the expansion experiments,
# minus one clause that referred to the lab's other request styles ("each
# request below says which it is asking for"); here there is one request.
EXPANSION_SYSTEM = (
    "You generate keyword bags used as BM25 search queries against a corpus of "
    "Bluesky posts (short, informal, public social-media text). "
    "A \"keyword\" here may be a single word *or* a short multi-word phrase — "
    "both are wanted. "
    "Your output is fed straight to the search engine, so what matters about "
    "every term is that its exact text plausibly appears verbatim in real "
    "posts on the topic. "
    "**The person's request itself is not searched — only the terms you "
    "return are.** Nothing in their wording reaches the search engine unless "
    "you put it in your list, so if a word from the request belongs in the "
    "query, include it deliberately. "
    "Return only the JSON object requested."
)

# Requirements block appended to the user message. `%(n)d` is filled with
# N_KEYWORDS. Text from the expansion experiments.
_EXPANSION_COMMON = """
Requirements for every term:
- Judge a term by what it would retrieve, not by the register it belongs to.
  Technical, subcultural and in-group vocabulary is welcome, and is often the
  most discriminating vocabulary available: the people posting about a topic
  are the people who use it. "singletrack", "cap-and-trade" and "TBR pile" are
  better search terms than "running", "climate" and "books", not worse.
- Prefer terms that discriminate. A word so common it appears in unrelated
  posts drags in noise, and on its own is worse than useless. Use one only as
  part of a longer term whose other word carries the specificity: "good boy"
  finds dog posts where "good" would match half the corpus, and "race report"
  works where "report" does not.

Requirements for the list as a whole:
- Keep it varied. Cover different facets, subtopics, communities, and phrasings
  of the request rather than listing near-synonyms of one concept. Three ways of
  saying the same thing buys nothing over one.
- The request's own wording is not searched, so any of it you want searched has
  to be in this list. Include the words from the request that would themselves
  make good search terms. Some requests contain none at all — a request can be
  entirely framing ("show me something that will cheer me up") with no term
  worth searching in it. In that case include none of it, and do not pad the
  list with words lifted from the request just because they are there.

Return JSON: {"keywords": ["...", "..."]}, exactly %(n)d entries, ordered from
most to least useful.
"""

# Structured-output schema: guarantees the reply is {"keywords": [str, ...]}.
# It cannot enforce the count (minItems/maxItems are not supported by the
# structured-output format), hence MIN_KEYWORDS above.
_EXPANSION_SCHEMA = {
    "type": "object",
    "properties": {"keywords": {"type": "array", "items": {"type": "string"}}},
    "required": ["keywords"],
    "additionalProperties": False,
}


def build_expansion_prompt(prompt: str, n: int = N_KEYWORDS) -> str:
    """The user message: the request, then the requirements block."""
    return (
        f'Someone wants to see Bluesky posts matching this request:\n\n'
        f'    "{prompt}"\n\n'
        f"Give {n} keywords to search for, to find posts they would find "
        f"relevant."
        + _EXPANSION_COMMON % {"n": n}
    )


def parse_keyword_bag(text: str) -> list[str]:
    """Keyword list out of the model's JSON reply.

    Structured output means `text` is valid JSON of the right shape; a
    malformed reply is a bug upstream, so this raises rather than salvaging.
    """
    obj = json.loads(text)
    return [k.strip() for k in obj["keywords"] if isinstance(k, str) and k.strip()]


class ExpansionError(FitError):
    """The model did not return a usable keyword bag."""


async def expand_keywords(
    client: AsyncAnthropic, prompt: str, usage: Usage | None = None
) -> list[str]:
    """One expansion call: system prompt + one user message, structured JSON
    output. No re-ask when the bag comes back short (see MIN_KEYWORDS).
    """
    resp = await client.messages.create(
        model=EXPANSION_MODEL,
        max_tokens=_EXPANSION_MAX_TOKENS,
        system=EXPANSION_SYSTEM,
        output_config={"format": {"type": "json_schema", "schema": _EXPANSION_SCHEMA}},
        messages=[{"role": "user", "content": build_expansion_prompt(prompt)}],
    )
    if usage is not None:
        usage.add(resp.usage)
    text = next((b.text for b in resp.content if b.type == "text"), "")
    if not text:
        raise ExpansionError(f"empty expansion reply (stop_reason={resp.stop_reason!r})")
    keywords = parse_keyword_bag(text)
    if len(keywords) < MIN_KEYWORDS:
        raise ExpansionError(f"expansion returned only {len(keywords)} keywords")
    if len(keywords) != N_KEYWORDS:
        logger.info("expansion returned %d keywords, asked for %d", len(keywords), N_KEYWORDS)
    return keywords


# --------------------------------------------------------------------------- #
# 2. Pool                                                                       #
#    One BM25 `match` query on `content` over posts_recent, built from the bag  #
#    with each distinct token once. Fetch a little over POOL_N hits, drop empty #
#    posts, exact duplicates and near-duplicates, keep the first POOL_N.        #
# --------------------------------------------------------------------------- #

POOL_N = 1000
# Fetch 20% + 5 more than POOL_N so the dedup below still leaves a full pool.
_POOL_OVERFETCH = 1.2
# Two posts whose token sets overlap this much (Jaccard) are the same post
# for our purposes: reposts, templated announcements, copied headlines. The
# index ranks such clones adjacently, and MMR (step 3) only penalises them,
# so without this filter a viral story fills a fifth of the sample.
NEAR_DUP_JACCARD = 0.8
# Fields pulled from _source. Deliberately not the embedding: a 384-float
# vector is ~30x the size of a short post, and only the ~200 posts in the
# fitting sample (80 LLM-scored keyword posts + 120 forced-to-1 random
# negatives, all of which the regression needs vectors for) ever need one.
# So the pool is fetched as text, and embeddings are hydrated for the sample
# alone in step 3 via fetch_post_embeddings (docvalue_fields), the way the
# candidate generators do it.
_POST_SOURCE_FIELDS = ["at_uri", "content"]


# The index analyses `content` with the standard tokenizer plus Lucene's
# `_english_` stop set. This is a replica of that token stream - lower-cased
# runs of letters/digits joined across an apostrophe, one token per emoji,
# one per Han character, stopwords dropped - so the query and the lexical
# similarities below count the same tokens BM25 does. The API key cannot call
# `_analyze`; the replica was checked term by term against the live index
# (count(match: term) == count(bool should [term: token, ...])) in the
# experiments, including emoji, which appear in more posts than most words.
_STOPWORDS = frozenset(
    "a an and are as at be but by for if in into is it no not of on or such that "
    "the their then there these they this to was will with".split()
)
_WORD_RE = re.compile(r"[^\W_]+(?:['’][^\W_]+)*", re.UNICODE)
_HAN_RE = re.compile("[\u4e00-\u9fff\uf900-\ufaff]")
_EMOJI_RE = re.compile(
    "[\U0001F000-\U0001FAFF\u2600-\u27BF\u2B00-\u2BFF\u2190-\u21FF]"
    "[\ufe0e\ufe0f\U0001F3FB-\U0001F3FF]*"
    "(?:\u200d[\U0001F000-\U0001FAFF\u2600-\u27BF][\ufe0e\ufe0f]*)*"
)


def analyze(text: str) -> list[str]:
    """Token stream the index would produce for `text` (see the note above)."""
    lowered = text.lower()
    out: list[str] = []
    pos = 0
    for m in _EMOJI_RE.finditer(lowered):
        _emit_words(lowered[pos : m.start()], out)
        out.append(m.group())
        pos = m.end()
    _emit_words(lowered[pos:], out)
    return out


def _emit_words(text: str, out: list[str]) -> None:
    for raw in _WORD_RE.findall(text):
        # The standard tokenizer breaks between every pair of Han ideographs.
        parts = [raw] if not _HAN_RE.search(raw) else _HAN_RE.split(raw) + _HAN_RE.findall(raw)
        for part in parts:
            if part and part not in _STOPWORDS:
                out.append(part)


def build_pool_query(keywords: list[str]) -> dict:
    """`match` on `content` with each distinct token of the bag exactly once.

    A bag's repeated tokens are not emphasis the expander chose: they are the
    topic's head noun shared across its multi-word terms ("ai" in 21 of the
    48 tokens of one AI-prompt bag, "garden" in 12 of a gardening one) - the
    token every relevant post already contains and the least discriminating
    one. `match` adds one clause per occurrence, so the repeat boosts
    linearly, and the pool fills with posts that merely say the head word a
    lot. Measured over 16 prompt x bag cells: once 4.31 vs repeated 3.92 mean
    judged score; on the worst cell the share of pool posts matching a single
    token fell from 63% to 0% and the duplicate share from 44% to 13%.
    """
    seen: set[str] = set()
    uniq: list[str] = []
    for tok in analyze(" ".join(keywords)):
        if tok not in seen:
            seen.add(tok)
            uniq.append(tok)
    return {"match": {"content": " ".join(uniq)}}


@dataclass
class Post:
    at_uri: str
    content: str
    tokens: frozenset[str]
    embedding: list[float] | None = field(default=None, repr=False)


def _posts_from_hits(resp) -> list[Post]:
    """Hits -> Posts, dropping blank content and exact duplicates (same text
    modulo whitespace), in response order."""
    seen: set[str] = set()
    posts: list[Post] = []
    for hit in unwrap_es_response(resp).get("hits", {}).get("hits", []):
        src = hit.get("_source") or {}
        text = (src.get("content") or "").strip()
        at_uri = src.get("at_uri")
        if not text or not at_uri:
            continue
        norm = " ".join(text.split())
        if norm in seen:
            continue
        seen.add(norm)
        posts.append(Post(at_uri=at_uri, content=text, tokens=frozenset(analyze(text))))
    return posts


def jaccard_matrix(token_sets: list[frozenset[str]]) -> np.ndarray:
    """Pairwise token-set Jaccard similarity, (m, m) float64, ones on the
    diagonal. Shared by the near-duplicate filter and the MMR selection.

    Built from posting lists rather than m*m set operations: every token
    contributes 1 to the intersection count of each pair of posts that
    contain it, so `inter[ix_(posts, posts)] += 1` per token is the whole
    computation. A 1,200-post pool has ~10k distinct tokens, most of them in
    a single post; those only touch the diagonal, which is set directly, so
    the loop runs over the few hundred shared tokens.
    """
    m = len(token_sets)
    if m == 0:
        return np.zeros((0, 0))
    vocab: dict[str, int] = {}
    rows: list[int] = []
    cols: list[int] = []
    for i, toks in enumerate(token_sets):
        for tok in toks:
            rows.append(i)
            cols.append(vocab.setdefault(tok, len(vocab)))
    inter = np.zeros((m, m), dtype=np.int32)
    if cols:
        r = np.asarray(rows)
        c = np.asarray(cols)
        order = np.argsort(c, kind="stable")
        r, c = r[order], c[order]
        starts = np.flatnonzero(np.r_[True, c[1:] != c[:-1]])
        ends = np.r_[starts[1:], len(c)]
        for s, e in zip(starts, ends, strict=True):
            if e - s > 1:
                posts = r[s:e]
                inter[np.ix_(posts, posts)] += 1
    sizes = np.asarray([len(t) for t in token_sets], dtype=np.float64)
    np.fill_diagonal(inter, sizes)
    union = sizes[:, None] + sizes[None, :] - inter
    return np.divide(inter, union, out=np.zeros((m, m)), where=union > 0)


def near_dup_keep(sim: np.ndarray, threshold: float, stop_after: int) -> list[int]:
    """Greedy first-wins filter: walk posts in order, keep one unless its
    similarity to an already-kept post reaches `threshold`; stop once
    `stop_after` are kept. Returns the kept indices, order preserved."""
    kept: list[int] = []
    for i in range(sim.shape[0]):
        if kept and sim[i, kept].max() >= threshold:
            continue
        kept.append(i)
        if len(kept) >= stop_after:
            break
    return kept


async def fetch_pool(es, keywords: list[str]) -> tuple[list[Post], np.ndarray]:
    """The deduplicated BM25 pool and its pairwise lexical similarity matrix."""
    size = min(int(POOL_N * _POOL_OVERFETCH) + 5, 10_000)
    resp = await es.search(
        index=POSTS_KNN_INDEX,
        op="llm_qv_pool",
        query=build_pool_query(keywords),
        size=size,
        _source=_POST_SOURCE_FIELDS,
    )
    posts = _posts_from_hits(resp)
    sim = jaccard_matrix([p.tokens for p in posts])
    keep = near_dup_keep(sim, NEAR_DUP_JACCARD, stop_after=POOL_N)
    logger.info("llm_qv_pool requested=%d distinct=%d kept=%d", size, len(posts), len(keep))
    return [posts[i] for i in keep], sim[np.ix_(keep, keep)]


# --------------------------------------------------------------------------- #
# 3. Fitting sample: 80 keyword posts + 120 random posts                        #
#    The keyword posts are chosen from the pool by greedy MMR on lexical        #
#    similarity, so the 80 the model scores cover the pool's facets instead of  #
#    its densest cluster. The random posts are uniform draws from the index:    #
#    near-certain negatives that anchor the low end of the regression. They    #
#    are never scored by the model - they get MIN_SCORE in step 4.             #
# --------------------------------------------------------------------------- #

N_KEYWORD_POSTS = 80
# Random posts per fit. The sweep's winning region was random fraction 0.4-0.8
# of a 200-post sample; 0.6 is its middle. Replacing the judged labels of the
# random block with the constant MIN_SCORE was validated on 90 fits: served-
# page quality unchanged (median delta +0.00), fitted-vector cosine 0.994 to
# the fully judged fit. Adding negatives showed a plateau from 50 to 1000, so
# 120 sits comfortably on it; the validated regime is <= 160.
N_RANDOM_POSTS = 120
# MMR trade-off: 1.0 is pure BM25 order, 0.0 is pure diversity. The sweep was
# flat for lambda >= 0.45 with the random fraction above; 0.5 is its middle.
MMR_LAMBDA = 0.5
# Below this many usable keyword posts the fit has too few positives to mean
# anything; the pool size, not the model, is the problem, so the caller gets
# a 422 rather than a 502. Between this and N_KEYWORD_POSTS the sample shrinks
# proportionally (random block kept at 1.5x the keyword block).
MIN_KEYWORD_POSTS = 20
# Overfetch for the random block, for posts that turn out to have no
# embedding.
_RANDOM_OVERFETCH = 1.2


def mmr_select(sim: np.ndarray, n: int, mmr_lambda: float) -> list[int]:
    """Greedy maximal-marginal-relevance selection of `n` pool indices.

    Each step takes argmax of `lambda * relevance - (1 - lambda) * max
    similarity to the posts already selected`. Relevance is a rank
    transform of the BM25 order, 1 / (60 + rank), min-max scaled to [0, 1]:
    raw BM25 scores have an arbitrary scale that changes with the query, and
    a linear rank transform flattens to nothing over a 1000-post pool, so
    lambda would mean something different for every prompt. The similarity
    matrix is the pool's lexical Jaccard matrix from step 2.
    """
    m = sim.shape[0]
    if n >= m:
        return list(range(m))
    rel = 1.0 / (60.0 + np.arange(m, dtype=np.float64))
    rel = (rel - rel.min()) / (rel.max() - rel.min())
    selected = [0]
    chosen = np.zeros(m, dtype=bool)
    chosen[0] = True
    max_sim = sim[0].copy()
    while len(selected) < n:
        mmr = mmr_lambda * rel - (1.0 - mmr_lambda) * max_sim
        mmr[chosen] = -np.inf
        i = int(np.argmax(mmr))
        selected.append(i)
        chosen[i] = True
        max_sim = np.maximum(max_sim, sim[i])
    return selected


async def fetch_random_posts(es, n: int, exclude_uris: list[str]) -> list[Post]:
    """~`n` uniformly random posts from the index (`random_score`, unseeded),
    excluding the keyword posts so the two blocks never overlap. Slightly more
    than `n` are returned; the caller trims after hydrating embeddings."""
    if n <= 0:
        return []
    inner: dict = {"match_all": {}}
    if exclude_uris:
        inner = {
            "bool": {
                "must": [{"match_all": {}}],
                "must_not": [{"terms": {"at_uri": exclude_uris}}],
            }
        }
    resp = await es.search(
        index=POSTS_KNN_INDEX,
        op="llm_qv_random",
        query={
            "function_score": {
                "query": inner,
                "random_score": {},
                "boost_mode": "replace",
            }
        },
        size=int(n * _RANDOM_OVERFETCH) + 5,
        _source=_POST_SOURCE_FIELDS,
    )
    return _posts_from_hits(resp)


async def hydrate_embeddings(es, posts: list[Post]) -> None:
    """Attach MiniLM embeddings to `posts` in place, one batched ES call.
    Posts the index has no embedding for are left with `embedding=None`."""
    pairs = await fetch_post_embeddings(es, [p.at_uri for p in posts], index=POSTS_KNN_INDEX)
    by_uri = dict(pairs)
    for p in posts:
        p.embedding = by_uri.get(p.at_uri)


# --------------------------------------------------------------------------- #
# 4. Score: Sonnet rates each keyword post 1-10 against the prompt              #
#    All calls are fired at once (each is its own HTTP request; there is no    #
#    batch), then the step waits SCORE_DEADLINE_S and fits on what has         #
#    arrived, so its wall time is about one call's latency, not 80 calls'.     #
# --------------------------------------------------------------------------- #

SCORING_MODEL = "claude-sonnet-5"
MIN_SCORE, MAX_SCORE = 1, 10
# A two-sentence explanation plus the score is ~70 output tokens (measured
# 68-74 per post); 500 means a verbose reply still closes its JSON.
_SCORE_MAX_TOKENS = 500
# Upper bound on calls in flight. Larger than any sample size we use, so in
# practice every call leaves immediately; measured 2026-08-30 on this org's
# tier: 200 calls at once, zero 429s, all 200 requests out within 70 ms.
# Env-overridable so a rate-limited key can be dialled down without a deploy.
SCORE_CONCURRENCY = int(os.environ.get("GE_LLM_QV_SCORE_CONCURRENCY", "200"))
# Seconds to wait after firing before fitting on what has arrived. Measured
# completion curve for 80 calls: 39% back at 2 s, 85% at 2.5 s, 94% at 3 s,
# 99% at 4 s, last one at 3.9 s (and 7-9 s stragglers on other runs). 3 s is
# the knee and the single-call p90. Calls still pending are cancelled - they
# are billed for what they generated, so the deadline saves time, not money.
SCORE_DEADLINE_S = 3.0
# The deadline never fails a fit: if fewer than this fraction of the keyword
# posts are back at the deadline (a slow day), keep waiting for the in-flight
# calls until the fraction is met or they all finish. Below the fraction after
# that, the fit is abandoned - the labels would be too few and the
# selection biased toward whatever the model answers fastest.
MIN_SCORED_FRACTION = 0.8
# Cost breaker. A normal fit is ~$0.13 (80 posts x ~420 in / ~70 out tokens
# plus the expansion); the worst case, every call at the input bound below
# and running to _SCORE_MAX_TOKENS, is ~$0.65. This exists so a
# misconfiguration (a much larger sample, a pricier model id) fails loudly
# instead of quietly spending: checked once before the scoring calls are
# fired, against that worst case, and once after, against what they cost.
MAX_COST_USD = 2.0
# claude-sonnet-5 list price per million tokens, 2026-09-01. Only used for the
# response's cost figure and the breaker above.
_USD_PER_MTOK_INPUT = 2.0
_USD_PER_MTOK_OUTPUT = 10.0
# Per-call input ceiling for the preflight check and for calls whose usage
# never came back: the rubric and template are ~250 tokens, a 2000-char
# prompt (the router's cap) ~500, a 300-grapheme post ~300. Measured calls
# use ~420.
_SCORE_INPUT_TOKENS_BOUND = 1500
_SCORE_CALL_COST_BOUND_USD = (
    _SCORE_INPUT_TOKENS_BOUND * _USD_PER_MTOK_INPUT + _SCORE_MAX_TOKENS * _USD_PER_MTOK_OUTPUT
) / 1_000_000

SCORING_SYSTEM = (
    "You curate someone's social media feed. They have told you what they want "
    "to see. For each post, rate how much you would want to serve it to them.\n\n"
    f"{MIN_SCORE} = you would not serve it; it wastes their time. "
    f"{MAX_SCORE} = you would put it at the top of their feed.\n\n"
    "Judge the post as the thing they will actually see. Matching the subject "
    "they named is not enough on its own: a post that only tags, links to, "
    "announces or comments on the thing is not the thing. If they asked for "
    "something with an effect - funny, uplifting, useful - the post has to "
    "have that effect on someone reading it. Partial credit is fine and "
    "expected: use the middle of the scale for posts that are worth serving "
    "but unremarkable.\n\n"
    f"Empty, truncated or unintelligible posts score {MIN_SCORE}. Judge "
    "content, not language. Never decline to rate."
)

SCORING_TEMPLATE = (
    "They want to see:\n{prompt}\n\n"
    "Candidate post:\n{text}\n\n"
    f"How much would you want to serve this post to them? Score {MIN_SCORE}-"
    f"{MAX_SCORE}. Reply with ONLY a JSON object of the form "
    '{{"explanation": "<one or two sentences>", "score": <integer>}} '
    "and nothing else."
)

_SCORE_RE = re.compile(r'"score"\s*:\s*(\d+)')


class ScoringError(FitError):
    """Too few posts could be scored, or the fit blew its cost cap."""


def parse_score(raw: str) -> int | None:
    """Integer score out of the model's reply, clamped to the scale; None if
    there is none. The reply is asked for as bare JSON, but a fenced or
    chatty reply with a `"score": N` in it still yields its score."""
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").split("\n", 1)[-1].rsplit("```", 1)[0]
    try:
        obj = json.loads(raw[raw.index("{") : raw.rindex("}") + 1])
        return max(MIN_SCORE, min(MAX_SCORE, int(obj["score"])))
    except (ValueError, KeyError, TypeError):
        pass
    m = _SCORE_RE.search(raw)
    if m:
        return max(MIN_SCORE, min(MAX_SCORE, int(m.group(1))))
    return None


async def _score_one(client: AsyncAnthropic, prompt: str, text: str, usage: Usage) -> int | None:
    """One post's score, or None if the reply had no usable score.

    A `refusal` stop is a verdict, not an error: a post the model will not
    look at is not one to serve, so it scores MIN_SCORE and is not retried
    (one such post in 80 on the measured run, deterministic across runs).
    Transport and rate-limit errors are retried by the SDK client; anything
    that still raises propagates to score_posts, which counts it as unscored.
    """
    resp = await client.messages.create(
        model=SCORING_MODEL,
        max_tokens=_SCORE_MAX_TOKENS,
        system=SCORING_SYSTEM,
        messages=[{"role": "user", "content": SCORING_TEMPLATE.format(prompt=prompt, text=text)}],
    )
    usage.add(resp.usage)
    if resp.stop_reason == "refusal":
        return MIN_SCORE
    raw = "".join(b.text for b in resp.content if b.type == "text")
    return parse_score(raw)


@dataclass
class ScoreOutcome:
    scores: list[int | None]  # one per input post; None = cancelled, failed or unparseable
    n_scored: int
    n_cancelled: int
    n_failed: int


async def score_posts(
    client: AsyncAnthropic, prompt: str, posts: list[Post], usage: Usage
) -> ScoreOutcome:
    """Score `posts` concurrently with the deadline-and-floor rule above."""
    n = len(posts)
    floor = math.ceil(MIN_SCORED_FRACTION * n)
    semaphore = asyncio.Semaphore(SCORE_CONCURRENCY)
    # Scoring's own counter, merged into `usage` at the end: the cancelled-call
    # estimate below needs this step's per-call figures, not the expansion's.
    scoring_usage = Usage()

    async def one(post: Post) -> int | None:
        async with semaphore:
            return await _score_one(client, prompt, post.content, scoring_usage)

    def n_scored_in(done: set[asyncio.Task[int | None]]) -> int:
        return sum(
            1
            for t in done
            if not t.cancelled() and t.exception() is None and t.result() is not None
        )

    tasks = [asyncio.create_task(one(p)) for p in posts]
    done, pending = await asyncio.wait(tasks, timeout=SCORE_DEADLINE_S)
    while pending and n_scored_in(done) < floor:
        more, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        done |= more
    for t in pending:
        t.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    scores: list[int | None] = []
    n_cancelled = n_failed = 0
    first_error: BaseException | None = None
    for t in tasks:
        if t.cancelled():
            n_cancelled += 1
            scores.append(None)
        elif t.exception() is not None:
            n_failed += 1
            first_error = first_error or t.exception()
            scores.append(None)
        elif t.result() is None:
            # The call returned but the reply had no readable score: no label,
            # counted as failed so the three counts always add up to `n` and a
            # change in the model's reply format shows up in the log.
            n_failed += 1
            scores.append(None)
        else:
            scores.append(t.result())
    if n_cancelled:
        # Cancelled calls are billed but report no usage. Charge each the mean
        # input of the calls that did report and the *longest* reply any of
        # them produced: a call cut off mid-reply cannot have generated more
        # than a whole reply to the same prompt (measured stragglers are slow,
        # not long: 56-76 output tokens against a 68-74 mean). With nothing
        # reported, fall back to the per-call bounds.
        if scoring_usage.calls:
            mean_input = math.ceil(scoring_usage.input_tokens / scoring_usage.calls)
            max_output = scoring_usage.max_output_tokens
        else:
            mean_input, max_output = _SCORE_INPUT_TOKENS_BOUND, _SCORE_MAX_TOKENS
        scoring_usage.add_estimate(n_cancelled * mean_input, n_cancelled * max_output)
    usage.merge(scoring_usage)
    n_scored = sum(1 for s in scores if s is not None)
    if n_scored < floor:
        detail = f"scored {n_scored} of {n} posts (need {floor})"
        if first_error is not None:
            detail += f"; first error: {first_error!r}"
        raise ScoringError(detail)
    return ScoreOutcome(
        scores=scores, n_scored=n_scored, n_cancelled=n_cancelled, n_failed=n_failed
    )


def cost_usd(usage: Usage) -> float:
    """Spend at list price: reported tokens plus the estimate for cancelled calls."""
    return (
        (usage.input_tokens + usage.est_input_tokens) * _USD_PER_MTOK_INPUT
        + (usage.output_tokens + usage.est_output_tokens) * _USD_PER_MTOK_OUTPUT
    ) / 1_000_000


# --------------------------------------------------------------------------- #
# 5. Fit: ridge regression from embedding to score                              #
#    The query vector is the coefficient vector of a ridge regression of the   #
#    scores on the L2-normalised embeddings. Retrieval ranks by cosine, so     #
#    fitting on unit vectors makes the objective the thing retrieval           #
#    optimises. With ~200 rows and 384 dimensions the system is                #
#    under-determined, hence the ridge penalty; alpha = 4 was the sweep's       #
#    choice and the value every downstream measurement used.                    #
# --------------------------------------------------------------------------- #

RIDGE_ALPHA = 4.0


def fit_ridge(
    embeddings: np.ndarray, scores: np.ndarray, alpha: float
) -> tuple[np.ndarray, float, float | None]:
    """Closed-form ridge with an intercept: (q, intercept, train R^2).

    Rows are L2-normalised, then both sides are centred and
    `(E'E + alpha I) q = E's` is solved - the same estimator as a standard
    ridge fit with intercept. The intercept is returned for the R^2 figure
    but not stored: adding a constant to every score never changes a
    ranking. R^2 is None when the scores are constant (an all-MIN_SCORE
    sample, which the router still stores - a vector that says "nothing
    matched" is a valid answer).
    """
    E = np.asarray(embeddings, dtype=np.float64)
    s = np.asarray(scores, dtype=np.float64)
    norms = np.linalg.norm(E, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    E = E / norms
    e_mean = E.mean(axis=0)
    s_mean = s.mean()
    Ec = E - e_mean
    sc = s - s_mean
    q = np.linalg.solve(Ec.T @ Ec + alpha * np.eye(E.shape[1]), Ec.T @ sc)
    intercept = float(s_mean - e_mean @ q)
    ss_tot = float(sc @ sc)
    if ss_tot == 0.0:
        return q, intercept, None
    resid = s - (E @ q + intercept)
    return q, intercept, 1.0 - float(resid @ resid) / ss_tot


# --------------------------------------------------------------------------- #
# 6. Entry point                                                                #
# --------------------------------------------------------------------------- #


class PoolTooSmallError(FitError):
    """The corpus has too few posts matching the bag to fit anything."""


def prompt_key(prompt: str) -> str:
    """Firestore document id for a prompt: sha256 of the stripped text, first
    16 hex chars. The same prompt from the same user overwrites its vector;
    different prompts coexist under the user document."""
    return hashlib.sha256(prompt.strip().encode("utf-8")).hexdigest()[:16]


@dataclass
class FitResult:
    query_vector: list[float]
    keywords: list[str]
    n_pool: int
    n_keyword_posts: int
    n_random_posts: int
    n_scored: int
    n_cancelled: int
    n_failed: int
    train_r2: float | None
    duration_s: float
    cost_usd: float
    input_tokens: int
    output_tokens: int


async def fit_query_vector(es, prompt: str) -> FitResult:
    """Run the whole pipeline for `prompt`. Raises PoolTooSmallError,
    ExpansionError or ScoringError for the pipeline's own failures; client
    errors from Elasticsearch or Anthropic propagate as they are."""
    started = time.monotonic()
    client = _get_anthropic_client()
    usage = Usage()

    keywords = await expand_keywords(client, prompt, usage)

    pool, sim = await fetch_pool(es, keywords)
    if len(pool) < MIN_KEYWORD_POSTS:
        raise PoolTooSmallError(
            f"only {len(pool)} posts match the keywords (need {MIN_KEYWORD_POSTS})"
        )

    n_keyword = min(N_KEYWORD_POSTS, len(pool))
    n_random = round(n_keyword * N_RANDOM_POSTS / N_KEYWORD_POSTS)
    keyword_posts = [pool[i] for i in mmr_select(sim, n_keyword, MMR_LAMBDA)]
    random_posts = await fetch_random_posts(es, n_random, [p.at_uri for p in keyword_posts])
    await hydrate_embeddings(es, keyword_posts + random_posts)
    keyword_posts = [p for p in keyword_posts if p.embedding]
    random_posts = [p for p in random_posts if p.embedding][:n_random]
    if len(keyword_posts) < MIN_KEYWORD_POSTS:
        raise PoolTooSmallError(
            f"only {len(keyword_posts)} keyword posts have embeddings (need {MIN_KEYWORD_POSTS})"
        )

    worst_case = cost_usd(usage) + len(keyword_posts) * _SCORE_CALL_COST_BOUND_USD
    if worst_case > MAX_COST_USD:
        raise ScoringError(
            f"scoring {len(keyword_posts)} posts could cost ${worst_case:.2f}, "
            f"over the ${MAX_COST_USD:.2f} cap; nothing sent"
        )
    outcome = await score_posts(client, prompt, keyword_posts, usage)
    cost = cost_usd(usage)
    if cost > MAX_COST_USD:
        raise ScoringError(f"fit cost ${cost:.2f} exceeds the ${MAX_COST_USD:.2f} cap")

    labelled = [(p, s) for p, s in zip(keyword_posts, outcome.scores, strict=True) if s is not None]
    embeddings = [p.embedding for p, _ in labelled]
    scores = [s for _, s in labelled]
    embeddings += [p.embedding for p in random_posts]
    scores += [MIN_SCORE] * len(random_posts)
    q, _intercept, train_r2 = fit_ridge(np.asarray(embeddings), np.asarray(scores), RIDGE_ALPHA)

    return FitResult(
        query_vector=q.tolist(),
        keywords=keywords,
        n_pool=len(pool),
        n_keyword_posts=len(keyword_posts),
        n_random_posts=len(random_posts),
        n_scored=outcome.n_scored,
        n_cancelled=outcome.n_cancelled,
        n_failed=outcome.n_failed,
        train_r2=train_r2,
        duration_s=time.monotonic() - started,
        cost_usd=cost,
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
    )
