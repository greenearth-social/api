"""Unit tests for the prompt -> query-vector pipeline (lib/llm_query_vector_fit.py).

No network: the Anthropic client and Elasticsearch are stand-ins. The pure
steps (tokenizer, pool query, similarity, MMR, score parsing, ridge, cost)
are checked against hand-worked answers; the scoring step's deadline-and-
floor rule is exercised with a fake client whose calls sleep for chosen
times, with the deadline patched down to tens of milliseconds.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import numpy as np
import pytest

from . import llm_query_vector_fit as fit
from .llm_query_vector_fit import (
    MAX_SCORE,
    MIN_SCORE,
    ExpansionError,
    PoolTooSmallError,
    Post,
    Usage,
    _posts_from_hits,
    analyze,
    build_pool_query,
    cost_usd,
    expand_keywords,
    fit_query_vector,
    fit_ridge,
    jaccard_matrix,
    mmr_select,
    near_dup_keep,
    parse_keyword_bag,
    parse_score,
    score_posts,
)

# --------------------------------------------------------------------------- #
# Stand-ins                                                                     #
# --------------------------------------------------------------------------- #


def _reply(text: str, *, input_tokens: int = 400, output_tokens: int = 70, stop_reason="end_turn"):
    """The parts of an Anthropic message the module reads."""
    return SimpleNamespace(
        content=[SimpleNamespace(type="text", text=text)],
        usage=SimpleNamespace(input_tokens=input_tokens, output_tokens=output_tokens),
        stop_reason=stop_reason,
    )


def _score_reply(score: int, **kw):
    return _reply(json.dumps({"explanation": "because", "score": score}), **kw)


class FakeAnthropic:
    """`messages.create` driven by a per-call handler.

    `handler(kwargs)` returns a reply, raises, or returns an awaitable delay
    `(seconds, reply)` so a test can make chosen calls slow. Every call is
    recorded so tests can see what was started and what finished.
    """

    def __init__(self, handler):
        self._handler = handler
        self.started: list[dict] = []
        self.finished: list[dict] = []
        self.messages = SimpleNamespace(create=self._create)

    async def _create(self, **kwargs):
        self.started.append(kwargs)
        out = self._handler(kwargs)
        if isinstance(out, tuple):
            delay, out = out
            await asyncio.sleep(delay)
        if isinstance(out, BaseException):
            raise out
        self.finished.append(kwargs)
        return out


def _post(i: int, text: str | None = None, embedding=None) -> Post:
    text = text or f"post number {i} about gardens"
    return Post(
        at_uri=f"at://p/{i}", content=text, tokens=frozenset(analyze(text)), embedding=embedding
    )


def _post_text(kwargs) -> str:
    """The candidate post text out of a scoring call's user message."""
    body = kwargs["messages"][0]["content"]
    return body.split("Candidate post:\n", 1)[1].split("\n\nHow much", 1)[0]


def _hits(posts: list[tuple[str, str]]) -> dict:
    return {"hits": {"hits": [{"_source": {"at_uri": u, "content": c}} for u, c in posts]}}


# --------------------------------------------------------------------------- #
# 1. Expansion                                                                  #
# --------------------------------------------------------------------------- #


def test_parse_keyword_bag_strips_and_drops_blanks_and_non_strings():
    text = json.dumps({"keywords": [" solar ", "", "   ", 7, None, "wind power"]})
    assert parse_keyword_bag(text) == ["solar", "wind power"]


def test_parse_keyword_bag_raises_on_malformed_reply():
    with pytest.raises((json.JSONDecodeError, KeyError)):
        parse_keyword_bag("not json")
    with pytest.raises(KeyError):
        parse_keyword_bag(json.dumps({"terms": ["x"]}))


@pytest.mark.asyncio
async def test_expand_keywords_returns_bag_and_counts_usage():
    bag = [f"kw{i}" for i in range(fit.N_KEYWORDS)]
    client = FakeAnthropic(
        lambda kw: _reply(json.dumps({"keywords": bag}), input_tokens=300, output_tokens=120)
    )
    usage = Usage()

    keywords = await expand_keywords(client, "hopeful science", usage)

    assert keywords == bag
    assert (usage.input_tokens, usage.output_tokens, usage.calls) == (300, 120, 1)
    call = client.started[0]
    assert call["model"] == fit.EXPANSION_MODEL
    assert call["output_config"]["format"]["type"] == "json_schema"
    assert "hopeful science" in call["messages"][0]["content"]


@pytest.mark.asyncio
async def test_expand_keywords_rejects_short_bag_and_empty_reply():
    short = [f"kw{i}" for i in range(fit.MIN_KEYWORDS - 1)]
    client = FakeAnthropic(lambda kw: _reply(json.dumps({"keywords": short})))
    with pytest.raises(ExpansionError, match="only"):
        await expand_keywords(client, "x")

    empty = FakeAnthropic(
        lambda kw: SimpleNamespace(
            content=[],
            usage=SimpleNamespace(input_tokens=1, output_tokens=0),
            stop_reason="max_tokens",
        )
    )
    with pytest.raises(ExpansionError, match="empty"):
        await expand_keywords(empty, "x")


# --------------------------------------------------------------------------- #
# 2. Pool: tokenizer, query, hit parsing, similarity, near-dup filter           #
# --------------------------------------------------------------------------- #


def test_analyze_replicates_the_index_token_stream():
    tokens = analyze("The cats' toys and Ann's 🌱🌱 garden for 日本語 fans")
    # Lower-cased, stopwords gone, apostrophe kept only inside a word, one
    # token per emoji, one per Han character.
    assert tokens == ["cats", "toys", "ann's", "🌱", "🌱", "garden", "日", "本", "語", "fans"]


def test_analyze_drops_stopwords_and_handles_empty():
    assert analyze("the and of") == []
    assert analyze("") == []
    assert analyze("Don't stop") == ["don't", "stop"]


def test_build_pool_query_uses_each_distinct_token_once_in_bag_order():
    q = build_pool_query(["AI safety", "ai news", "the AI"])
    assert q == {"match": {"content": "ai safety news"}}


def test_posts_from_hits_drops_blank_missing_and_duplicate_content():
    resp = _hits(
        [
            ("at://1", "Solar panels on my roof"),
            ("at://2", "   "),
            ("at://3", "Solar   panels on   my roof"),  # same text modulo whitespace
            ("at://4", "Wind turbines at sea"),
        ]
    )
    resp["hits"]["hits"].append({"_source": {"content": "no uri"}})

    posts = _posts_from_hits(resp)

    assert [p.at_uri for p in posts] == ["at://1", "at://4"]
    assert posts[0].tokens == frozenset({"solar", "panels", "my", "roof"})


def test_jaccard_matrix_matches_hand_computation():
    sets = [frozenset("ab"), frozenset("bc"), frozenset("ab"), frozenset("xyz")]
    sim = jaccard_matrix(sets)

    assert sim.shape == (4, 4)
    assert np.allclose(np.diag(sim), 1.0)
    assert sim[0, 1] == pytest.approx(1 / 3)  # {a,b} vs {b,c}: 1 shared of 3
    assert sim[0, 2] == pytest.approx(1.0)
    assert sim[0, 3] == 0.0
    assert np.allclose(sim, sim.T)


def test_jaccard_matrix_handles_empty_input_and_empty_token_sets():
    assert jaccard_matrix([]).shape == (0, 0)
    # A post with no tokens (all stopwords) has 0/0 similarity to everything,
    # itself included, which the matrix reports as 0: it can never be flagged
    # as a near-duplicate, nor pull anything else out as one.
    assert jaccard_matrix([frozenset(), frozenset("ab")]).tolist() == [[0.0, 0.0], [0.0, 1.0]]


def test_near_dup_keep_is_first_wins_and_stops_at_limit():
    sim = jaccard_matrix([frozenset("ab"), frozenset("bc"), frozenset("ab"), frozenset("xy")])

    assert near_dup_keep(sim, threshold=0.8, stop_after=10) == [0, 1, 3]
    assert near_dup_keep(sim, threshold=0.8, stop_after=2) == [0, 1]
    # A threshold above 1 never triggers; one at 0 keeps only the first.
    assert near_dup_keep(sim, threshold=1.1, stop_after=10) == [0, 1, 2, 3]
    assert near_dup_keep(sim, threshold=0.0, stop_after=10) == [0]


# --------------------------------------------------------------------------- #
# 3. Sample: MMR                                                                #
# --------------------------------------------------------------------------- #


def test_mmr_select_prefers_a_different_post_over_a_duplicate():
    # Post 1 is a copy of post 0 (sim 1.0); post 2 is unrelated to both.
    sim = np.array([[1.0, 1.0, 0.0], [1.0, 1.0, 0.0], [0.0, 0.0, 1.0]])

    assert mmr_select(sim, n=2, mmr_lambda=0.5) == [0, 2]
    # Pure relevance ignores similarity and follows BM25 order.
    assert mmr_select(sim, n=2, mmr_lambda=1.0) == [0, 1]


def test_mmr_select_returns_everything_when_n_covers_the_pool():
    sim = np.eye(3)
    assert mmr_select(sim, n=3, mmr_lambda=0.5) == [0, 1, 2]
    assert mmr_select(sim, n=7, mmr_lambda=0.5) == [0, 1, 2]


def test_mmr_select_returns_distinct_indices_of_requested_length():
    rng = np.random.default_rng(0)
    a = rng.random((40, 40))
    sim = (a + a.T) / 2
    np.fill_diagonal(sim, 1.0)

    picked = mmr_select(sim, n=15, mmr_lambda=0.5)

    assert len(picked) == 15 and len(set(picked)) == 15
    assert picked[0] == 0


# --------------------------------------------------------------------------- #
# 4. Score: parsing, the concurrent step, cost                                  #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "raw, expected",
    [
        ('{"explanation": "fits", "score": 7}', 7),
        ('```json\n{"explanation": "fits", "score": 4}\n```', 4),
        ('Sure! {"explanation": "x", "score": 12}', MAX_SCORE),  # clamped
        ('{"score": 0}', MIN_SCORE),  # clamped
        ('{"score": "seven"} "score": 8', 8),  # JSON path fails, regex path wins
        ("score: 7 maybe", None),
        ("", None),
    ],
)
def test_parse_score(raw, expected):
    assert parse_score(raw) == expected


def test_usage_add_merge_and_cost_at_list_price():
    u = Usage()
    u.add(SimpleNamespace(input_tokens=400, output_tokens=60))
    u.add(SimpleNamespace(input_tokens=600, output_tokens=90))
    assert (u.input_tokens, u.output_tokens, u.calls, u.max_output_tokens) == (1000, 150, 2, 90)

    other = Usage()
    other.add(SimpleNamespace(input_tokens=1, output_tokens=200))
    other.add_estimate(10, 20)
    u.merge(other)
    assert (u.input_tokens, u.output_tokens, u.calls, u.max_output_tokens) == (1001, 350, 3, 200)
    assert (u.est_input_tokens, u.est_output_tokens) == (10, 20)

    # One million input tokens is $2, one million output tokens $10; the
    # estimate for cancelled calls is charged the same way.
    assert cost_usd(Usage(input_tokens=1_000_000)) == pytest.approx(2.0)
    assert cost_usd(Usage(output_tokens=1_000_000)) == pytest.approx(10.0)
    assert cost_usd(Usage(est_input_tokens=500_000, est_output_tokens=100_000)) == pytest.approx(
        2.0
    )


@pytest.mark.asyncio
async def test_score_posts_all_fast_scores_everything(monkeypatch):
    monkeypatch.setattr(fit, "SCORE_DEADLINE_S", 0.5)
    posts = [_post(i, f"post {i}") for i in range(5)]
    client = FakeAnthropic(lambda kw: _score_reply(int(_post_text(kw).split()[1]) + 1))
    usage = Usage()

    out = await score_posts(client, "prompt", posts, usage)

    assert out.scores == [1, 2, 3, 4, 5]
    assert (out.n_scored, out.n_cancelled, out.n_failed) == (5, 0, 0)
    assert usage.calls == 5 and usage.est_input_tokens == 0
    assert len(client.started) == 5


@pytest.mark.asyncio
async def test_score_posts_cancels_stragglers_once_the_floor_is_met(monkeypatch):
    monkeypatch.setattr(fit, "SCORE_DEADLINE_S", 0.05)
    posts = [_post(i, f"post {i}") for i in range(5)]  # floor = ceil(0.8 * 5) = 4

    def handler(kw):
        reply = _score_reply(5, input_tokens=400, output_tokens=70)
        return (1.0, reply) if _post_text(kw) == "post 4" else reply

    client = FakeAnthropic(handler)
    usage = Usage()

    started = asyncio.get_running_loop().time()
    out = await score_posts(client, "prompt", posts, usage)
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 0.5, "the straggler was not cancelled at the deadline"
    assert out.scores == [5, 5, 5, 5, None]
    assert (out.n_scored, out.n_cancelled, out.n_failed) == (4, 1, 0)
    # The cancelled call reports nothing but is billed: charged the mean
    # input and the longest reply of the calls that did report.
    assert usage.calls == 4
    assert usage.est_input_tokens == 400
    assert usage.est_output_tokens == 70
    assert len(client.finished) == 4


@pytest.mark.asyncio
async def test_score_posts_waits_past_the_deadline_until_the_floor_is_met(monkeypatch):
    monkeypatch.setattr(fit, "SCORE_DEADLINE_S", 0.02)
    posts = [_post(i, f"post {i}") for i in range(5)]  # floor 4; only 3 are fast

    def handler(kw):
        text = _post_text(kw)
        if text == "post 3":
            return (0.15, _score_reply(6))
        if text == "post 4":
            return (1.0, _score_reply(6))
        return _score_reply(6)

    client = FakeAnthropic(handler)

    started = asyncio.get_running_loop().time()
    out = await score_posts(client, "prompt", posts, Usage())
    elapsed = asyncio.get_running_loop().time() - started

    assert 0.15 <= elapsed < 0.8
    assert out.scores == [6, 6, 6, 6, None]
    assert (out.n_scored, out.n_cancelled, out.n_failed) == (4, 1, 0)


@pytest.mark.asyncio
async def test_score_posts_counts_failures_and_unparseable_replies(monkeypatch):
    monkeypatch.setattr(fit, "SCORE_DEADLINE_S", 0.5)
    posts = [_post(i, f"post {i}") for i in range(10)]  # floor 8

    def handler(kw):
        text = _post_text(kw)
        if text == "post 0":
            return RuntimeError("boom")
        if text == "post 1":
            return _reply("I would rather not say.")
        if text == "post 2":
            return _reply("", stop_reason="refusal")
        return _score_reply(3)

    out = await score_posts(FakeAnthropic(handler), "prompt", posts, Usage())

    assert out.scores[:3] == [None, None, MIN_SCORE]  # a refusal is a verdict
    assert (out.n_scored, out.n_cancelled, out.n_failed) == (8, 0, 2)


@pytest.mark.asyncio
async def test_score_posts_raises_below_the_floor_and_names_the_first_error(monkeypatch):
    monkeypatch.setattr(fit, "SCORE_DEADLINE_S", 0.5)
    posts = [_post(i, f"post {i}") for i in range(5)]

    def handler(kw):
        return RuntimeError("boom") if _post_text(kw) in {"post 0", "post 1"} else _score_reply(3)

    with pytest.raises(
        fit.ScoringError, match=r"scored 3 of 5 posts \(need 4\); first error: RuntimeError"
    ):
        await score_posts(FakeAnthropic(handler), "prompt", posts, Usage())


@pytest.mark.asyncio
async def test_score_posts_cancelled_from_outside_leaves_no_call_running(monkeypatch):
    # The router's request deadline cancels the fit mid-wait; the finally in
    # score_posts must take the in-flight calls down with it.
    monkeypatch.setattr(fit, "SCORE_DEADLINE_S", 10.0)
    posts = [_post(i, f"post {i}") for i in range(5)]
    client = FakeAnthropic(lambda kw: (5.0, _score_reply(5)))

    task = asyncio.create_task(score_posts(client, "prompt", posts, Usage()))
    await asyncio.sleep(0.05)
    assert len(client.started) == 5
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    leftovers = [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    assert leftovers == []
    assert client.finished == []


# --------------------------------------------------------------------------- #
# 5. Fit: ridge                                                                 #
# --------------------------------------------------------------------------- #


def test_fit_ridge_recovers_the_scoring_direction():
    rng = np.random.default_rng(1)
    dim, n = 16, 120
    direction = rng.normal(size=dim)
    direction /= np.linalg.norm(direction)
    E = rng.normal(size=(n, dim))
    E /= np.linalg.norm(E, axis=1, keepdims=True)
    scores = 5.0 + 4.0 * (E @ direction)

    q, intercept, r2 = fit_ridge(E, scores, alpha=0.01)

    cosine = float(q @ direction / np.linalg.norm(q))
    assert cosine > 0.99
    assert r2 is not None and r2 > 0.99
    assert intercept == pytest.approx(5.0, abs=0.2)


def test_fit_ridge_normalises_rows_and_penalty_shrinks_the_vector():
    rng = np.random.default_rng(2)
    E = rng.normal(size=(50, 8))
    scores = rng.integers(1, 11, size=50).astype(float)

    q_unit, _, _ = fit_ridge(E, scores, alpha=1.0)
    q_scaled, _, _ = fit_ridge(E * 7.0, scores, alpha=1.0)  # same directions, longer rows
    assert np.allclose(q_unit, q_scaled)

    q_loose, _, _ = fit_ridge(E, scores, alpha=0.1)
    q_tight, _, _ = fit_ridge(E, scores, alpha=100.0)
    assert np.linalg.norm(q_tight) < np.linalg.norm(q_loose)


def test_fit_ridge_constant_scores_give_no_r2_but_a_vector():
    E = np.random.default_rng(3).normal(size=(20, 6))
    q, intercept, r2 = fit_ridge(E, np.full(20, float(MIN_SCORE)), alpha=4.0)

    assert r2 is None
    assert q.shape == (6,) and np.allclose(q, 0.0)
    assert intercept == pytest.approx(MIN_SCORE)


# --------------------------------------------------------------------------- #
# 6. Entry point                                                                #
# --------------------------------------------------------------------------- #

_DIM = 12


def _embedding(at_uri: str) -> list[float]:
    seed = sum(map(ord, at_uri))
    return np.random.default_rng(seed).normal(size=_DIM).tolist()


class FakeES:
    """`search` answers by the `op` tag the module passes."""

    def __init__(self, pool: list[tuple[str, str]], random_posts: list[tuple[str, str]]):
        self._by_op = {"llm_qv_pool": _hits(pool), "llm_qv_random": _hits(random_posts)}
        self.calls: list[dict] = []

    async def search(self, **kwargs):
        self.calls.append(kwargs)
        return self._by_op[kwargs["op"]]


@pytest.fixture
def small_sample(monkeypatch):
    """Shrink the sample so a handful of fake posts is a full fit."""
    monkeypatch.setattr(fit, "MIN_KEYWORD_POSTS", 3)
    monkeypatch.setattr(fit, "N_KEYWORD_POSTS", 4)
    monkeypatch.setattr(fit, "N_RANDOM_POSTS", 6)
    monkeypatch.setattr(fit, "SCORE_DEADLINE_S", 0.5)


def _pipeline_client(*, score_tokens=(400, 70)):
    bag = [f"kw{i}" for i in range(fit.N_KEYWORDS)]

    def handler(kw):
        if "output_config" in kw:  # the expansion call
            return _reply(json.dumps({"keywords": bag}), input_tokens=300, output_tokens=150)
        text = _post_text(kw)
        return _score_reply(
            9 if "solar" in text else 2, input_tokens=score_tokens[0], output_tokens=score_tokens[1]
        )

    return FakeAnthropic(handler)


def _hydrate_all(es, at_uris, index):
    return [(u, _embedding(u), None) for u in at_uris]


@pytest.mark.asyncio
async def test_fit_query_vector_happy_path(small_sample):
    pool = [
        (f"at://k/{i}", f"solar panels story {i}" if i % 2 else f"wind turbines story {i}")
        for i in range(8)
    ]
    randoms = [(f"at://r/{i}", f"unrelated chatter {i}") for i in range(12)]
    es = FakeES(pool, randoms)
    client = _pipeline_client()

    with (
        patch.object(fit, "_get_anthropic_client", return_value=client),
        patch.object(
            fit, "fetch_post_embeddings_and_politics_scores", AsyncMock(side_effect=_hydrate_all)
        ),
    ):
        result = await fit_query_vector(es, "hopeful solar news")

    assert len(result.query_vector) == _DIM
    assert result.keywords == [f"kw{i}" for i in range(fit.N_KEYWORDS)]
    assert result.n_pool == 8
    assert (result.n_keyword_posts, result.n_random_posts) == (4, 6)
    assert (result.n_scored, result.n_cancelled, result.n_failed) == (4, 0, 0)
    assert result.train_r2 is not None
    # Expansion plus four scoring calls, at list price.
    assert result.input_tokens == 300 + 4 * 400
    assert result.output_tokens == 150 + 4 * 70
    assert result.cost_usd == pytest.approx(cost_usd(Usage(input_tokens=1900, output_tokens=430)))
    assert len(client.started) == 5

    ops = [c["op"] for c in es.calls]
    assert ops == ["llm_qv_pool", "llm_qv_random"]
    assert es.calls[0]["size"] == min(
        int(fit.POOL_N * fit._POOL_OVERFETCH), fit._ES_MAX_RESULT_WINDOW
    )
    # The random block excludes the keyword posts so the two never overlap.
    excluded = es.calls[1]["query"]["function_score"]["query"]["bool"]["must_not"][0]["terms"][
        "at_uri"
    ]
    assert len(excluded) == 4 and all(u.startswith("at://k/") for u in excluded)


@pytest.mark.asyncio
async def test_fit_query_vector_vector_points_toward_high_scoring_posts(small_sample):
    pool = [
        (f"at://k/{i}", f"solar panels story {i}" if i % 2 else f"wind turbines story {i}")
        for i in range(8)
    ]
    randoms = [(f"at://r/{i}", f"unrelated chatter {i}") for i in range(12)]
    client = _pipeline_client()

    with (
        patch.object(fit, "_get_anthropic_client", return_value=client),
        patch.object(
            fit, "fetch_post_embeddings_and_politics_scores", AsyncMock(side_effect=_hydrate_all)
        ),
    ):
        result = await fit_query_vector(FakeES(pool, randoms), "solar")

    q = np.asarray(result.query_vector)
    scored = {_post_text(kw): 9 if "solar" in _post_text(kw) else 2 for kw in client.started[1:]}
    uri_by_text = {c: u for u, c in pool}

    def cosine(text: str) -> float:
        e = np.asarray(_embedding(uri_by_text[text]))
        return float(q @ e / (np.linalg.norm(q) * np.linalg.norm(e)))

    high = [cosine(t) for t, s in scored.items() if s == 9]
    low = [cosine(t) for t, s in scored.items() if s == 2]
    assert min(high) > max(low)


@pytest.mark.asyncio
async def test_fit_query_vector_rejects_a_pool_that_is_too_small(small_sample):
    es = FakeES([("at://k/1", "one lonely post"), ("at://k/2", "another lonely post")], [])
    client = _pipeline_client()

    with patch.object(fit, "_get_anthropic_client", return_value=client):
        with pytest.raises(PoolTooSmallError, match="only 2 posts match"):
            await fit_query_vector(es, "something niche")

    assert len(client.started) == 1, "no scoring calls should be sent"


@pytest.mark.asyncio
async def test_fit_query_vector_rejects_when_too_few_posts_have_embeddings(small_sample):
    pool = [(f"at://k/{i}", f"story {i}") for i in range(8)]
    randoms = [(f"at://r/{i}", f"chatter {i}") for i in range(12)]
    client = _pipeline_client()

    def hydrate_two(es, at_uris, index):
        return [(u, _embedding(u), None) for u in at_uris if u in {"at://k/0", "at://k/1"}]

    with (
        patch.object(fit, "_get_anthropic_client", return_value=client),
        patch.object(
            fit, "fetch_post_embeddings_and_politics_scores", AsyncMock(side_effect=hydrate_two)
        ),
    ):
        with pytest.raises(PoolTooSmallError, match="have embeddings"):
            await fit_query_vector(FakeES(pool, randoms), "x")

    assert len(client.started) == 1


@pytest.mark.asyncio
async def test_fit_query_vector_preflight_refuses_a_fit_that_could_blow_the_cap(
    small_sample, monkeypatch
):
    monkeypatch.setattr(fit, "MAX_COST_USD", 0.001)
    pool = [(f"at://k/{i}", f"story {i}") for i in range(8)]
    randoms = [(f"at://r/{i}", f"chatter {i}") for i in range(12)]
    client = _pipeline_client()

    with (
        patch.object(fit, "_get_anthropic_client", return_value=client),
        patch.object(
            fit, "fetch_post_embeddings_and_politics_scores", AsyncMock(side_effect=_hydrate_all)
        ),
    ):
        with pytest.raises(fit.ScoringError, match="nothing sent"):
            await fit_query_vector(FakeES(pool, randoms), "x")

    assert len(client.started) == 1, "the preflight must fire before any scoring call"


@pytest.mark.asyncio
async def test_fit_query_vector_over_cap_after_scoring_logs_and_still_fits(small_sample, caplog):
    # The preflight passes on the token bounds; the calls then report far more
    # tokens than bounded. The spend is sunk, so the fit is kept and logged.
    pool = [(f"at://k/{i}", f"story {i}") for i in range(8)]
    randoms = [(f"at://r/{i}", f"chatter {i}") for i in range(12)]
    client = _pipeline_client(score_tokens=(5_000_000, 70))

    with (
        patch.object(fit, "_get_anthropic_client", return_value=client),
        patch.object(
            fit, "fetch_post_embeddings_and_politics_scores", AsyncMock(side_effect=_hydrate_all)
        ),
        caplog.at_level(logging.ERROR, logger=fit.__name__),
    ):
        result = await fit_query_vector(FakeES(pool, randoms), "x")

    assert result.cost_usd > fit.MAX_COST_USD
    assert len(result.query_vector) == _DIM
    assert any(
        "exceeds" in r.getMessage() and "fitting anyway" in r.getMessage() for r in caplog.records
    )


def test_score_call_cost_bound_is_the_worst_case_of_one_call():
    assert fit._SCORE_CALL_COST_BOUND_USD == pytest.approx(
        (fit._SCORE_INPUT_TOKENS_BOUND * 2.0 + fit._SCORE_MAX_TOKENS * 10.0) / 1_000_000
    )
    # A normal fit's worst case stays well under the cap, so the breaker only
    # trips on a misconfiguration.
    assert fit.N_KEYWORD_POSTS * fit._SCORE_CALL_COST_BOUND_USD < fit.MAX_COST_USD
    assert math.ceil(fit.MIN_SCORED_FRACTION * fit.N_KEYWORD_POSTS) == 64
