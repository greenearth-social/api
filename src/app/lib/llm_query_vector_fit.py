"""Fit a MiniLM query vector to a free-text prompt (ingex#482).

Pipeline, one recipe, no tunable axes:

    expand  -> one Sonnet call turns the prompt into a 24-term keyword bag
    pool    -> BM25 over posts_recent with each distinct token once
    sample  -> 80 keyword posts (lexical MMR) + 120 random posts
    score   -> Sonnet rates the 80 keyword posts 1-10; random posts are forced to 1
    fit     -> ridge regression on L2-normalised embeddings -> query vector

The prompt text and the numeric choices come from the keyword-expansion and
query-vector-fitting experiments (llm-approximation-experiments-jplk, Juan's
repo). Comments here give the reasoning; the measurements live there.

This module is pure library code: no FastAPI, no Firestore. The router
(routers/llm_query_vectors.py) calls `fit_query_vector` and stores the result.
"""

from __future__ import annotations

import json
import logging
import os

from anthropic import AsyncAnthropic

logger = logging.getLogger(__name__)


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


class ExpansionError(RuntimeError):
    """The model did not return a usable keyword bag."""


async def expand_keywords(client: AsyncAnthropic, prompt: str) -> list[str]:
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
    text = next((b.text for b in resp.content if b.type == "text"), "")
    if not text:
        raise ExpansionError(f"empty expansion reply (stop_reason={resp.stop_reason!r})")
    keywords = parse_keyword_bag(text)
    if len(keywords) < MIN_KEYWORDS:
        raise ExpansionError(f"expansion returned only {len(keywords)} keywords")
    if len(keywords) != N_KEYWORDS:
        logger.info("expansion returned %d keywords, asked for %d", len(keywords), N_KEYWORDS)
    return keywords
