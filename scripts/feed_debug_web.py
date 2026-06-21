#!/usr/bin/env python3
"""Local web viewer for the newest feed-debug record for one Bluesky user.

Run from the api/ directory:
    pipenv run python scripts/feed_debug_web.py --port 8000

Then open:
    http://127.0.0.1:8000
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from html import escape
from typing import Any, Literal

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from app.documents import FeedDebugDocument
from app.lib.firestore import (
    get_recent_feed_debug,
    get_user,
    get_user_by_username,
    init_firestore_client,
)

logger = logging.getLogger(__name__)

GCP_PROJECT = "greenearth-471522"
_ENVIRONMENTS = {
    "stage": "greenearth-stage",
    "prod": "greenearth-prod",
}
DEFAULT_ENVIRONMENT = "stage"
TARGET_FEED_NAME = "your-feed"
FEED_DEBUG_LOOKUP_LIMIT = 50
SCORE_BREAKDOWN_MAX = 0.5

_GENERATOR_TONES = {
    "two_tower": "green",
    "followed_users": "blue",
    "popularity": "amber",
    "post_similarity": "violet",
    "network_likes": "cyan",
    "random_posts": "slate",
}
_MODEL_TONES = {
    "two_tower": "green",
    "perspective": "violet",
}

app = FastAPI(title="Feed Debug Viewer")


@dataclass(frozen=True)
class GeneratorView:
    name: str
    score: float | None


@dataclass(frozen=True)
class ModelScoreView:
    name: str
    weight: float
    score: float


@dataclass(frozen=True)
class RankContributionView:
    name: str
    score: float
    weight: float
    contribution: float
    scaled_contribution: float
    tone: str
    bottom_pct: float
    height_pct: float


@dataclass(frozen=True)
class DiversificationView:
    relevance: float
    score: float
    author_penalty: float
    content_penalty: float


@dataclass(frozen=True)
class ItemView:
    at_uri: str
    post_url: str | None
    final_position: int
    author: str
    content: str
    media_labels: list[str]
    generators: list[GeneratorView]
    rank_position: int | None
    rank_score: float | None
    after_rank_position: int | None
    model_scores: list[ModelScoreView]
    diversification: DiversificationView | None


LookupStatus = Literal["not_found", "no_records", "record"]


@dataclass(frozen=True)
class FeedDebugLookup:
    status: LookupStatus
    query_user: str
    user_did: str | None = None
    debug_enabled: bool = False
    doc: FeedDebugDocument | None = None


def _configure_environment(env: str) -> None:
    """Point Firestore at the selected deployed environment."""
    os.environ["GE_FIRESTORE_PROJECT"] = GCP_PROJECT
    os.environ["GE_FIRESTORE_DATABASE"] = _ENVIRONMENTS[env]
    os.environ.pop("GE_FIRESTORE_EMULATOR_HOST", None)
    os.environ.pop("FIRESTORE_EMULATOR_HOST", None)


async def _resolve_user_did(db: Any, user: str) -> str | None:
    """Resolve a handle or DID argument to a user DID."""
    if user.startswith("did:"):
        return user
    doc = await get_user_by_username(db, user)
    return doc.user_did if doc else None


def _close_firestore_client(db: Any) -> None:
    close = getattr(db, "close", None)
    if close is not None:
        close()


async def _load_latest_feed_debug(user: str, environment: str) -> FeedDebugLookup:
    _configure_environment(environment)
    db = init_firestore_client()
    try:
        user_did = await _resolve_user_did(db, user)
        if user_did is None:
            return FeedDebugLookup(status="not_found", query_user=user)

        user_doc = await get_user(db, user_did)
        docs = await get_recent_feed_debug(db, user_did, limit=FEED_DEBUG_LOOKUP_LIMIT)
        doc = _latest_target_feed_debug(docs)
        if doc is None:
            return FeedDebugLookup(
                status="no_records",
                query_user=user,
                user_did=user_did,
                debug_enabled=bool(user_doc and user_doc.debug_feeds),
            )
        return FeedDebugLookup(
            status="record",
            query_user=user,
            user_did=user_did,
            debug_enabled=bool(user_doc and user_doc.debug_feeds),
            doc=doc,
        )
    finally:
        _close_firestore_client(db)


def _latest_target_feed_debug(docs: list[FeedDebugDocument]) -> FeedDebugDocument | None:
    return next((doc for doc in docs if doc.feed_name == TARGET_FEED_NAME), None)


def _h(value: object | None) -> str:
    return escape("" if value is None else str(value), quote=True)


def _relative_time(dt: datetime) -> str:
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        delta = datetime.now(UTC) - dt.astimezone(UTC)
        secs = delta.total_seconds()
        if secs < 60:
            return "just now"
        if secs < 3_600:
            return f"{int(secs / 60)}m ago"
        if secs < 86_400:
            return f"{int(secs / 3_600)}h ago"
        if delta.days < 30:
            return f"{delta.days}d ago"
        return dt.strftime("%b %d, %Y")
    except Exception:
        return str(dt)


def _fmt_score(score: float | None) -> str:
    return f"{score:.2f}" if score is not None else "--"


def _fmt_short_score(score: float) -> str:
    return f"{score:.2f}"


def _diversification_relevance_contribution(div: DiversificationView) -> float:
    """Displayed relevance term so: score = rel - author_penalty - content_penalty."""
    return div.score + div.author_penalty + div.content_penalty


def _model_specs_str(doc: FeedDebugDocument) -> str:
    if doc.model_scores:
        return ", ".join(f"{m.model_name}({m.weight:g})" for m in doc.model_scores)
    return doc.ranker_model or "(none)"


def _generator_tone(name: str) -> str:
    return _GENERATOR_TONES.get(name, "slate")


def _model_tone(name: str) -> str:
    return _MODEL_TONES.get(name, "slate")


def _rank_contributions(
    model_scores: list[ModelScoreView],
    *,
    rank_total: float | None = None,
    target_total: float | None = None,
) -> list[RankContributionView]:
    total_weight = sum(score.weight for score in model_scores)
    if total_weight <= 0:
        return []

    raw = [
        (
            score,
            score.score * score.weight / total_weight,
        )
        for score in model_scores
    ]
    if rank_total is None:
        rank_total = sum(contribution for _, contribution in raw)
    if target_total is not None and rank_total > 0:
        scale_factor = target_total / rank_total
    else:
        scale_factor = 1.0

    scaled = [
        (
            score,
            contribution,
            contribution * scale_factor,
        )
        for score, contribution in raw
    ]
    positive_only = all(scaled_contribution >= 0 for _, _, scaled_contribution in scaled)
    scale = (100.0 if positive_only else 50.0) / SCORE_BREAKDOWN_MAX
    positive_seen = 0.0
    negative_seen = 0.0
    contributions = []
    for score, contribution, scaled_contribution in scaled:
        height_pct = min(100.0, abs(scaled_contribution) * scale)
        if positive_only:
            bottom_pct = min(100.0, positive_seen * scale)
            positive_seen += max(0.0, scaled_contribution)
        elif scaled_contribution >= 0:
            bottom_pct = min(100.0, 50.0 + positive_seen * scale)
            positive_seen += scaled_contribution
        else:
            negative_seen += abs(scaled_contribution)
            bottom_pct = max(0.0, 50.0 - negative_seen * scale)
        contributions.append(
            RankContributionView(
                name=score.name,
                score=score.score,
                weight=score.weight,
                contribution=contribution,
                scaled_contribution=scaled_contribution,
                tone=_model_tone(score.name),
                bottom_pct=bottom_pct,
                height_pct=height_pct,
            )
        )
    return contributions


def _at_uri_to_bsky_url(at_uri: str) -> str | None:
    match = re.match(r"^at://([^/]+)/app\.bsky\.feed\.post/([^/]+)$", at_uri)
    if match is None:
        return None
    did, post_id = match.groups()
    return f"https://bsky.app/profile/{did}/post/{post_id}"


def _media_labels(candidate: Any) -> list[str]:
    labels = []
    if candidate.image_count:
        labels.append(f"{candidate.image_count} image{'s' if candidate.image_count != 1 else ''}")
    elif candidate.contains_images:
        labels.append("image")
    if candidate.video_count:
        labels.append(f"{candidate.video_count} video{'s' if candidate.video_count != 1 else ''}")
    elif candidate.contains_video:
        labels.append("video")
    if candidate.external_uri:
        labels.append("link")
    return labels


def _build_item_views(doc: FeedDebugDocument) -> list[ItemView]:
    generators_by_uri: dict[str, list[GeneratorView]] = {}
    for result in doc.generator_outputs:
        for candidate in result.candidates:
            if candidate.at_uri:
                generators_by_uri.setdefault(candidate.at_uri, []).append(
                    GeneratorView(result.generator_name, candidate.score)
                )

    rank_by_uri = {
        ranking.at_uri: (ranking.rank, ranking.rank_score)
        for ranking in (doc.ranking.rankings if doc.ranking else [])
    }
    model_scores_by_uri: dict[str, list[ModelScoreView]] = {}
    for entry in doc.model_scores:
        for score in entry.scores:
            model_scores_by_uri.setdefault(score.at_uri, []).append(
                ModelScoreView(entry.model_name, entry.weight, score.score)
            )
    after_rank_pos = {uri: index for index, uri in enumerate(doc.order_after_rank, start=1)}
    div_by_uri = {
        entry.at_uri: DiversificationView(
            entry.relevance,
            entry.score,
            entry.author_penalty,
            entry.content_penalty,
        )
        for entry in doc.diversification
    }

    metadata_by_uri: dict[str, Any] = {}
    for result in doc.generator_outputs:
        for candidate in result.candidates:
            if candidate.at_uri:
                metadata_by_uri.setdefault(candidate.at_uri, candidate)
    for candidate in doc.final_candidates:
        if candidate.at_uri:
            metadata_by_uri[candidate.at_uri] = candidate

    items = []
    for final_position, at_uri in enumerate(doc.final_order, start=1):
        candidate = metadata_by_uri.get(at_uri)
        rank_position, rank_score = rank_by_uri.get(at_uri, (None, None))
        if candidate is None:
            author = "unknown author"
            content = ""
            media_labels: list[str] = []
        else:
            handle = candidate.author_username or candidate.author_did
            author = f"@{handle}" if handle else "unknown author"
            content = (candidate.content or "").replace("\n", " ")
            media_labels = _media_labels(candidate)
        items.append(
            ItemView(
                at_uri=at_uri,
                post_url=_at_uri_to_bsky_url(at_uri),
                final_position=final_position,
                author=author,
                content=content,
                media_labels=media_labels,
                generators=generators_by_uri.get(at_uri, []),
                rank_position=rank_position,
                rank_score=rank_score,
                after_rank_position=after_rank_pos.get(at_uri),
                model_scores=model_scores_by_uri.get(at_uri, []),
                diversification=div_by_uri.get(at_uri),
            )
        )
    return items


def _chip(label: str, value: str, kind: str) -> str:
    return (
        f'<span class="chip chip-{_h(kind)}">'
        f'<span>{_h(label)}</span><strong>{_h(value)}</strong></span>'
    )


def _generator_badge(name: str, detail: str | None = None) -> str:
    body = f"<strong>{_h(name)}</strong>"
    if detail:
        body += f"<span>{_h(detail)}</span>"
    return f'<span class="generator-badge gen-{_h(_generator_tone(name))}">{body}</span>'


def _render_generator_legend(doc: FeedDebugDocument) -> str:
    badges = [
        _generator_badge(generator.name, f"weight {generator.weight:g}")
        for generator in doc.generate_request.generators
    ]
    if doc.generate_request.infill:
        badges.append(_generator_badge(doc.generate_request.infill, "infill"))
    return f"""
    <section class="generator-panel">
      <h2>Candidate generators</h2>
      <div class="generator-row">{"".join(badges)}</div>
    </section>
    """


def _render_chip_group(label: str, chips: list[str], kind: str) -> str:
    if not chips:
        return ""
    return (
        f'<section class="debug-group debug-group-{_h(kind)}">'
        f'<h3>{_h(label)}</h3>'
        f'<div class="chip-row">{"".join(chips)}</div>'
        "</section>"
    )


def _render_rank_visual(item: ItemView) -> str:
    if item.final_position == 1:
        return ""

    div_score = item.diversification.score if item.diversification is not None else None
    target_total = (
        _diversification_relevance_contribution(item.diversification)
        if item.diversification is not None
        else None
    )
    contributions = _rank_contributions(
        item.model_scores,
        rank_total=item.rank_score,
        target_total=target_total,
    )
    if not contributions:
        return ""

    total = item.rank_score
    if total is None:
        total = sum(contribution.contribution for contribution in contributions)
    display_total = div_score if div_score is not None else total
    chart_mode = (
        "positive"
        if all(contribution.contribution >= 0 for contribution in contributions)
        else "diverging"
    )
    segments = "".join(
        (
            f'<span class="rank-segment rank-model-{_h(contribution.tone)}" '
            f'style="height: {contribution.height_pct:.2f}%; '
            f'bottom: {contribution.bottom_pct:.2f}%;" '
            f'data-scaled-contribution="{contribution.scaled_contribution:.6f}" '
            f'title="{_h(contribution.name)}: {_fmt_score(contribution.scaled_contribution)}">'
            "</span>"
        )
        for contribution in contributions
    )
    diversity_bar = ""
    if div_score is not None:
        div_height_pct = min(100.0, max(0.0, div_score / SCORE_BREAKDOWN_MAX * 100.0))
        diversity_bar = f"""
        <div class="score-bar-column">
          <div class="rank-bar score-bar score-bar-diversity">
            <span class="rank-baseline"></span>
            <span class="div-score-segment" style="height: {div_height_pct:.2f}%; bottom: 0;"
              data-div-score="{div_score:.6f}" title="div score: {_fmt_score(div_score)}"></span>
          </div>
          <span class="bar-label">div score</span>
        </div>
        """
    legend = "".join(
        (
            f'<div class="rank-legend-item">'
            f'<span class="rank-swatch rank-model-{_h(contribution.tone)}"></span>'
            f'<strong>{_h(contribution.name)}</strong>'
            f'<span>score {_fmt_score(contribution.score)} -> '
            f'{_fmt_score(contribution.scaled_contribution)}</span>'
            "</div>"
        )
        for contribution in contributions
    )
    if div_score is not None:
        legend += (
            '<div class="rank-legend-item">'
            '<span class="rank-swatch rank-model-diversity"></span>'
            "<strong>diversity</strong>"
            f"<span>score {_fmt_score(div_score)}</span>"
            "</div>"
        )
    return f"""
    <section class="rank-visual">
      <div class="rank-visual-head">
        <span>Score breakdown</span>
        <strong>{_fmt_score(display_total)}</strong>
      </div>
      <div class="rank-visual-body">
        <div class="rank-chart rank-chart-{_h(chart_mode)}">
          <div class="score-bars">
            <div class="score-bar-column">
              <div class="rank-bar score-bar score-bar-rank">
                <span class="rank-baseline"></span>
                {segments}
              </div>
              <span class="bar-label">rank rel</span>
            </div>
            {diversity_bar}
          </div>
        </div>
        <div class="rank-legend">{legend}</div>
      </div>
    </section>
    """


def _render_item(item: ItemView) -> str:
    rank_chips = []
    diversity_chips = []
    if item.rank_position is not None:
        rank_chips.append(
            _chip("rank", f"#{item.rank_position} model {_fmt_score(item.rank_score)}", "rank")
        )
    elif item.after_rank_position is not None:
        rank_chips.append(_chip("pre-diversify", f"#{item.after_rank_position}", "rank"))
    for model_score in item.model_scores:
        rank_chips.append(
            _chip(
                model_score.name,
                f"{_fmt_score(model_score.score)} w={model_score.weight:g}",
                "model",
            )
        )
    if item.diversification is not None:
        diversity_chips.append(
            _chip(
                "div rel",
                _fmt_short_score(_diversification_relevance_contribution(item.diversification)),
                "div",
            )
        )
        diversity_chips.append(
            _chip("div score", _fmt_short_score(item.diversification.score), "div")
        )
        diversity_chips.append(
            _chip(
                "author penalty",
                _fmt_short_score(item.diversification.author_penalty),
                "penalty",
            )
        )
        diversity_chips.append(
            _chip(
                "content penalty",
                _fmt_short_score(item.diversification.content_penalty),
                "penalty",
            )
        )

    media = "".join(f'<span class="media-badge">{_h(label)}</span>' for label in item.media_labels)
    content = f'<p class="post-text">{_h(item.content)}</p>' if item.content else ""
    if item.generators:
        primary_tone = _generator_tone(item.generators[0].name)
        generators = "".join(
            _generator_badge(generator.name, _fmt_score(generator.score))
            for generator in item.generators
        )
    else:
        primary_tone = "slate"
        generators = _generator_badge("infill/unknown")
    uri = f'<div class="uri">{_h(item.at_uri)}</div>'
    post_link = ""
    if item.post_url:
        uri = (
            f'<a class="uri uri-link" href="{_h(item.post_url)}" '
            f'target="_blank" rel="noopener noreferrer">{_h(item.at_uri)}</a>'
        )
        post_link = (
            f'<a class="post-link" href="{_h(item.post_url)}" '
            f'target="_blank" rel="noopener noreferrer">Open in Bluesky</a>'
        )
    rank_group = _render_chip_group("Ranking", rank_chips, "rank")
    diversity_group = _render_chip_group("Diversity", diversity_chips, "diversity")
    rank_visual = _render_rank_visual(item)
    layout_class = "post-card-layout"
    if not rank_visual:
        layout_class += " post-card-layout-simple"
    return f"""
    <article class="feed-card">
      <div class="position position-{_h(primary_tone)}">#{item.final_position}</div>
      <div class="item-main">
        <div class="item-header">
          <div>
            <div class="author">{_h(item.author)}</div>
            {uri}
          </div>
          <div class="card-actions">
            {post_link}
            <div class="media-row">{media}</div>
          </div>
        </div>
        <div class="generator-row generator-row-card">{generators}</div>
        <div class="{layout_class}">
          <div class="post-detail">
            {content}
            <div class="debug-groups">{rank_group}{diversity_group}</div>
          </div>
          {rank_visual}
        </div>
      </div>
    </article>
    """


def _render_debug_doc(doc: FeedDebugDocument, *, debug_enabled: bool) -> str:
    request = doc.generate_request
    debug_note = ""
    if not debug_enabled:
        debug_note = (
            '<div class="notice notice-warn">'
            "Feed debugging is currently off for this user. "
            "Showing the newest saved record that still exists."
            "</div>"
        )

    metadata_bits = [
        doc.username or doc.user_did,
        f"ranker {_model_specs_str(doc)}",
        f"diversify {'on' if doc.diversify else 'off'}",
        f"generated {_relative_time(doc.generated_at)}",
        f"{len(doc.final_order)} items",
        f"{request.num_candidates} candidates",
        f"request {doc.request_id}",
    ]
    if request.video_only:
        metadata_bits.append("video only")
    if request.exclude_uris:
        metadata_bits.append(f"{len(request.exclude_uris)} excluded")

    cards = "".join(_render_item(item) for item in _build_item_views(doc))
    if not cards:
        cards = '<div class="notice">This record has no final feed items.</div>'

    return f"""
    {debug_note}
    <section class="metadata-line">{_h(" | ".join(metadata_bits))}</section>
    {_render_generator_legend(doc)}
    <section class="feed-list">
      <h2>Final Feed <span>your-feed only</span></h2>
      {cards}
    </section>
    """


def _render_notice(title: str, body: str, *, kind: str = "info") -> str:
    return f"""
    <section class="empty-state empty-{_h(kind)}">
      <h2>{_h(title)}</h2>
      <p>{_h(body)}</p>
    </section>
    """


def _render_lookup_result(result: FeedDebugLookup) -> str:
    if result.status == "not_found":
        return _render_notice(
            "User not found",
            f"No user document was found for {result.query_user}.",
            kind="warn",
        )
    if result.status == "no_records":
        body = (
            "This is expected if feed debugging has not been enabled, "
            f"or if no recent {TARGET_FEED_NAME} feed has been generated with debugging enabled."
        )
        if result.debug_enabled:
            body = f"Feed debugging is enabled, but no saved {TARGET_FEED_NAME} records were found."
        return _render_notice(
            f"No {TARGET_FEED_NAME} feed-debug information found for this user.",
            body,
            kind="info",
        )
    if result.doc is None:
        return _render_notice("No record loaded", "The newest feed-debug record could not be read.")
    return _render_debug_doc(result.doc, debug_enabled=result.debug_enabled)


def _checked(value: str, selected: str) -> str:
    return " checked" if value == selected else ""


def _render_page(user: str, environment: str, main_html: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Feed Debug Viewer</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f5f7fb;
      --panel: #ffffff;
      --ink: #182033;
      --muted: #697387;
      --line: #d9deea;
      --green: #16724a;
      --blue: #245fca;
      --cyan: #0f7285;
      --amber: #996b11;
      --violet: #6d3bbf;
      --red: #a23a3a;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font: 14px/1.45 Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont,
        "Segoe UI", sans-serif;
    }}
    .shell {{
      width: min(1120px, calc(100vw - 32px));
      margin: 28px auto 48px;
    }}
    header.top {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 18px;
    }}
    h1 {{
      margin: 0;
      font-size: 28px;
      letter-spacing: 0;
    }}
    .toolbar {{
      display: grid;
      grid-template-columns: minmax(220px, 1fr) auto auto;
      gap: 10px;
      align-items: end;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px;
      margin-bottom: 18px;
      box-shadow: 0 6px 20px rgb(24 32 51 / 6%);
    }}
    .field label {{
      display: block;
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      margin-bottom: 6px;
      text-transform: uppercase;
    }}
    input[type="text"] {{
      width: 100%;
      height: 40px;
      border: 1px solid var(--line);
      border-radius: 7px;
      padding: 0 12px;
      color: var(--ink);
      font: inherit;
      background: #fff;
    }}
    .segmented {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 3px;
      background: #edf1f7;
      min-width: 156px;
    }}
    .segmented label {{
      cursor: pointer;
      font-weight: 700;
      text-align: center;
    }}
    .segmented span {{
      display: block;
      border-radius: 6px;
      color: var(--muted);
      padding: 8px 12px;
    }}
    .segmented input:checked + span {{
      background: #fff;
      color: var(--blue);
      box-shadow: 0 1px 4px rgb(24 32 51 / 12%);
    }}
    .segmented input:focus-visible + span {{
      outline: 2px solid var(--blue);
      outline-offset: 2px;
    }}
    .segmented input {{ position: absolute; opacity: 0; pointer-events: none; }}
    button {{
      height: 40px;
      border: 0;
      border-radius: 7px;
      padding: 0 16px;
      background: var(--green);
      color: #fff;
      cursor: pointer;
      font: inherit;
      font-weight: 800;
    }}
    .metadata-line {{
      color: var(--muted);
      font-size: 12px;
      margin: 4px 0 12px;
      overflow-wrap: anywhere;
    }}
    .generator-panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px 14px;
      margin-bottom: 18px;
      box-shadow: 0 5px 18px rgb(24 32 51 / 5%);
    }}
    .generator-panel h2 {{
      margin: 0 0 9px;
      font-size: 15px;
    }}
    .generator-row {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
    }}
    .generator-row-card {{ margin: 9px 0 10px; }}
    .generator-badge {{
      display: inline-flex;
      align-items: center;
      gap: 7px;
      border: 1px solid var(--gen-border);
      border-radius: 999px;
      background: var(--gen-bg);
      color: var(--gen-fg);
      font-size: 12px;
      font-weight: 850;
      padding: 5px 9px;
    }}
    .generator-badge span {{ color: var(--gen-subtle); font-weight: 800; }}
    .gen-green {{
      --gen-bg: #e7f4ee;
      --gen-border: #a9d8c0;
      --gen-fg: #0f6b43;
      --gen-subtle: #32765a;
    }}
    .gen-blue {{
      --gen-bg: #edf4ff;
      --gen-border: #b8cef7;
      --gen-fg: #245fca;
      --gen-subtle: #426eaf;
    }}
    .gen-amber {{
      --gen-bg: #fff5db;
      --gen-border: #f2d48c;
      --gen-fg: #996b11;
      --gen-subtle: #946f2b;
    }}
    .gen-violet {{
      --gen-bg: #f4efff;
      --gen-border: #d2bef6;
      --gen-fg: #6d3bbf;
      --gen-subtle: #7655ad;
    }}
    .gen-cyan {{
      --gen-bg: #e9f8fb;
      --gen-border: #afd9e1;
      --gen-fg: #0f7285;
      --gen-subtle: #347987;
    }}
    .gen-slate {{
      --gen-bg: #f1f4f8;
      --gen-border: #cbd3df;
      --gen-fg: #4b5668;
      --gen-subtle: #637084;
    }}
    .notice,
    .empty-state {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-left: 5px solid var(--blue);
      border-radius: 8px;
      padding: 16px;
      margin-bottom: 16px;
    }}
    .notice-warn,
    .empty-warn {{ border-left-color: var(--amber); }}
    .empty-state h2,
    .feed-list h2 {{
      margin: 0 0 8px;
      font-size: 18px;
    }}
    .empty-state p {{ margin: 0; color: var(--muted); }}
    .feed-list h2 {{ margin-top: 18px; }}
    .feed-card {{
      display: grid;
      grid-template-columns: 64px minmax(0, 1fr);
      gap: 0;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      margin: 10px 0;
      overflow: hidden;
      box-shadow: 0 5px 18px rgb(24 32 51 / 5%);
    }}
    .position {{
      display: flex;
      align-items: center;
      justify-content: center;
      background: var(--pos-bg, #e7f4ee);
      color: var(--pos-fg, var(--green));
      font-size: 18px;
      font-weight: 900;
      border-right: 1px solid var(--pos-border, #cce4d8);
    }}
    .position-green {{ --pos-bg: #e7f4ee; --pos-fg: #0f6b43; --pos-border: #a9d8c0; }}
    .position-blue {{ --pos-bg: #edf4ff; --pos-fg: #245fca; --pos-border: #b8cef7; }}
    .position-amber {{ --pos-bg: #fff5db; --pos-fg: #996b11; --pos-border: #f2d48c; }}
    .position-violet {{ --pos-bg: #f4efff; --pos-fg: #6d3bbf; --pos-border: #d2bef6; }}
    .position-cyan {{ --pos-bg: #e9f8fb; --pos-fg: #0f7285; --pos-border: #afd9e1; }}
    .position-slate {{ --pos-bg: #f1f4f8; --pos-fg: #4b5668; --pos-border: #cbd3df; }}
    .item-main {{ padding: 13px 14px 14px; min-width: 0; }}
    .item-header {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: start;
      margin-bottom: 8px;
    }}
    .author {{ font-weight: 850; }}
    .uri {{
      display: block;
      color: var(--muted);
      font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace;
      font-size: 12px;
      overflow-wrap: anywhere;
    }}
    .uri-link:hover {{ color: var(--blue); }}
    .card-actions {{
      display: flex;
      flex-direction: column;
      gap: 8px;
      align-items: flex-end;
    }}
    .post-link {{
      color: var(--blue);
      border: 1px solid #bfd0f4;
      border-radius: 999px;
      background: #f0f5ff;
      font-size: 12px;
      font-weight: 850;
      line-height: 1;
      padding: 6px 9px;
      text-decoration: none;
      white-space: nowrap;
    }}
    .post-link:hover {{
      background: #e2ebff;
      border-color: #9cb7ed;
    }}
    .media-row {{
      display: flex;
      gap: 6px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }}
    .media-badge {{
      background: #fff5db;
      color: var(--amber);
      border: 1px solid #f2d48c;
      border-radius: 999px;
      padding: 3px 8px;
      font-size: 12px;
      font-weight: 800;
      white-space: nowrap;
    }}
    .post-text {{
      margin: 0 0 12px;
      color: #293349;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }}
    .post-card-layout {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) 390px;
      gap: 14px;
      align-items: start;
    }}
    .post-card-layout-simple {{ grid-template-columns: minmax(0, 1fr); }}
    .post-detail {{ min-width: 0; }}
    .rank-visual {{
      display: grid;
      gap: 10px;
      background: #fbfcff;
      border: 1px solid #ccd8ef;
      border-radius: 8px;
      margin: 0;
      padding: 12px;
    }}
    .rank-visual-head {{
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 12px;
    }}
    .rank-visual-head span {{
      color: var(--muted);
      font-size: 12px;
      font-weight: 900;
      text-transform: uppercase;
    }}
    .rank-visual-head strong {{
      color: var(--ink);
      font-size: 24px;
      line-height: 1;
    }}
    .rank-visual-body {{
      display: grid;
      grid-template-columns: 150px minmax(0, 1fr);
      gap: 14px;
      align-items: end;
    }}
    .rank-chart {{
      display: flex;
      justify-content: center;
      align-items: end;
      min-height: 132px;
      border-left: 1px solid #d9e1f1;
      border-bottom: 1px solid #d9e1f1;
      padding: 0 10px;
    }}
    .score-bars {{
      display: flex;
      align-items: flex-end;
      gap: 18px;
    }}
    .score-bar-column {{
      display: grid;
      justify-items: center;
      gap: 7px;
    }}
    .rank-bar {{
      position: relative;
      width: 44px;
      height: 120px;
      background: #eef2f9;
      border-radius: 7px 7px 3px 3px;
      overflow: hidden;
    }}
    .rank-baseline {{
      position: absolute;
      left: 0;
      right: 0;
      bottom: 0;
      height: 1px;
      background: #718096;
      z-index: 2;
    }}
    .rank-chart-diverging .rank-baseline {{ bottom: 50%; }}
    .rank-segment {{
      position: absolute;
      left: 0;
      right: 0;
      min-height: 2px;
      box-shadow: inset 0 1px 0 rgb(255 255 255 / 30%);
    }}
    .rank-model-green {{ background: #20996a; }}
    .rank-model-violet {{ background: #7c4bd1; }}
    .rank-model-blue {{ background: #2f6dd2; }}
    .rank-model-slate {{ background: #637084; }}
    .rank-model-diversity,
    .div-score-segment {{ background: #16724a; }}
    .div-score-segment {{
      position: absolute;
      left: 0;
      right: 0;
      min-height: 2px;
      box-shadow: inset 0 1px 0 rgb(255 255 255 / 30%);
    }}
    .bar-label {{
      color: var(--muted);
      font-size: 11px;
      font-weight: 850;
      text-transform: uppercase;
    }}
    .rank-legend {{
      display: grid;
      gap: 7px;
      align-content: end;
    }}
    .rank-legend-item {{
      display: grid;
      grid-template-columns: auto max-content 1fr;
      gap: 7px;
      align-items: center;
      color: var(--muted);
      font-size: 12px;
    }}
    .rank-legend-item strong {{ color: var(--ink); }}
    .rank-swatch {{
      display: inline-block;
      width: 11px;
      height: 11px;
      border-radius: 3px;
    }}
    .debug-groups {{
      display: grid;
      gap: 8px;
      grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
      margin-top: 8px;
    }}
    .debug-group {{
      border: 1px solid var(--group-border);
      border-radius: 8px;
      background: var(--group-bg);
      padding: 7px;
    }}
    .debug-group h3 {{
      color: var(--group-label);
      font-size: 10px;
      font-weight: 900;
      letter-spacing: 0;
      margin: 0 0 6px;
      text-transform: uppercase;
    }}
    .debug-group-rank {{
      --group-bg: #fff9e8;
      --group-border: #ead391;
      --group-label: #8a6417;
    }}
    .debug-group-diversity {{
      --group-bg: #eefaf5;
      --group-border: #b9decf;
      --group-label: #16724a;
    }}
    .chip-row {{ display: flex; gap: 7px; flex-wrap: wrap; }}
    .chip {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: #f8fafc;
      padding: 3px 7px;
      font-size: 11px;
    }}
    .chip span {{ color: var(--muted); font-weight: 750; }}
    .chip strong {{ color: var(--ink); font-weight: 850; }}
    .chip-rank {{ border-color: #efd488; background: #fff8df; }}
    .chip-model {{ border-color: #d7c3f8; background: #f5efff; }}
    .chip-div {{ border-color: #b9decf; background: #eefaf5; }}
    .chip-penalty {{ border-color: #e8c0c0; background: #fff1f1; }}
    @media (max-width: 760px) {{
      .toolbar {{ grid-template-columns: 1fr; }}
      .feed-card {{ grid-template-columns: 52px minmax(0, 1fr); }}
      .post-card-layout {{ grid-template-columns: 1fr; }}
      .rank-visual {{ max-width: 390px; }}
      header.top {{ display: block; }}
    }}
    @media (max-width: 520px) {{
      .shell {{ width: min(100vw - 20px, 1120px); margin-top: 16px; }}
      .item-header {{ display: block; }}
      .rank-visual {{ max-width: none; }}
      .rank-visual-body {{ grid-template-columns: 120px minmax(0, 1fr); }}
      .rank-chart {{ min-height: 112px; padding: 0 6px; }}
      .rank-bar {{ width: 34px; height: 100px; }}
      .card-actions {{ align-items: flex-start; margin-top: 8px; }}
      .media-row {{ justify-content: flex-start; margin-top: 8px; }}
    }}
  </style>
</head>
<body>
  <main class="shell">
    <header class="top">
      <h1>Feed Debug Viewer</h1>
    </header>
    <form class="toolbar" method="get" action="/">
      <div class="field">
        <label for="user">Bluesky user</label>
        <input id="user" name="user" type="text" value="{_h(user)}"
          placeholder="alice.bsky.social or did:plc:..." autocomplete="off">
      </div>
      <div class="segmented" role="radiogroup" aria-label="Environment">
        <label>
          <input type="radio" name="environment" value="stage"{_checked("stage", environment)}>
          <span>stage</span>
        </label>
        <label>
          <input type="radio" name="environment" value="prod"{_checked("prod", environment)}>
          <span>prod</span>
        </label>
      </div>
      <button type="submit">Load latest</button>
    </form>
    {main_html}
  </main>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def index(user: str = "", environment: str = DEFAULT_ENVIRONMENT) -> HTMLResponse:
    user = user.strip()
    if environment not in _ENVIRONMENTS:
        main_html = _render_notice(
            "Unsupported environment",
            "Choose either stage or prod.",
            kind="warn",
        )
        return HTMLResponse(_render_page(user, DEFAULT_ENVIRONMENT, main_html))

    main_html = ""
    if user:
        try:
            result = await _load_latest_feed_debug(user, environment)
            main_html = _render_lookup_result(result)
        except Exception:
            logger.exception("Failed to load feed debug record for user %s", user)
            main_html = _render_notice(
                "Could not load feed-debug information",
                "Check your GCP credentials and Firestore access, then try again.",
                kind="warn",
            )
    return HTMLResponse(
        _render_page(user, environment, main_html),
        headers={"Cache-Control": "no-store"},
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Local Green Earth feed-debug web viewer")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind (default: 8000)")
    args = parser.parse_args()

    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
