import importlib.util
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace


MODULE_PATH = Path(__file__).with_name("feed_debug.py")
spec = importlib.util.spec_from_file_location("feed_debug_cli", MODULE_PATH)
assert spec and spec.loader
feed_debug = importlib.util.module_from_spec(spec)
spec.loader.exec_module(feed_debug)


def _candidate(uri: str):
    return SimpleNamespace(at_uri=uri)


def _generator(name: str):
    return SimpleNamespace(name=name, weight=1.0)


def _result(name: str, uris: list[str]):
    return SimpleNamespace(generator_name=name, candidates=[_candidate(uri) for uri in uris])


def _model_score(name: str, scores: dict[str, float]):
    return SimpleNamespace(
        model_name=name,
        scores=[
            SimpleNamespace(at_uri=uri, score=score)
            for uri, score in scores.items()
        ],
    )


def _diversification(penalties: dict[str, float]):
    return [
        SimpleNamespace(at_uri=uri, author_penalty=penalty)
        for uri, penalty in penalties.items()
    ]


def _politics_adjustment(
    *, topic_score=0.8, score_multiplier=1.4, score_before=0.5, score_after=0.7
):
    return SimpleNamespace(
        at_uri="at://p/1",
        topic_score=topic_score,
        score_multiplier=score_multiplier,
        score_before=score_before,
        score_after=score_after,
    )


def _doc(
    *,
    generators: list[str],
    infill: str | None,
    outputs,
    final_order: list[str],
    model_scores=None,
    diversification=None,
    ranking=None,
    cutoff_uris=None,
    n_retrieved=0,
    politics_setting=None,
    politics_adjustments=None,
):
    return SimpleNamespace(
        generate_request=SimpleNamespace(
            generators=[_generator(name) for name in generators],
            infill=infill,
            num_candidates=100,
            video_only=False,
            exclude_uris=[],
        ),
        request_id="request-1",
        username="user.test",
        user_did="did:plc:user",
        feed_name="your-feed",
        generated_at=datetime.now(UTC),
        regenerated=False,
        ranker_model="heavy_ranker" if ranking else None,
        diversify=bool(diversification),
        user_features=[],
        generator_outputs=outputs,
        final_order=final_order,
        model_scores=model_scores or [],
        politics_setting=politics_setting,
        politics_adjustments=politics_adjustments or [],
        diversification=diversification or [],
        ranking=ranking,
        cutoff_uris=cutoff_uris or {},
        n_retrieved=n_retrieved,
    )


def test_generator_output_stats_labels_primary_and_infill_with_average_rank():
    doc = _doc(
        generators=["two_tower", "popularity"],
        infill="popularity",
        outputs=[
            _result("two_tower", ["at://p/1", "at://p/3", "at://p/missing"]),
            _result("popularity", ["at://p/2"]),
            _result("popularity", ["at://p/4", "at://p/not-final"]),
        ],
        final_order=["at://p/3", "at://p/2", "at://p/1", "at://p/4"],
        model_scores=[
            _model_score(
                "heavy_ranker",
                {
                    "at://p/1": 0.2,
                    "at://p/2": 0.8,
                    "at://p/3": 0.6,
                    "at://p/4": -0.2,
                    "at://p/not-final": 0.4,
                },
            ),
            _model_score(
                "perspective",
                {
                    "at://p/1": -1.0,
                    "at://p/2": -1.0,
                    "at://p/3": -1.0,
                    "at://p/4": -1.0,
                    "at://p/not-final": -1.0,
                },
            ),
        ],
        diversification=_diversification(
            {
                "at://p/1": 0.1,
                "at://p/2": 0.2,
                "at://p/3": 0.4,
                "at://p/4": 0.0,
            }
        ),
    )

    assert feed_debug._candidate_stats_rows(doc) == [
        ("two_tower", "3", "2.0", "0.40", "0.250"),
        ("popularity", "1", "2.0", "0.80", "0.200"),
        ("infill popularity", "2", "4.0", "0.10", "0.000"),
    ]
    assert [
        column.header
        for column in feed_debug._candidate_stats_table(doc).columns
    ] == [
        "generator",
        "count",
        "placement",
        "heavy_ranker",
        "author_penalty",
    ]


def test_politics_adjustment_line_shows_factor_applied_to_score():
    line = feed_debug._politics_adjustment_line(_politics_adjustment())

    assert line.plain == "politics     topic 0.800   score 0.500 × 1.400 → 0.700"


def test_politics_adjustment_line_distinguishes_missing_topic_score():
    line = feed_debug._politics_adjustment_line(
        _politics_adjustment(
            topic_score=None,
            score_multiplier=1.0,
            score_before=0.5,
            score_after=0.5,
        )
    )

    assert line.plain == "politics     topic —   score 0.500 × 1.000 → 0.500"


def test_item_panel_includes_politics_adjustment():
    adjustment = _politics_adjustment()

    panel = feed_debug._item_panel(
        "at://p/1",
        0,
        1,
        {"at://p/1": [("two_tower", 0.9)]},
        {"at://p/1": (1, 0.7)},
        {"at://p/1": 0},
        {},
        {"at://p/1": adjustment},
        {},
        {},
    )

    lines = [renderable.plain for renderable in panel.renderable.renderables]
    assert "politics     topic 0.800   score 0.500 × 1.400 → 0.700" in lines


def test_header_shows_politics_setting_when_captured():
    doc = _doc(
        generators=["two_tower"],
        infill=None,
        outputs=[],
        final_order=[],
        politics_setting=1.5,
    )

    panel = feed_debug._header_panel(doc)

    assert "politics    setting=1.5" in panel.renderable.plain


def test_generator_output_stats_includes_missing_primary_as_zero():
    missing = "\u2014"
    doc = _doc(
        generators=["two_tower", "followed_users"],
        infill="popularity",
        outputs=[_result("two_tower", ["at://p/1"])],
        final_order=["at://p/1"],
    )

    assert feed_debug._candidate_stats_rows(doc) == [
        ("two_tower", "1", "1.0", missing, missing),
        ("followed_users", "0", missing, missing, missing),
        ("infill popularity", "0", missing, missing, missing),
    ]


def test_discarded_table_labels_cutoff_reasons():
    doc = _doc(
        generators=["two_tower"],
        infill=None,
        outputs=[_result("two_tower", ["at://p/1", "at://p/cut", "at://p/capped", "at://p/unranked"])],
        final_order=["at://p/1"],
        ranking=SimpleNamespace(
            rankings=[
                SimpleNamespace(at_uri="at://p/1"),
                SimpleNamespace(at_uri="at://p/cut"),
                SimpleNamespace(at_uri="at://p/capped"),
            ]
        ),
        cutoff_uris={"rank_score": ["at://p/cut"], "share": ["at://p/capped"]},
    )

    table = feed_debug._discarded_table(
        doc, ["at://p/cut", "at://p/capped", "at://p/unranked"], {}
    )

    reasons = list(table.columns[1]._cells)
    assert reasons == ["rank floor", "share cap", "not ranked"]


def test_discarded_table_dash_reason_when_no_ranking_or_cutoffs():
    doc = _doc(
        generators=["two_tower"],
        infill=None,
        outputs=[_result("two_tower", ["at://p/1", "at://p/2"])],
        final_order=["at://p/1"],
    )

    table = feed_debug._discarded_table(doc, ["at://p/2"], {})

    assert list(table.columns[1]._cells) == ["—"]


def test_generator_output_stats_counts_duplicate_candidates_in_average():
    doc = _doc(
        generators=["two_tower"],
        infill=None,
        outputs=[_result("two_tower", ["at://p/1", "at://p/1", "at://p/2"])],
        final_order=["at://p/2", "at://p/1"],
        model_scores=[
            _model_score("heavy_ranker", {"at://p/1": 0.4, "at://p/2": 1.0}),
        ],
        diversification=_diversification({"at://p/1": 0.5, "at://p/2": 0.2}),
    )

    assert feed_debug._candidate_stats_rows(doc) == [
        ("two_tower", "3", "1.7", "0.60", "0.400"),
    ]


def test_candidate_stats_uses_single_empty_history_heavy_ranker_column():
    doc = _doc(
        generators=["two_tower_empty_history"],
        infill=None,
        outputs=[
            _result(
                "two_tower_empty_history",
                ["at://p/1", "at://p/2"],
            )
        ],
        final_order=["at://p/2", "at://p/1"],
        model_scores=[
            _model_score(
                "heavy_ranker_empty_history",
                {"at://p/1": 0.2, "at://p/2": 0.8},
            ),
            _model_score(
                "perspective",
                {"at://p/1": -1.0, "at://p/2": -1.0},
            ),
        ],
    )

    assert feed_debug._candidate_stats_rows(doc) == [
        ("two_tower_empty_history", "2", "1.5", "0.50", "—"),
    ]

    table = feed_debug._candidate_stats_table(doc)
    assert [column.header for column in table.columns] == [
        "generator",
        "count",
        "placement",
        "heavy_ranker_empty_history",
        "author_penalty",
    ]
