import importlib.util
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
    return SimpleNamespace(name=name)


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
):
    return SimpleNamespace(
        generate_request=SimpleNamespace(
            generators=[_generator(name) for name in generators],
            infill=infill,
        ),
        generator_outputs=outputs,
        final_order=final_order,
        model_scores=model_scores or [],
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
