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


def _doc(*, generators: list[str], infill: str | None, outputs, final_order: list[str]):
    return SimpleNamespace(
        generate_request=SimpleNamespace(
            generators=[_generator(name) for name in generators],
            infill=infill,
        ),
        generator_outputs=outputs,
        final_order=final_order,
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
    )

    assert feed_debug._generator_output_stats_str(doc) == (
        "two_tower=3 in_final=2 avg_rank=2.0, "
        "popularity=1 in_final=1 avg_rank=2.0, "
        "infill popularity=2 in_final=1 avg_rank=4.0"
    )


def test_generator_output_stats_includes_missing_primary_as_zero():
    missing = "\u2014"
    doc = _doc(
        generators=["two_tower", "followed_users"],
        infill="popularity",
        outputs=[_result("two_tower", ["at://p/1"])],
        final_order=["at://p/1"],
    )

    assert feed_debug._generator_output_stats_str(doc) == (
        "two_tower=1 in_final=1 avg_rank=1.0, "
        f"followed_users=0 in_final=0 avg_rank={missing}, "
        f"infill popularity=0 in_final=0 avg_rank={missing}"
    )


def test_generator_output_stats_counts_duplicate_candidates_in_average():
    doc = _doc(
        generators=["two_tower"],
        infill=None,
        outputs=[_result("two_tower", ["at://p/1", "at://p/1", "at://p/2"])],
        final_order=["at://p/2", "at://p/1"],
    )

    assert feed_debug._generator_output_stats_str(doc) == "two_tower=3 in_final=3 avg_rank=1.7"
