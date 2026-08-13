import pytest

from app.lib import inflight


@pytest.fixture(autouse=True)
def _reset():
    inflight.reset_for_test()
    yield
    inflight.reset_for_test()


def test_starts_idle():
    assert inflight.current() == 0
    assert not inflight.busy_throughout(inflight.snapshot())


def test_begin_marks_process_busy():
    inflight.begin()
    assert inflight.current() == 1
    assert inflight.busy_throughout(inflight.snapshot())


def test_busy_throughout_false_when_snapshot_taken_while_idle():
    snap = inflight.snapshot()
    inflight.begin()
    # A request that arrived *after* the snapshot does not make the preceding
    # window busy — part of it was still idle (and therefore CPU-throttled).
    assert not inflight.busy_throughout(snap)


def test_busy_throughout_false_when_process_went_idle_mid_window():
    inflight.begin()
    snap = inflight.snapshot()
    inflight.end()
    inflight.begin()
    # Busy at both ends of the window, but idle in between: the sample is
    # contaminated by throttled time and must not be recorded.
    assert inflight.current() == 1
    assert not inflight.busy_throughout(snap)


def test_busy_throughout_true_across_overlapping_requests():
    inflight.begin()
    snap = inflight.snapshot()
    inflight.begin()
    inflight.end()
    # Never dropped to zero, so the whole window had CPU allocated.
    assert inflight.current() == 1
    assert inflight.busy_throughout(snap)


def test_end_without_begin_does_not_go_negative():
    inflight.end()
    assert inflight.current() == 0


def test_counter_returns_to_zero_and_bumps_idle_epoch():
    _, epoch_before = inflight.snapshot()
    inflight.begin()
    inflight.end()
    _, epoch_after = inflight.snapshot()
    assert inflight.current() == 0
    assert epoch_after == epoch_before + 1
