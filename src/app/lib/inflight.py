"""Process-wide in-flight HTTP request tracking.

Cloud Run runs this service with ``cpuIdle: true``, which throttles the
container's CPU to near zero whenever no request is being handled. Any
background task that keeps running while the instance is idle — notably the
event-loop lag monitor — is descheduled for as long as the throttle lasts,
and cannot tell that apart from the event loop being blocked by real work.

This module provides the "was CPU allocated for the whole of this window?"
predicate that lets such a task discard contaminated samples. A window only
counts as busy if a request was in flight at both ends *and* the count never
touched zero in between; ``idle_epoch`` increments on every drop to zero, so
a window that dipped idle mid-flight is detectable even though the count is
non-zero again by the time it is checked.

Not thread-safe by design: a single uvicorn worker runs one event loop, and
every mutation here happens on that loop thread.
"""

from __future__ import annotations

_in_flight = 0
_idle_epoch = 0

Snapshot = tuple[int, int]


def begin() -> int:
    """Record a request entering the service. Returns the new in-flight count."""
    global _in_flight
    _in_flight += 1
    return _in_flight


def end() -> int:
    """Record a request leaving the service. Returns the new in-flight count."""
    global _in_flight, _idle_epoch
    if _in_flight <= 0:
        return 0
    _in_flight -= 1
    if _in_flight == 0:
        _idle_epoch += 1
    return _in_flight


def current() -> int:
    return _in_flight


def snapshot() -> Snapshot:
    """Opaque marker for the start of a measurement window."""
    return (_in_flight, _idle_epoch)


def busy_throughout(snap: Snapshot) -> bool:
    """True if a request was in flight continuously since *snap* was taken."""
    in_flight_at_start, epoch_at_start = snap
    return in_flight_at_start > 0 and _in_flight > 0 and _idle_epoch == epoch_at_start


def reset_for_test() -> None:
    global _in_flight, _idle_epoch
    _in_flight = 0
    _idle_epoch = 0
