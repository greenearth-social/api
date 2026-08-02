"""Tests for the Cloud Monitoring query builder in analyze.py."""

from google.cloud import monitoring_v3

from load_test.analyze import ENV_RESOURCE_LABEL, build_percentile_request


def _interval():
    return monitoring_v3.TimeInterval(
        {"start_time": {"seconds": 1_000}, "end_time": {"seconds": 2_000}}
    )


def test_filters_by_metric_type_and_environment():
    req = build_percentile_request(
        monitoring_v3, "greenearth-471522", "custom.googleapis.com/x", "prod",
        _interval(), 95, alignment_seconds=1000,
    )
    assert 'metric.type = "custom.googleapis.com/x"' in req.filter
    assert f'resource.labels.{ENV_RESOURCE_LABEL} = "prod"' in req.filter
    assert req.name == "projects/greenearth-471522"


def test_aggregates_percentile_grouped_by_traffic():
    req = build_percentile_request(
        monitoring_v3, "p", "m", "stage", _interval(), 99, alignment_seconds=600,
    )
    agg = req.aggregation
    # ALIGN_DELTA is the only aligner valid on a CUMULATIVE DISTRIBUTION; the
    # percentile is taken by the cross-series reducer, not the aligner.
    assert agg.per_series_aligner == monitoring_v3.Aggregation.Aligner.ALIGN_DELTA
    assert agg.cross_series_reducer == monitoring_v3.Aggregation.Reducer.REDUCE_PERCENTILE_99
    assert list(agg.group_by_fields) == ["metric.label.traffic"]
    assert agg.alignment_period.seconds == 600


def test_each_percentile_maps_to_its_reducer():
    reducers = {
        p: build_percentile_request(
            monitoring_v3, "p", "m", "stage", _interval(), p, alignment_seconds=60
        ).aggregation.cross_series_reducer
        for p in (50, 95, 99)
    }
    assert reducers[50] == monitoring_v3.Aggregation.Reducer.REDUCE_PERCENTILE_50
    assert reducers[95] == monitoring_v3.Aggregation.Reducer.REDUCE_PERCENTILE_95
    assert reducers[99] == monitoring_v3.Aggregation.Reducer.REDUCE_PERCENTILE_99
    # Every percentile is aligned the same way — with ALIGN_DELTA.
    aligners = {
        p: build_percentile_request(
            monitoring_v3, "p", "m", "stage", _interval(), p, alignment_seconds=60
        ).aggregation.per_series_aligner
        for p in (50, 95, 99)
    }
    assert set(aligners.values()) == {monitoring_v3.Aggregation.Aligner.ALIGN_DELTA}
