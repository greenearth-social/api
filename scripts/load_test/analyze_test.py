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
    assert agg.per_series_aligner == monitoring_v3.Aggregation.Aligner.ALIGN_PERCENTILE_99
    assert agg.cross_series_reducer == monitoring_v3.Aggregation.Reducer.REDUCE_MEAN
    assert list(agg.group_by_fields) == ["metric.label.traffic"]
    assert agg.alignment_period.seconds == 600


def test_each_percentile_maps_to_its_aligner():
    aligners = {
        p: build_percentile_request(
            monitoring_v3, "p", "m", "stage", _interval(), p, alignment_seconds=60
        ).aggregation.per_series_aligner
        for p in (50, 95, 99)
    }
    assert aligners[50] == monitoring_v3.Aggregation.Aligner.ALIGN_PERCENTILE_50
    assert aligners[95] == monitoring_v3.Aggregation.Aligner.ALIGN_PERCENTILE_95
    assert aligners[99] == monitoring_v3.Aggregation.Aligner.ALIGN_PERCENTILE_99
