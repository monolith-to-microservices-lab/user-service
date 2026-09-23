"""Unit tests for latency calculation edge cases and the counters not already
covered by tests/integration/test_cdc_metrics.py (which exercises them via a
full process_message call). These call the metrics functions directly.
"""
from __future__ import annotations

import time

from app.cdc import metrics


def _histogram_count(histogram, **labels):
    for metric in histogram.collect():
        total_sum = total_count = None
        for sample in metric.samples:
            if not all(sample.labels.get(k) == v for k, v in labels.items()):
                continue
            if sample.name.endswith("_sum"):
                total_sum = sample.value
            elif sample.name.endswith("_count"):
                total_count = sample.value
        if total_sum is not None and total_count is not None:
            return total_sum, total_count
    return 0.0, 0.0


class TestLatencyCalculation:
    def test_latency_equals_now_minus_source_ts(self):
        sum_before, count_before = _histogram_count(metrics.END_TO_END_LATENCY, service=metrics.SERVICE_NAME)
        source_ts_ms = int((time.time() - 3) * 1000)  # 3 seconds ago

        metrics.record_end_to_end_latency(source_ts_ms)

        sum_after, count_after = _histogram_count(metrics.END_TO_END_LATENCY, service=metrics.SERVICE_NAME)
        assert count_after == count_before + 1
        observed = sum_after - sum_before
        assert 2.5 <= observed <= 6  # generous bound for slow CI

    def test_missing_source_ts_ms_is_skipped_without_error(self):
        sum_before, count_before = _histogram_count(metrics.END_TO_END_LATENCY, service=metrics.SERVICE_NAME)

        metrics.record_end_to_end_latency(None)  # must not raise

        sum_after, count_after = _histogram_count(metrics.END_TO_END_LATENCY, service=metrics.SERVICE_NAME)
        assert (sum_after, count_after) == (sum_before, count_before)

    def test_future_source_timestamp_is_not_recorded(self):
        """A source timestamp in the future (clock skew between the legacy
        Postgres host and this consumer) would produce a negative latency,
        which is meaningless - record_end_to_end_latency guards against
        that (`if latency >= 0`) instead of polluting the histogram with a
        negative bucket.
        """
        sum_before, count_before = _histogram_count(metrics.END_TO_END_LATENCY, service=metrics.SERVICE_NAME)
        future_ts_ms = int((time.time() + 3600) * 1000)  # 1 hour in the future

        metrics.record_end_to_end_latency(future_ts_ms)

        sum_after, count_after = _histogram_count(metrics.END_TO_END_LATENCY, service=metrics.SERVICE_NAME)
        assert (sum_after, count_after) == (sum_before, count_before)


class TestOffsetCommitCounters:
    def test_offset_commit_counter_names_exist_and_are_independent(self):
        # Sanity: the two counters are genuinely separate time series, not
        # aliases of each other - a failure must never also bump the success
        # counter and vice versa.
        success_before = metrics.KAFKA_OFFSET_COMMIT_TOTAL.labels(service=metrics.SERVICE_NAME)._value.get()
        failed_before = metrics.KAFKA_OFFSET_COMMIT_FAILED_TOTAL.labels(service=metrics.SERVICE_NAME)._value.get()

        metrics.KAFKA_OFFSET_COMMIT_TOTAL.labels(service=metrics.SERVICE_NAME).inc()

        assert metrics.KAFKA_OFFSET_COMMIT_TOTAL.labels(service=metrics.SERVICE_NAME)._value.get() == success_before + 1
        assert metrics.KAFKA_OFFSET_COMMIT_FAILED_TOTAL.labels(service=metrics.SERVICE_NAME)._value.get() == failed_before
