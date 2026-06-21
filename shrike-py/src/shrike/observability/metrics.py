"""Process-local Prometheus metrics.

The registry deliberately contains only Shrike metrics.  In particular it does
not install the prometheus-client process/GC collectors: this surface describes
the daemon's behaviour, is cheap to scrape, and has bounded, privacy-safe
labels.  All durations use seconds so the instruments map directly to OTel.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

import shrike_native
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, HistogramMetricFamily
from prometheus_client.exposition import CONTENT_TYPE_LATEST

_DURATION_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)
_INDEX_STATES = ("ready", "building", "unavailable", "error")


class _NativeRuntimeCollector:
    """Translate the kernel's atomic snapshot into Prometheus families."""

    def collect(self) -> Iterable[Any]:
        snapshot_fn = getattr(shrike_native, "runtime_metrics_json", None)
        if snapshot_fn is None:
            return
        try:
            raw = json.loads(snapshot_fn())
        except (RuntimeError, TypeError, ValueError):
            return

        workers = GaugeMetricFamily(
            "shrike_runtime_pool_workers",
            "Driven runtime workers by pool and state.",
            labels=["pool", "state"],
        )
        queue = GaugeMetricFamily(
            "shrike_runtime_pool_queue_depth",
            "Jobs waiting in a driven runtime pool.",
            labels=["pool"],
        )
        active = GaugeMetricFamily(
            "shrike_runtime_pool_active_jobs",
            "Jobs currently executing in a driven runtime pool.",
            labels=["pool"],
        )
        completed = CounterMetricFamily(
            "shrike_runtime_pool_jobs_total",
            "Jobs completed by a driven runtime pool.",
            labels=["pool"],
        )
        for pool in ("collection", "compute"):
            row = raw.get(pool, {})
            live = float(row.get("workers", 0))
            workers.add_metric([pool, "configured"], float(row.get("configured", live)))
            workers.add_metric([pool, "live"], live)
            queue.add_metric([pool], float(row.get("queued", 0)))
            active.add_metric([pool], float(row.get("active", 0)))
            completed.add_metric([pool], float(row.get("completed", 0)))
        yield workers
        yield queue
        yield active
        yield completed

        for key, help_text in (
            ("queue_wait", "Time jobs spent queued before execution."),
            ("job_duration", "Time spent executing driven runtime jobs."),
        ):
            family = HistogramMetricFamily(
                f"shrike_runtime_pool_{key}_seconds",
                help_text,
                labels=["pool"],
            )
            for pool in ("collection", "compute"):
                hist = raw.get(pool, {}).get(key, {})
                buckets = [(str(b), int(n)) for b, n in hist.get("buckets", [])]
                buckets.append(("+Inf", int(hist.get("count", 0))))
                family.add_metric(
                    [pool],
                    buckets=buckets,
                    sum_value=float(hist.get("sum_seconds", 0.0)),
                )
            yield family

        io = GaugeMetricFamily("shrike_runtime_io_alive", "Whether drive_io is live.")
        io.add_metric([], float(bool(raw.get("io_alive", False))))
        yield io
        overlap = GaugeMetricFamily(
            "shrike_runtime_compute_overlap_ratio",
            "Active compute jobs divided by live compute workers.",
        )
        compute = raw.get("compute", {})
        live = int(compute.get("workers", 0))
        overlap.add_metric([], float(compute.get("active", 0)) / live if live else 0.0)
        yield overlap

        saver = raw.get("saver", {})
        saver_events = CounterMetricFamily(
            "shrike_index_saver_events_total",
            "Debounced index saver events.",
            labels=["event", "result"],
        )
        saver_events.add_metric(["request", "ok"], float(saver.get("requests", 0)))
        runs = int(saver.get("runs", 0))
        errors = int(saver.get("errors", 0))
        saver_events.add_metric(["flush", "ok"], float(max(0, runs - errors)))
        saver_events.add_metric(["flush", "error"], float(errors))
        yield saver_events
        saver_pending = GaugeMetricFamily(
            "shrike_index_saver_pending", "Unsaved changes waiting for the debounced saver."
        )
        saver_pending.add_metric([], float(saver.get("pending", 0)))
        yield saver_pending
        saver_duration = HistogramMetricFamily(
            "shrike_index_saver_duration_seconds", "Debounced index flush latency."
        )
        saver_hist = saver.get("duration", {})
        saver_buckets = [(str(b), int(n)) for b, n in saver_hist.get("buckets", [])]
        saver_buckets.append(("+Inf", int(saver_hist.get("count", 0))))
        saver_duration.add_metric(
            [],
            buckets=saver_buckets,
            sum_value=float(saver_hist.get("sum_seconds", 0)),
        )
        yield saver_duration

        embedding_calls = CounterMetricFamily(
            "shrike_embedding_batches_total",
            "Embedding batches by modality and result.",
            labels=["modality", "result"],
        )
        embedding_items = CounterMetricFamily(
            "shrike_embedding_items_total",
            "Items submitted for embedding.",
            labels=["modality"],
        )
        embedding_duration = HistogramMetricFamily(
            "shrike_embedding_duration_seconds",
            "Embedding batch latency.",
            labels=["modality"],
        )
        for modality in ("text", "image"):
            row = raw.get("embedding", {}).get(modality, {})
            calls = int(row.get("calls", 0))
            errors = int(row.get("errors", 0))
            embedding_calls.add_metric([modality, "ok"], float(max(0, calls - errors)))
            embedding_calls.add_metric([modality, "error"], float(errors))
            embedding_items.add_metric([modality], float(row.get("items", 0)))
            hist = row.get("duration", {})
            buckets = [(str(b), int(n)) for b, n in hist.get("buckets", [])]
            buckets.append(("+Inf", int(hist.get("count", 0))))
            embedding_duration.add_metric(
                [modality], buckets=buckets, sum_value=float(hist.get("sum_seconds", 0))
            )
        yield embedding_calls
        yield embedding_items
        yield embedding_duration


class Metrics:
    """The daemon's single process-local metrics registry."""

    def __init__(self) -> None:
        self.registry = CollectorRegistry(auto_describe=True)
        self.action_requests = Counter(
            "shrike_action_requests",
            "Action requests by action, transport, and result.",
            ("action", "transport", "result"),
            registry=self.registry,
        )
        self.action_duration = Histogram(
            "shrike_action_request_duration_seconds",
            "Action request latency.",
            ("action", "transport", "result"),
            buckets=_DURATION_BUCKETS,
            registry=self.registry,
        )
        self.http_requests = Counter(
            "shrike_http_requests",
            "HTTP requests by plane, method, normalized route, and status.",
            ("plane", "method", "route", "status_code"),
            registry=self.registry,
        )
        self.http_duration = Histogram(
            "shrike_http_request_duration_seconds",
            "HTTP request latency.",
            ("plane", "method", "route", "status_code"),
            buckets=_DURATION_BUCKETS,
            registry=self.registry,
        )
        self.index_size = Gauge(
            "shrike_index_entries",
            "Entries in a Shrike index.",
            ("collection", "index"),
            registry=self.registry,
        )
        self.index_state = Gauge(
            "shrike_index_state",
            "One-hot index state.",
            ("collection", "index", "state"),
            registry=self.registry,
        )
        self.index_operations = Counter(
            "shrike_index_operations",
            "Index maintenance operations.",
            ("collection", "index", "operation", "result"),
            registry=self.registry,
        )
        self.index_operation_duration = Histogram(
            "shrike_index_operation_duration_seconds",
            "Index maintenance latency.",
            ("collection", "index", "operation", "result"),
            buckets=_DURATION_BUCKETS,
            registry=self.registry,
        )
        self.collection_size = Gauge(
            "shrike_collection_notes",
            "Last observed collection note count.",
            ("collection",),
            registry=self.registry,
        )
        self.lock_attempts = Counter(
            "shrike_collection_lock_attempts",
            "Cooperative collection lock attempts.",
            ("result",),
            registry=self.registry,
        )
        self.lock_wait = Histogram(
            "shrike_collection_lock_wait_seconds",
            "Time spent acquiring the cooperative collection lock.",
            ("result",),
            buckets=_DURATION_BUCKETS,
            registry=self.registry,
        )
        self.lock_held = Gauge(
            "shrike_collection_lock_held",
            "Whether Shrike currently holds the collection lock.",
            ("collection",),
            registry=self.registry,
        )
        self.recognition_sweeps = Counter(
            "shrike_recognition_sweeps",
            "Recognition sweeps.",
            ("result",),
            registry=self.registry,
        )
        self.recognition_duration = Histogram(
            "shrike_recognition_sweep_duration_seconds",
            "Recognition sweep latency.",
            ("result",),
            buckets=_DURATION_BUCKETS,
            registry=self.registry,
        )
        self.recognition_items = Counter(
            "shrike_recognition_items",
            "Items stored by recognition sweeps.",
            registry=self.registry,
        )
        self.recognition_running = Gauge(
            "shrike_recognition_sweep_running",
            "Whether a recognition sweep is running.",
            registry=self.registry,
        )
        self.registry.register(_NativeRuntimeCollector())

    def observe_action(self, action: str, transport: str, result: str, seconds: float) -> None:
        labels = (action, transport, result)
        self.action_requests.labels(*labels).inc()
        self.action_duration.labels(*labels).observe(seconds)

    def observe_http(
        self, plane: str, method: str, route: str, status_code: int, seconds: float
    ) -> None:
        labels = (plane, method, route, str(status_code))
        self.http_requests.labels(*labels).inc()
        self.http_duration.labels(*labels).observe(seconds)

    def update_index(
        self, index: str, state: str, size: int, *, collection: str = "default"
    ) -> None:
        self.index_size.labels(collection, index).set(size)
        for candidate in _INDEX_STATES:
            self.index_state.labels(collection, index, candidate).set(candidate == state)

    def render(self) -> tuple[bytes, str]:
        return generate_latest(self.registry), CONTENT_TYPE_LATEST


metrics = Metrics()
