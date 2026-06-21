"""Prometheus registry shape and native snapshot translation."""

from __future__ import annotations

import json

import shrike_native
from prometheus_client.parser import text_string_to_metric_families

from shrike.observability.metrics import Metrics


def _samples(text: str) -> dict[str, float]:
    return {
        sample.name + str(sorted(sample.labels.items())): sample.value
        for family in text_string_to_metric_families(text)
        for sample in family.samples
    }


def test_action_and_http_metrics_use_bounded_labels() -> None:
    registry = Metrics()
    registry.observe_action("search_notes", "mcp", "ok", 0.01)
    registry.observe_http("data", "GET", "/media/{filename:path}", 404, 0.02)

    body, content_type = registry.render()
    text = body.decode()
    assert content_type.startswith("text/plain")
    assert (
        'shrike_action_requests_total{action="search_notes",result="ok",transport="mcp"} 1.0'
        in text
    )
    assert 'route="/media/{filename:path}"' in text
    assert "a-secret-filename" not in text


def test_native_runtime_snapshot_is_exposed(monkeypatch) -> None:
    histogram = {
        "count": 2,
        "sum_seconds": 0.03,
        "buckets": [[0.01, 1], [0.1, 2]],
    }
    snapshot = {
        "io_alive": True,
        "collection": {
            "workers": 1,
            "queued": 2,
            "active": 1,
            "completed": 2,
            "queue_wait": histogram,
            "job_duration": histogram,
        },
        "compute": {
            "workers": 4,
            "queued": 3,
            "active": 2,
            "completed": 2,
            "queue_wait": histogram,
            "job_duration": histogram,
        },
        "embedding": {
            "text": {
                "calls": 2,
                "items": 9,
                "errors": 1,
                "duration": histogram,
            },
            "image": {},
        },
    }
    monkeypatch.setattr(shrike_native, "runtime_metrics_json", lambda: json.dumps(snapshot))

    body, _ = Metrics().render()
    samples = _samples(body.decode())
    assert samples["shrike_runtime_io_alive[]"] == 1
    assert samples["shrike_runtime_compute_overlap_ratio[]"] == 0.5
    assert samples[
        "shrike_runtime_pool_queue_depth[('pool', 'compute')]"
    ] == 3
    assert samples["shrike_embedding_items_total[('modality', 'text')]"] == 9
    assert samples[
        "shrike_embedding_batches_total[('modality', 'text'), ('result', 'error')]"
    ] == 1


def test_index_state_is_one_hot() -> None:
    registry = Metrics()
    registry.update_index("vector", "building", 42)
    samples = _samples(registry.render()[0].decode())
    assert samples[
        "shrike_index_entries[('collection', 'default'), ('index', 'vector')]"
    ] == 42
    assert samples[
        "shrike_index_state[('collection', 'default'), ('index', 'vector'), ('state', 'building')]"
    ] == 1
    assert samples[
        "shrike_index_state[('collection', 'default'), ('index', 'vector'), ('state', 'ready')]"
    ] == 0
