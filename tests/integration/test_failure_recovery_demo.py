"""Verify the public demo's evidence, not just its printed success message."""

import importlib.util
import json
from pathlib import Path

from enterprise_crawler.events import SQLiteDeadLetterQueue, SQLiteEventQueue


def test_demo_persists_recovery_evidence(tmp_path):
    path = (
        Path(__file__).resolve().parents[2]
        / "examples/event_pipeline/failure_recovery_demo.py"
    )
    spec = importlib.util.spec_from_file_location("failure_demo", path)
    demo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(demo)
    demo.run_demo(tmp_path)

    with SQLiteEventQueue(tmp_path / "queue.sqlite3", name="collection") as queue:
        assert queue.active_count == 0
    with SQLiteDeadLetterQueue(tmp_path / "dlq.sqlite3", name="collection") as dlq:
        assert {r.failure_type for r in dlq.records()} == {
            "EmptyCollectionError",
            "RateLimitError",
        }
    results = [json.loads(p.read_text()) for p in (tmp_path / "results").glob("*.json")]
    assert {r["job_id"] for r in results} == {
        "good",
        "transient",
        "later",
        "limited-reviewed",
    }
    assert all(r["data"] == {"value": 42} for r in results)
    report = json.loads((tmp_path / "demo-report.json").read_text())
    assert report["requests"] == {
        "/ok": 1,
        "/transient": 2,
        "/empty": 1,
        "/limited": 2,
        "/later": 1,
    }
