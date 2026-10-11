from __future__ import annotations

import importlib.util
import signal
from datetime import datetime, timezone
from pathlib import Path

import pytest
import requests

from enterprise_crawler.contracts import Event


SPEC = importlib.util.spec_from_file_location(
    "collection_example",
    Path(__file__).resolve().parents[2] / "examples/event_pipeline/collect_json.py",
)
example = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(example)


def test_collection_preserves_error_and_caller_ownership(tmp_path):
    error = requests.Timeout("test timeout")

    class Session:
        closed = False

        def get(self, *args, **kwargs):
            raise error

        def close(self):
            self.closed = True

    session = Session()
    handler = example.make_handler(session, tmp_path)
    with pytest.raises(requests.Timeout) as caught:
        handler(
            Event(
                "COLLECT_JSON",
                datetime.now(timezone.utc),
                {"job_id": "one", "url": "unused"},
            )
        )
    assert caught.value is error
    assert not session.closed
    assert not (tmp_path / "results").exists()


def test_stop_request_does_not_interrupt_current_scope_and_restores_signals():
    class Worker:
        stopped = False

        def request_stop(self):
            self.stopped = True

    before = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
    worker = Worker()
    with pytest.raises(ValueError):
        with example.stop_between_jobs(worker):
            signal.raise_signal(signal.SIGINT)
            assert worker.stopped
            raise ValueError("resource scope still unwinds")
    assert {s: signal.getsignal(s) for s in before} == before
