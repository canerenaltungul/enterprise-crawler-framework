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


@pytest.mark.parametrize("data", [None, {}, [], "", "  \n"])
def test_empty_data_is_not_success(data):
    with pytest.raises(example.EmptyCollectionError):
        example.validate_data(data)


@pytest.mark.parametrize("data", [0, False, {"value": 0}, [1], "value"])
def test_nonempty_and_valid_scalar_data_are_accepted(data):
    example.validate_data(data)


def test_rate_limit_preserves_error_and_requests_stop(tmp_path, monkeypatch):
    error = example.RateLimitError("HTTP 429")
    stops = []

    def limited(*args):
        raise error

    monkeypatch.setattr(example, "collect", limited)
    handler = example.make_handler(object(), tmp_path, lambda: stops.append(True))
    with pytest.raises(example.RateLimitError) as caught:
        handler(
            Event(
                "COLLECT_JSON",
                datetime.now(timezone.utc),
                {"job_id": "job", "url": "unused"},
            )
        )
    assert caught.value is error
    assert stops == [True]
    assert not (tmp_path / "results").exists()
