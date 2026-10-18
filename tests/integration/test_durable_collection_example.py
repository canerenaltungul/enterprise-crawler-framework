"""Exercise the user-facing CLI in separate processes against a local HTTP server."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "examples" / "event_pipeline" / "collect_json.py"


@pytest.fixture
def endpoint():
    calls = {}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            calls[self.path] = calls.get(self.path, 0) + 1
            status = 200
            if self.path == "/missing":
                status = 404
            elif self.path == "/limited" and calls[self.path] == 1:
                status = 429
            elif self.path == "/always" or (
                self.path == "/transient" and calls[self.path] == 1
            ):
                status = 503
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            if status == 429:
                self.send_header("Retry-After", "60")
            self.end_headers()
            self.wfile.write(b"[]" if self.path == "/empty" else b'{"value": 42}')

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def cli(state, *args, expected=0):
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--state-dir", str(state), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == expected, result.stdout + result.stderr
    return json.loads(result.stdout)


def test_retry_survives_process_restart_and_output_is_stable(tmp_path, endpoint):
    url, calls = endpoint
    cli(tmp_path, "enqueue", "job", url + "/transient")
    first = cli(tmp_path, "work", "--retry-delay", "0", expected=2)
    assert first["last_retry_decision"]["action"] == "retry"
    pending = cli(tmp_path, "inspect", "--job-id", "job")
    assert pending["job"]["delivery_count"] == 1
    assert pending["job"]["state"] == "pending"
    second = cli(tmp_path, "work")
    assert second["summary"]["processed_count"] == 1
    assert second["active_count"] == 0
    assert calls["/transient"] == 2
    result = cli(tmp_path, "inspect", "--job-id", "job")
    assert result["job"] is None
    target = Path(result["output"])
    assert json.loads(target.read_text()) == {"job_id": "job", "data": {"value": 42}}
    # A duplicate logical job after ACK replaces the same output, not an append.
    cli(tmp_path, "enqueue", "job", url + "/transient")
    cli(tmp_path, "work")
    assert list((tmp_path / "results").glob("*.json")) == [target]
    assert json.loads(target.read_text())["data"] == {"value": 42}


def test_future_retry_does_not_block_due_job(tmp_path, endpoint):
    url, calls = endpoint
    cli(tmp_path, "enqueue", "later", url + "/always")
    cli(tmp_path, "work", "--retry-delay", "3600", expected=2)
    cli(tmp_path, "enqueue", "now", url + "/ok")
    result = cli(tmp_path, "work", expected=2)
    assert result["summary"]["processed_count"] == 1
    assert result["active_count"] == 1
    assert calls["/always"] == 1
    pending = cli(tmp_path, "inspect", "--job-id", "later")["job"]
    assert pending["delivery_count"] == 1
    assert pending["next_attempt_at"] is not None


@pytest.mark.parametrize(
    "path, attempts, error",
    [("/missing", 1, "HTTPError"), ("/always", 2, "TemporaryCollectionError")],
)
def test_terminal_failure_is_durable(tmp_path, endpoint, path, attempts, error):
    url, calls = endpoint
    cli(tmp_path, "enqueue", "bad", url + path)
    for _ in range(attempts):
        cli(tmp_path, "work", "--retry-delay", "0", "--max-deliveries", "2", expected=2)
    snapshot = cli(tmp_path, "inspect", "--job-id", "bad")
    assert snapshot["active_count"] == 0
    assert len(snapshot["dlq"]) == 1
    record = snapshot["dlq"][0]
    assert record["delivery_count"] == attempts
    assert record["failure_type"] == error
    assert calls[path] == attempts


def test_output_failure_never_acks_as_success(tmp_path, endpoint):
    url, _ = endpoint
    cli(tmp_path, "enqueue", "job", url + "/ok")
    (tmp_path / "results").mkdir()
    # A directory at the target is a portable way to force atomic output failure.
    (tmp_path / "results" / (hashlib.sha256(b"job").hexdigest() + ".json")).mkdir()
    cli(tmp_path, "work", expected=2)
    snapshot = cli(tmp_path, "inspect")
    assert snapshot["dlq"][0]["failure_type"] == "AtomicWriteError"


def test_rate_limit_stops_batch_and_only_selected_failure_is_replayed(
    tmp_path, endpoint
):
    url, calls = endpoint
    cli(tmp_path, "enqueue", "good", url + "/ok")
    cli(tmp_path, "enqueue", "limited", url + "/limited")
    cli(tmp_path, "enqueue", "later", url + "/later")
    first = cli(tmp_path, "work", expected=2)
    assert first["summary"]["processed_count"] == 1
    assert first["summary"]["stop_requested"] is True
    assert first["active_count"] == 1
    assert calls == {"/ok": 1, "/limited": 1}
    record = cli(tmp_path, "inspect")["dlq"][0]
    assert record["failure_type"] == "RateLimitError"
    assert "Retry-After=60" in record["failure_message"]
    record_id = record["dead_letter_id"]
    cli(tmp_path, "replay", record_id, "--new-job-id", "good", expected=2)
    cli(tmp_path, "replay", record_id, "--new-job-id", "later", expected=2)
    cli(tmp_path, "replay", "unknown", "--new-job-id", "new", expected=2)
    cli(tmp_path, "replay", record_id, "--new-job-id", "reviewed")
    # A repeated command cannot enqueue a second active delivery with this ID.
    cli(tmp_path, "replay", record_id, "--new-job-id", "reviewed", expected=2)
    pending = cli(tmp_path, "inspect", "--job-id", "reviewed")["job"]
    assert pending["delivery_count"] == 0
    # Test server now permits the request: operator has resolved the condition.
    result = cli(tmp_path, "work", expected=2)  # historical DLQ remains visible
    assert result["summary"]["processed_count"] == 2
    assert result["active_count"] == 0
    assert calls == {"/ok": 1, "/limited": 2, "/later": 1}
    assert cli(tmp_path, "inspect")["dlq"][0] == record
    cli(tmp_path, "replay", record_id, "--new-job-id", "reviewed", expected=2)


def test_empty_response_preserves_previous_output_and_is_visible_in_dlq(
    tmp_path, endpoint
):
    url, _ = endpoint
    cli(tmp_path, "enqueue", "job", url + "/ok")
    cli(tmp_path, "work")
    target = Path(cli(tmp_path, "inspect", "--job-id", "job")["output"])
    previous = target.read_bytes()
    cli(tmp_path, "enqueue", "job", url + "/empty")
    cli(tmp_path, "work", expected=2)
    assert target.read_bytes() == previous
    record = cli(tmp_path, "inspect")["dlq"][0]
    assert record["failure_type"] == "EmptyCollectionError"
    assert record["delivery_count"] == 1
    cli(tmp_path, "enqueue", "empty-is-valid", url + "/empty", "--allow-empty")
    cli(tmp_path, "work", expected=2)
    target = Path(cli(tmp_path, "inspect", "--job-id", "empty-is-valid")["output"])
    assert json.loads(target.read_text())["data"] == []


def test_replay_refuses_original_job_still_active(tmp_path, endpoint):
    url, _ = endpoint
    cli(tmp_path, "enqueue", "job", url + "/missing")
    cli(tmp_path, "work", expected=2)
    record_id = cli(tmp_path, "inspect")["dlq"][0]["dead_letter_id"]
    cli(tmp_path, "enqueue", "job", url + "/ok")
    result = cli(tmp_path, "replay", record_id, "--new-job-id", "duplicate", expected=2)
    assert "still active" in result["error"]
    state = cli(tmp_path, "inspect", "--job-id", "duplicate")
    assert state["job"] is None
    assert state["active_count"] == 1
    assert len(state["dlq"]) == 1
