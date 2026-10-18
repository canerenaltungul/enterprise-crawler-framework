"""Run a local failure/recovery story with real CLI processes and SQLite state."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


SCRIPT = Path(__file__).with_name("collect_json.py")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def run_demo(root: Path, pace: float = 0):
    calls = {}
    reviewed = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            calls[self.path] = calls.get(self.path, 0) + 1
            status = 200
            if self.path == "/limited" and not reviewed.is_set():
                status = 429
            elif self.path == "/transient" and calls[self.path] == 1:
                status = 503
            body = b"[]" if self.path == "/empty" else b'{"value": 42}'
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    def cli(*args, expected=0):
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--state-dir", str(root), *args],
            capture_output=True,
            text=True,
            timeout=30,
        )
        require(
            result.returncode == expected,
            f"CLI {args} exited {result.returncode}: {result.stdout}{result.stderr}",
        )
        return json.loads(result.stdout)

    def show(message):
        print(message, flush=True)
        time.sleep(pace)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}"
    try:
        cli("enqueue", "good", url + "/ok")
        cli("work")
        good = Path(cli("inspect", "--job-id", "good")["output"])
        original = good.read_bytes()
        require(json.loads(original)["data"] == {"value": 42}, "Missing result")
        show("[1/5] SUCCESS: collected data written, job acknowledged.")

        cli("enqueue", "transient", url + "/transient")
        cli("work", "--retry-delay", "0", expected=2)
        pending = cli("inspect", "--job-id", "transient")["job"]
        require(pending["delivery_count"] == 1, "Retry attempt was not persisted")
        require(pending["state"] == "pending", "Retry is not pending")
        cli("work")
        require(calls["/transient"] == 2, "Retry did not resume")
        show("[2/5] HTTP 503: retry persisted; a NEW worker process completed it.")

        cli("enqueue", "empty", url + "/empty")
        cli("work", expected=2)
        empty = cli("inspect", "--job-id", "empty")
        require(not Path(empty["output"]).exists(), "Empty output was written")
        require(
            empty["dlq"][0]["failure_type"] == "EmptyCollectionError",
            "Missing empty-data failure",
        )
        show(
            "[3/5] EMPTY DATA: rejected before writing output; failure kept in SQLite DLQ."
        )

        cli("enqueue", "limited", url + "/limited")
        cli("enqueue", "later", url + "/later")
        stopped = cli("work", expected=2)
        require(stopped["summary"]["stop_requested"], "Batch did not stop")
        require(
            stopped["active_count"] == 1 and "/later" not in calls,
            "Later job was not preserved",
        )
        records = cli("inspect")["dlq"]
        record = next(r for r in records if r["message_id"] == "limited")
        require(
            record["failure_type"] == "RateLimitError", "Missing rate-limit failure"
        )
        show("[4/5] HTTP 429: batch stopped after ONE request; next job stays pending.")

        # Simulate an operator resolving the local fixture's condition. This is
        # not automatic rate-limit recovery or a production cooldown policy.
        reviewed.set()
        cli("replay", record["dead_letter_id"], "--new-job-id", "limited-reviewed")
        cli("work", expected=2)  # Historical DLQ records intentionally remain.
        final = cli("inspect", "--job-id", "limited-reviewed")
        require(final["active_count"] == 0, "Pending work remains")
        require(final["dlq"] == records, "Failure history changed during replay")
        require(Path(final["output"]).is_file(), "Replayed result missing")
        require(
            calls
            == {"/ok": 1, "/transient": 2, "/empty": 1, "/limited": 2, "/later": 1},
            "Unexpected repeated collection",
        )
        require(good.read_bytes() == original, "Successful output changed")
        show(
            "[5/5] REVIEW + REPLAY: selected failed job recovered; successful job ran only ONCE."
        )
        report = {
            "verified": True,
            "requests": calls,
            "active_count": 0,
            "retained_dlq_count": len(records),
        }
        (root / "demo-report.json").write_text(
            json.dumps(report, indent=2), encoding="utf-8"
        )
        print(
            "VERIFIED: all checks passed. Two historical DLQ records remain for inspection.",
            flush=True,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pace",
        type=float,
        default=0,
        help="Pause 0-10 seconds after each verified step, for recording",
    )
    args = parser.parse_args()
    if not 0 <= args.pace <= 10:
        parser.error("--pace must be between 0 and 10 seconds")
    root = Path(tempfile.mkdtemp(prefix="ecf-failure-demo-"))
    print(f"ECF local failure/recovery demo\nState retained at: {root}", flush=True)
    run_demo(root, args.pace)
    print(
        "Local fixture stopped. Its URLs are no longer available; run this demo again for a fresh scenario."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
