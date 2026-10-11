"""Durable JSON collection using existing ECF APIs. See docs/getting-started/durable-collection.md."""

from __future__ import annotations

import argparse
import hashlib
import json
import signal
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import requests

from enterprise_crawler.contracts import Event
from enterprise_crawler.events import (
    EventDispatcher,
    EventWorker,
    RetryPolicy,
    SQLiteDeadLetterQueue,
    SQLiteEventQueue,
)
from enterprise_crawler.storage import AtomicFileWriter


class TemporaryCollectionError(Exception):
    """An HTTP response that this example chooses to retry."""


def collect(session: requests.Session, url: str):
    """Replace this function with your source-specific collection/parsing code.

    Return JSON-serializable data on success; raise on failure. The caller owns
    the session. Requests' default adapter does not add transport retries here.
    """
    with session.get(url, timeout=(5, 15)) as response:
        if response.status_code in {408, 429, 500, 502, 503, 504}:
            raise TemporaryCollectionError(f"HTTP {response.status_code}")
        response.raise_for_status()
        return response.json()


def output_path(root: Path, job_id: str) -> Path:
    # Stable per logical job; no untrusted path segments in the output filename.
    key = hashlib.sha256(job_id.encode("utf-8")).hexdigest()
    return root / "results" / f"{key}.json"


def make_handler(session: requests.Session, root: Path):
    def handle(event: Event):
        data = collect(session, event.payload["url"])
        target = output_path(root, event.payload["job_id"])
        AtomicFileWriter().write_json(
            target, {"job_id": event.payload["job_id"], "data": data}, overwrite=True
        )
        return str(target)

    return handle


@contextmanager
def stop_between_jobs(worker: EventWorker):
    """Main-thread CLI only: finish the current handler, then stop claiming."""
    previous = {}
    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, lambda *_: worker.request_stop())
        yield
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--lease-seconds", type=float, default=300)
    commands = parser.add_subparsers(dest="command", required=True)
    enqueue = commands.add_parser("enqueue")
    enqueue.add_argument("job_id")
    enqueue.add_argument("url")
    work = commands.add_parser("work")
    work.add_argument("--retry-delay", type=float, default=5)
    work.add_argument("--max-deliveries", type=int, default=3)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--job-id")
    args = parser.parse_args(argv)
    root = args.state_dir.resolve()
    # All resources are created and owned here, including when a command fails.
    with (
        SQLiteEventQueue(
            root / "queue.sqlite3", name="collection", lease_seconds=args.lease_seconds
        ) as queue,
        SQLiteDeadLetterQueue(root / "dlq.sqlite3", name="collection") as dlq,
    ):
        if args.command == "enqueue":
            queue.publish(
                Event(
                    event_type="COLLECT_JSON",
                    timestamp=datetime.now(timezone.utc),
                    payload={"job_id": args.job_id, "url": args.url},
                ),
                message_id=args.job_id,
            )
            print(json.dumps({"published": args.job_id}))
            return 0
        if args.command == "inspect":
            result = {
                "active_count": queue.active_count,
                "dlq": [r.to_dict() for r in dlq.records()],
            }
            if args.job_id:
                result["job"] = (
                    queue.message_snapshot(args.job_id)
                    if queue.contains(args.job_id)
                    else None
                )
                result["output"] = str(output_path(root, args.job_id))
            print(json.dumps(result))
            return 0
        dispatcher = EventDispatcher()
        with requests.Session() as session:
            dispatcher.register("COLLECT_JSON", make_handler(session, root))
            with EventWorker(
                queue=queue,
                dispatcher=dispatcher,
                dead_letter_queue=dlq,
                retry_policy=RetryPolicy(
                    max_deliveries=args.max_deliveries,
                    retry_exceptions=(
                        TemporaryCollectionError,
                        requests.Timeout,
                        requests.ConnectionError,
                    ),
                    base_delay_seconds=args.retry_delay,
                ),
            ) as worker:
                with stop_between_jobs(worker):
                    summary = worker.run_until_empty()
                print(
                    json.dumps(
                        {
                            "summary": summary.to_dict(),
                            "active_count": queue.active_count,
                            "dlq_count": dlq.count,
                            "last_retry_decision": worker.last_retry_decision.to_dict()
                            if worker.last_retry_decision
                            else None,
                        }
                    )
                )
                # A batch exits on retry or when no due job remains. Pending is
                # not success: inspect and invoke work again after the due time.
                return (
                    2 if queue.active_count or dlq.count or summary.failure_count else 0
                )


if __name__ == "__main__":
    raise SystemExit(main())
