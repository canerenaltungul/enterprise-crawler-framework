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


class RateLimitError(Exception):
    """Requires operator review, not repeated requests against a rate limit."""


class EmptyCollectionError(Exception):
    """The source returned no data where this job expected a result."""


def validate_data(data):
    """Example policy: reject null/empty results, but accept false and zero.

    Replace with source-specific required fields and completeness checks.
    """
    if data is None or (isinstance(data, (dict, list, str)) and not data):
        raise EmptyCollectionError("Expected non-empty collection data")
    if isinstance(data, str) and not data.strip():
        raise EmptyCollectionError("Expected non-blank collection data")


def collect(session: requests.Session, url: str):
    """Replace this function with your source-specific collection/parsing code.

    Return JSON-serializable data on success; raise on failure. The caller owns
    the session. Requests' default adapter does not add transport retries here.
    """
    with session.get(url, timeout=(5, 15)) as response:
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "not supplied")
            raise RateLimitError(
                f"HTTP 429; Retry-After={retry_after}; review before replay"
            )
        if response.status_code in {408, 500, 502, 503, 504}:
            raise TemporaryCollectionError(f"HTTP {response.status_code}")
        response.raise_for_status()
        return response.json()


def output_path(root: Path, job_id: str) -> Path:
    # Stable per logical job; no untrusted path segments in the output filename.
    key = hashlib.sha256(job_id.encode("utf-8")).hexdigest()
    return root / "results" / f"{key}.json"


def make_handler(session: requests.Session, root: Path, request_stop=None):
    def handle(event: Event):
        try:
            data = collect(session, event.payload["url"])
        except RateLimitError:
            if request_stop is not None:
                request_stop()
            raise
        if not event.payload.get("allow_empty", False):
            validate_data(data)
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
    enqueue.add_argument(
        "--allow-empty",
        action="store_true",
        help="Allow empty JSON when it is valid for this source",
    )
    replay = commands.add_parser(
        "replay", help="Requeue one reviewed DLQ record; preserve its history"
    )
    replay.add_argument("dead_letter_id")
    replay.add_argument("--new-job-id", required=True)
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
                    payload={
                        "job_id": args.job_id,
                        "url": args.url,
                        "allow_empty": args.allow_empty,
                    },
                ),
                message_id=args.job_id,
            )
            print(json.dumps({"published": args.job_id}))
            return 0
        if args.command == "replay":
            if not dlq.contains(args.dead_letter_id):
                print(json.dumps({"error": "Unknown dead-letter ID"}))
                return 2
            record = dlq.get(args.dead_letter_id)
            # A DLQ write can have succeeded while source finalization failed.
            # Do not replay that record while its original delivery is active.
            if queue.contains(record.message_id):
                print(
                    json.dumps(
                        {
                            "error": "Original job is still active; resolve it before replay"
                        }
                    )
                )
                return 2
            if (
                not args.new_job_id.strip()
                or args.new_job_id != args.new_job_id.strip()
                or args.new_job_id == record.message_id
                or queue.contains(args.new_job_id)
                or output_path(root, args.new_job_id).exists()
                or any(r.message_id == args.new_job_id for r in dlq.records())
            ):
                print(
                    json.dumps(
                        {
                            "error": "Choose a new, unused job ID without surrounding whitespace"
                        }
                    )
                )
                return 2
            queue.publish(
                Event(
                    event_type=record.event.event_type,
                    timestamp=datetime.now(timezone.utc),
                    payload={**record.event.payload, "job_id": args.new_job_id},
                    metadata={
                        **record.event.metadata,
                        "replayed_from": record.dead_letter_id,
                        "original_message_id": record.message_id,
                    },
                ),
                message_id=args.new_job_id,
            )
            # Keep the original DLQ record even if publication or later work fails.
            print(
                json.dumps(
                    {
                        "published": args.new_job_id,
                        "replayed_from": record.dead_letter_id,
                        "dlq_record_retained": True,
                    }
                )
            )
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
                dispatcher.register(
                    "COLLECT_JSON", make_handler(session, root, worker.request_stop)
                )
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
