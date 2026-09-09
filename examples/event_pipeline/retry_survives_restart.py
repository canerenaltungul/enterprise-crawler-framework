from __future__ import annotations

"""
Enterprise Crawler Framework - Durable Retry Across Process Restart Demo

This demo shows that retry state stored by SQLiteEventQueue survives the
lifetime of the worker process.

Run from the repository root:

    python examples/event_pipeline/retry_survives_restart.py fail

Then start a new Python process:

    python examples/event_pipeline/retry_survives_restart.py recover

Optional cleanup:

    python examples/event_pipeline/retry_survives_restart.py reset

The demo intentionally uses existing ECF event contracts only.
It does not implement retry persistence itself.
"""

import argparse
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from enterprise_crawler.contracts import Event
from enterprise_crawler.events import (
    EventDispatcher,
    EventWorker,
    InMemoryDeadLetterQueue,
    RetryPolicy,
    SQLiteEventQueue,
)


UTC = timezone.utc

EVENT_TYPE = "DEMO_JOB"
MESSAGE_ID = "restart-demo-job"

DEMO_DIRECTORY = (
    Path(tempfile.gettempdir())
    / "enterprise-crawler-framework-demo"
)

DATABASE_PATH = (
    DEMO_DIRECTORY
    / "retry-survives-restart.sqlite3"
)


class TemporaryError(RuntimeError):
    """
    Simulated retryable handler failure.
    """


def make_event() -> Event:
    return Event(
        event_type=EVENT_TYPE,
        timestamp=datetime.now(
            UTC
        ),
        payload={
            "job_id": "demo-1",
            "source": "retry-restart-demo",
        },
        metadata={
            "demo": True,
        },
    )


def make_queue() -> SQLiteEventQueue:
    return SQLiteEventQueue(
        DATABASE_PATH,
        name="restart-demo",
        lease_seconds=30.0,
        timeout_seconds=10.0,
    )


def make_retry_policy() -> RetryPolicy:
    return RetryPolicy(
        max_deliveries=3,
        retry_exceptions=(
            TemporaryError,
        ),
        base_delay_seconds=0.0,
    )


def reset_demo_state() -> int:
    if DATABASE_PATH.exists():
        DATABASE_PATH.unlink()

    wal_path = Path(
        f"{DATABASE_PATH}-wal"
    )

    shm_path = Path(
        f"{DATABASE_PATH}-shm"
    )

    for path in (
        wal_path,
        shm_path,
    ):
        if path.exists():
            path.unlink()

    print(
        "demo state reset"
    )

    print(
        f"database={DATABASE_PATH}"
    )

    return 0


def run_failure_phase() -> int:
    reset_demo_state()

    queue = make_queue()

    dispatcher = (
        EventDispatcher(
            name="restart-demo"
        )
    )

    def failing_handler(
        event: Event,
    ) -> None:
        print(
            "handler: delivery received"
        )

        print(
            "handler: simulated transient failure"
        )

        raise TemporaryError(
            "simulated transient failure"
        )

    dispatcher.register(
        EVENT_TYPE,
        failing_handler,
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        name="restart-demo-worker-1",
        retry_policy=(
            make_retry_policy()
        ),
        dead_letter_queue=(
            InMemoryDeadLetterQueue()
        ),
        idle_sleep_seconds=0,
    )

    try:
        published = queue.publish(
            make_event(),
            message_id=MESSAGE_ID,
        )

        print(
            f"published: {published.message_id}"
        )

        summary = (
            worker.run_until_empty()
        )

        snapshot = (
            queue.message_snapshot(
                MESSAGE_ID
            )
        )

        if summary.failure_count != 1:
            raise RuntimeError(
                "Expected exactly one "
                "handler failure."
            )

        if snapshot["state"] != "pending":
            raise RuntimeError(
                "Expected event to be "
                "pending after retry."
            )

        if snapshot["delivery_count"] != 1:
            raise RuntimeError(
                "Expected delivery_count=1 "
                "after first failure."
            )

        print()
        print(
            "retry persisted to SQLite"
        )

        print(
            "state="
            f"{snapshot['state']}"
        )

        print(
            "delivery_count="
            f"{snapshot['delivery_count']}"
        )

        print(
            "active_count="
            f"{queue.active_count}"
        )

        print()
        print(
            "Process 1 can now exit."
        )

        print(
            "Run the recover command "
            "in a new Python process."
        )

        print()
        print(
            "python "
            "examples/event_pipeline/"
            "retry_survives_restart.py "
            "recover"
        )

        return 0

    finally:
        queue.close()


def run_recovery_phase() -> int:
    if not DATABASE_PATH.exists():
        print(
            "No persisted demo state found."
        )

        print(
            "Run the fail phase first:"
        )

        print()
        print(
            "python "
            "examples/event_pipeline/"
            "retry_survives_restart.py "
            "fail"
        )

        return 1

    queue = make_queue()

    try:
        before = (
            queue.message_snapshot(
                MESSAGE_ID
            )
        )

        print(
            "reopened SQLite queue"
        )

        print(
            "state="
            f"{before['state']}"
        )

        print(
            "delivery_count="
            f"{before['delivery_count']}"
        )

        if before["state"] != "pending":
            raise RuntimeError(
                "Expected persisted event "
                "to be pending."
            )

        if before["delivery_count"] != 1:
            raise RuntimeError(
                "Expected persisted "
                "delivery_count=1."
            )

        dispatcher = (
            EventDispatcher(
                name="restart-demo"
            )
        )

        def successful_handler(
            event: Event,
        ) -> str:
            print()
            print(
                "handler: recovered event"
            )

            print(
                "handler: success"
            )

            return "success"

        dispatcher.register(
            EVENT_TYPE,
            successful_handler,
        )

        worker = EventWorker(
            queue=queue,
            dispatcher=dispatcher,
            name="restart-demo-worker-2",
            retry_policy=(
                make_retry_policy()
            ),
            dead_letter_queue=(
                InMemoryDeadLetterQueue()
            ),
            idle_sleep_seconds=0,
        )

        result = (
            worker.run_once()
        )

        if result is None:
            raise RuntimeError(
                "Expected persisted event "
                "to be delivered."
            )

        if result.delivery_count != 2:
            raise RuntimeError(
                "Expected delivery_count=2 "
                "after recovery."
            )

        if result.value != "success":
            raise RuntimeError(
                "Unexpected handler result."
            )

        if queue.active_count != 0:
            raise RuntimeError(
                "Expected queue to be empty "
                "after successful ACK."
            )

        print()
        print(
            "delivery_count="
            f"{result.delivery_count}"
        )

        print(
            "result="
            f"{result.value}"
        )

        print(
            "active_count="
            f"{queue.active_count}"
        )

        print()
        print(
            "SUCCESS:"
        )

        print(
            "retry state survived the "
            "process boundary"
        )

        print(
            "and the recovered delivery "
            "was acknowledged."
        )

        return 0

    finally:
        queue.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Demonstrate durable ECF retry "
            "state across two Python processes."
        )
    )

    parser.add_argument(
        "phase",
        choices=(
            "fail",
            "recover",
            "reset",
        ),
        help=(
            "fail: persist a retry; "
            "recover: reopen and process it; "
            "reset: remove demo state"
        ),
    )

    return parser


def main() -> int:
    args = (
        build_parser().parse_args()
    )

    if args.phase == "fail":
        return run_failure_phase()

    if args.phase == "recover":
        return run_recovery_phase()

    return reset_demo_state()


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
