from __future__ import annotations

import threading
from datetime import datetime, timezone
from pathlib import Path
from time import sleep
from typing import Any

import pytest

from enterprise_crawler.contracts import Event
from enterprise_crawler.events import (
    EventClaimOwnershipError,
    EventDispatcher,
    EventHandlerExecutionError,
    EventWorker,
    SQLiteEventQueue,
    WorkerRunSummary,
)


UTC = timezone.utc


# =============================================================================
# HELPERS
# =============================================================================
def make_event(
    event_type: str = "ORDER_CREATED",
    *,
    event_id: int = 1,
    value: Any = None,
) -> Event:
    return Event(
        event_type=event_type,
        timestamp=datetime.now(
            UTC
        ),
        payload={
            "event_id": event_id,
            "value": value,
        },
        metadata={
            "source": (
                "sqlite-integration"
            ),
            "trace_id": (
                f"trace-{event_id}"
            ),
        },
    )


def make_queue(
    database_path: Path,
    *,
    name: str = "orders",
    lease_seconds: float = 30.0,
) -> SQLiteEventQueue:
    return SQLiteEventQueue(
        database_path,
        name=name,
        lease_seconds=(
            lease_seconds
        ),
        timeout_seconds=10,
    )


# =============================================================================
# PERSISTENCE -> WORKER
# =============================================================================
def test_persisted_event_is_processed_after_queue_reopen(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    producer = make_queue(
        database_path
    )

    producer.publish(
        make_event(
            event_id=101,
            value="persisted",
        ),
        message_id="message-101",
    )

    producer.close()

    queue = make_queue(
        database_path
    )

    dispatcher = (
        EventDispatcher(
            name="orders"
        )
    )

    received: list[
        tuple[int, str]
    ] = []

    def handler(
        event: Event,
    ) -> str:
        received.append(
            (
                int(
                    event.payload[
                        "event_id"
                    ]
                ),
                str(
                    event.payload[
                        "value"
                    ]
                ),
            )
        )

        return "processed"

    dispatcher.register(
        "ORDER_CREATED",
        handler,
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        name="sqlite-worker",
        idle_sleep_seconds=0,
    )

    try:
        summary = (
            worker.run_until_empty()
        )

        assert (
            received
            == [
                (
                    101,
                    "persisted",
                )
            ]
        )

        assert (
            summary.processed_count
            == 1
        )

        assert (
            summary.failure_count
            == 0
        )

        assert (
            queue.active_count
            == 0
        )

    finally:
        queue.close()


# =============================================================================
# SUCCESS -> DURABLE ACK
# =============================================================================
def test_successful_worker_dispatch_removes_sqlite_row(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    queue = make_queue(
        database_path
    )

    dispatcher = (
        EventDispatcher()
    )

    dispatcher.register(
        "ORDER_CREATED",
        lambda event: (
            event.payload[
                "event_id"
            ]
        ),
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        idle_sleep_seconds=0,
    )

    try:
        queue.publish(
            make_event(
                event_id=1
            ),
            message_id="ack-me",
        )

        result = (
            worker.run_once()
        )

        assert (
            result
            is not None
        )

        assert (
            result.message_id
            == "ack-me"
        )

        assert (
            result.delivery_count
            == 1
        )

        assert (
            queue.contains(
                "ack-me"
            )
            is False
        )

        assert (
            queue.active_count
            == 0
        )

    finally:
        queue.close()

    reopened = make_queue(
        database_path
    )

    try:
        assert (
            reopened.active_count
            == 0
        )

        assert (
            reopened.contains(
                "ack-me"
            )
            is False
        )

    finally:
        reopened.close()


# =============================================================================
# TRANSIENT FAILURE -> REQUEUE -> SUCCESS
# =============================================================================
def test_transient_failure_requeues_and_succeeds_on_second_worker_run(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    queue = make_queue(
        database_path
    )

    dispatcher = (
        EventDispatcher()
    )

    attempts = 0

    def handler(
        event: Event,
    ) -> str:
        nonlocal attempts

        attempts += 1

        if attempts == 1:
            raise RuntimeError(
                "temporary failure"
            )

        return "success"

    dispatcher.register(
        "ORDER_CREATED",
        handler,
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        requeue_on_error=True,
        stop_on_error=False,
        idle_sleep_seconds=0,
    )

    try:
        queue.publish(
            make_event(
                event_id=77
            ),
            message_id="message-77",
        )

        first = (
            worker.run_until_empty()
        )

        assert (
            first.processed_count
            == 0
        )

        assert (
            first.failure_count
            == 1
        )

        assert (
            queue.pending_count
            == 1
        )

        state_after_failure = (
            queue.message_snapshot(
                "message-77"
            )
        )

        assert (
            state_after_failure[
                "state"
            ]
            == "pending"
        )

        assert (
            state_after_failure[
                "delivery_count"
            ]
            == 1
        )

        second = (
            worker.run_until_empty()
        )

        assert (
            second.processed_count
            == 1
        )

        assert (
            second.failure_count
            == 0
        )

        assert (
            attempts
            == 2
        )

        assert (
            queue.active_count
            == 0
        )

        assert (
            worker.total_failures
            == 1
        )

        assert (
            worker.total_processed
            == 1
        )

    finally:
        queue.close()


# =============================================================================
# POISON EVENT -> DISCARD
# =============================================================================
def test_poison_event_can_be_discarded_and_following_events_continue(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    queue = make_queue(
        database_path
    )

    dispatcher = (
        EventDispatcher()
    )

    processed: list[
        int
    ] = []

    def handler(
        event: Event,
    ) -> None:
        event_id = int(
            event.payload[
                "event_id"
            ]
        )

        if event_id == 1:
            raise RuntimeError(
                "poison event"
            )

        processed.append(
            event_id
        )

    dispatcher.register(
        "ORDER_CREATED",
        handler,
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        requeue_on_error=False,
        stop_on_error=False,
        idle_sleep_seconds=0,
    )

    try:
        queue.publish(
            make_event(
                event_id=1
            ),
            message_id="bad",
        )

        queue.publish(
            make_event(
                event_id=2
            ),
            message_id="good-2",
        )

        queue.publish(
            make_event(
                event_id=3
            ),
            message_id="good-3",
        )

        summary = (
            worker.run_until_empty()
        )

        assert (
            processed
            == [
                2,
                3,
            ]
        )

        assert (
            summary.processed_count
            == 2
        )

        assert (
            summary.failure_count
            == 1
        )

        assert (
            queue.active_count
            == 0
        )

        assert (
            queue.contains(
                "bad"
            )
            is False
        )

    finally:
        queue.close()


# =============================================================================
# LEASE EXPIRY -> DIFFERENT QUEUE INSTANCE
# =============================================================================
def test_expired_claim_can_be_recovered_by_another_queue_instance(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    first = make_queue(
        database_path,
        lease_seconds=0.02,
    )

    second = make_queue(
        database_path,
        lease_seconds=0.02,
    )

    try:
        first.publish(
            make_event(
                event_id=5
            ),
            message_id="lease-message",
        )

        original = (
            first.claim()
        )

        assert (
            original
            is not None
        )

        assert (
            original.delivery_count
            == 1
        )

        sleep(
            0.04
        )

        recovered = (
            second.claim()
        )

        assert (
            recovered
            is not None
        )

        assert (
            recovered.message_id
            == original.message_id
        )

        assert (
            recovered.delivery_count
            == 2
        )

        assert (
            recovered.claim_token
            != original.claim_token
        )

        second.ack(
            recovered.message_id,
            recovered.claim_token,
        )

        assert (
            first.active_count
            == 0
        )

    finally:
        first.close()
        second.close()


# =============================================================================
# STALE OWNER PROTECTION
# =============================================================================
def test_stale_worker_cannot_ack_event_after_lease_recovery(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    first = make_queue(
        database_path,
        lease_seconds=0.02,
    )

    second = make_queue(
        database_path,
        lease_seconds=0.02,
    )

    try:
        first.publish(
            make_event(
                event_id=8
            ),
            message_id="ownership-test",
        )

        stale_claim = (
            first.claim()
        )

        assert (
            stale_claim
            is not None
        )

        sleep(
            0.04
        )

        active_claim = (
            second.claim()
        )

        assert (
            active_claim
            is not None
        )

        with pytest.raises(
            EventClaimOwnershipError
        ):
            first.ack(
                stale_claim.message_id,
                stale_claim.claim_token,
            )

        second.ack(
            active_claim.message_id,
            active_claim.claim_token,
        )

        assert (
            first.active_count
            == 0
        )

    finally:
        first.close()
        second.close()


# =============================================================================
# TWO SQLITE WORKERS / SAME DB
# =============================================================================
def test_two_sqlite_workers_do_not_duplicate_processing(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    producer = make_queue(
        database_path,
        name="jobs",
    )

    queue_a = make_queue(
        database_path,
        name="jobs",
    )

    queue_b = make_queue(
        database_path,
        name="jobs",
    )

    dispatcher = (
        EventDispatcher()
    )

    processed: list[
        int
    ] = []

    processed_lock = (
        threading.Lock()
    )

    def handler(
        event: Event,
    ) -> None:
        event_id = int(
            event.payload[
                "event_id"
            ]
        )

        with processed_lock:
            processed.append(
                event_id
            )

    dispatcher.register(
        "ORDER_CREATED",
        handler,
    )

    event_count = 200

    for event_id in range(
        event_count
    ):
        producer.publish(
            make_event(
                event_id=event_id
            ),
            message_id=(
                f"message-{event_id}"
            ),
        )

    worker_a = EventWorker(
        queue=queue_a,
        dispatcher=dispatcher,
        name="sqlite-worker-a",
        idle_sleep_seconds=0,
    )

    worker_b = EventWorker(
        queue=queue_b,
        dispatcher=dispatcher,
        name="sqlite-worker-b",
        idle_sleep_seconds=0,
    )

    summaries: list[
        WorkerRunSummary
    ] = []

    summaries_lock = (
        threading.Lock()
    )

    def run_worker(
        worker: EventWorker,
    ) -> None:
        summary = (
            worker.run_until_empty()
        )

        with summaries_lock:
            summaries.append(
                summary
            )

    thread_a = threading.Thread(
        target=run_worker,
        args=(
            worker_a,
        ),
    )

    thread_b = threading.Thread(
        target=run_worker,
        args=(
            worker_b,
        ),
    )

    try:
        thread_a.start()
        thread_b.start()

        thread_a.join(
            timeout=10
        )

        thread_b.join(
            timeout=10
        )

        assert (
            thread_a.is_alive()
            is False
        )

        assert (
            thread_b.is_alive()
            is False
        )

        assert (
            len(
                processed
            )
            == event_count
        )

        assert (
            len(
                set(
                    processed
                )
            )
            == event_count
        )

        assert (
            sorted(
                processed
            )
            == list(
                range(
                    event_count
                )
            )
        )

        assert (
            worker_a.total_processed
            + worker_b.total_processed
            == event_count
        )

        assert (
            producer.active_count
            == 0
        )

    finally:
        producer.close()
        queue_a.close()
        queue_b.close()


# =============================================================================
# DELIVERY COUNT THROUGH WORKER
# =============================================================================
def test_delivery_count_survives_failure_reopen_and_worker_redelivery(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    first_queue = make_queue(
        database_path
    )

    first_dispatcher = (
        EventDispatcher()
    )

    def failing_handler(
        event: Event,
    ) -> None:
        raise RuntimeError(
            "temporary"
        )

    first_dispatcher.register(
        "ORDER_CREATED",
        failing_handler,
    )

    first_worker = EventWorker(
        queue=first_queue,
        dispatcher=first_dispatcher,
        requeue_on_error=True,
        stop_on_error=False,
        idle_sleep_seconds=0,
    )

    first_queue.publish(
        make_event(
            event_id=15
        ),
        message_id="delivery-test",
    )

    first_summary = (
        first_worker.run_until_empty()
    )

    assert (
        first_summary.failure_count
        == 1
    )

    assert (
        first_queue.message_snapshot(
            "delivery-test"
        )[
            "delivery_count"
        ]
        == 1
    )

    first_queue.close()

    second_queue = make_queue(
        database_path
    )

    second_dispatcher = (
        EventDispatcher()
    )

    delivery_counts: list[
        int
    ] = []

    second_dispatcher.register(
        "ORDER_CREATED",
        lambda event: "ok",
    )

    second_worker = EventWorker(
        queue=second_queue,
        dispatcher=second_dispatcher,
        idle_sleep_seconds=0,
    )

    try:
        result = (
            second_worker.run_once()
        )

        assert (
            result
            is not None
        )

        delivery_counts.append(
            int(
                result.delivery_count
                or 0
            )
        )

        assert (
            delivery_counts
            == [
                2
            ]
        )

        assert (
            second_queue.active_count
            == 0
        )

    finally:
        second_queue.close()


# =============================================================================
# PAYLOAD / METADATA ROUNDTRIP
# =============================================================================
def test_payload_and_metadata_survive_sqlite_worker_roundtrip(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    producer = make_queue(
        database_path
    )

    original = Event(
        event_type="ORDER_CREATED",
        timestamp=datetime.now(
            UTC
        ),
        payload={
            "şehir": "İstanbul",
            "order": {
                "id": 99,
                "items": [
                    "a",
                    "b",
                ],
            },
        },
        metadata={
            "source": "İBB",
            "trace": {
                "id": "abc-123",
            },
        },
    )

    producer.publish(
        original,
        message_id="roundtrip",
    )

    producer.close()

    queue = make_queue(
        database_path
    )

    dispatcher = (
        EventDispatcher()
    )

    received: list[
        Event
    ] = []

    def handler(
        event: Event,
    ) -> None:
        received.append(
            event
        )

    dispatcher.register(
        "ORDER_CREATED",
        handler,
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        idle_sleep_seconds=0,
    )

    try:
        worker.run_until_empty()

        assert (
            len(
                received
            )
            == 1
        )

        assert (
            received[
                0
            ].payload
            == original.payload
        )

        assert (
            received[
                0
            ].metadata
            == original.metadata
        )

        assert (
            received[
                0
            ].timestamp
            == original.timestamp
        )

    finally:
        queue.close()


# =============================================================================
# QUEUE NAME ISOLATION
# =============================================================================
def test_queue_name_isolation_is_preserved_through_workers(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    alpha_queue = make_queue(
        database_path,
        name="alpha",
    )

    beta_queue = make_queue(
        database_path,
        name="beta",
    )

    alpha_seen: list[
        int
    ] = []

    beta_seen: list[
        int
    ] = []

    alpha_dispatcher = (
        EventDispatcher()
    )

    beta_dispatcher = (
        EventDispatcher()
    )

    alpha_dispatcher.register(
        "ORDER_CREATED",
        lambda event: (
            alpha_seen.append(
                int(
                    event.payload[
                        "event_id"
                    ]
                )
            )
        ),
    )

    beta_dispatcher.register(
        "ORDER_CREATED",
        lambda event: (
            beta_seen.append(
                int(
                    event.payload[
                        "event_id"
                    ]
                )
            )
        ),
    )

    alpha_worker = EventWorker(
        queue=alpha_queue,
        dispatcher=alpha_dispatcher,
        idle_sleep_seconds=0,
    )

    beta_worker = EventWorker(
        queue=beta_queue,
        dispatcher=beta_dispatcher,
        idle_sleep_seconds=0,
    )

    try:
        alpha_queue.publish(
            make_event(
                event_id=1
            ),
            message_id="same-message-id",
        )

        beta_queue.publish(
            make_event(
                event_id=2
            ),
            message_id="same-message-id",
        )

        alpha_summary = (
            alpha_worker.run_until_empty()
        )

        assert (
            alpha_seen
            == [
                1
            ]
        )

        assert (
            beta_seen
            == []
        )

        assert (
            alpha_summary.processed_count
            == 1
        )

        assert (
            alpha_queue.active_count
            == 0
        )

        assert (
            beta_queue.active_count
            == 1
        )

        beta_summary = (
            beta_worker.run_until_empty()
        )

        assert (
            beta_seen
            == [
                2
            ]
        )

        assert (
            beta_summary.processed_count
            == 1
        )

        assert (
            beta_queue.active_count
            == 0
        )

    finally:
        alpha_queue.close()
        beta_queue.close()


# =============================================================================
# STOP ON ERROR / DURABLE REQUEUE
# =============================================================================
def test_stop_on_error_preserves_failed_event_durably(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    queue = make_queue(
        database_path
    )

    dispatcher = (
        EventDispatcher()
    )

    dispatcher.register(
        "ORDER_CREATED",
        lambda event: (
            (_ for _ in ()).throw(
                RuntimeError(
                    "fatal"
                )
            )
        ),
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        requeue_on_error=True,
        stop_on_error=True,
        idle_sleep_seconds=0,
    )

    try:
        queue.publish(
            make_event(
                event_id=500
            ),
            message_id="fatal-event",
        )

        with pytest.raises(
            EventHandlerExecutionError
        ):
            worker.run_until_empty()

        assert (
            worker.is_running
            is False
        )

        assert (
            queue.pending_count
            == 1
        )

        assert (
            queue.message_snapshot(
                "fatal-event"
            )[
                "delivery_count"
            ]
            == 1
        )

    finally:
        queue.close()

    reopened = make_queue(
        database_path
    )

    try:
        assert (
            reopened.pending_count
            == 1
        )

        assert (
            reopened.contains(
                "fatal-event"
            )
            is True
        )

    finally:
        reopened.close()