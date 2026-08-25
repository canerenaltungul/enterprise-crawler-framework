from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import sleep
from typing import Any

import pytest

from enterprise_crawler.contracts import Event
from enterprise_crawler.events import (
    EventDispatcher,
    EventWorker,
    RetryAction,
    RetryPolicy,
    SQLiteDeadLetterQueue,
    SQLiteEventQueue,
)


UTC = timezone.utc


# =============================================================================
# TEST CLOCK
# =============================================================================
class FakeClock:
    def __init__(
        self,
        value: datetime,
    ) -> None:
        self.current = value

    def __call__(
        self,
    ) -> datetime:
        return self.current

    def advance(
        self,
        *,
        seconds: float,
    ) -> None:
        self.current = (
            self.current
            + timedelta(
                seconds=seconds
            )
        )


def make_clock() -> FakeClock:
    return FakeClock(
        datetime(
            2026,
            8,
            19,
            12,
            0,
            0,
            tzinfo=UTC,
        )
    )


# =============================================================================
# TEST ERRORS
# =============================================================================
class TemporaryError(
    RuntimeError
):
    pass


class PermanentError(
    RuntimeError
):
    pass


# =============================================================================
# HELPERS
# =============================================================================
def make_event(
    *,
    event_id: int = 1,
    value: Any = None,
) -> Event:
    return Event(
        event_type="TEST_EVENT",
        timestamp=datetime.now(
            UTC
        ),
        payload={
            "event_id": (
                event_id
            ),
            "value": (
                value
            ),
        },
        metadata={
            "source": (
                "sqlite-retry-dlq-integration"
            ),
            "trace_id": (
                f"trace-{event_id}"
            ),
        },
    )


def make_source_queue(
    database_path: Path,
    *,
    name: str = "events",
    lease_seconds: float = 30.0,
    clock: Any = None,
) -> SQLiteEventQueue:
    return SQLiteEventQueue(
        database_path,
        name=name,
        lease_seconds=(
            lease_seconds
        ),
        timeout_seconds=10,
        clock=clock,
    )


def make_dlq(
    database_path: Path,
    *,
    name: str = "dead-letter",
) -> SQLiteDeadLetterQueue:
    return SQLiteDeadLetterQueue(
        database_path,
        name=name,
        timeout_seconds=10,
    )


def make_dispatcher(
    handler: Any,
) -> EventDispatcher:
    dispatcher = (
        EventDispatcher()
    )

    dispatcher.register(
        "TEST_EVENT",
        handler,
    )

    return dispatcher


# =============================================================================
# DIRECT DEAD LETTER
# =============================================================================
def test_non_retryable_event_moves_from_sqlite_source_to_sqlite_dlq(
    tmp_path: Path,
) -> None:
    source_db = (
        tmp_path
        / "source.sqlite3"
    )

    dlq_db = (
        tmp_path
        / "dlq.sqlite3"
    )

    source = make_source_queue(
        source_db
    )

    dlq = make_dlq(
        dlq_db
    )

    worker = EventWorker(
        queue=source,
        dispatcher=(
            make_dispatcher(
                lambda event: (
                    (_ for _ in ()).throw(
                        PermanentError(
                            "permanent"
                        )
                    )
                )
            )
        ),
        retry_policy=RetryPolicy(
            max_deliveries=3,
            retry_exceptions=(
                TemporaryError,
            ),
            dead_letter_non_retryable=True,
        ),
        dead_letter_queue=dlq,
        idle_sleep_seconds=0,
    )

    try:
        source.publish(
            make_event(
                event_id=1
            ),
            message_id="message-1",
        )

        summary = (
            worker.run_until_empty()
        )

        assert (
            summary.failure_count
            == 1
        )

        assert (
            source.active_count
            == 0
        )

        assert (
            dlq.count
            == 1
        )

        record = (
            dlq.records()[
                0
            ]
        )

        assert (
            record.message_id
            == "message-1"
        )

        assert (
            record.delivery_count
            == 1
        )

        assert (
            record.failure_type
            == "PermanentError"
        )

        assert (
            record.failure_message
            == "permanent"
        )

        assert (
            record.source_queue
            == "events"
        )

        assert (
            record.event.payload[
                "event_id"
            ]
            == 1
        )

        assert (
            record.metadata[
                "retry_decision"
            ][
                "action"
            ]
            == "dead_letter"
        )

        assert (
            worker.dead_letter_count
            == 1
        )

    finally:
        source.close()
        dlq.close()


# =============================================================================
# RETRY EXHAUSTION -> DURABLE DLQ
# =============================================================================
def test_retry_exhaustion_moves_event_to_sqlite_dlq(
    tmp_path: Path,
) -> None:
    source_db = (
        tmp_path
        / "source.sqlite3"
    )

    dlq_db = (
        tmp_path
        / "dlq.sqlite3"
    )

    source = make_source_queue(
        source_db
    )

    dlq = make_dlq(
        dlq_db
    )

    worker = EventWorker(
        queue=source,
        dispatcher=(
            make_dispatcher(
                lambda event: (
                    (_ for _ in ()).throw(
                        TemporaryError(
                            "still failing"
                        )
                    )
                )
            )
        ),
        retry_policy=RetryPolicy(
            max_deliveries=3,
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        dead_letter_queue=dlq,
        idle_sleep_seconds=0,
    )

    try:
        source.publish(
            make_event(
                event_id=2
            ),
            message_id="message-2",
        )

        first = (
            worker.run_until_empty()
        )

        assert (
            first.failure_count
            == 1
        )

        assert (
            source.pending_count
            == 1
        )

        assert (
            dlq.count
            == 0
        )

        second = (
            worker.run_until_empty()
        )

        assert (
            second.failure_count
            == 1
        )

        assert (
            source.pending_count
            == 1
        )

        assert (
            dlq.count
            == 0
        )

        third = (
            worker.run_until_empty()
        )

        assert (
            third.failure_count
            == 1
        )

        assert (
            source.active_count
            == 0
        )

        assert (
            dlq.count
            == 1
        )

        record = (
            dlq.records()[
                0
            ]
        )

        assert (
            record.message_id
            == "message-2"
        )

        assert (
            record.delivery_count
            == 3
        )

        assert (
            record.failure_type
            == "TemporaryError"
        )

        assert (
            record.failure_message
            == "still failing"
        )

        assert (
            record.metadata[
                "retry_decision"
            ][
                "reason"
            ]
            == "max_deliveries_exhausted"
        )

        assert (
            worker.retry_count
            == 2
        )

        assert (
            worker.dead_letter_count
            == 1
        )

    finally:
        source.close()
        dlq.close()


# =============================================================================
# DURABLE REOPEN
# =============================================================================
def test_dead_letter_record_survives_dlq_reopen(
    tmp_path: Path,
) -> None:
    source_db = (
        tmp_path
        / "source.sqlite3"
    )

    dlq_db = (
        tmp_path
        / "dlq.sqlite3"
    )

    source = make_source_queue(
        source_db
    )

    first_dlq = make_dlq(
        dlq_db
    )

    worker = EventWorker(
        queue=source,
        dispatcher=(
            make_dispatcher(
                lambda event: (
                    (_ for _ in ()).throw(
                        PermanentError(
                            "permanent"
                        )
                    )
                )
            )
        ),
        retry_policy=RetryPolicy(
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        dead_letter_queue=first_dlq,
        idle_sleep_seconds=0,
    )

    source.publish(
        make_event(
            event_id=3,
            value="persistent",
        ),
        message_id="message-3",
    )

    worker.run_until_empty()

    assert (
        first_dlq.count
        == 1
    )

    first_record = (
        first_dlq.records()[
            0
        ]
    )

    dead_letter_id = (
        first_record.dead_letter_id
    )

    source.close()
    first_dlq.close()

    reopened = make_dlq(
        dlq_db
    )

    try:
        assert (
            reopened.count
            == 1
        )

        record = reopened.get(
            dead_letter_id
        )

        assert (
            record.message_id
            == "message-3"
        )

        assert (
            record.event.payload[
                "event_id"
            ]
            == 3
        )

        assert (
            record.event.payload[
                "value"
            ]
            == "persistent"
        )

        assert (
            record.failure_type
            == "PermanentError"
        )

    finally:
        reopened.close()


# =============================================================================
# SOURCE REMAINS EMPTY AFTER REOPEN
# =============================================================================
def test_source_event_remains_removed_after_successful_dlq_transfer_and_reopen(
    tmp_path: Path,
) -> None:
    source_db = (
        tmp_path
        / "source.sqlite3"
    )

    dlq_db = (
        tmp_path
        / "dlq.sqlite3"
    )

    source = make_source_queue(
        source_db
    )

    dlq = make_dlq(
        dlq_db
    )

    worker = EventWorker(
        queue=source,
        dispatcher=(
            make_dispatcher(
                lambda event: (
                    (_ for _ in ()).throw(
                        PermanentError()
                    )
                )
            )
        ),
        retry_policy=RetryPolicy(
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        dead_letter_queue=dlq,
        idle_sleep_seconds=0,
    )

    source.publish(
        make_event(),
        message_id="move-me",
    )

    worker.run_until_empty()

    assert (
        source.active_count
        == 0
    )

    assert (
        dlq.count
        == 1
    )

    source.close()
    dlq.close()

    reopened_source = (
        make_source_queue(
            source_db
        )
    )

    reopened_dlq = (
        make_dlq(
            dlq_db
        )
    )

    try:
        assert (
            reopened_source.active_count
            == 0
        )

        assert (
            reopened_source.contains(
                "move-me"
            )
            is False
        )

        assert (
            reopened_dlq.count
            == 1
        )

    finally:
        reopened_source.close()
        reopened_dlq.close()


# =============================================================================
# DLQ RECORD FULL ROUNDTRIP
# =============================================================================
def test_event_payload_metadata_and_retry_metadata_survive_sqlite_dlq_roundtrip(
    tmp_path: Path,
) -> None:
    source_db = (
        tmp_path
        / "source.sqlite3"
    )

    dlq_db = (
        tmp_path
        / "dlq.sqlite3"
    )

    source = make_source_queue(
        source_db,
        name="source-events",
    )

    dlq = make_dlq(
        dlq_db,
        name="failed-events",
    )

    event = Event(
        event_type="TEST_EVENT",
        timestamp=datetime.now(
            UTC
        ),
        payload={
            "şehir": "İstanbul",
            "nested": {
                "items": [
                    1,
                    2,
                    {
                        "ok": True,
                    },
                ]
            },
        },
        metadata={
            "kaynak": "İBB",
            "trace": {
                "id": "trace-123",
            },
        },
    )

    worker = EventWorker(
        queue=source,
        dispatcher=(
            make_dispatcher(
                lambda event: (
                    (_ for _ in ()).throw(
                        PermanentError(
                            "kalıcı hata"
                        )
                    )
                )
            )
        ),
        retry_policy=RetryPolicy(
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        dead_letter_queue=dlq,
        idle_sleep_seconds=0,
    )

    try:
        source.publish(
            event,
            message_id="unicode-message",
        )

        worker.run_until_empty()

        record = (
            dlq.records()[
                0
            ]
        )

        assert (
            record.message_id
            == "unicode-message"
        )

        assert (
            record.source_queue
            == "source-events"
        )

        assert (
            record.failure_message
            == "kalıcı hata"
        )

        assert (
            record.event.payload
            == event.payload
        )

        assert (
            record.event.metadata
            == event.metadata
        )

        assert (
            record.metadata[
                "worker"
            ]
            == "event-worker"
        )

        assert (
            record.metadata[
                "retry_decision"
            ][
                "action"
            ]
            == "dead_letter"
        )

        assert (
            record.metadata[
                "retry_decision"
            ][
                "error_type"
            ]
            == "PermanentError"
        )

    finally:
        source.close()
        dlq.close()


# =============================================================================
# WORKER SNAPSHOT
# =============================================================================
def test_worker_snapshot_contains_sqlite_dlq_state(
    tmp_path: Path,
) -> None:
    source = make_source_queue(
        tmp_path
        / "source.sqlite3"
    )

    dlq = make_dlq(
        tmp_path
        / "dlq.sqlite3",
        name="failed",
    )

    worker = EventWorker(
        queue=source,
        dispatcher=(
            make_dispatcher(
                lambda event: (
                    (_ for _ in ()).throw(
                        PermanentError()
                    )
                )
            )
        ),
        retry_policy=RetryPolicy(
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        dead_letter_queue=dlq,
        idle_sleep_seconds=0,
    )

    try:
        source.publish(
            make_event(),
            message_id="message",
        )

        worker.run_until_empty()

        snapshot = (
            worker.snapshot()
        )

        assert (
            snapshot[
                "dead_letter_queue_enabled"
            ]
            is True
        )

        assert (
            snapshot[
                "dead_letter_queue"
            ][
                "backend"
            ]
            == "sqlite"
        )

        assert (
            snapshot[
                "dead_letter_queue"
            ][
                "name"
            ]
            == "failed"
        )

        assert (
            snapshot[
                "dead_letter_queue"
            ][
                "count"
            ]
            == 1
        )

        assert (
            snapshot[
                "dead_letter_count"
            ]
            == 1
        )

        assert (
            snapshot[
                "last_retry_decision"
            ][
                "action"
            ]
            == "dead_letter"
        )

    finally:
        source.close()
        dlq.close()


# =============================================================================
# RETRY PERSISTS ACROSS SOURCE REOPEN THEN DLQ
# =============================================================================
def test_retry_state_survives_source_reopen_before_final_sqlite_dlq_transfer(
    tmp_path: Path,
) -> None:
    source_db = (
        tmp_path
        / "source.sqlite3"
    )

    dlq_db = (
        tmp_path
        / "dlq.sqlite3"
    )

    first_source = make_source_queue(
        source_db
    )

    first_dlq = make_dlq(
        dlq_db
    )

    first_worker = EventWorker(
        queue=first_source,
        dispatcher=(
            make_dispatcher(
                lambda event: (
                    (_ for _ in ()).throw(
                        TemporaryError()
                    )
                )
            )
        ),
        retry_policy=RetryPolicy(
            max_deliveries=2,
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        dead_letter_queue=(
            first_dlq
        ),
        idle_sleep_seconds=0,
    )

    first_source.publish(
        make_event(
            event_id=7
        ),
        message_id="persistent-retry",
    )

    first_worker.run_until_empty()

    assert (
        first_source.message_snapshot(
            "persistent-retry"
        )[
            "delivery_count"
        ]
        == 1
    )

    assert (
        first_dlq.count
        == 0
    )

    first_source.close()
    first_dlq.close()

    second_source = (
        make_source_queue(
            source_db
        )
    )

    second_dlq = (
        make_dlq(
            dlq_db
        )
    )

    second_worker = EventWorker(
        queue=second_source,
        dispatcher=(
            make_dispatcher(
                lambda event: (
                    (_ for _ in ()).throw(
                        TemporaryError(
                            "still failing"
                        )
                    )
                )
            )
        ),
        retry_policy=RetryPolicy(
            max_deliveries=2,
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        dead_letter_queue=(
            second_dlq
        ),
        idle_sleep_seconds=0,
    )

    try:
        second_worker.run_until_empty()

        assert (
            second_source.active_count
            == 0
        )

        assert (
            second_dlq.count
            == 1
        )

        record = (
            second_dlq.records()[
                0
            ]
        )

        assert (
            record.message_id
            == "persistent-retry"
        )

        assert (
            record.delivery_count
            == 2
        )

        assert (
            record.event.payload[
                "event_id"
            ]
            == 7
        )

    finally:
        second_source.close()
        second_dlq.close()


# =============================================================================
# LEASE RECOVERY -> DURABLE DLQ
# =============================================================================
def test_claimed_event_recovers_after_lease_and_moves_to_sqlite_dlq(
    tmp_path: Path,
) -> None:
    source_db = (
        tmp_path
        / "source.sqlite3"
    )

    dlq_db = (
        tmp_path
        / "dlq.sqlite3"
    )

    first_source = make_source_queue(
        source_db,
        lease_seconds=0.02,
    )

    first_source.publish(
        make_event(
            event_id=8
        ),
        message_id="lease-recovery",
    )

    claimed = (
        first_source.claim()
    )

    assert (
        claimed
        is not None
    )

    assert (
        claimed.delivery_count
        == 1
    )

    sleep(
        0.04
    )

    second_source = (
        make_source_queue(
            source_db,
            lease_seconds=0.02,
        )
    )

    dlq = make_dlq(
        dlq_db
    )

    worker = EventWorker(
        queue=second_source,
        dispatcher=(
            make_dispatcher(
                lambda event: (
                    (_ for _ in ()).throw(
                        PermanentError()
                    )
                )
            )
        ),
        retry_policy=RetryPolicy(
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        dead_letter_queue=dlq,
        idle_sleep_seconds=0,
    )

    try:
        summary = (
            worker.run_until_empty()
        )

        assert (
            summary.failure_count
            == 1
        )

        assert (
            second_source.active_count
            == 0
        )

        assert (
            dlq.count
            == 1
        )

        record = (
            dlq.records()[
                0
            ]
        )

        assert (
            record.message_id
            == "lease-recovery"
        )

        assert (
            record.delivery_count
            == 2
        )

    finally:
        first_source.close()
        second_source.close()
        dlq.close()


# =============================================================================
# QUEUE NAME ISOLATION
# =============================================================================
def test_sqlite_dlq_name_isolation_is_preserved_through_workers(
    tmp_path: Path,
) -> None:
    source_db = (
        tmp_path
        / "source.sqlite3"
    )

    dlq_db = (
        tmp_path
        / "dlq.sqlite3"
    )

    alpha_source = (
        make_source_queue(
            source_db,
            name="alpha-source",
        )
    )

    beta_source = (
        make_source_queue(
            source_db,
            name="beta-source",
        )
    )

    alpha_dlq = make_dlq(
        dlq_db,
        name="alpha-dlq",
    )

    beta_dlq = make_dlq(
        dlq_db,
        name="beta-dlq",
    )

    alpha_worker = EventWorker(
        queue=alpha_source,
        dispatcher=(
            make_dispatcher(
                lambda event: (
                    (_ for _ in ()).throw(
                        PermanentError(
                            "alpha"
                        )
                    )
                )
            )
        ),
        retry_policy=RetryPolicy(
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        dead_letter_queue=(
            alpha_dlq
        ),
        idle_sleep_seconds=0,
    )

    beta_worker = EventWorker(
        queue=beta_source,
        dispatcher=(
            make_dispatcher(
                lambda event: (
                    (_ for _ in ()).throw(
                        PermanentError(
                            "beta"
                        )
                    )
                )
            )
        ),
        retry_policy=RetryPolicy(
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        dead_letter_queue=(
            beta_dlq
        ),
        idle_sleep_seconds=0,
    )

    try:
        alpha_source.publish(
            make_event(
                event_id=101
            ),
            message_id="same-message",
        )

        beta_source.publish(
            make_event(
                event_id=202
            ),
            message_id="same-message",
        )

        alpha_worker.run_until_empty()
        beta_worker.run_until_empty()

        assert (
            alpha_dlq.count
            == 1
        )

        assert (
            beta_dlq.count
            == 1
        )

        assert (
            alpha_dlq.records()[
                0
            ].event.payload[
                "event_id"
            ]
            == 101
        )

        assert (
            beta_dlq.records()[
                0
            ].event.payload[
                "event_id"
            ]
            == 202
        )

        assert (
            alpha_dlq.records()[
                0
            ].source_queue
            == "alpha-source"
        )

        assert (
            beta_dlq.records()[
                0
            ].source_queue
            == "beta-source"
        )

    finally:
        alpha_source.close()
        beta_source.close()
        alpha_dlq.close()
        beta_dlq.close()


# =============================================================================
# DLQ REMOVE AFTER REOPEN
# =============================================================================
def test_persisted_dead_letter_can_be_removed_after_reopen(
    tmp_path: Path,
) -> None:
    source_db = (
        tmp_path
        / "source.sqlite3"
    )

    dlq_db = (
        tmp_path
        / "dlq.sqlite3"
    )

    source = make_source_queue(
        source_db
    )

    dlq = make_dlq(
        dlq_db
    )

    worker = EventWorker(
        queue=source,
        dispatcher=(
            make_dispatcher(
                lambda event: (
                    (_ for _ in ()).throw(
                        PermanentError()
                    )
                )
            )
        ),
        retry_policy=RetryPolicy(
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        dead_letter_queue=dlq,
        idle_sleep_seconds=0,
    )

    source.publish(
        make_event(),
        message_id="remove-after-reopen",
    )

    worker.run_until_empty()

    record = (
        dlq.records()[
            0
        ]
    )

    dead_letter_id = (
        record.dead_letter_id
    )

    source.close()
    dlq.close()

    reopened = make_dlq(
        dlq_db
    )

    try:
        removed = reopened.remove(
            dead_letter_id
        )

        assert (
            removed.dead_letter_id
            == dead_letter_id
        )

        assert (
            reopened.count
            == 0
        )

    finally:
        reopened.close()


# =============================================================================
# SCHEDULED RETRY -> DUE -> SUCCESS
# =============================================================================
def test_delayed_retry_waits_until_due_then_succeeds(
    tmp_path: Path,
) -> None:
    source_db = (
        tmp_path
        / "source.sqlite3"
    )

    dlq_db = (
        tmp_path
        / "dlq.sqlite3"
    )

    clock = make_clock()

    source = make_source_queue(
        source_db,
        clock=clock,
    )

    dlq = make_dlq(
        dlq_db
    )

    attempts = 0

    def handler(
        event: Event,
    ) -> str:
        nonlocal attempts

        attempts += 1

        if attempts == 1:
            raise TemporaryError(
                "temporary"
            )

        return "success"

    worker = EventWorker(
        queue=source,
        dispatcher=(
            make_dispatcher(
                handler
            )
        ),
        retry_policy=RetryPolicy(
            max_deliveries=3,
            retry_exceptions=(
                TemporaryError,
            ),
            base_delay_seconds=30.0,
        ),
        dead_letter_queue=dlq,
        idle_sleep_seconds=0,
    )

    try:
        source.publish(
            make_event(
                event_id=301
            ),
            message_id="scheduled-success",
        )

        first = (
            worker.run_until_empty()
        )

        assert (
            first.failure_count
            == 1
        )

        assert (
            attempts
            == 1
        )

        scheduled = (
            source.message_snapshot(
                "scheduled-success"
            )
        )

        assert (
            scheduled[
                "state"
            ]
            == "pending"
        )

        assert (
            scheduled[
                "delivery_count"
            ]
            == 1
        )

        assert (
            scheduled[
                "next_attempt_at"
            ]
            == (
                clock.current
                + timedelta(
                    seconds=30
                )
            ).isoformat()
        )

        immediate = (
            worker.run_once()
        )

        assert (
            immediate
            is None
        )

        assert (
            attempts
            == 1
        )

        assert (
            source.message_snapshot(
                "scheduled-success"
            )[
                "delivery_count"
            ]
            == 1
        )

        clock.advance(
            seconds=30
        )

        result = (
            worker.run_once()
        )

        assert (
            result
            is not None
        )

        assert (
            result.value
            == "success"
        )

        assert (
            result.message_id
            == "scheduled-success"
        )

        assert (
            result.delivery_count
            == 2
        )

        assert (
            attempts
            == 2
        )

        assert (
            source.active_count
            == 0
        )

        assert (
            dlq.count
            == 0
        )

    finally:
        source.close()
        dlq.close()


# =============================================================================
# SCHEDULED RETRY SURVIVES SOURCE REOPEN
# =============================================================================
def test_scheduled_retry_survives_source_reopen_until_due(
    tmp_path: Path,
) -> None:
    source_db = (
        tmp_path
        / "source.sqlite3"
    )

    dlq_db = (
        tmp_path
        / "dlq.sqlite3"
    )

    clock = make_clock()

    first_source = make_source_queue(
        source_db,
        clock=clock,
    )

    first_dlq = make_dlq(
        dlq_db
    )

    first_worker = EventWorker(
        queue=first_source,
        dispatcher=(
            make_dispatcher(
                lambda event: (
                    (_ for _ in ()).throw(
                        TemporaryError(
                            "temporary"
                        )
                    )
                )
            )
        ),
        retry_policy=RetryPolicy(
            max_deliveries=3,
            retry_exceptions=(
                TemporaryError,
            ),
            base_delay_seconds=60.0,
        ),
        dead_letter_queue=first_dlq,
        idle_sleep_seconds=0,
    )

    first_source.publish(
        make_event(
            event_id=302
        ),
        message_id="reopen-scheduled",
    )

    first_worker.run_until_empty()

    before_close = (
        first_source.message_snapshot(
            "reopen-scheduled"
        )
    )

    expected_next_attempt_at = (
        clock.current
        + timedelta(
            seconds=60
        )
    ).isoformat()

    assert (
        before_close[
            "delivery_count"
        ]
        == 1
    )

    assert (
        before_close[
            "next_attempt_at"
        ]
        == expected_next_attempt_at
    )

    first_source.close()
    first_dlq.close()

    second_source = make_source_queue(
        source_db,
        clock=clock,
    )

    second_dlq = make_dlq(
        dlq_db
    )

    second_worker = EventWorker(
        queue=second_source,
        dispatcher=(
            make_dispatcher(
                lambda event: "recovered"
            )
        ),
        retry_policy=RetryPolicy(
            max_deliveries=3,
            retry_exceptions=(
                TemporaryError,
            ),
            base_delay_seconds=60.0,
        ),
        dead_letter_queue=second_dlq,
        idle_sleep_seconds=0,
    )

    try:
        reopened = (
            second_source.message_snapshot(
                "reopen-scheduled"
            )
        )

        assert (
            reopened[
                "delivery_count"
            ]
            == 1
        )

        assert (
            reopened[
                "next_attempt_at"
            ]
            == expected_next_attempt_at
        )

        assert (
            second_worker.run_once()
            is None
        )

        assert (
            second_source.message_snapshot(
                "reopen-scheduled"
            )[
                "delivery_count"
            ]
            == 1
        )

        clock.advance(
            seconds=60
        )

        result = (
            second_worker.run_once()
        )

        assert (
            result
            is not None
        )

        assert (
            result.value
            == "recovered"
        )

        assert (
            result.delivery_count
            == 2
        )

        assert (
            second_source.active_count
            == 0
        )

        assert (
            second_dlq.count
            == 0
        )

    finally:
        second_source.close()
        second_dlq.close()


# =============================================================================
# EXPONENTIAL SCHEDULED RETRY -> SQLITE DLQ
# =============================================================================
def test_exponential_scheduled_retries_exhaust_into_sqlite_dlq(
    tmp_path: Path,
) -> None:
    source_db = (
        tmp_path
        / "source.sqlite3"
    )

    dlq_db = (
        tmp_path
        / "dlq.sqlite3"
    )

    clock = make_clock()

    source = make_source_queue(
        source_db,
        clock=clock,
    )

    dlq = make_dlq(
        dlq_db
    )

    worker = EventWorker(
        queue=source,
        dispatcher=(
            make_dispatcher(
                lambda event: (
                    (_ for _ in ()).throw(
                        TemporaryError(
                            "still failing"
                        )
                    )
                )
            )
        ),
        retry_policy=RetryPolicy(
            max_deliveries=3,
            retry_exceptions=(
                TemporaryError,
            ),
            base_delay_seconds=5.0,
            backoff_multiplier=2.0,
        ),
        dead_letter_queue=dlq,
        idle_sleep_seconds=0,
    )

    try:
        source.publish(
            make_event(
                event_id=303
            ),
            message_id="scheduled-exhaustion",
        )

        first = (
            worker.run_until_empty()
        )

        assert (
            first.failure_count
            == 1
        )

        first_state = (
            source.message_snapshot(
                "scheduled-exhaustion"
            )
        )

        assert (
            first_state[
                "delivery_count"
            ]
            == 1
        )

        assert (
            first_state[
                "next_attempt_at"
            ]
            == (
                clock.current
                + timedelta(
                    seconds=5
                )
            ).isoformat()
        )

        assert (
            worker.last_retry_decision
            is not None
        )

        assert (
            worker.last_retry_decision.retry_delay_seconds
            == 5.0
        )

        assert (
            worker.run_once()
            is None
        )

        clock.advance(
            seconds=5
        )

        second = (
            worker.run_until_empty()
        )

        assert (
            second.failure_count
            == 1
        )

        second_state = (
            source.message_snapshot(
                "scheduled-exhaustion"
            )
        )

        assert (
            second_state[
                "delivery_count"
            ]
            == 2
        )

        assert (
            second_state[
                "next_attempt_at"
            ]
            == (
                clock.current
                + timedelta(
                    seconds=10
                )
            ).isoformat()
        )

        assert (
            worker.last_retry_decision
            is not None
        )

        assert (
            worker.last_retry_decision.retry_delay_seconds
            == 10.0
        )

        clock.advance(
            seconds=10
        )

        third = (
            worker.run_until_empty()
        )

        assert (
            third.failure_count
            == 1
        )

        assert (
            source.active_count
            == 0
        )

        assert (
            dlq.count
            == 1
        )

        record = (
            dlq.records()[
                0
            ]
        )

        assert (
            record.message_id
            == "scheduled-exhaustion"
        )

        assert (
            record.delivery_count
            == 3
        )

        assert (
            record.failure_type
            == "TemporaryError"
        )

        assert (
            record.metadata[
                "retry_decision"
            ][
                "reason"
            ]
            == "max_deliveries_exhausted"
        )

        assert (
            record.metadata[
                "retry_decision"
            ][
                "retry_delay_seconds"
            ]
            == 0.0
        )

        assert (
            worker.retry_count
            == 2
        )

        assert (
            worker.dead_letter_count
            == 1
        )

    finally:
        source.close()
        dlq.close()


# =============================================================================
# FUTURE HEAD DOES NOT BLOCK DUE MESSAGE THROUGH WORKER
# =============================================================================
def test_future_scheduled_retry_does_not_block_due_event_through_worker(
    tmp_path: Path,
) -> None:
    source_db = (
        tmp_path
        / "source.sqlite3"
    )

    dlq_db = (
        tmp_path
        / "dlq.sqlite3"
    )

    clock = make_clock()

    source = make_source_queue(
        source_db,
        clock=clock,
    )

    dlq = make_dlq(
        dlq_db
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

        if event_id == 304:
            raise TemporaryError(
                "delay this event"
            )

        processed.append(
            event_id
        )

    worker = EventWorker(
        queue=source,
        dispatcher=(
            make_dispatcher(
                handler
            )
        ),
        retry_policy=RetryPolicy(
            max_deliveries=4,
            retry_exceptions=(
                TemporaryError,
            ),
            base_delay_seconds=120.0,
        ),
        dead_letter_queue=dlq,
        idle_sleep_seconds=0,
    )

    try:
        source.publish(
            make_event(
                event_id=304
            ),
            message_id="future-head",
        )

        first = (
            worker.run_until_empty()
        )

        assert (
            first.failure_count
            == 1
        )

        assert (
            source.message_snapshot(
                "future-head"
            )[
                "next_attempt_at"
            ]
            is not None
        )

        source.publish(
            make_event(
                event_id=305
            ),
            message_id="due-behind-head",
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
            processed
            == [
                305
            ]
        )

        assert (
            source.contains(
                "future-head"
            )
            is True
        )

        assert (
            source.contains(
                "due-behind-head"
            )
            is False
        )

        assert (
            source.pending_count
            == 1
        )

        assert (
            dlq.count
            == 0
        )

    finally:
        source.close()
        dlq.close()


# =============================================================================
# TWO WORKERS / DUE SCHEDULED EVENT
# =============================================================================
def test_two_workers_do_not_duplicate_due_scheduled_retry(
    tmp_path: Path,
) -> None:
    source_db = (
        tmp_path
        / "source.sqlite3"
    )

    dlq_db = (
        tmp_path
        / "dlq.sqlite3"
    )

    clock = make_clock()

    producer = make_source_queue(
        source_db,
        name="jobs",
        clock=clock,
    )

    scheduler_dlq = make_dlq(
        dlq_db,
        name="scheduler-dlq",
    )

    scheduling_worker = EventWorker(
        queue=producer,
        dispatcher=(
            make_dispatcher(
                lambda event: (
                    (_ for _ in ()).throw(
                        TemporaryError(
                            "schedule once"
                        )
                    )
                )
            )
        ),
        retry_policy=RetryPolicy(
            max_deliveries=3,
            retry_exceptions=(
                TemporaryError,
            ),
            base_delay_seconds=10.0,
        ),
        dead_letter_queue=(
            scheduler_dlq
        ),
        idle_sleep_seconds=0,
    )

    producer.publish(
        make_event(
            event_id=306
        ),
        message_id="due-once",
    )

    scheduling_worker.run_until_empty()

    assert (
        producer.message_snapshot(
            "due-once"
        )[
            "delivery_count"
        ]
        == 1
    )

    assert (
        producer.claim()
        is None
    )

    clock.advance(
        seconds=10
    )

    queue_a = make_source_queue(
        source_db,
        name="jobs",
        clock=clock,
    )

    queue_b = make_source_queue(
        source_db,
        name="jobs",
        clock=clock,
    )

    processed: list[
        int
    ] = []

    processed_lock = (
        threading.Lock()
    )

    barrier = (
        threading.Barrier(
            2
        )
    )

    def success_handler(
        event: Event,
    ) -> str:
        with processed_lock:
            processed.append(
                int(
                    event.payload[
                        "event_id"
                    ]
                )
            )

        return "ok"

    dispatcher = (
        make_dispatcher(
            success_handler
        )
    )

    worker_a = EventWorker(
        queue=queue_a,
        dispatcher=dispatcher,
        name="scheduled-worker-a",
        retry_policy=RetryPolicy(
            max_deliveries=3,
            retry_exceptions=(
                TemporaryError,
            ),
            base_delay_seconds=10.0,
        ),
        dead_letter_queue=(
            scheduler_dlq
        ),
        idle_sleep_seconds=0,
    )

    worker_b = EventWorker(
        queue=queue_b,
        dispatcher=dispatcher,
        name="scheduled-worker-b",
        retry_policy=RetryPolicy(
            max_deliveries=3,
            retry_exceptions=(
                TemporaryError,
            ),
            base_delay_seconds=10.0,
        ),
        dead_letter_queue=(
            scheduler_dlq
        ),
        idle_sleep_seconds=0,
    )

    results: list[
        Any
    ] = []

    results_lock = (
        threading.Lock()
    )

    def run_worker(
        worker: EventWorker,
    ) -> None:
        barrier.wait(
            timeout=2
        )

        result = (
            worker.run_once()
        )

        with results_lock:
            results.append(
                result
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
            timeout=5
        )

        thread_b.join(
            timeout=5
        )

        assert (
            thread_a.is_alive()
            is False
        )

        assert (
            thread_b.is_alive()
            is False
        )

        successful_results = [
            result
            for result
            in results
            if result
            is not None
        ]

        assert (
            len(
                successful_results
            )
            == 1
        )

        assert (
            successful_results[
                0
            ].message_id
            == "due-once"
        )

        assert (
            successful_results[
                0
            ].delivery_count
            == 2
        )

        assert (
            processed
            == [
                306
            ]
        )

        assert (
            producer.active_count
            == 0
        )

        assert (
            scheduler_dlq.count
            == 0
        )

    finally:
        producer.close()
        queue_a.close()
        queue_b.close()
        scheduler_dlq.close()


# =============================================================================
# LEASE RECOVERY -> SCHEDULED RETRY
# =============================================================================
def test_lease_recovery_can_schedule_next_retry_without_losing_delivery_count(
    tmp_path: Path,
) -> None:
    source_db = (
        tmp_path
        / "source.sqlite3"
    )

    dlq_db = (
        tmp_path
        / "dlq.sqlite3"
    )

    clock = make_clock()

    first_source = make_source_queue(
        source_db,
        lease_seconds=10.0,
        clock=clock,
    )

    first_source.publish(
        make_event(
            event_id=307
        ),
        message_id="lease-to-schedule",
    )

    stale_claim = (
        first_source.claim()
    )

    assert (
        stale_claim
        is not None
    )

    assert (
        stale_claim.delivery_count
        == 1
    )

    clock.advance(
        seconds=10
    )

    second_source = make_source_queue(
        source_db,
        lease_seconds=10.0,
        clock=clock,
    )

    dlq = make_dlq(
        dlq_db
    )

    worker = EventWorker(
        queue=second_source,
        dispatcher=(
            make_dispatcher(
                lambda event: (
                    (_ for _ in ()).throw(
                        TemporaryError(
                            "retry after recovery"
                        )
                    )
                )
            )
        ),
        retry_policy=RetryPolicy(
            max_deliveries=4,
            retry_exceptions=(
                TemporaryError,
            ),
            base_delay_seconds=5.0,
            backoff_multiplier=2.0,
        ),
        dead_letter_queue=dlq,
        idle_sleep_seconds=0,
    )

    try:
        summary = (
            worker.run_until_empty()
        )

        assert (
            summary.failure_count
            == 1
        )

        state = (
            second_source.message_snapshot(
                "lease-to-schedule"
            )
        )

        assert (
            state[
                "state"
            ]
            == "pending"
        )

        assert (
            state[
                "delivery_count"
            ]
            == 2
        )

        assert (
            state[
                "next_attempt_at"
            ]
            == (
                clock.current
                + timedelta(
                    seconds=10
                )
            ).isoformat()
        )

        assert (
            worker.last_retry_decision
            is not None
        )

        assert (
            worker.last_retry_decision.delivery_count
            == 2
        )

        assert (
            worker.last_retry_decision.retry_delay_seconds
            == 10.0
        )

        assert (
            worker.run_once()
            is None
        )

        assert (
            second_source.message_snapshot(
                "lease-to-schedule"
            )[
                "delivery_count"
            ]
            == 2
        )

    finally:
        first_source.close()
        second_source.close()
        dlq.close()