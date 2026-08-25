from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from time import sleep
from typing import Any

import pytest

from enterprise_crawler.contracts import Event
from enterprise_crawler.events import (
    EventDispatcher,
    EventWorker,
    EventWorkerDeadLetterError,
    InMemoryDeadLetterQueue,
    RetryAction,
    RetryPolicy,
    SQLiteEventQueue,
)


UTC = timezone.utc


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


class DiscardError(
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
                "sqlite-retry-integration"
            ),
            "trace_id": (
                f"trace-{event_id}"
            ),
        },
    )


def make_queue(
    database_path: Path,
    *,
    name: str = "events",
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
# RETRY
# =============================================================================
def test_sqlite_event_is_retried_after_first_failure(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    queue = make_queue(
        database_path
    )

    worker = EventWorker(
        queue=queue,
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
        ),
        dead_letter_queue=(
            InMemoryDeadLetterQueue()
        ),
        idle_sleep_seconds=0,
    )

    try:
        queue.publish(
            make_event(),
            message_id="retry-event",
        )

        summary = (
            worker.run_until_empty()
        )

        assert (
            summary.failure_count
            == 1
        )

        assert (
            queue.pending_count
            == 1
        )

        assert (
            queue.claimed_count
            == 0
        )

        snapshot = (
            queue.message_snapshot(
                "retry-event"
            )
        )

        assert (
            snapshot[
                "delivery_count"
            ]
            == 1
        )

        assert (
            worker.retry_count
            == 1
        )

        assert (
            worker.last_retry_decision
            is not None
        )

        assert (
            worker.last_retry_decision.action
            is RetryAction.RETRY
        )

    finally:
        queue.close()


def test_sqlite_delivery_count_persists_across_retry_runs(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    queue = make_queue(
        database_path
    )

    worker = EventWorker(
        queue=queue,
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
            max_deliveries=5,
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        dead_letter_queue=(
            InMemoryDeadLetterQueue()
        ),
        idle_sleep_seconds=0,
    )

    try:
        queue.publish(
            make_event(),
            message_id="delivery-count",
        )

        worker.run_until_empty()

        first = (
            queue.message_snapshot(
                "delivery-count"
            )
        )

        assert (
            first[
                "delivery_count"
            ]
            == 1
        )

        worker.run_until_empty()

        second = (
            queue.message_snapshot(
                "delivery-count"
            )
        )

        assert (
            second[
                "delivery_count"
            ]
            == 2
        )

        worker.run_until_empty()

        third = (
            queue.message_snapshot(
                "delivery-count"
            )
        )

        assert (
            third[
                "delivery_count"
            ]
            == 3
        )

    finally:
        queue.close()


# =============================================================================
# EXHAUSTION -> DLQ
# =============================================================================
def test_exhausted_sqlite_event_moves_to_dead_letter_queue(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    queue = make_queue(
        database_path
    )

    dlq = (
        InMemoryDeadLetterQueue()
    )

    worker = EventWorker(
        queue=queue,
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
        queue.publish(
            make_event(
                event_id=10
            ),
            message_id="exhaust-me",
        )

        first = (
            worker.run_until_empty()
        )

        assert (
            first.failure_count
            == 1
        )

        assert (
            queue.pending_count
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
            queue.pending_count
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
            queue.active_count
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
            == "exhaust-me"
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
            record.source_queue
            == "events"
        )

        assert (
            record.event.payload[
                "event_id"
            ]
            == 10
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
        queue.close()


# =============================================================================
# NON-RETRYABLE -> DIRECT DLQ
# =============================================================================
def test_non_retryable_sqlite_event_goes_directly_to_dlq(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    queue = make_queue(
        database_path
    )

    dlq = (
        InMemoryDeadLetterQueue()
    )

    worker = EventWorker(
        queue=queue,
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
            max_deliveries=5,
            retry_exceptions=(
                TemporaryError,
            ),
            dead_letter_non_retryable=True,
        ),
        dead_letter_queue=dlq,
        idle_sleep_seconds=0,
    )

    try:
        queue.publish(
            make_event(
                event_id=20
            ),
            message_id="permanent-event",
        )

        summary = (
            worker.run_until_empty()
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
            dlq.count
            == 1
        )

        record = (
            dlq.records()[
                0
            ]
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
            record.metadata[
                "retry_decision"
            ][
                "reason"
            ]
            == "non_retryable_error"
        )

        assert (
            worker.retry_count
            == 0
        )

        assert (
            worker.dead_letter_count
            == 1
        )

    finally:
        queue.close()


# =============================================================================
# DISCARD
# =============================================================================
def test_discard_exception_removes_sqlite_event_without_dlq(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    queue = make_queue(
        database_path
    )

    dlq = (
        InMemoryDeadLetterQueue()
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=(
            make_dispatcher(
                lambda event: (
                    (_ for _ in ()).throw(
                        DiscardError(
                            "invalid payload"
                        )
                    )
                )
            )
        ),
        retry_policy=RetryPolicy(
            retry_exceptions=(
                Exception,
            ),
            discard_exceptions=(
                DiscardError,
            ),
        ),
        dead_letter_queue=dlq,
        idle_sleep_seconds=0,
    )

    try:
        queue.publish(
            make_event(),
            message_id="discard-event",
        )

        summary = (
            worker.run_until_empty()
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
            dlq.count
            == 0
        )

        assert (
            worker.discard_count
            == 1
        )

        assert (
            worker.dead_letter_count
            == 0
        )

        assert (
            worker.last_retry_decision
            is not None
        )

        assert (
            worker.last_retry_decision.action
            is RetryAction.DISCARD
        )

    finally:
        queue.close()


# =============================================================================
# RETRY -> SUCCESS
# =============================================================================
def test_retryable_sqlite_event_can_succeed_on_next_delivery(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    queue = make_queue(
        database_path
    )

    dlq = (
        InMemoryDeadLetterQueue()
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
        queue=queue,
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
        ),
        dead_letter_queue=dlq,
        idle_sleep_seconds=0,
    )

    try:
        queue.publish(
            make_event(),
            message_id="event",
        )

        first = (
            worker.run_until_empty()
        )

        assert (
            first.failure_count
            == 1
        )

        assert (
            queue.pending_count
            == 1
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
            result.delivery_count
            == 2
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
            dlq.count
            == 0
        )

        assert (
            worker.total_processed
            == 1
        )

        assert (
            worker.total_failures
            == 1
        )

    finally:
        queue.close()


# =============================================================================
# REOPEN PERSISTENCE
# =============================================================================
def test_retry_state_survives_sqlite_queue_reopen(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    first_queue = make_queue(
        database_path
    )

    first_worker = EventWorker(
        queue=first_queue,
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
            max_deliveries=3,
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        dead_letter_queue=(
            InMemoryDeadLetterQueue()
        ),
        idle_sleep_seconds=0,
    )

    first_queue.publish(
        make_event(),
        message_id="persistent-retry",
    )

    first_worker.run_until_empty()

    assert (
        first_queue.message_snapshot(
            "persistent-retry"
        )[
            "delivery_count"
        ]
        == 1
    )

    first_queue.close()

    second_queue = make_queue(
        database_path
    )

    second_worker = EventWorker(
        queue=second_queue,
        dispatcher=(
            make_dispatcher(
                lambda event: "ok"
            )
        ),
        retry_policy=RetryPolicy(
            max_deliveries=3,
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        dead_letter_queue=(
            InMemoryDeadLetterQueue()
        ),
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

        assert (
            result.delivery_count
            == 2
        )

        assert (
            second_queue.active_count
            == 0
        )

    finally:
        second_queue.close()


# =============================================================================
# DLQ FAILURE -> FAIL CLOSED
# =============================================================================
class FailingDeadLetterQueue:
    @property
    def count(
        self,
    ) -> int:
        return 0

    def store(
        self,
        event: Event,
        *,
        message_id: str,
        delivery_count: int,
        error: BaseException,
        dead_letter_id: str | None = None,
        source_queue: str | None = None,
        claim_token: str | None = None,
        metadata: Any = None,
    ) -> Any:
        raise RuntimeError(
            "DLQ unavailable"
        )

    def get(
        self,
        dead_letter_id: str,
    ) -> Any:
        raise KeyError(
            dead_letter_id
        )

    def remove(
        self,
        dead_letter_id: str,
    ) -> Any:
        raise KeyError(
            dead_letter_id
        )

    def snapshot(
        self,
    ) -> dict[str, Any]:
        return {
            "backend": "failing",
            "count": 0,
        }


def test_dlq_failure_keeps_sqlite_source_event_claimed(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    queue = make_queue(
        database_path,
        lease_seconds=30,
    )

    worker = EventWorker(
        queue=queue,
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
        dead_letter_queue=(
            FailingDeadLetterQueue()
        ),
        idle_sleep_seconds=0,
    )

    try:
        queue.publish(
            make_event(),
            message_id="protected",
        )

        with pytest.raises(
            RuntimeError,
            match="DLQ unavailable",
        ):
            worker.run_once()

        snapshot = (
            queue.message_snapshot(
                "protected"
            )
        )

        assert (
            snapshot[
                "state"
            ]
            == "claimed"
        )

        assert (
            snapshot[
                "delivery_count"
            ]
            == 1
        )

        assert (
            queue.active_count
            == 1
        )

        assert (
            queue.claimed_count
            == 1
        )

        assert (
            worker.dead_letter_count
            == 0
        )

    finally:
        queue.close()


def test_missing_dlq_keeps_sqlite_source_event_claimed(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    queue = make_queue(
        database_path
    )

    worker = EventWorker(
        queue=queue,
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
        dead_letter_queue=None,
        idle_sleep_seconds=0,
    )

    try:
        queue.publish(
            make_event(),
            message_id="protected",
        )

        with pytest.raises(
            EventWorkerDeadLetterError
        ):
            worker.run_once()

        snapshot = (
            queue.message_snapshot(
                "protected"
            )
        )

        assert (
            snapshot[
                "state"
            ]
            == "claimed"
        )

        assert (
            queue.active_count
            == 1
        )

    finally:
        queue.close()


# =============================================================================
# LEASE RECOVERY AFTER DLQ FAILURE
# =============================================================================
def test_failed_dlq_event_can_be_recovered_after_lease_expiry(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    first_queue = make_queue(
        database_path,
        lease_seconds=0.02,
    )

    failing_worker = EventWorker(
        queue=first_queue,
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
        dead_letter_queue=(
            FailingDeadLetterQueue()
        ),
        idle_sleep_seconds=0,
    )

    first_queue.publish(
        make_event(
            event_id=50
        ),
        message_id="recover-me",
    )

    with pytest.raises(
        RuntimeError,
        match="DLQ unavailable",
    ):
        failing_worker.run_once()

    assert (
        first_queue.claimed_count
        == 1
    )

    sleep(
        0.04
    )

    second_queue = make_queue(
        database_path,
        lease_seconds=0.02,
    )

    dlq = (
        InMemoryDeadLetterQueue()
    )

    recovery_worker = EventWorker(
        queue=second_queue,
        dispatcher=(
            make_dispatcher(
                lambda event: (
                    (_ for _ in ()).throw(
                        PermanentError(
                            "still permanent"
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
        summary = (
            recovery_worker.run_until_empty()
        )

        assert (
            summary.failure_count
            == 1
        )

        assert (
            second_queue.active_count
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
            == "recover-me"
        )

        assert (
            record.delivery_count
            == 2
        )

        assert (
            record.event.payload[
                "event_id"
            ]
            == 50
        )

    finally:
        first_queue.close()
        second_queue.close()


# =============================================================================
# SOURCE REMOVAL ORDER
# =============================================================================
class ObservingDeadLetterQueue(
    InMemoryDeadLetterQueue
):
    def __init__(
        self,
        source_queue: SQLiteEventQueue,
    ) -> None:
        super().__init__()

        self.source_queue = (
            source_queue
        )

        self.source_was_present_during_store = False

    def store(
        self,
        event: Event,
        *,
        message_id: str,
        delivery_count: int,
        error: BaseException,
        dead_letter_id: str | None = None,
        source_queue: str | None = None,
        claim_token: str | None = None,
        metadata: Any = None,
    ) -> Any:
        self.source_was_present_during_store = (
            self.source_queue.contains(
                message_id
            )
        )

        return super().store(
            event,
            message_id=(
                message_id
            ),
            delivery_count=(
                delivery_count
            ),
            error=error,
            dead_letter_id=(
                dead_letter_id
            ),
            source_queue=(
                source_queue
            ),
            claim_token=(
                claim_token
            ),
            metadata=(
                metadata
            ),
        )


def test_sqlite_source_is_removed_only_after_dlq_store_succeeds(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "events.sqlite3"
    )

    queue = make_queue(
        database_path
    )

    dlq = (
        ObservingDeadLetterQueue(
            queue
        )
    )

    worker = EventWorker(
        queue=queue,
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
        queue.publish(
            make_event(),
            message_id="ordered-transfer",
        )

        worker.run_until_empty()

        assert (
            dlq.source_was_present_during_store
            is True
        )

        assert (
            dlq.count
            == 1
        )

        assert (
            queue.contains(
                "ordered-transfer"
            )
            is False
        )

    finally:
        queue.close()