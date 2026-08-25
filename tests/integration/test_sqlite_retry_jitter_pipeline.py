from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from enterprise_crawler.contracts import Event
from enterprise_crawler.events import (
    EventDispatcher,
    EventWorker,
    RetryPolicy,
    SQLiteDeadLetterQueue,
    SQLiteEventQueue,
)


UTC = timezone.utc


class TemporaryError(RuntimeError):
    pass


class FakeClock:
    def __init__(self, value: datetime) -> None:
        self.current = value

    def __call__(self) -> datetime:
        return self.current

    def advance(self, *, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)


class SequenceRandomSource:
    def __init__(self, *values: float) -> None:
        self._values = list(values)
        self.call_count = 0

    def __call__(self) -> float:
        if self.call_count >= len(self._values):
            raise AssertionError(
                "random_source beklenenden fazla çağrıldı."
            )

        value = self._values[self.call_count]
        self.call_count += 1
        return value


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


def make_event(
    *,
    event_id: int = 1,
    value: Any = None,
) -> Event:
    return Event(
        event_type="TEST_EVENT",
        timestamp=datetime.now(UTC),
        payload={
            "event_id": event_id,
            "value": value,
        },
        metadata={
            "source": "sqlite-retry-jitter-integration",
            "trace_id": f"trace-{event_id}",
        },
    )


def make_queue(
    database_path: Path,
    *,
    clock: FakeClock,
    name: str = "events",
) -> SQLiteEventQueue:
    return SQLiteEventQueue(
        database_path,
        name=name,
        lease_seconds=30.0,
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


def make_dispatcher(handler: Any) -> EventDispatcher:
    dispatcher = EventDispatcher()
    dispatcher.register("TEST_EVENT", handler)
    return dispatcher


def test_full_jitter_delay_is_persisted_and_enforced_by_sqlite_worker(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "events.sqlite3"
    clock = make_clock()
    random_source = SequenceRandomSource(0.25)
    queue = make_queue(database_path, clock=clock)

    attempts = 0

    def handler(event: Event) -> str:
        nonlocal attempts
        attempts += 1

        if attempts == 1:
            raise TemporaryError("temporary")

        return "success"

    worker = EventWorker(
        queue=queue,
        dispatcher=make_dispatcher(handler),
        retry_policy=RetryPolicy(
            max_deliveries=3,
            retry_exceptions=(TemporaryError,),
            base_delay_seconds=40.0,
            jitter_ratio=1.0,
            random_source=random_source,
        ),
        idle_sleep_seconds=0,
    )

    try:
        queue.publish(
            make_event(event_id=1),
            message_id="jittered-success",
        )

        first = worker.run_until_empty()

        assert first.failure_count == 1
        assert attempts == 1
        assert random_source.call_count == 1
        assert worker.last_retry_decision is not None
        assert worker.last_retry_decision.retry_delay_seconds == 10.0

        state = queue.message_snapshot("jittered-success")

        assert state["delivery_count"] == 1
        assert state["next_attempt_at"] == (
            clock.current + timedelta(seconds=10)
        ).isoformat()

        assert worker.run_once() is None
        assert attempts == 1
        assert (
            queue.message_snapshot("jittered-success")["delivery_count"]
            == 1
        )

        clock.advance(seconds=10)

        result = worker.run_once()

        assert result is not None
        assert result.value == "success"
        assert result.delivery_count == 2
        assert attempts == 2
        assert queue.active_count == 0

    finally:
        queue.close()


def test_partial_jitter_is_recomputed_for_each_retry_delivery(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "events.sqlite3"
    clock = make_clock()
    random_source = SequenceRandomSource(0.0, 1.0)
    queue = make_queue(database_path, clock=clock)

    worker = EventWorker(
        queue=queue,
        dispatcher=make_dispatcher(
            lambda event: (
                (_ for _ in ()).throw(
                    TemporaryError("still failing")
                )
            )
        ),
        retry_policy=RetryPolicy(
            max_deliveries=4,
            retry_exceptions=(TemporaryError,),
            base_delay_seconds=10.0,
            backoff_multiplier=2.0,
            jitter_ratio=0.5,
            random_source=random_source,
        ),
        idle_sleep_seconds=0,
    )

    try:
        queue.publish(
            make_event(event_id=2),
            message_id="partial-jitter",
        )

        first = worker.run_until_empty()

        assert first.failure_count == 1
        assert worker.last_retry_decision is not None
        assert worker.last_retry_decision.delivery_count == 1
        assert worker.last_retry_decision.retry_delay_seconds == 5.0
        assert queue.message_snapshot("partial-jitter")[
            "next_attempt_at"
        ] == (
            clock.current + timedelta(seconds=5)
        ).isoformat()

        clock.advance(seconds=5)

        second = worker.run_until_empty()

        assert second.failure_count == 1
        assert worker.last_retry_decision is not None
        assert worker.last_retry_decision.delivery_count == 2
        assert worker.last_retry_decision.retry_delay_seconds == 20.0
        assert queue.message_snapshot("partial-jitter")[
            "next_attempt_at"
        ] == (
            clock.current + timedelta(seconds=20)
        ).isoformat()
        assert random_source.call_count == 2
        assert (
            queue.message_snapshot("partial-jitter")["delivery_count"]
            == 2
        )

    finally:
        queue.close()


def test_jittered_retry_schedule_survives_sqlite_reopen(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "events.sqlite3"
    clock = make_clock()
    random_source = SequenceRandomSource(0.5)

    first_queue = make_queue(
        database_path,
        clock=clock,
    )

    first_worker = EventWorker(
        queue=first_queue,
        dispatcher=make_dispatcher(
            lambda event: (
                (_ for _ in ()).throw(
                    TemporaryError("temporary")
                )
            )
        ),
        retry_policy=RetryPolicy(
            max_deliveries=3,
            retry_exceptions=(TemporaryError,),
            base_delay_seconds=60.0,
            jitter_ratio=1.0,
            random_source=random_source,
        ),
        idle_sleep_seconds=0,
    )

    first_queue.publish(
        make_event(event_id=3),
        message_id="jitter-reopen",
    )

    first_worker.run_until_empty()

    expected_next_attempt_at = (
        clock.current + timedelta(seconds=30)
    ).isoformat()

    before_close = first_queue.message_snapshot("jitter-reopen")

    assert before_close["delivery_count"] == 1
    assert before_close["next_attempt_at"] == expected_next_attempt_at
    assert random_source.call_count == 1

    first_queue.close()

    second_queue = make_queue(
        database_path,
        clock=clock,
    )

    second_worker = EventWorker(
        queue=second_queue,
        dispatcher=make_dispatcher(
            lambda event: "recovered"
        ),
        retry_policy=RetryPolicy(
            max_deliveries=3,
            retry_exceptions=(TemporaryError,),
            base_delay_seconds=60.0,
            jitter_ratio=1.0,
            random_source=lambda: 1.0,
        ),
        idle_sleep_seconds=0,
    )

    try:
        reopened = second_queue.message_snapshot("jitter-reopen")

        assert reopened["delivery_count"] == 1
        assert reopened["next_attempt_at"] == expected_next_attempt_at
        assert second_worker.run_once() is None

        clock.advance(seconds=30)

        result = second_worker.run_once()

        assert result is not None
        assert result.value == "recovered"
        assert result.delivery_count == 2
        assert second_queue.active_count == 0

    finally:
        second_queue.close()


def test_jittered_capped_retries_exhaust_into_sqlite_dlq(
    tmp_path: Path,
) -> None:
    source_db = tmp_path / "source.sqlite3"
    dlq_db = tmp_path / "dlq.sqlite3"
    clock = make_clock()
    random_source = SequenceRandomSource(1.0, 0.5)

    queue = make_queue(
        source_db,
        clock=clock,
    )
    dlq = make_dlq(dlq_db)

    worker = EventWorker(
        queue=queue,
        dispatcher=make_dispatcher(
            lambda event: (
                (_ for _ in ()).throw(
                    TemporaryError("still failing")
                )
            )
        ),
        retry_policy=RetryPolicy(
            max_deliveries=3,
            retry_exceptions=(TemporaryError,),
            base_delay_seconds=10.0,
            backoff_multiplier=10.0,
            max_delay_seconds=30.0,
            jitter_ratio=1.0,
            random_source=random_source,
        ),
        dead_letter_queue=dlq,
        idle_sleep_seconds=0,
    )

    try:
        queue.publish(
            make_event(event_id=4),
            message_id="jitter-to-dlq",
        )

        first = worker.run_until_empty()

        assert first.failure_count == 1
        assert worker.last_retry_decision is not None
        assert worker.last_retry_decision.retry_delay_seconds == 10.0
        assert queue.message_snapshot("jitter-to-dlq")[
            "next_attempt_at"
        ] == (
            clock.current + timedelta(seconds=10)
        ).isoformat()

        clock.advance(seconds=10)

        second = worker.run_until_empty()

        assert second.failure_count == 1
        assert worker.last_retry_decision is not None
        assert worker.last_retry_decision.delivery_count == 2
        assert worker.last_retry_decision.retry_delay_seconds == 15.0
        assert queue.message_snapshot("jitter-to-dlq")[
            "next_attempt_at"
        ] == (
            clock.current + timedelta(seconds=15)
        ).isoformat()
        assert random_source.call_count == 2

        clock.advance(seconds=15)

        third = worker.run_until_empty()

        assert third.failure_count == 1
        assert queue.active_count == 0
        assert dlq.count == 1

        record = dlq.records()[0]

        assert record.message_id == "jitter-to-dlq"
        assert record.delivery_count == 3
        assert record.failure_type == "TemporaryError"
        assert record.metadata["retry_decision"]["reason"] == (
            "max_deliveries_exhausted"
        )
        assert record.metadata["retry_decision"][
            "retry_delay_seconds"
        ] == 0.0
        assert worker.retry_count == 2
        assert worker.dead_letter_count == 1
        assert random_source.call_count == 2

    finally:
        queue.close()
        dlq.close()