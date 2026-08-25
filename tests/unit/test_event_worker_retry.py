from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from enterprise_crawler.contracts import Event
from enterprise_crawler.events import (
    EventDispatcher,
    EventHandlerExecutionError,
    EventWorker,
    EventWorkerDeadLetterError,
    EventWorkerValidationError,
    InMemoryDeadLetterQueue,
    InMemoryEventQueue,
    RetryAction,
    RetryPolicy,
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
        },
        metadata={
            "source": "test",
        },
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


class RecordingEventQueue(
    InMemoryEventQueue
):
    """
    Worker'ın RetryDecision gecikmesini queue contract'ına aynen
    ilettiğini doğrulamak için kullanılan test queue'su.

    Parent queue'ya gecikme 0.0 verilerek test içinde gerçek zaman
    beklemeden bir sonraki delivery'nin hemen alınabilmesi sağlanır.
    """

    def __init__(
        self,
    ) -> None:
        super().__init__()

        self.retry_delays: list[
            float
        ] = []

    def nack(
        self,
        message_id: str,
        claim_token: str,
        *,
        requeue: bool = True,
        retry_delay_seconds: float = 0.0,
    ) -> Event:
        if requeue:
            self.retry_delays.append(
                retry_delay_seconds
            )

        return super().nack(
            message_id,
            claim_token,
            requeue=requeue,
            retry_delay_seconds=0.0,
        )


# =============================================================================
# CONFIGURATION
# =============================================================================
def test_retry_policy_is_optional() -> None:
    worker = EventWorker(
        queue=(
            InMemoryEventQueue()
        ),
        dispatcher=(
            EventDispatcher()
        ),
    )

    assert (
        worker.retry_policy
        is None
    )

    assert (
        worker.dead_letter_queue
        is None
    )


def test_retry_policy_can_be_configured() -> None:
    policy = (
        RetryPolicy()
    )

    worker = EventWorker(
        queue=(
            InMemoryEventQueue()
        ),
        dispatcher=(
            EventDispatcher()
        ),
        retry_policy=policy,
    )

    assert (
        worker.retry_policy
        is policy
    )


def test_dead_letter_queue_can_be_configured() -> None:
    dlq = (
        InMemoryDeadLetterQueue()
    )

    worker = EventWorker(
        queue=(
            InMemoryEventQueue()
        ),
        dispatcher=(
            EventDispatcher()
        ),
        dead_letter_queue=dlq,
    )

    assert (
        worker.dead_letter_queue
        is dlq
    )


def test_invalid_retry_policy_is_rejected() -> None:
    with pytest.raises(
        EventWorkerValidationError
    ):
        EventWorker(
            queue=(
                InMemoryEventQueue()
            ),
            dispatcher=(
                EventDispatcher()
            ),
            retry_policy=object(),  # type: ignore[arg-type]
        )


def test_invalid_dead_letter_queue_is_rejected() -> None:
    with pytest.raises(
        EventWorkerValidationError
    ):
        EventWorker(
            queue=(
                InMemoryEventQueue()
            ),
            dispatcher=(
                EventDispatcher()
            ),
            dead_letter_queue=object(),  # type: ignore[arg-type]
        )


# =============================================================================
# LEGACY COMPATIBILITY
# =============================================================================
def test_legacy_requeue_behavior_is_preserved() -> None:
    queue = (
        InMemoryEventQueue()
    )

    queue.publish(
        make_event(),
        message_id="message",
    )

    dispatcher = make_dispatcher(
        lambda event: (
            (_ for _ in ()).throw(
                RuntimeError(
                    "failure"
                )
            )
        )
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        requeue_on_error=True,
        retry_policy=None,
        idle_sleep_seconds=0,
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

    assert (
        worker.retry_count
        == 0
    )


def test_legacy_discard_behavior_is_preserved() -> None:
    queue = (
        InMemoryEventQueue()
    )

    queue.publish(
        make_event(),
        message_id="message",
    )

    dispatcher = make_dispatcher(
        lambda event: (
            (_ for _ in ()).throw(
                RuntimeError(
                    "failure"
                )
            )
        )
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        requeue_on_error=False,
        retry_policy=None,
        idle_sleep_seconds=0,
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
        worker.discard_count
        == 0
    )


# =============================================================================
# RETRY
# =============================================================================
def test_retryable_failure_is_requeued_by_policy() -> None:
    queue = (
        InMemoryEventQueue()
    )

    queue.publish(
        make_event(),
        message_id="retry-me",
    )

    dispatcher = make_dispatcher(
        lambda event: (
            (_ for _ in ()).throw(
                TemporaryError(
                    "temporary"
                )
            )
        )
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        retry_policy=RetryPolicy(
            max_deliveries=3,
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        idle_sleep_seconds=0,
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

    assert (
        worker.retry_count
        == 1
    )

    assert (
        worker.dead_letter_count
        == 0
    )

    decision = (
        worker.last_retry_decision
    )

    assert (
        decision
        is not None
    )

    assert (
        decision.action
        is RetryAction.RETRY
    )


def test_default_retry_delay_is_forwarded_as_zero() -> None:
    queue = (
        RecordingEventQueue()
    )

    queue.publish(
        make_event(),
        message_id="retry-zero-delay",
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
        idle_sleep_seconds=0,
    )

    summary = (
        worker.run_until_empty()
    )

    assert (
        summary.failure_count
        == 1
    )

    assert (
        queue.retry_delays
        == [
            0.0
        ]
    )

    assert (
        queue.pending_count
        == 1
    )

    assert (
        worker.last_retry_decision
        is not None
    )

    assert (
        worker.last_retry_decision.retry_delay_seconds
        == 0.0
    )


def test_retry_delay_is_forwarded_to_queue_nack() -> None:
    queue = (
        RecordingEventQueue()
    )

    queue.publish(
        make_event(),
        message_id="retry-delayed",
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
            max_deliveries=5,
            retry_exceptions=(
                TemporaryError,
            ),
            base_delay_seconds=2.0,
            backoff_multiplier=3.0,
        ),
        idle_sleep_seconds=0,
    )

    first = (
        worker.run_until_empty()
    )

    assert (
        first.failure_count
        == 1
    )

    assert (
        queue.retry_delays
        == [
            2.0
        ]
    )

    assert (
        worker.last_retry_decision
        is not None
    )

    assert (
        worker.last_retry_decision.retry_delay_seconds
        == 2.0
    )

    second = (
        worker.run_until_empty()
    )

    assert (
        second.failure_count
        == 1
    )

    assert (
        queue.retry_delays
        == [
            2.0,
            6.0,
        ]
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
        == 6.0
    )


def test_retry_policy_uses_original_handler_exception() -> None:
    queue = (
        InMemoryEventQueue()
    )

    queue.publish(
        make_event()
    )

    dispatcher = make_dispatcher(
        lambda event: (
            (_ for _ in ()).throw(
                TemporaryError()
            )
        )
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        retry_policy=RetryPolicy(
            max_deliveries=3,
            retry_exceptions=(
                TemporaryError,
            ),
            dead_letter_non_retryable=True,
        ),
        idle_sleep_seconds=0,
    )

    worker.run_until_empty()

    decision = (
        worker.last_retry_decision
    )

    assert (
        decision
        is not None
    )

    assert (
        decision.action
        is RetryAction.RETRY
    )

    assert (
        decision.error_type
        == "TemporaryError"
    )


def test_retry_delivery_count_increases_across_runs() -> None:
    queue = (
        InMemoryEventQueue()
    )

    queue.publish(
        make_event(),
        message_id="message",
    )

    dispatcher = make_dispatcher(
        lambda event: (
            (_ for _ in ()).throw(
                TemporaryError()
            )
        )
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        retry_policy=RetryPolicy(
            max_deliveries=5,
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        idle_sleep_seconds=0,
    )

    first = (
        worker.run_until_empty()
    )

    assert (
        first.failure_count
        == 1
    )

    assert (
        worker.last_retry_decision
        is not None
    )

    assert (
        worker.last_retry_decision.delivery_count
        == 1
    )

    second = (
        worker.run_until_empty()
    )

    assert (
        second.failure_count
        == 1
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
        worker.retry_count
        == 2
    )


# =============================================================================
# DISCARD
# =============================================================================
def test_explicit_discard_exception_removes_event() -> None:
    queue = (
        InMemoryEventQueue()
    )

    queue.publish(
        make_event(),
        message_id="discard-me",
    )

    dispatcher = make_dispatcher(
        lambda event: (
            (_ for _ in ()).throw(
                DiscardError(
                    "invalid"
                )
            )
        )
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        retry_policy=RetryPolicy(
            retry_exceptions=(
                Exception,
            ),
            discard_exceptions=(
                DiscardError,
            ),
        ),
        idle_sleep_seconds=0,
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
        worker.discard_count
        == 1
    )

    assert (
        worker.retry_count
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


def test_discard_failure_does_not_block_following_event() -> None:
    queue = (
        InMemoryEventQueue()
    )

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
        message_id="good",
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
            raise DiscardError(
                "discard"
            )

        processed.append(
            event_id
        )

    worker = EventWorker(
        queue=queue,
        dispatcher=(
            make_dispatcher(
                handler
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
        idle_sleep_seconds=0,
    )

    summary = (
        worker.run_until_empty()
    )

    assert (
        processed
        == [
            2
        ]
    )

    assert (
        summary.failure_count
        == 1
    )

    assert (
        summary.processed_count
        == 1
    )

    assert (
        queue.active_count
        == 0
    )


# =============================================================================
# DEAD LETTER
# =============================================================================
def test_non_retryable_failure_is_dead_lettered() -> None:
    queue = (
        InMemoryEventQueue(
            name="source"
        )
    )

    dlq = (
        InMemoryDeadLetterQueue()
    )

    queue.publish(
        make_event(),
        message_id="dead-me",
    )

    dispatcher = make_dispatcher(
        lambda event: (
            (_ for _ in ()).throw(
                PermanentError(
                    "permanent"
                )
            )
        )
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        retry_policy=RetryPolicy(
            retry_exceptions=(
                TemporaryError,
            ),
            dead_letter_non_retryable=True,
        ),
        dead_letter_queue=dlq,
        idle_sleep_seconds=0,
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

    assert (
        worker.dead_letter_count
        == 1
    )

    record = (
        dlq.records()[
            0
        ]
    )

    assert (
        record.message_id
        == "dead-me"
    )

    assert (
        record.failure_type
        == "PermanentError"
    )

    assert (
        record.source_queue
        == "source"
    )

    assert (
        record.delivery_count
        == 1
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


def test_exhausted_retry_is_dead_lettered() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dlq = (
        InMemoryDeadLetterQueue()
    )

    queue.publish(
        make_event(),
        message_id="event",
    )

    dispatcher = make_dispatcher(
        lambda event: (
            (_ for _ in ()).throw(
                TemporaryError()
            )
        )
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        retry_policy=RetryPolicy(
            max_deliveries=2,
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        dead_letter_queue=dlq,
        idle_sleep_seconds=0,
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
        queue.active_count
        == 0
    )

    assert (
        dlq.count
        == 1
    )

    assert (
        worker.retry_count
        == 1
    )

    assert (
        worker.dead_letter_count
        == 1
    )

    record = (
        dlq.records()[
            0
        ]
    )

    assert (
        record.delivery_count
        == 2
    )


def test_dead_letter_action_without_dlq_fails_closed() -> None:
    queue = (
        InMemoryEventQueue()
    )

    queue.publish(
        make_event(),
        message_id="protected",
    )

    dispatcher = make_dispatcher(
        lambda event: (
            (_ for _ in ()).throw(
                PermanentError()
            )
        )
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        retry_policy=RetryPolicy(
            retry_exceptions=(
                TemporaryError,
            ),
            dead_letter_non_retryable=True,
        ),
        dead_letter_queue=None,
        idle_sleep_seconds=0,
    )

    with pytest.raises(
        EventWorkerDeadLetterError
    ):
        worker.run_once()

    # Fail-closed:
    # Event silinmedi veya requeue edilmedi.
    assert (
        queue.active_count
        == 1
    )

    assert (
        queue.claimed_count
        == 1
    )

    assert (
        queue.pending_count
        == 0
    )

    assert (
        worker.dead_letter_count
        == 0
    )


# =============================================================================
# DLQ STORE FAILURE - FAIL CLOSED
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


def test_dlq_store_failure_does_not_remove_source_event() -> None:
    queue = (
        InMemoryEventQueue()
    )

    queue.publish(
        make_event(),
        message_id="protected",
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
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        dead_letter_queue=(
            FailingDeadLetterQueue()
        ),
        idle_sleep_seconds=0,
    )

    with pytest.raises(
        RuntimeError,
        match="DLQ unavailable",
    ):
        worker.run_once()

    assert (
        queue.active_count
        == 1
    )

    assert (
        queue.claimed_count
        == 1
    )

    assert (
        queue.pending_count
        == 0
    )

    assert (
        worker.dead_letter_count
        == 0
    )


# =============================================================================
# SUCCESS
# =============================================================================
def test_successful_event_ignores_retry_policy() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dlq = (
        InMemoryDeadLetterQueue()
    )

    queue.publish(
        make_event(),
        message_id="success",
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=(
            make_dispatcher(
                lambda event: "ok"
            )
        ),
        retry_policy=(
            RetryPolicy()
        ),
        dead_letter_queue=dlq,
        idle_sleep_seconds=0,
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
        == "ok"
    )

    assert (
        result.message_id
        == "success"
    )

    assert (
        result.delivery_count
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
        worker.last_retry_decision
        is None
    )


# =============================================================================
# STOP ON ERROR
# =============================================================================
def test_stop_on_error_still_propagates_retryable_failure() -> None:
    queue = (
        InMemoryEventQueue()
    )

    queue.publish(
        make_event()
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
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        stop_on_error=True,
        idle_sleep_seconds=0,
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
        worker.retry_count
        == 1
    )


# =============================================================================
# SNAPSHOT
# =============================================================================
def test_snapshot_reports_retry_configuration() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dlq = (
        InMemoryDeadLetterQueue()
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=(
            EventDispatcher()
        ),
        retry_policy=RetryPolicy(
            max_deliveries=4
        ),
        dead_letter_queue=dlq,
    )

    snapshot = (
        worker.snapshot()
    )

    assert (
        snapshot[
            "retry_policy_enabled"
        ]
        is True
    )

    assert (
        snapshot[
            "retry_policy"
        ][
            "max_deliveries"
        ]
        == 4
    )

    assert (
        snapshot[
            "dead_letter_queue_enabled"
        ]
        is True
    )

    assert (
        snapshot[
            "retry_count"
        ]
        == 0
    )

    assert (
        snapshot[
            "discard_count"
        ]
        == 0
    )

    assert (
        snapshot[
            "dead_letter_count"
        ]
        == 0
    )


def test_snapshot_tracks_dead_letter_decision() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dlq = (
        InMemoryDeadLetterQueue()
    )

    queue.publish(
        make_event()
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
            retry_exceptions=(
                TemporaryError,
            ),
        ),
        dead_letter_queue=dlq,
        idle_sleep_seconds=0,
    )

    worker.run_until_empty()

    snapshot = (
        worker.snapshot()
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


# =============================================================================
# REPRESENTATION
# =============================================================================
def test_repr_reports_failure_routing_configuration() -> None:
    worker = EventWorker(
        queue=(
            InMemoryEventQueue()
        ),
        dispatcher=(
            EventDispatcher()
        ),
        retry_policy=(
            RetryPolicy()
        ),
        dead_letter_queue=(
            InMemoryDeadLetterQueue()
        ),
    )

    rendered = repr(
        worker
    )

    assert (
        "retry_policy_enabled=True"
        in rendered
    )

    assert (
        "dead_letter_queue_enabled=True"
        in rendered
    )