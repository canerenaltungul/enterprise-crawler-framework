from __future__ import annotations

import threading
from datetime import datetime, timezone
from time import sleep
from typing import Any

import pytest

from enterprise_crawler.contracts import Event
from enterprise_crawler.events import (
    EventDispatcher,
    EventHandlerExecutionError,
    EventWorker,
    InMemoryEventQueue,
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
                "integration-test"
            ),
        },
    )


def wait_until(
    predicate: Any,
    *,
    attempts: int = 500,
    delay_seconds: float = 0.001,
) -> bool:
    for _ in range(
        attempts
    ):
        if predicate():
            return True

        sleep(
            delay_seconds
        )

    return False


# =============================================================================
# SUCCESS PIPELINE
# =============================================================================
def test_event_worker_full_success_pipeline() -> None:
    """
    Event
        ↓
    publish
        ↓
    claim
        ↓
    worker
        ↓
    dispatcher
        ↓
    handler
        ↓
    ack
    """

    queue = (
        InMemoryEventQueue(
            name="orders"
        )
    )

    dispatcher = (
        EventDispatcher(
            name="orders"
        )
    )

    processed: list[
        dict[str, Any]
    ] = []

    def handle_order_created(
        event: Event,
    ) -> dict[str, Any]:
        result = {
            "event_id": (
                event.payload[
                    "event_id"
                ]
            ),
            "value": (
                event.payload[
                    "value"
                ]
            ),
        }

        processed.append(
            result
        )

        return result

    dispatcher.register(
        "ORDER_CREATED",
        handle_order_created,
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        name="orders-worker",
        idle_sleep_seconds=0.001,
    )

    published = queue.publish(
        make_event(
            event_id=101,
            value="created",
        ),
        message_id="order-101",
    )

    summary = (
        worker.run_until_empty()
    )

    assert isinstance(
        summary,
        WorkerRunSummary,
    )

    assert (
        published.message_id
        == "order-101"
    )

    assert (
        processed
        == [
            {
                "event_id": 101,
                "value": "created",
            }
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
        summary.queue_empty
        is True
    )

    assert (
        queue.active_count
        == 0
    )

    assert (
        queue.pending_count
        == 0
    )

    assert (
        queue.claimed_count
        == 0
    )

    assert (
        worker.total_processed
        == 1
    )

    assert (
        worker.total_failures
        == 0
    )

    assert (
        dispatcher.snapshot()[
            "dispatch_count"
        ]
        == 1
    )

    assert (
        queue.snapshot()[
            "acked_count"
        ]
        == 1
    )


# =============================================================================
# MULTI-EVENT FIFO
# =============================================================================
def test_event_worker_processes_multiple_events_in_fifo_order() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    seen: list[
        int
    ] = []

    def handler(
        event: Event,
    ) -> int:
        event_id = int(
            event.payload[
                "event_id"
            ]
        )

        seen.append(
            event_id
        )

        return event_id

    dispatcher.register(
        "ORDER_CREATED",
        handler,
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        idle_sleep_seconds=0.001,
    )

    for event_id in [
        10,
        20,
        30,
        40,
        50,
    ]:
        queue.publish(
            make_event(
                event_id=event_id
            )
        )

    summary = (
        worker.run_until_empty()
    )

    assert (
        seen
        == [
            10,
            20,
            30,
            40,
            50,
        ]
    )

    assert (
        summary.processed_count
        == 5
    )

    assert (
        summary.failure_count
        == 0
    )

    assert (
        queue.active_count
        == 0
    )

    assert (
        queue.snapshot()[
            "acked_count"
        ]
        == 5
    )


# =============================================================================
# MULTIPLE EVENT TYPES
# =============================================================================
def test_worker_routes_multiple_event_types_to_correct_handlers() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    results: list[
        tuple[
            str,
            int,
        ]
    ] = []

    def handle_created(
        event: Event,
    ) -> None:
        results.append(
            (
                "created",
                int(
                    event.payload[
                        "event_id"
                    ]
                ),
            )
        )

    def handle_deleted(
        event: Event,
    ) -> None:
        results.append(
            (
                "deleted",
                int(
                    event.payload[
                        "event_id"
                    ]
                ),
            )
        )

    dispatcher.register(
        "ORDER_CREATED",
        handle_created,
    )

    dispatcher.register(
        "ORDER_DELETED",
        handle_deleted,
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        idle_sleep_seconds=0.001,
    )

    queue.publish(
        make_event(
            event_type=(
                "ORDER_CREATED"
            ),
            event_id=1,
        )
    )

    queue.publish(
        make_event(
            event_type=(
                "ORDER_DELETED"
            ),
            event_id=2,
        )
    )

    queue.publish(
        make_event(
            event_type=(
                "ORDER_CREATED"
            ),
            event_id=3,
        )
    )

    summary = (
        worker.run_until_empty()
    )

    assert (
        results
        == [
            (
                "created",
                1,
            ),
            (
                "deleted",
                2,
            ),
            (
                "created",
                3,
            ),
        ]
    )

    assert (
        summary.processed_count
        == 3
    )

    assert (
        dispatcher.snapshot()[
            "dispatch_count"
        ]
        == 3
    )


# =============================================================================
# FAILURE -> REQUEUE
# =============================================================================
def test_handler_failure_requeues_event() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    call_count = 0

    def failing_handler(
        event: Event,
    ) -> None:
        nonlocal call_count

        call_count += 1

        raise RuntimeError(
            "temporary failure"
        )

    dispatcher.register(
        "ORDER_CREATED",
        failing_handler,
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        requeue_on_error=True,
        stop_on_error=False,
        idle_sleep_seconds=0.001,
    )

    published = queue.publish(
        make_event(
            event_id=1
        ),
        message_id="retry-me",
    )

    summary = (
        worker.run_until_empty()
    )

    assert (
        call_count
        == 1
    )

    assert (
        summary.processed_count
        == 0
    )

    assert (
        summary.failure_count
        == 1
    )

    assert (
        queue.active_count
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
        queue.contains(
            published.message_id
        )
        is True
    )

    queue_snapshot = (
        queue.snapshot()
    )

    assert (
        queue_snapshot[
            "nacked_count"
        ]
        == 1
    )

    assert (
        queue_snapshot[
            "requeued_count"
        ]
        == 1
    )

    assert (
        worker.total_failures
        == 1
    )


# =============================================================================
# REQUEUE -> SECOND DELIVERY
# =============================================================================
def test_requeued_event_can_succeed_on_next_worker_run() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    attempts = 0

    successful_deliveries: list[
        int
    ] = []

    def eventually_successful_handler(
        event: Event,
    ) -> str:
        nonlocal attempts

        attempts += 1

        if attempts == 1:
            raise RuntimeError(
                "transient failure"
            )

        successful_deliveries.append(
            int(
                event.payload[
                    "event_id"
                ]
            )
        )

        return "done"

    dispatcher.register(
        "ORDER_CREATED",
        eventually_successful_handler,
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        requeue_on_error=True,
        stop_on_error=False,
        idle_sleep_seconds=0.001,
    )

    queue.publish(
        make_event(
            event_id=77
        ),
        message_id="event-77",
    )

    first_summary = (
        worker.run_until_empty()
    )

    assert (
        first_summary.failure_count
        == 1
    )

    assert (
        queue.pending_count
        == 1
    )

    second_summary = (
        worker.run_until_empty()
    )

    assert (
        second_summary.processed_count
        == 1
    )

    assert (
        second_summary.failure_count
        == 0
    )

    assert (
        successful_deliveries
        == [
            77
        ]
    )

    assert (
        attempts
        == 2
    )

    assert (
        queue.active_count
        == 0
    )

    queue_snapshot = (
        queue.snapshot()
    )

    assert (
        queue_snapshot[
            "claim_count"
        ]
        == 2
    )

    assert (
        queue_snapshot[
            "acked_count"
        ]
        == 1
    )

    assert (
        queue_snapshot[
            "requeued_count"
        ]
        == 1
    )


# =============================================================================
# FAILURE -> DISCARD
# =============================================================================
def test_handler_failure_can_discard_event_and_continue_pipeline() -> None:
    queue = (
        InMemoryEventQueue()
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
        idle_sleep_seconds=0.001,
    )

    queue.publish(
        make_event(
            event_id=1
        )
    )

    queue.publish(
        make_event(
            event_id=2
        )
    )

    queue.publish(
        make_event(
            event_id=3
        )
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
        summary.queue_empty
        is True
    )

    assert (
        queue.active_count
        == 0
    )

    queue_snapshot = (
        queue.snapshot()
    )

    assert (
        queue_snapshot[
            "discarded_count"
        ]
        == 1
    )

    assert (
        queue_snapshot[
            "acked_count"
        ]
        == 2
    )


# =============================================================================
# STOP ON ERROR
# =============================================================================
def test_stop_on_error_propagates_handler_failure() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    def handler(
        event: Event,
    ) -> None:
        raise RuntimeError(
            "fatal failure"
        )

    dispatcher.register(
        "ORDER_CREATED",
        handler,
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        requeue_on_error=True,
        stop_on_error=True,
        idle_sleep_seconds=0.001,
    )

    queue.publish(
        make_event(
            event_id=1
        )
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
        worker.total_failures
        == 1
    )

    assert (
        queue.pending_count
        == 1
    )


# =============================================================================
# COOPERATIVE SHUTDOWN
# =============================================================================
def test_run_forever_processes_event_and_stops_cooperatively() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    processed: list[
        int
    ] = []

    worker_holder: dict[
        str,
        EventWorker
    ] = {}

    def handler(
        event: Event,
    ) -> None:
        processed.append(
            int(
                event.payload[
                    "event_id"
                ]
            )
        )

        worker_holder[
            "worker"
        ].request_stop()

    dispatcher.register(
        "ORDER_CREATED",
        handler,
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        idle_sleep_seconds=0.001,
    )

    worker_holder[
        "worker"
    ] = worker

    queue.publish(
        make_event(
            event_id=999
        )
    )

    summary = (
        worker.run_forever()
    )

    assert (
        processed
        == [
            999
        ]
    )

    assert (
        summary.processed_count
        == 1
    )

    assert (
        summary.stop_requested
        is True
    )

    assert (
        queue.active_count
        == 0
    )


# =============================================================================
# EXTERNAL THREAD SHUTDOWN
# =============================================================================
def test_idle_worker_can_be_stopped_from_external_thread() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        idle_sleep_seconds=0.01,
    )

    summaries: list[
        WorkerRunSummary
    ] = []

    def target() -> None:
        summaries.append(
            worker.run_forever()
        )

    thread = threading.Thread(
        target=target
    )

    thread.start()

    assert (
        wait_until(
            lambda: (
                worker.is_running
            )
        )
        is True
    )

    worker.request_stop()

    thread.join(
        timeout=2
    )

    assert (
        thread.is_alive()
        is False
    )

    assert (
        len(
            summaries
        )
        == 1
    )

    assert (
        summaries[
            0
        ].stop_requested
        is True
    )

    assert (
        summaries[
            0
        ].processed_count
        == 0
    )


# =============================================================================
# TWO WORKERS / SAME QUEUE
# =============================================================================
def test_two_workers_do_not_duplicate_event_processing() -> None:
    """
    İki worker aynı queue üzerinde yarışır.

    Atomic queue.claim() nedeniyle her message yalnız bir kez
    handler'a ulaşmalıdır.
    """

    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    processed_ids: list[
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
            processed_ids.append(
                event_id
            )

    dispatcher.register(
        "ORDER_CREATED",
        handler,
    )

    event_count = 250

    for event_id in range(
        event_count
    ):
        queue.publish(
            make_event(
                event_id=event_id
            ),
            message_id=(
                f"message-{event_id}"
            ),
        )

    worker_a = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        name="worker-a",
        idle_sleep_seconds=0,
    )

    worker_b = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        name="worker-b",
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

    assert (
        len(
            processed_ids
        )
        == event_count
    )

    assert (
        len(
            set(
                processed_ids
            )
        )
        == event_count
    )

    assert (
        sorted(
            processed_ids
        )
        == list(
            range(
                event_count
            )
        )
    )

    assert (
        queue.active_count
        == 0
    )

    assert (
        queue.pending_count
        == 0
    )

    assert (
        queue.claimed_count
        == 0
    )

    assert (
        worker_a.total_processed
        + worker_b.total_processed
        == event_count
    )

    assert (
        queue.snapshot()[
            "acked_count"
        ]
        == event_count
    )


# =============================================================================
# DELIVERY OWNERSHIP UNDER CONCURRENCY
# =============================================================================
def test_claimed_event_is_owned_by_only_one_worker() -> None:
    """
    Tek event için iki worker aynı anda çalıştırılır.

    Handler tam olarak bir kez çağrılmalıdır.
    """

    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    handler_calls = 0

    handler_lock = (
        threading.Lock()
    )

    def handler(
        event: Event,
    ) -> None:
        nonlocal handler_calls

        with handler_lock:
            handler_calls += 1

        sleep(
            0.01
        )

    dispatcher.register(
        "ORDER_CREATED",
        handler,
    )

    queue.publish(
        make_event(
            event_id=1
        )
    )

    worker_a = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        name="worker-a",
        idle_sleep_seconds=0,
    )

    worker_b = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        name="worker-b",
        idle_sleep_seconds=0,
    )

    thread_a = threading.Thread(
        target=worker_a.run_until_empty
    )

    thread_b = threading.Thread(
        target=worker_b.run_until_empty
    )

    thread_a.start()
    thread_b.start()

    thread_a.join(
        timeout=2
    )

    thread_b.join(
        timeout=2
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
        handler_calls
        == 1
    )

    assert (
        worker_a.total_processed
        + worker_b.total_processed
        == 1
    )

    assert (
        queue.active_count
        == 0
    )


# =============================================================================
# EVENT DATA INTEGRITY
# =============================================================================
def test_event_payload_and_metadata_survive_complete_pipeline() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    captured: list[
        Event
    ] = []

    def handler(
        event: Event,
    ) -> None:
        captured.append(
            event
        )

    dispatcher.register(
        "ORDER_CREATED",
        handler,
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        idle_sleep_seconds=0.001,
    )

    source_event = Event(
        event_type="ORDER_CREATED",
        timestamp=datetime.now(
            UTC
        ),
        payload={
            "order": {
                "id": 123,
                "items": [
                    "a",
                    "b",
                ],
            }
        },
        metadata={
            "trace_id": (
                "trace-123"
            ),
            "source": (
                "integration"
            ),
        },
    )

    queue.publish(
        source_event
    )

    worker.run_until_empty()

    assert (
        len(
            captured
        )
        == 1
    )

    received = (
        captured[
            0
        ]
    )

    assert (
        received.event_type
        == "ORDER_CREATED"
    )

    assert (
        received.payload
        == {
            "order": {
                "id": 123,
                "items": [
                    "a",
                    "b",
                ],
            }
        }
    )

    assert (
        received.metadata
        == {
            "trace_id": (
                "trace-123"
            ),
            "source": (
                "integration"
            ),
        }
    )


# =============================================================================
# PUBLISH BOUNDARY IMMUTABILITY
# =============================================================================
def test_source_event_mutation_after_publish_does_not_change_delivery() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    received_values: list[
        str
    ] = []

    def handler(
        event: Event,
    ) -> None:
        received_values.append(
            str(
                event.payload[
                    "value"
                ]
            )
        )

    dispatcher.register(
        "ORDER_CREATED",
        handler,
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        idle_sleep_seconds=0.001,
    )

    event = make_event(
        event_id=1,
        value="original",
    )

    queue.publish(
        event
    )

    event.payload[
        "value"
    ] = "mutated"

    event.metadata[
        "source"
    ] = "mutated"

    worker.run_until_empty()

    assert (
        received_values
        == [
            "original"
        ]
    )


# =============================================================================
# MAX EVENTS BOUNDARY
# =============================================================================
def test_worker_can_process_large_queue_in_bounded_batches() -> None:
    queue = (
        InMemoryEventQueue()
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
        processed.append(
            int(
                event.payload[
                    "event_id"
                ]
            )
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

    for event_id in range(
        25
    ):
        queue.publish(
            make_event(
                event_id=event_id
            )
        )

    first = worker.run_until_empty(
        max_events=10
    )

    second = worker.run_until_empty(
        max_events=10
    )

    third = worker.run_until_empty(
        max_events=10
    )

    assert (
        first.processed_count
        == 10
    )

    assert (
        first.max_events_reached
        is True
    )

    assert (
        second.processed_count
        == 10
    )

    assert (
        second.max_events_reached
        is True
    )

    assert (
        third.processed_count
        == 5
    )

    assert (
        third.queue_empty
        is True
    )

    assert (
        len(
            processed
        )
        == 25
    )

    assert (
        processed
        == list(
            range(
                25
            )
        )
    )


# =============================================================================
# SNAPSHOT INTEGRATION
# =============================================================================
def test_worker_queue_dispatcher_snapshots_are_consistent_after_pipeline() -> None:
    queue = (
        InMemoryEventQueue(
            name="orders"
        )
    )

    dispatcher = (
        EventDispatcher(
            name="orders"
        )
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
        name="orders-worker",
        idle_sleep_seconds=0,
    )

    for event_id in range(
        4
    ):
        queue.publish(
            make_event(
                event_id=event_id
            )
        )

    worker.run_until_empty()

    snapshot = (
        worker.snapshot()
    )

    assert (
        snapshot[
            "name"
        ]
        == "orders-worker"
    )

    assert (
        snapshot[
            "total_processed"
        ]
        == 4
    )

    assert (
        snapshot[
            "total_failures"
        ]
        == 0
    )

    assert (
        snapshot[
            "queue"
        ][
            "name"
        ]
        == "orders"
    )

    assert (
        snapshot[
            "queue"
        ][
            "active_count"
        ]
        == 0
    )

    assert (
        snapshot[
            "queue"
        ][
            "acked_count"
        ]
        == 4
    )

    assert (
        snapshot[
            "dispatcher"
        ][
            "name"
        ]
        == "orders"
    )

    assert (
        snapshot[
            "dispatcher"
        ][
            "dispatch_count"
        ]
        == 4
    )

    assert (
        snapshot[
            "last_summary"
        ][
            "processed_count"
        ]
        == 4
    )