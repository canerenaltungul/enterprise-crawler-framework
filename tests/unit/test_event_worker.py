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
    EventWorkerAlreadyRunningError,
    EventWorkerClosedError,
    EventWorkerValidationError,
    InMemoryEventQueue,
    WorkerRunSummary,
)


UTC = timezone.utc


# =============================================================================
# HELPERS
# =============================================================================
def make_event(
    event_type: str = "TEST_EVENT",
    *,
    value: Any = 1,
) -> Event:
    return Event(
        event_type=event_type,
        timestamp=datetime.now(
            UTC
        ),
        payload={
            "value": value,
        },
        metadata={},
    )


def make_runtime(
    *,
    handler: Any = None,
    **worker_kwargs: Any,
) -> tuple[
    InMemoryEventQueue,
    EventDispatcher,
    EventWorker,
]:
    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    if handler is None:
        handler = (
            lambda event: (
                event.payload[
                    "value"
                ]
            )
        )

    dispatcher.register(
        "TEST_EVENT",
        handler,
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        idle_sleep_seconds=0.001,
        **worker_kwargs,
    )

    return (
        queue,
        dispatcher,
        worker,
    )


# =============================================================================
# CONSTRUCTION
# =============================================================================
def test_worker_default_configuration() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
    )

    assert (
        worker.name
        == "event-worker"
    )

    assert (
        worker.requeue_on_error
        is True
    )

    assert (
        worker.stop_on_error
        is False
    )


def test_worker_reuses_dependencies() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
    )

    assert (
        worker.queue
        is queue
    )

    assert (
        worker.dispatcher
        is dispatcher
    )


def test_invalid_queue_is_rejected() -> None:
    with pytest.raises(
        EventWorkerValidationError
    ):
        EventWorker(
            queue=object(),  # type: ignore[arg-type]
            dispatcher=(
                EventDispatcher()
            ),
        )


def test_invalid_dispatcher_is_rejected() -> None:
    with pytest.raises(
        EventWorkerValidationError
    ):
        EventWorker(
            queue=(
                InMemoryEventQueue()
            ),
            dispatcher=object(),  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "name",
    [
        "",
        " ",
        "\n\t",
    ],
)
def test_empty_worker_name_is_rejected(
    name: str,
) -> None:
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
            name=name,
        )


def test_worker_name_is_trimmed() -> None:
    worker = EventWorker(
        queue=(
            InMemoryEventQueue()
        ),
        dispatcher=(
            EventDispatcher()
        ),
        name="  worker-a  ",
    )

    assert (
        worker.name
        == "worker-a"
    )


@pytest.mark.parametrize(
    "field_name,value",
    [
        (
            "requeue_on_error",
            1,
        ),
        (
            "stop_on_error",
            "true",
        ),
    ],
)
def test_invalid_boolean_configuration_is_rejected(
    field_name: str,
    value: Any,
) -> None:
    kwargs = {
        field_name: value,
    }

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
            **kwargs,
        )


@pytest.mark.parametrize(
    "value",
    [
        -1,
        True,
        "1",
    ],
)
def test_invalid_idle_sleep_is_rejected(
    value: Any,
) -> None:
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
            idle_sleep_seconds=value,
        )


def test_zero_idle_sleep_is_allowed() -> None:
    worker = EventWorker(
        queue=(
            InMemoryEventQueue()
        ),
        dispatcher=(
            EventDispatcher()
        ),
        idle_sleep_seconds=0,
    )

    assert (
        worker.idle_sleep_seconds
        == 0.0
    )


# =============================================================================
# STOP SIGNAL
# =============================================================================
def test_stop_is_not_requested_by_default() -> None:
    _, _, worker = (
        make_runtime()
    )

    assert (
        worker.should_stop()
        is False
    )


def test_request_stop_sets_signal() -> None:
    _, _, worker = (
        make_runtime()
    )

    worker.request_stop()

    assert (
        worker.should_stop()
        is True
    )


def test_reset_stop_request() -> None:
    _, _, worker = (
        make_runtime()
    )

    worker.request_stop()

    worker.reset_stop_request()

    assert (
        worker.should_stop()
        is False
    )


def test_external_stop_event_is_reused() -> None:
    stop_event = (
        threading.Event()
    )

    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        stop_event=stop_event,
    )

    assert (
        worker.stop_event
        is stop_event
    )

    stop_event.set()

    assert (
        worker.should_stop()
        is True
    )


def test_boolean_stop_signal_is_supported() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    worker = EventWorker(
        queue=queue,
        dispatcher=dispatcher,
        stop_event=False,
    )

    assert (
        worker.should_stop()
        is False
    )

    worker.request_stop()

    assert (
        worker.should_stop()
        is True
    )

    worker.reset_stop_request()

    assert (
        worker.should_stop()
        is False
    )


# =============================================================================
# RUN ONCE
# =============================================================================
def test_run_once_empty_queue_returns_none() -> None:
    _, _, worker = (
        make_runtime()
    )

    result = (
        worker.run_once()
    )

    assert (
        result
        is None
    )

    assert (
        worker.run_count
        == 1
    )


def test_run_once_processes_event() -> None:
    queue, _, worker = (
        make_runtime()
    )

    queue.publish(
        make_event(
            value=42
        )
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
        == 42
    )

    assert (
        queue.active_count
        == 0
    )

    assert (
        worker.total_processed
        == 1
    )


def test_run_once_processes_at_most_one_event() -> None:
    queue, _, worker = (
        make_runtime()
    )

    queue.publish(
        make_event(
            value=1
        )
    )

    queue.publish(
        make_event(
            value=2
        )
    )

    worker.run_once()

    assert (
        queue.active_count
        == 1
    )

    assert (
        worker.total_processed
        == 1
    )


def test_run_once_respects_preexisting_stop_request() -> None:
    queue, _, worker = (
        make_runtime()
    )

    queue.publish(
        make_event()
    )

    worker.request_stop()

    result = (
        worker.run_once()
    )

    assert (
        result
        is None
    )

    assert (
        queue.active_count
        == 1
    )


def test_run_once_failure_is_raised() -> None:
    def failing_handler(
        event: Event,
    ) -> None:
        raise RuntimeError(
            "boom"
        )

    queue, _, worker = (
        make_runtime(
            handler=failing_handler
        )
    )

    queue.publish(
        make_event()
    )

    with pytest.raises(
        EventHandlerExecutionError
    ):
        worker.run_once()

    assert (
        worker.total_failures
        == 1
    )

    assert (
        queue.pending_count
        == 1
    )


def test_run_once_can_discard_failed_event() -> None:
    def failing_handler(
        event: Event,
    ) -> None:
        raise RuntimeError(
            "boom"
        )

    queue, _, worker = (
        make_runtime(
            handler=failing_handler,
            requeue_on_error=False,
        )
    )

    queue.publish(
        make_event()
    )

    with pytest.raises(
        EventHandlerExecutionError
    ):
        worker.run_once()

    assert (
        queue.active_count
        == 0
    )


# =============================================================================
# RUN UNTIL EMPTY
# =============================================================================
def test_run_until_empty_processes_all_events() -> None:
    queue, _, worker = (
        make_runtime()
    )

    for value in range(
        5
    ):
        queue.publish(
            make_event(
                value=value
            )
        )

    summary = (
        worker.run_until_empty()
    )

    assert isinstance(
        summary,
        WorkerRunSummary,
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
        summary.queue_empty
        is True
    )

    assert (
        worker.total_processed
        == 5
    )


def test_run_until_empty_on_empty_queue() -> None:
    _, _, worker = (
        make_runtime()
    )

    summary = (
        worker.run_until_empty()
    )

    assert (
        summary.processed_count
        == 0
    )

    assert (
        summary.queue_empty
        is True
    )


def test_run_until_empty_preserves_fifo_order() -> None:
    seen: list[
        int
    ] = []

    def handler(
        event: Event,
    ) -> int:
        value = int(
            event.payload[
                "value"
            ]
        )

        seen.append(
            value
        )

        return value

    queue, _, worker = (
        make_runtime(
            handler=handler
        )
    )

    for value in [
        3,
        1,
        4,
        2,
    ]:
        queue.publish(
            make_event(
                value=value
            )
        )

    worker.run_until_empty()

    assert (
        seen
        == [
            3,
            1,
            4,
            2,
        ]
    )


def test_run_until_empty_max_events() -> None:
    queue, _, worker = (
        make_runtime()
    )

    for value in range(
        5
    ):
        queue.publish(
            make_event(
                value=value
            )
        )

    summary = (
        worker.run_until_empty(
            max_events=2
        )
    )

    assert (
        summary.processed_count
        == 2
    )

    assert (
        summary.max_events_reached
        is True
    )

    assert (
        queue.active_count
        == 3
    )


@pytest.mark.parametrize(
    "value",
    [
        0,
        -1,
        True,
        1.5,
        "2",
    ],
)
def test_invalid_max_events_is_rejected(
    value: Any,
) -> None:
    _, _, worker = (
        make_runtime()
    )

    with pytest.raises(
        EventWorkerValidationError
    ):
        worker.run_until_empty(
            max_events=value,  # type: ignore[arg-type]
        )


def test_run_until_empty_requeued_failure_does_not_hot_loop() -> None:
    call_count = 0

    def failing_handler(
        event: Event,
    ) -> None:
        nonlocal call_count

        call_count += 1

        raise RuntimeError(
            "boom"
        )

    queue, _, worker = (
        make_runtime(
            handler=failing_handler,
            requeue_on_error=True,
            stop_on_error=False,
        )
    )

    queue.publish(
        make_event()
    )

    summary = (
        worker.run_until_empty()
    )

    assert (
        call_count
        == 1
    )

    assert (
        summary.failure_count
        == 1
    )

    assert (
        queue.pending_count
        == 1
    )


def test_run_until_empty_can_continue_after_discarded_failure() -> None:
    seen: list[
        int
    ] = []

    def handler(
        event: Event,
    ) -> None:
        value = int(
            event.payload[
                "value"
            ]
        )

        if value == 1:
            raise RuntimeError(
                "bad event"
            )

        seen.append(
            value
        )

    queue, _, worker = (
        make_runtime(
            handler=handler,
            requeue_on_error=False,
            stop_on_error=False,
        )
    )

    queue.publish(
        make_event(
            value=1
        )
    )

    queue.publish(
        make_event(
            value=2
        )
    )

    summary = (
        worker.run_until_empty()
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
        seen
        == [
            2
        ]
    )

    assert (
        queue.active_count
        == 0
    )


def test_run_until_empty_stop_on_error_raises() -> None:
    def handler(
        event: Event,
    ) -> None:
        raise RuntimeError(
            "boom"
        )

    queue, _, worker = (
        make_runtime(
            handler=handler,
            stop_on_error=True,
        )
    )

    queue.publish(
        make_event()
    )

    with pytest.raises(
        EventHandlerExecutionError
    ):
        worker.run_until_empty()

    assert (
        worker.is_running
        is False
    )


# =============================================================================
# RUN FOREVER
# =============================================================================
def test_run_forever_can_be_bounded_by_max_events() -> None:
    queue, _, worker = (
        make_runtime()
    )

    for value in range(
        3
    ):
        queue.publish(
            make_event(
                value=value
            )
        )

    summary = (
        worker.run_forever(
            max_events=3
        )
    )

    assert (
        summary.processed_count
        == 3
    )

    assert (
        summary.max_events_reached
        is True
    )

    assert (
        queue.active_count
        == 0
    )


def test_run_forever_stops_cooperatively() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    worker_holder: dict[
        str,
        EventWorker
    ] = {}

    def handler(
        event: Event,
    ) -> str:
        worker_holder[
            "worker"
        ].request_stop()

        return "done"

    dispatcher.register(
        "TEST_EVENT",
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
        make_event()
    )

    summary = (
        worker.run_forever()
    )

    assert (
        summary.processed_count
        == 1
    )

    assert (
        summary.stop_requested
        is True
    )


def test_run_forever_can_be_stopped_from_another_thread() -> None:
    _, _, worker = (
        make_runtime()
    )

    result: list[
        WorkerRunSummary
    ] = []

    def target() -> None:
        result.append(
            worker.run_forever()
        )

    thread = threading.Thread(
        target=target
    )

    thread.start()

    for _ in range(
        100
    ):
        if worker.is_running:
            break

        sleep(
            0.001
        )

    assert (
        worker.is_running
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
            result
        )
        == 1
    )

    assert (
        result[
            0
        ].stop_requested
        is True
    )


def test_run_forever_stop_on_error_raises() -> None:
    def handler(
        event: Event,
    ) -> None:
        raise RuntimeError(
            "boom"
        )

    queue, _, worker = (
        make_runtime(
            handler=handler,
            stop_on_error=True,
        )
    )

    queue.publish(
        make_event()
    )

    with pytest.raises(
        EventHandlerExecutionError
    ):
        worker.run_forever(
            max_events=1
        )


# =============================================================================
# CONCURRENCY
# =============================================================================
def test_same_worker_cannot_run_concurrently() -> None:
    _, _, worker = (
        make_runtime()
    )

    started = (
        threading.Event()
    )

    release = (
        threading.Event()
    )

    original_idle_wait = (
        worker._idle_wait
    )

    def blocking_idle_wait() -> None:
        started.set()

        release.wait(
            timeout=2
        )

        original_idle_wait()

    worker._idle_wait = (  # type: ignore[method-assign]
        blocking_idle_wait
    )

    thread = threading.Thread(
        target=worker.run_forever
    )

    thread.start()

    assert (
        started.wait(
            timeout=2
        )
        is True
    )

    with pytest.raises(
        EventWorkerAlreadyRunningError
    ):
        worker.run_once()

    release.set()

    worker.request_stop()

    thread.join(
        timeout=2
    )

    assert (
        thread.is_alive()
        is False
    )


def test_reset_stop_request_is_rejected_while_running() -> None:
    _, _, worker = (
        make_runtime()
    )

    started = (
        threading.Event()
    )

    def target() -> None:
        started.set()

        worker.run_forever()

    thread = threading.Thread(
        target=target
    )

    thread.start()

    assert (
        started.wait(
            timeout=2
        )
        is True
    )

    for _ in range(
        100
    ):
        if worker.is_running:
            break

        sleep(
            0.001
        )

    with pytest.raises(
        EventWorkerAlreadyRunningError
    ):
        worker.reset_stop_request()

    worker.request_stop()

    thread.join(
        timeout=2
    )


# =============================================================================
# SEQUENTIAL REUSE
# =============================================================================
def test_worker_can_run_multiple_times() -> None:
    queue, _, worker = (
        make_runtime()
    )

    queue.publish(
        make_event(
            value=1
        )
    )

    first = (
        worker.run_once()
    )

    queue.publish(
        make_event(
            value=2
        )
    )

    second = (
        worker.run_once()
    )

    assert (
        first
        is not None
    )

    assert (
        second
        is not None
    )

    assert (
        first.value
        == 1
    )

    assert (
        second.value
        == 2
    )

    assert (
        worker.run_count
        == 2
    )

    assert (
        worker.total_processed
        == 2
    )


# =============================================================================
# SUMMARY
# =============================================================================
def test_worker_run_summary_to_dict() -> None:
    _, _, worker = (
        make_runtime()
    )

    summary = (
        worker.run_until_empty()
    )

    payload = (
        summary.to_dict()
    )

    assert (
        payload[
            "mode"
        ]
        == "until_empty"
    )

    assert (
        payload[
            "processed_count"
        ]
        == 0
    )

    assert (
        isinstance(
            payload[
                "started_at"
            ],
            str,
        )
    )

    assert (
        isinstance(
            payload[
                "finished_at"
            ],
            str,
        )
    )


def test_run_summary_duration_is_non_negative() -> None:
    _, _, worker = (
        make_runtime()
    )

    summary = (
        worker.run_until_empty()
    )

    assert (
        summary.duration_seconds
        >= 0
    )


def test_last_summary_tracks_latest_run() -> None:
    queue, _, worker = (
        make_runtime()
    )

    worker.run_until_empty()

    queue.publish(
        make_event()
    )

    second = (
        worker.run_until_empty()
    )

    assert (
        worker.last_summary
        is second
    )

    assert (
        second.processed_count
        == 1
    )


# =============================================================================
# SNAPSHOT
# =============================================================================
def test_initial_snapshot() -> None:
    _, _, worker = (
        make_runtime()
    )

    snapshot = (
        worker.snapshot()
    )

    assert (
        snapshot[
            "run_count"
        ]
        == 0
    )

    assert (
        snapshot[
            "total_processed"
        ]
        == 0
    )

    assert (
        snapshot[
            "total_failures"
        ]
        == 0
    )

    assert (
        snapshot[
            "is_running"
        ]
        is False
    )


def test_snapshot_tracks_execution() -> None:
    queue, _, worker = (
        make_runtime()
    )

    queue.publish(
        make_event(
            value=10
        )
    )

    worker.run_once()

    snapshot = (
        worker.snapshot()
    )

    assert (
        snapshot[
            "run_count"
        ]
        == 1
    )

    assert (
        snapshot[
            "total_processed"
        ]
        == 1
    )

    assert (
        snapshot[
            "last_dispatch"
        ][
            "value"
        ]
        == 10
    )

    assert (
        snapshot[
            "last_summary"
        ][
            "mode"
        ]
        == "once"
    )


def test_snapshot_contains_queue_and_dispatcher() -> None:
    _, _, worker = (
        make_runtime()
    )

    snapshot = (
        worker.snapshot()
    )

    assert (
        isinstance(
            snapshot[
                "queue"
            ],
            dict,
        )
    )

    assert (
        isinstance(
            snapshot[
                "dispatcher"
            ],
            dict,
        )
    )


# =============================================================================
# CLOSE
# =============================================================================
def test_close_is_idempotent() -> None:
    _, _, worker = (
        make_runtime()
    )

    worker.close()

    worker.close()

    assert (
        worker.is_closed
        is True
    )


def test_closed_worker_rejects_run_once() -> None:
    _, _, worker = (
        make_runtime()
    )

    worker.close()

    with pytest.raises(
        EventWorkerClosedError
    ):
        worker.run_once()


def test_closed_worker_rejects_run_until_empty() -> None:
    _, _, worker = (
        make_runtime()
    )

    worker.close()

    with pytest.raises(
        EventWorkerClosedError
    ):
        worker.run_until_empty()


def test_closed_worker_rejects_run_forever() -> None:
    _, _, worker = (
        make_runtime()
    )

    worker.close()

    with pytest.raises(
        EventWorkerClosedError
    ):
        worker.run_forever(
            max_events=1
        )


def test_context_manager_closes_worker() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    with EventWorker(
        queue=queue,
        dispatcher=dispatcher,
    ) as worker:
        assert (
            worker.is_closed
            is False
        )

    assert (
        worker.is_closed
        is True
    )


def test_close_does_not_modify_queue() -> None:
    queue, _, worker = (
        make_runtime()
    )

    queue.publish(
        make_event()
    )

    worker.close()

    assert (
        queue.active_count
        == 1
    )


# =============================================================================
# REPRESENTATION
# =============================================================================
def test_repr_contains_worker_state() -> None:
    _, _, worker = (
        make_runtime()
    )

    rendered = repr(
        worker
    )

    assert (
        "event-worker"
        in rendered
    )

    assert (
        "total_processed"
        in rendered
    )

    assert (
        "total_failures"
        in rendered
    )