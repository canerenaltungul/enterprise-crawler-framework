from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from enterprise_crawler.contracts import Event
from enterprise_crawler.events import (
    DuplicateEventHandlerError,
    EventDispatcher,
    EventDispatcherValidationError,
    EventHandlerExecutionError,
    EventHandlerNotFoundError,
    InMemoryEventQueue,
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


# =============================================================================
# CONSTRUCTION
# =============================================================================
def test_default_dispatcher_name() -> None:
    dispatcher = (
        EventDispatcher()
    )

    assert (
        dispatcher.name
        == "default"
    )


def test_custom_dispatcher_name() -> None:
    dispatcher = (
        EventDispatcher(
            name="events"
        )
    )

    assert (
        dispatcher.name
        == "events"
    )


# =============================================================================
# REGISTRATION
# =============================================================================
def test_register_handler() -> None:
    dispatcher = (
        EventDispatcher()
    )

    def handler(
        event: Event,
    ) -> None:
        return None

    dispatcher.register(
        "TEST_EVENT",
        handler,
    )

    assert (
        dispatcher.contains(
            "TEST_EVENT"
        )
        is True
    )

    assert len(
        dispatcher
    ) == 1


def test_event_type_is_trimmed_on_registration() -> None:
    dispatcher = (
        EventDispatcher()
    )

    dispatcher.register(
        "  TEST_EVENT  ",
        lambda event: None,
    )

    assert (
        "TEST_EVENT"
        in dispatcher
    )


def test_lookup_is_case_insensitive() -> None:
    dispatcher = (
        EventDispatcher()
    )

    dispatcher.register(
        "TEST_EVENT",
        lambda event: None,
    )

    assert (
        dispatcher.contains(
            "test_event"
        )
        is True
    )


def test_duplicate_handler_is_rejected() -> None:
    dispatcher = (
        EventDispatcher()
    )

    dispatcher.register(
        "TEST_EVENT",
        lambda event: None,
    )

    with pytest.raises(
        DuplicateEventHandlerError
    ):
        dispatcher.register(
            "test_event",
            lambda event: None,
        )


def test_handler_can_be_replaced_explicitly() -> None:
    dispatcher = (
        EventDispatcher()
    )

    first = (
        lambda event: "first"
    )

    second = (
        lambda event: "second"
    )

    dispatcher.register(
        "TEST_EVENT",
        first,
    )

    dispatcher.register(
        "TEST_EVENT",
        second,
        replace=True,
    )

    result = dispatcher.dispatch(
        make_event()
    )

    assert (
        result.value
        == "second"
    )


def test_non_callable_handler_is_rejected() -> None:
    dispatcher = (
        EventDispatcher()
    )

    with pytest.raises(
        EventDispatcherValidationError
    ):
        dispatcher.register(
            "TEST_EVENT",
            object(),  # type: ignore[arg-type]
        )


def test_invalid_replace_flag_is_rejected() -> None:
    dispatcher = (
        EventDispatcher()
    )

    with pytest.raises(
        EventDispatcherValidationError
    ):
        dispatcher.register(
            "TEST_EVENT",
            lambda event: None,
            replace=1,  # type: ignore[arg-type]
        )


# =============================================================================
# UNREGISTER
# =============================================================================
def test_unregister_handler() -> None:
    dispatcher = (
        EventDispatcher()
    )

    def handler(
        event: Event,
    ) -> None:
        return None

    dispatcher.register(
        "TEST_EVENT",
        handler,
    )

    removed = (
        dispatcher.unregister(
            "test_event"
        )
    )

    assert (
        removed
        is handler
    )

    assert (
        len(
            dispatcher
        )
        == 0
    )


def test_unregister_unknown_handler_is_rejected() -> None:
    dispatcher = (
        EventDispatcher()
    )

    with pytest.raises(
        EventHandlerNotFoundError
    ):
        dispatcher.unregister(
            "missing"
        )


# =============================================================================
# LOOKUP
# =============================================================================
def test_handler_for_returns_registered_handler() -> None:
    dispatcher = (
        EventDispatcher()
    )

    def handler(
        event: Event,
    ) -> None:
        return None

    dispatcher.register(
        "TEST_EVENT",
        handler,
    )

    assert (
        dispatcher.handler_for(
            "test_event"
        )
        is handler
    )


def test_handler_for_unknown_type_is_rejected() -> None:
    dispatcher = (
        EventDispatcher()
    )

    with pytest.raises(
        EventHandlerNotFoundError
    ):
        dispatcher.handler_for(
            "missing"
        )


def test_event_types_are_deterministically_sorted() -> None:
    dispatcher = (
        EventDispatcher()
    )

    dispatcher.register(
        "zeta",
        lambda event: None,
    )

    dispatcher.register(
        "Alpha",
        lambda event: None,
    )

    dispatcher.register(
        "beta",
        lambda event: None,
    )

    assert (
        dispatcher.event_types()
        == [
            "Alpha",
            "beta",
            "zeta",
        ]
    )


# =============================================================================
# DISPATCH
# =============================================================================
def test_dispatch_event() -> None:
    dispatcher = (
        EventDispatcher()
    )

    seen: list[
        Event
    ] = []

    def handler(
        event: Event,
    ) -> str:
        seen.append(
            event
        )

        return "done"

    dispatcher.register(
        "TEST_EVENT",
        handler,
    )

    event = make_event()

    result = dispatcher.dispatch(
        event
    )

    assert (
        result.event_type
        == "TEST_EVENT"
    )

    assert (
        result.value
        == "done"
    )

    assert (
        seen
        == [
            event
        ]
    )


def test_dispatch_invalid_event_is_rejected() -> None:
    dispatcher = (
        EventDispatcher()
    )

    with pytest.raises(
        EventDispatcherValidationError
    ):
        dispatcher.dispatch(
            object()  # type: ignore[arg-type]
        )


def test_dispatch_without_handler_fails_closed() -> None:
    dispatcher = (
        EventDispatcher()
    )

    with pytest.raises(
        EventHandlerNotFoundError
    ):
        dispatcher.dispatch(
            make_event()
        )


def test_handler_exception_is_wrapped() -> None:
    dispatcher = (
        EventDispatcher()
    )

    def handler(
        event: Event,
    ) -> None:
        raise ValueError(
            "boom"
        )

    dispatcher.register(
        "TEST_EVENT",
        handler,
    )

    with pytest.raises(
        EventHandlerExecutionError
    ) as exc_info:
        dispatcher.dispatch(
            make_event()
        )

    assert isinstance(
        exc_info.value.cause,
        ValueError,
    )

    assert (
        exc_info.value.event_type
        == "TEST_EVENT"
    )


def test_dispatch_duration_is_non_negative() -> None:
    dispatcher = (
        EventDispatcher()
    )

    dispatcher.register(
        "TEST_EVENT",
        lambda event: None,
    )

    result = dispatcher.dispatch(
        make_event()
    )

    assert (
        result.duration_seconds
        >= 0
    )


# =============================================================================
# CLAIMED DISPATCH
# =============================================================================
def test_dispatch_claimed_acks_successful_event() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    dispatcher.register(
        "TEST_EVENT",
        lambda event: (
            event.payload[
                "value"
            ]
            * 2
        ),
    )

    published = queue.publish(
        make_event(
            value=5
        )
    )

    claimed = queue.claim()

    assert claimed is not None

    result = (
        dispatcher.dispatch_claimed(
            queue,
            claimed,
        )
    )

    assert (
        result.value
        == 10
    )

    assert (
        result.message_id
        == published.message_id
    )

    assert (
        result.delivery_count
        == 1
    )

    assert (
        queue.active_count
        == 0
    )


def test_dispatch_claimed_requeues_failure_by_default() -> None:
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
            "failure"
        )

    dispatcher.register(
        "TEST_EVENT",
        handler,
    )

    queue.publish(
        make_event()
    )

    claimed = queue.claim()

    assert claimed is not None

    with pytest.raises(
        EventHandlerExecutionError
    ):
        dispatcher.dispatch_claimed(
            queue,
            claimed,
        )

    assert (
        queue.pending_count
        == 1
    )

    assert (
        queue.claimed_count
        == 0
    )


def test_dispatch_claimed_can_discard_failure() -> None:
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
            "failure"
        )

    dispatcher.register(
        "TEST_EVENT",
        handler,
    )

    queue.publish(
        make_event()
    )

    claimed = queue.claim()

    assert claimed is not None

    with pytest.raises(
        EventHandlerExecutionError
    ):
        dispatcher.dispatch_claimed(
            queue,
            claimed,
            requeue_on_error=False,
        )

    assert (
        queue.active_count
        == 0
    )


def test_dispatch_next_returns_none_for_empty_queue() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    assert (
        dispatcher.dispatch_next(
            queue
        )
        is None
    )


def test_dispatch_next_runs_one_event() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    dispatcher.register(
        "TEST_EVENT",
        lambda event: (
            event.payload[
                "value"
            ]
        ),
    )

    queue.publish(
        make_event(
            value=42
        )
    )

    result = (
        dispatcher.dispatch_next(
            queue
        )
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


def test_dispatch_next_only_processes_one_event() -> None:
    queue = (
        InMemoryEventQueue()
    )

    dispatcher = (
        EventDispatcher()
    )

    dispatcher.register(
        "TEST_EVENT",
        lambda event: None,
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

    dispatcher.dispatch_next(
        queue
    )

    assert (
        queue.pending_count
        == 1
    )


def test_invalid_queue_is_rejected() -> None:
    dispatcher = (
        EventDispatcher()
    )

    with pytest.raises(
        EventDispatcherValidationError
    ):
        dispatcher.dispatch_next(
            object()  # type: ignore[arg-type]
        )


# =============================================================================
# COUNTERS / SNAPSHOT
# =============================================================================
def test_snapshot_initial_state() -> None:
    dispatcher = (
        EventDispatcher(
            name="events"
        )
    )

    snapshot = (
        dispatcher.snapshot()
    )

    assert (
        snapshot[
            "name"
        ]
        == "events"
    )

    assert (
        snapshot[
            "handler_count"
        ]
        == 0
    )

    assert (
        snapshot[
            "dispatch_count"
        ]
        == 0
    )


def test_snapshot_tracks_successful_dispatch() -> None:
    dispatcher = (
        EventDispatcher()
    )

    dispatcher.register(
        "TEST_EVENT",
        lambda event: None,
    )

    dispatcher.dispatch(
        make_event()
    )

    snapshot = (
        dispatcher.snapshot()
    )

    assert (
        snapshot[
            "dispatch_count"
        ]
        == 1
    )

    assert (
        snapshot[
            "failure_count"
        ]
        == 0
    )


def test_snapshot_tracks_failed_dispatch() -> None:
    dispatcher = (
        EventDispatcher()
    )

    def handler(
        event: Event,
    ) -> None:
        raise RuntimeError(
            "failure"
        )

    dispatcher.register(
        "TEST_EVENT",
        handler,
    )

    with pytest.raises(
        EventHandlerExecutionError
    ):
        dispatcher.dispatch(
            make_event()
        )

    snapshot = (
        dispatcher.snapshot()
    )

    assert (
        snapshot[
            "dispatch_count"
        ]
        == 0
    )

    assert (
        snapshot[
            "failure_count"
        ]
        == 1
    )


# =============================================================================
# PUBLIC RESULT CONTRACT
# =============================================================================
def test_dispatch_result_to_dict() -> None:
    dispatcher = (
        EventDispatcher()
    )

    dispatcher.register(
        "TEST_EVENT",
        lambda event: "done",
    )

    result = dispatcher.dispatch(
        make_event()
    )

    payload = (
        result.to_dict()
    )

    assert (
        payload[
            "event_type"
        ]
        == "TEST_EVENT"
    )

    assert (
        payload[
            "value"
        ]
        == "done"
    )


# =============================================================================
# REPRESENTATION
# =============================================================================
def test_repr_contains_dispatcher_state() -> None:
    dispatcher = (
        EventDispatcher(
            name="events"
        )
    )

    rendered = repr(
        dispatcher
    )

    assert (
        "events"
        in rendered
    )

    assert (
        "handler_count"
        in rendered
    )