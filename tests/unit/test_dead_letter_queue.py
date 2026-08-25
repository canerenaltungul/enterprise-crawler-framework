from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any

import pytest

from enterprise_crawler.contracts import Event
from enterprise_crawler.events import (
    DeadLetterQueue,
    DeadLetterQueueProtocol,
    DeadLetterRecord,
    DeadLetterValidationError,
    DuplicateDeadLetterError,
    InMemoryDeadLetterQueue,
    UnknownDeadLetterError,
)


UTC = timezone.utc


# =============================================================================
# HELPERS
# =============================================================================
def make_event(
    *,
    value: Any = 1,
) -> Event:
    return Event(
        event_type="TEST_EVENT",
        timestamp=datetime.now(
            UTC
        ),
        payload={
            "value": value,
        },
        metadata={
            "source": "test",
        },
    )


# =============================================================================
# CONSTRUCTION
# =============================================================================
def test_default_queue_name() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    assert (
        queue.name
        == "dead-letter"
    )


def test_custom_queue_name_is_trimmed() -> None:
    queue = InMemoryDeadLetterQueue(
        name="  failed-events  "
    )

    assert (
        queue.name
        == "failed-events"
    )


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        "\n\t",
    ],
)
def test_empty_name_is_rejected(
    value: str,
) -> None:
    with pytest.raises(
        DeadLetterValidationError
    ):
        InMemoryDeadLetterQueue(
            name=value
        )


def test_public_dead_letter_queue_alias() -> None:
    queue = (
        DeadLetterQueue()
    )

    assert isinstance(
        queue,
        InMemoryDeadLetterQueue,
    )


def test_queue_satisfies_protocol() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    assert isinstance(
        queue,
        DeadLetterQueueProtocol,
    )


def test_initial_queue_is_empty() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    assert (
        queue.count
        == 0
    )

    assert (
        queue.is_empty
        is True
    )

    assert (
        len(
            queue
        )
        == 0
    )


# =============================================================================
# STORE
# =============================================================================
def test_store_dead_letter() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    record = queue.store(
        make_event(),
        message_id="message-1",
        delivery_count=3,
        error=RuntimeError(
            "boom"
        ),
    )

    assert isinstance(
        record,
        DeadLetterRecord,
    )

    assert (
        record.dead_letter_id
    )

    assert (
        record.message_id
        == "message-1"
    )

    assert (
        record.delivery_count
        == 3
    )

    assert (
        record.failure_type
        == "RuntimeError"
    )

    assert (
        record.failure_message
        == "boom"
    )

    assert (
        queue.count
        == 1
    )


def test_explicit_dead_letter_id() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    record = queue.store(
        make_event(),
        message_id="message-1",
        delivery_count=1,
        error=RuntimeError(),
        dead_letter_id="dlq-1",
    )

    assert (
        record.dead_letter_id
        == "dlq-1"
    )

    assert (
        queue.contains(
            "dlq-1"
        )
        is True
    )


def test_duplicate_dead_letter_id_is_rejected() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    queue.store(
        make_event(),
        message_id="message-1",
        delivery_count=1,
        error=RuntimeError(),
        dead_letter_id="same",
    )

    with pytest.raises(
        DuplicateDeadLetterError
    ):
        queue.store(
            make_event(),
            message_id="message-2",
            delivery_count=1,
            error=RuntimeError(),
            dead_letter_id="same",
        )


def test_source_queue_and_claim_token_are_stored() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    record = queue.store(
        make_event(),
        message_id="message-1",
        delivery_count=2,
        error=RuntimeError(),
        source_queue="orders",
        claim_token="claim-123",
    )

    assert (
        record.source_queue
        == "orders"
    )

    assert (
        record.claim_token
        == "claim-123"
    )


def test_metadata_is_stored() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    record = queue.store(
        make_event(),
        message_id="message-1",
        delivery_count=1,
        error=RuntimeError(),
        metadata={
            "reason": "exhausted",
        },
    )

    assert (
        record.metadata
        == {
            "reason": "exhausted",
        }
    )


# =============================================================================
# INPUT VALIDATION
# =============================================================================
def test_non_event_is_rejected() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    with pytest.raises(
        DeadLetterValidationError
    ):
        queue.store(
            object(),  # type: ignore[arg-type]
            message_id="message",
            delivery_count=1,
            error=RuntimeError(),
        )


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        "\n",
    ],
)
def test_empty_message_id_is_rejected(
    value: str,
) -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    with pytest.raises(
        DeadLetterValidationError
    ):
        queue.store(
            make_event(),
            message_id=value,
            delivery_count=1,
            error=RuntimeError(),
        )


@pytest.mark.parametrize(
    "value",
    [
        0,
        -1,
        True,
        1.5,
        "3",
    ],
)
def test_invalid_delivery_count_is_rejected(
    value: Any,
) -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    with pytest.raises(
        DeadLetterValidationError
    ):
        queue.store(
            make_event(),
            message_id="message",
            delivery_count=value,  # type: ignore[arg-type]
            error=RuntimeError(),
        )


def test_error_must_be_exception() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    with pytest.raises(
        DeadLetterValidationError
    ):
        queue.store(
            make_event(),
            message_id="message",
            delivery_count=1,
            error="boom",  # type: ignore[arg-type]
        )


def test_invalid_metadata_is_rejected() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    with pytest.raises(
        DeadLetterValidationError
    ):
        queue.store(
            make_event(),
            message_id="message",
            delivery_count=1,
            error=RuntimeError(),
            metadata="bad",  # type: ignore[arg-type]
        )


# =============================================================================
# COPY BOUNDARIES
# =============================================================================
def test_event_is_copied_on_store() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    event = make_event(
        value="original"
    )

    record = queue.store(
        event,
        message_id="message",
        delivery_count=1,
        error=RuntimeError(),
    )

    event.payload[
        "value"
    ] = "mutated"

    assert (
        record.event.payload[
            "value"
        ]
        == "original"
    )

    stored = queue.get(
        record.dead_letter_id
    )

    assert (
        stored.event.payload[
            "value"
        ]
        == "original"
    )


def test_metadata_is_copied_on_store() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    metadata = {
        "nested": {
            "value": 1,
        }
    }

    record = queue.store(
        make_event(),
        message_id="message",
        delivery_count=1,
        error=RuntimeError(),
        metadata=metadata,
    )

    metadata[
        "nested"
    ][
        "value"
    ] = 99

    assert (
        record.metadata[
            "nested"
        ][
            "value"
        ]
        == 1
    )


def test_get_returns_independent_copy() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    stored = queue.store(
        make_event(
            value="safe"
        ),
        message_id="message",
        delivery_count=1,
        error=RuntimeError(),
    )

    first = queue.get(
        stored.dead_letter_id
    )

    first.event.payload[
        "value"
    ] = "mutated"

    second = queue.get(
        stored.dead_letter_id
    )

    assert (
        second.event.payload[
            "value"
        ]
        == "safe"
    )


# =============================================================================
# LOOKUP
# =============================================================================
def test_get_dead_letter() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    stored = queue.store(
        make_event(),
        message_id="message",
        delivery_count=1,
        error=RuntimeError(),
    )

    loaded = queue.get(
        stored.dead_letter_id
    )

    assert (
        loaded.dead_letter_id
        == stored.dead_letter_id
    )


def test_unknown_dead_letter_is_rejected() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    with pytest.raises(
        UnknownDeadLetterError
    ):
        queue.get(
            "missing"
        )


def test_contains_operator() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    record = queue.store(
        make_event(),
        message_id="message",
        delivery_count=1,
        error=RuntimeError(),
    )

    assert (
        record.dead_letter_id
        in queue
    )

    assert (
        "missing"
        not in queue
    )


def test_contains_non_string_is_false() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    assert (
        queue.__contains__(
            123
        )
        is False
    )


# =============================================================================
# REMOVE
# =============================================================================
def test_remove_dead_letter() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    stored = queue.store(
        make_event(),
        message_id="message",
        delivery_count=1,
        error=RuntimeError(),
    )

    removed = queue.remove(
        stored.dead_letter_id
    )

    assert (
        removed.dead_letter_id
        == stored.dead_letter_id
    )

    assert (
        queue.count
        == 0
    )


def test_remove_unknown_dead_letter_is_rejected() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    with pytest.raises(
        UnknownDeadLetterError
    ):
        queue.remove(
            "missing"
        )


# =============================================================================
# ORDER
# =============================================================================
def test_records_preserve_insertion_order() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    ids: list[
        str
    ] = []

    for index in range(
        5
    ):
        record = queue.store(
            make_event(
                value=index
            ),
            message_id=(
                f"message-{index}"
            ),
            delivery_count=1,
            error=RuntimeError(),
            dead_letter_id=(
                f"dead-{index}"
            ),
        )

        ids.append(
            record.dead_letter_id
        )

    assert [
        record.dead_letter_id
        for record
        in queue.records()
    ] == ids


# =============================================================================
# CONCURRENCY
# =============================================================================
def test_concurrent_stores_are_safe() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    count = 100

    threads: list[
        threading.Thread
    ] = []

    def store(
        index: int,
    ) -> None:
        queue.store(
            make_event(
                value=index
            ),
            message_id=(
                f"message-{index}"
            ),
            delivery_count=3,
            error=RuntimeError(),
            dead_letter_id=(
                f"dead-{index}"
            ),
        )

    for index in range(
        count
    ):
        thread = threading.Thread(
            target=store,
            args=(
                index,
            ),
        )

        threads.append(
            thread
        )

        thread.start()

    for thread in threads:
        thread.join(
            timeout=2
        )

        assert (
            thread.is_alive()
            is False
        )

    assert (
        queue.count
        == count
    )

    assert (
        len(
            {
                record.dead_letter_id
                for record
                in queue.records()
            }
        )
        == count
    )


# =============================================================================
# SERIALIZATION
# =============================================================================
def test_dead_letter_record_to_dict() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    record = queue.store(
        make_event(
            value=5
        ),
        message_id="message",
        delivery_count=3,
        error=RuntimeError(
            "boom"
        ),
        source_queue="events",
        claim_token="token",
        metadata={
            "reason": "exhausted",
        },
    )

    payload = (
        record.to_dict()
    )

    assert (
        payload[
            "message_id"
        ]
        == "message"
    )

    assert (
        payload[
            "delivery_count"
        ]
        == 3
    )

    assert (
        payload[
            "failure_type"
        ]
        == "RuntimeError"
    )

    assert (
        payload[
            "event"
        ][
            "payload"
        ][
            "value"
        ]
        == 5
    )


# =============================================================================
# SNAPSHOT
# =============================================================================
def test_initial_snapshot() -> None:
    queue = (
        InMemoryDeadLetterQueue(
            name="failed"
        )
    )

    snapshot = (
        queue.snapshot()
    )

    assert (
        snapshot[
            "name"
        ]
        == "failed"
    )

    assert (
        snapshot[
            "backend"
        ]
        == "memory"
    )

    assert (
        snapshot[
            "count"
        ]
        == 0
    )

    assert (
        snapshot[
            "stored_count"
        ]
        == 0
    )


def test_snapshot_tracks_runtime_counts() -> None:
    queue = (
        InMemoryDeadLetterQueue()
    )

    first = queue.store(
        make_event(),
        message_id="one",
        delivery_count=1,
        error=RuntimeError(),
    )

    queue.store(
        make_event(),
        message_id="two",
        delivery_count=1,
        error=RuntimeError(),
    )

    queue.remove(
        first.dead_letter_id
    )

    snapshot = (
        queue.snapshot()
    )

    assert (
        snapshot[
            "count"
        ]
        == 1
    )

    assert (
        snapshot[
            "stored_count"
        ]
        == 2
    )

    assert (
        snapshot[
            "removed_count"
        ]
        == 1
    )


# =============================================================================
# REPRESENTATION
# =============================================================================
def test_repr_contains_state() -> None:
    queue = (
        InMemoryDeadLetterQueue(
            name="failed"
        )
    )

    rendered = repr(
        queue
    )

    assert (
        "InMemoryDeadLetterQueue"
        in rendered
    )

    assert (
        "failed"
        in rendered
    )

    assert (
        "count=0"
        in rendered
    )