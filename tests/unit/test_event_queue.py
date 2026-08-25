from __future__ import annotations

import threading
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from typing import Any

import pytest

from enterprise_crawler.contracts import Event
from enterprise_crawler.events import (
    DuplicateEventMessageError,
    EventClaimOwnershipError,
    EventNotClaimedError,
    EventQueueValidationError,
    InMemoryEventQueue,
    UnknownEventMessageError,
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
        metadata={
            "source": "test",
        },
    )


# =============================================================================
# CONSTRUCTION
# =============================================================================
def test_default_queue_name() -> None:
    queue = InMemoryEventQueue()

    assert (
        queue.name
        == "default"
    )


def test_custom_queue_name_is_trimmed() -> None:
    queue = InMemoryEventQueue(
        name="  events  "
    )

    assert (
        queue.name
        == "events"
    )


@pytest.mark.parametrize(
    "name",
    [
        "",
        " ",
        "\n\t",
    ],
)
def test_empty_queue_name_is_rejected(
    name: str,
) -> None:
    with pytest.raises(
        EventQueueValidationError
    ):
        InMemoryEventQueue(
            name=name
        )


def test_non_string_queue_name_is_rejected() -> None:
    with pytest.raises(
        EventQueueValidationError
    ):
        InMemoryEventQueue(
            name=123  # type: ignore[arg-type]
        )


def test_custom_clock_is_used() -> None:
    clock = make_clock()

    queue = InMemoryEventQueue(
        clock=clock
    )

    published = queue.publish(
        make_event()
    )

    assert (
        published.published_at
        == clock.current
    )


def test_non_callable_clock_is_rejected() -> None:
    with pytest.raises(
        EventQueueValidationError
    ):
        InMemoryEventQueue(
            clock=123  # type: ignore[arg-type]
        )


def test_clock_must_return_datetime() -> None:
    queue = InMemoryEventQueue(
        clock=lambda: "now"  # type: ignore[return-value]
    )

    with pytest.raises(
        EventQueueValidationError
    ):
        queue.publish(
            make_event()
        )


def test_clock_must_return_timezone_aware_datetime() -> None:
    queue = InMemoryEventQueue(
        clock=lambda: datetime(
            2026,
            8,
            19,
            12,
            0,
            0,
        )
    )

    with pytest.raises(
        EventQueueValidationError
    ):
        queue.publish(
            make_event()
        )


# =============================================================================
# PUBLISH
# =============================================================================
def test_publish_event() -> None:
    queue = InMemoryEventQueue()

    published = queue.publish(
        make_event()
    )

    assert (
        published.event_type
        == "TEST_EVENT"
    )

    assert (
        published.message_id
    )

    assert (
        queue.pending_count
        == 1
    )

    assert (
        queue.active_count
        == 1
    )


def test_explicit_message_id() -> None:
    queue = InMemoryEventQueue()

    published = queue.publish(
        make_event(),
        message_id="message-1",
    )

    assert (
        published.message_id
        == "message-1"
    )


def test_explicit_message_id_is_trimmed() -> None:
    queue = InMemoryEventQueue()

    published = queue.publish(
        make_event(),
        message_id="  message-1  ",
    )

    assert (
        published.message_id
        == "message-1"
    )


def test_duplicate_message_id_is_rejected() -> None:
    queue = InMemoryEventQueue()

    queue.publish(
        make_event(),
        message_id="same",
    )

    with pytest.raises(
        DuplicateEventMessageError
    ):
        queue.publish(
            make_event(),
            message_id="same",
        )


def test_non_event_is_rejected() -> None:
    queue = InMemoryEventQueue()

    with pytest.raises(
        EventQueueValidationError
    ):
        queue.publish(
            object()  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "event_type",
    [
        "",
        " ",
        "\n",
    ],
)
def test_empty_event_type_is_rejected(
    event_type: str,
) -> None:
    queue = InMemoryEventQueue()

    with pytest.raises(
        EventQueueValidationError
    ):
        queue.publish(
            make_event(
                event_type
            )
        )


def test_event_type_is_trimmed() -> None:
    queue = InMemoryEventQueue()

    queue.publish(
        make_event(
            "  TEST_EVENT  "
        )
    )

    claimed = queue.claim()

    assert (
        claimed
        is not None
    )

    assert (
        claimed.event.event_type
        == "TEST_EVENT"
    )


def test_invalid_timestamp_is_rejected() -> None:
    queue = InMemoryEventQueue()

    event = make_event()

    event.timestamp = "now"  # type: ignore[assignment]

    with pytest.raises(
        EventQueueValidationError
    ):
        queue.publish(
            event
        )


def test_invalid_payload_is_rejected() -> None:
    queue = InMemoryEventQueue()

    event = make_event()

    event.payload = []  # type: ignore[assignment]

    with pytest.raises(
        EventQueueValidationError
    ):
        queue.publish(
            event
        )


def test_invalid_metadata_is_rejected() -> None:
    queue = InMemoryEventQueue()

    event = make_event()

    event.metadata = []  # type: ignore[assignment]

    with pytest.raises(
        EventQueueValidationError
    ):
        queue.publish(
            event
        )


def test_publish_copies_event_payload() -> None:
    queue = InMemoryEventQueue()

    event = make_event()

    queue.publish(
        event
    )

    event.payload[
        "value"
    ] = 999

    claimed = queue.claim()

    assert (
        claimed
        is not None
    )

    assert (
        claimed.event.payload[
            "value"
        ]
        == 1
    )


def test_publish_copies_event_metadata() -> None:
    queue = InMemoryEventQueue()

    event = make_event()

    queue.publish(
        event
    )

    event.metadata[
        "source"
    ] = "changed"

    claimed = queue.claim()

    assert (
        claimed
        is not None
    )

    assert (
        claimed.event.metadata[
            "source"
        ]
        == "test"
    )


# =============================================================================
# CLAIM
# =============================================================================
def test_empty_queue_claim_returns_none() -> None:
    queue = InMemoryEventQueue()

    assert (
        queue.claim()
        is None
    )


def test_claim_event() -> None:
    queue = InMemoryEventQueue()

    published = queue.publish(
        make_event()
    )

    claimed = queue.claim()

    assert (
        claimed
        is not None
    )

    assert (
        claimed.message_id
        == published.message_id
    )

    assert (
        claimed.claim_token
    )

    assert (
        claimed.delivery_count
        == 1
    )

    assert (
        queue.pending_count
        == 0
    )

    assert (
        queue.claimed_count
        == 1
    )


def test_claim_is_fifo() -> None:
    queue = InMemoryEventQueue()

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

    first = queue.claim()
    second = queue.claim()

    assert (
        first
        is not None
    )

    assert (
        second
        is not None
    )

    assert (
        first.event.payload[
            "value"
        ]
        == 1
    )

    assert (
        second.event.payload[
            "value"
        ]
        == 2
    )


def test_claimed_event_is_not_claimed_twice() -> None:
    queue = InMemoryEventQueue()

    queue.publish(
        make_event()
    )

    first = queue.claim()
    second = queue.claim()

    assert (
        first
        is not None
    )

    assert (
        second
        is None
    )


# =============================================================================
# SCHEDULED CLAIM
# =============================================================================
def test_delayed_requeue_is_not_immediately_claimable() -> None:
    clock = make_clock()

    queue = InMemoryEventQueue(
        clock=clock
    )

    queue.publish(
        make_event(),
        message_id="scheduled",
    )

    first = queue.claim()

    assert (
        first
        is not None
    )

    queue.nack(
        first.message_id,
        first.claim_token,
        retry_delay_seconds=30,
    )

    assert (
        queue.pending_count
        == 1
    )

    assert (
        queue.claim()
        is None
    )


def test_delayed_requeue_becomes_claimable_when_due() -> None:
    clock = make_clock()

    queue = InMemoryEventQueue(
        clock=clock
    )

    queue.publish(
        make_event(),
        message_id="scheduled",
    )

    first = queue.claim()

    assert (
        first
        is not None
    )

    queue.nack(
        first.message_id,
        first.claim_token,
        retry_delay_seconds=30,
    )

    clock.advance(
        seconds=29
    )

    assert (
        queue.claim()
        is None
    )

    clock.advance(
        seconds=1
    )

    second = queue.claim()

    assert (
        second
        is not None
    )

    assert (
        second.message_id
        == "scheduled"
    )

    assert (
        second.delivery_count
        == 2
    )


def test_zero_retry_delay_preserves_immediate_requeue() -> None:
    clock = make_clock()

    queue = InMemoryEventQueue(
        clock=clock
    )

    queue.publish(
        make_event(),
        message_id="message",
    )

    first = queue.claim()

    assert (
        first
        is not None
    )

    queue.nack(
        first.message_id,
        first.claim_token,
        retry_delay_seconds=0,
    )

    second = queue.claim()

    assert (
        second
        is not None
    )

    assert (
        second.message_id
        == first.message_id
    )

    assert (
        second.delivery_count
        == 2
    )


def test_future_head_does_not_block_due_event() -> None:
    clock = make_clock()

    queue = InMemoryEventQueue(
        clock=clock
    )

    queue.publish(
        make_event(
            value="future"
        ),
        message_id="future",
    )

    first = queue.claim()

    assert (
        first
        is not None
    )

    queue.nack(
        first.message_id,
        first.claim_token,
        retry_delay_seconds=60,
    )

    queue.publish(
        make_event(
            value="due"
        ),
        message_id="due",
    )

    claimed = queue.claim()

    assert (
        claimed
        is not None
    )

    assert (
        claimed.message_id
        == "due"
    )

    assert (
        claimed.event.payload[
            "value"
        ]
        == "due"
    )


def test_eligible_fifo_is_preserved_when_future_event_is_skipped() -> None:
    clock = make_clock()

    queue = InMemoryEventQueue(
        clock=clock
    )

    queue.publish(
        make_event(
            value="future"
        ),
        message_id="future",
    )

    future_claim = queue.claim()

    assert (
        future_claim
        is not None
    )

    queue.nack(
        future_claim.message_id,
        future_claim.claim_token,
        retry_delay_seconds=60,
    )

    queue.publish(
        make_event(
            value="due-1"
        ),
        message_id="due-1",
    )

    queue.publish(
        make_event(
            value="due-2"
        ),
        message_id="due-2",
    )

    first_due = queue.claim()

    second_due = queue.claim()

    assert (
        first_due
        is not None
    )

    assert (
        second_due
        is not None
    )

    assert [
        first_due.message_id,
        second_due.message_id,
    ] == [
        "due-1",
        "due-2",
    ]


def test_original_fifo_is_preserved_after_future_event_becomes_due() -> None:
    clock = make_clock()

    queue = InMemoryEventQueue(
        clock=clock
    )

    queue.publish(
        make_event(
            value="future"
        ),
        message_id="future",
    )

    future_claim = queue.claim()

    assert (
        future_claim
        is not None
    )

    queue.nack(
        future_claim.message_id,
        future_claim.claim_token,
        retry_delay_seconds=30,
    )

    queue.publish(
        make_event(
            value="later"
        ),
        message_id="later",
    )

    clock.advance(
        seconds=30
    )

    first = queue.claim()

    second = queue.claim()

    assert (
        first
        is not None
    )

    assert (
        second
        is not None
    )

    assert [
        first.message_id,
        second.message_id,
    ] == [
        "future",
        "later",
    ]


def test_scheduled_wait_does_not_increment_delivery_count() -> None:
    clock = make_clock()

    queue = InMemoryEventQueue(
        clock=clock
    )

    queue.publish(
        make_event(),
        message_id="message",
    )

    first = queue.claim()

    assert (
        first
        is not None
    )

    assert (
        first.delivery_count
        == 1
    )

    queue.nack(
        first.message_id,
        first.claim_token,
        retry_delay_seconds=30,
    )

    clock.advance(
        seconds=10
    )

    assert (
        queue.claim()
        is None
    )

    clock.advance(
        seconds=20
    )

    second = queue.claim()

    assert (
        second
        is not None
    )

    assert (
        second.delivery_count
        == 2
    )


# =============================================================================
# ACK
# =============================================================================
def test_ack_removes_event() -> None:
    queue = InMemoryEventQueue()

    published = queue.publish(
        make_event()
    )

    claimed = queue.claim()

    assert claimed is not None

    event = queue.ack(
        claimed.message_id,
        claimed.claim_token,
    )

    assert (
        event.event_type
        == "TEST_EVENT"
    )

    assert (
        queue.active_count
        == 0
    )

    assert (
        queue.is_empty
        is True
    )

    assert (
        queue.contains(
            published.message_id
        )
        is False
    )


def test_ack_unknown_message_is_rejected() -> None:
    queue = InMemoryEventQueue()

    with pytest.raises(
        UnknownEventMessageError
    ):
        queue.ack(
            "missing",
            "token",
        )


def test_wrong_ack_claim_token_is_rejected() -> None:
    queue = InMemoryEventQueue()

    queue.publish(
        make_event()
    )

    claimed = queue.claim()

    assert claimed is not None

    with pytest.raises(
        EventClaimOwnershipError
    ):
        queue.ack(
            claimed.message_id,
            "wrong-token",
        )


# =============================================================================
# NACK
# =============================================================================
def test_nack_requeues_by_default() -> None:
    queue = InMemoryEventQueue()

    queue.publish(
        make_event()
    )

    claimed = queue.claim()

    assert claimed is not None

    queue.nack(
        claimed.message_id,
        claimed.claim_token,
    )

    assert (
        queue.pending_count
        == 1
    )

    assert (
        queue.claimed_count
        == 0
    )

    second = queue.claim()

    assert second is not None

    assert (
        second.message_id
        == claimed.message_id
    )

    assert (
        second.delivery_count
        == 2
    )

    assert (
        second.claim_token
        != claimed.claim_token
    )


def test_nack_can_discard_event() -> None:
    queue = InMemoryEventQueue()

    queue.publish(
        make_event()
    )

    claimed = queue.claim()

    assert claimed is not None

    queue.nack(
        claimed.message_id,
        claimed.claim_token,
        requeue=False,
    )

    assert (
        queue.active_count
        == 0
    )

    assert (
        queue.claim()
        is None
    )


def test_wrong_nack_claim_token_is_rejected() -> None:
    queue = InMemoryEventQueue()

    queue.publish(
        make_event()
    )

    claimed = queue.claim()

    assert claimed is not None

    with pytest.raises(
        EventClaimOwnershipError
    ):
        queue.nack(
            claimed.message_id,
            "wrong",
        )


def test_nack_invalid_requeue_flag_is_rejected() -> None:
    queue = InMemoryEventQueue()

    queue.publish(
        make_event()
    )

    claimed = queue.claim()

    assert claimed is not None

    with pytest.raises(
        EventQueueValidationError
    ):
        queue.nack(
            claimed.message_id,
            claimed.claim_token,
            requeue=1,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "value",
    [
        -1,
        True,
        "5",
        None,
        float("inf"),
        float("-inf"),
        float("nan"),
    ],
)
def test_invalid_retry_delay_is_rejected(
    value: Any,
) -> None:
    queue = InMemoryEventQueue()

    queue.publish(
        make_event()
    )

    claimed = queue.claim()

    assert claimed is not None

    with pytest.raises(
        EventQueueValidationError
    ):
        queue.nack(
            claimed.message_id,
            claimed.claim_token,
            retry_delay_seconds=value,  # type: ignore[arg-type]
        )


def test_discard_rejects_nonzero_retry_delay() -> None:
    queue = InMemoryEventQueue()

    queue.publish(
        make_event()
    )

    claimed = queue.claim()

    assert claimed is not None

    with pytest.raises(
        EventQueueValidationError
    ):
        queue.nack(
            claimed.message_id,
            claimed.claim_token,
            requeue=False,
            retry_delay_seconds=5,
        )

    assert (
        queue.claimed_count
        == 1
    )


def test_pending_event_cannot_be_acked() -> None:
    queue = InMemoryEventQueue()

    published = queue.publish(
        make_event()
    )

    with pytest.raises(
        EventNotClaimedError
    ):
        queue.ack(
            published.message_id,
            "token",
        )


def test_pending_event_cannot_be_nacked() -> None:
    queue = InMemoryEventQueue()

    published = queue.publish(
        make_event()
    )

    with pytest.raises(
        EventNotClaimedError
    ):
        queue.nack(
            published.message_id,
            "token",
        )


# =============================================================================
# LOOKUP
# =============================================================================
def test_contains_message() -> None:
    queue = InMemoryEventQueue()

    published = queue.publish(
        make_event()
    )

    assert (
        queue.contains(
            published.message_id
        )
        is True
    )


def test_contains_operator() -> None:
    queue = InMemoryEventQueue()

    published = queue.publish(
        make_event()
    )

    assert (
        published.message_id
        in queue
    )


def test_contains_non_string_is_false() -> None:
    queue = InMemoryEventQueue()

    assert (
        queue.contains(
            123  # type: ignore[arg-type]
        )
        is False
    )


# =============================================================================
# SNAPSHOT
# =============================================================================
def test_initial_snapshot() -> None:
    queue = InMemoryEventQueue(
        name="events"
    )

    snapshot = queue.snapshot()

    assert (
        snapshot[
            "name"
        ]
        == "events"
    )

    assert (
        snapshot[
            "active_count"
        ]
        == 0
    )

    assert (
        snapshot[
            "published_count"
        ]
        == 0
    )


def test_snapshot_tracks_runtime_counts() -> None:
    queue = InMemoryEventQueue()

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

    first = queue.claim()

    assert first is not None

    queue.ack(
        first.message_id,
        first.claim_token,
    )

    second = queue.claim()

    assert second is not None

    queue.nack(
        second.message_id,
        second.claim_token,
    )

    snapshot = queue.snapshot()

    assert (
        snapshot[
            "published_count"
        ]
        == 2
    )

    assert (
        snapshot[
            "claim_count"
        ]
        == 2
    )

    assert (
        snapshot[
            "acked_count"
        ]
        == 1
    )

    assert (
        snapshot[
            "nacked_count"
        ]
        == 1
    )

    assert (
        snapshot[
            "requeued_count"
        ]
        == 1
    )


# =============================================================================
# CONCURRENCY
# =============================================================================
def test_concurrent_claim_gives_event_to_only_one_consumer() -> None:
    queue = InMemoryEventQueue()

    queue.publish(
        make_event()
    )

    results: list[
        Any
    ] = []

    lock = threading.Lock()

    def worker() -> None:
        result = queue.claim()

        with lock:
            results.append(
                result
            )

    threads = [
        threading.Thread(
            target=worker
        )
        for _ in range(
            20
        )
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    claimed = [
        result
        for result
        in results
        if result is not None
    ]

    assert (
        len(
            claimed
        )
        == 1
    )


def test_concurrent_claims_do_not_duplicate_messages() -> None:
    queue = InMemoryEventQueue()

    message_count = 100

    for index in range(
        message_count
    ):
        queue.publish(
            make_event(
                value=index
            )
        )

    claimed_ids: list[
        str
    ] = []

    lock = threading.Lock()

    def worker() -> None:
        while True:
            claimed = (
                queue.claim()
            )

            if claimed is None:
                return

            with lock:
                claimed_ids.append(
                    claimed.message_id
                )

    threads = [
        threading.Thread(
            target=worker
        )
        for _ in range(
            8
        )
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert (
        len(
            claimed_ids
        )
        == message_count
    )

    assert (
        len(
            set(
                claimed_ids
            )
        )
        == message_count
    )


def test_concurrent_claims_do_not_duplicate_due_messages_with_scheduled_head() -> None:
    clock = make_clock()

    queue = InMemoryEventQueue(
        clock=clock
    )

    queue.publish(
        make_event(
            value="future"
        ),
        message_id="future",
    )

    future = queue.claim()

    assert (
        future
        is not None
    )

    queue.nack(
        future.message_id,
        future.claim_token,
        retry_delay_seconds=60,
    )

    message_count = 50

    for index in range(
        message_count
    ):
        queue.publish(
            make_event(
                value=index
            ),
            message_id=(
                f"due-{index}"
            ),
        )

    claimed_ids: list[
        str
    ] = []

    result_lock = (
        threading.Lock()
    )

    def consume() -> None:
        while True:
            claimed = queue.claim()

            if claimed is None:
                return

            with result_lock:
                claimed_ids.append(
                    claimed.message_id
                )

            queue.ack(
                claimed.message_id,
                claimed.claim_token,
            )

    threads = [
        threading.Thread(
            target=consume
        )
        for _ in range(
            4
        )
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert (
        len(
            claimed_ids
        )
        == message_count
    )

    assert (
        len(
            set(
                claimed_ids
            )
        )
        == message_count
    )

    assert (
        "future"
        not in claimed_ids
    )

    assert (
        queue.pending_count
        == 1
    )


# =============================================================================
# REPRESENTATION
# =============================================================================
def test_len_reports_active_events() -> None:
    queue = InMemoryEventQueue()

    assert len(
        queue
    ) == 0

    queue.publish(
        make_event()
    )

    assert len(
        queue
    ) == 1


def test_repr_contains_queue_state() -> None:
    queue = InMemoryEventQueue(
        name="events"
    )

    rendered = repr(
        queue
    )

    assert (
        "events"
        in rendered
    )

    assert (
        "pending_count"
        in rendered
    )