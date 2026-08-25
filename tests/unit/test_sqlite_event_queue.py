from __future__ import annotations

import sqlite3
import threading
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from pathlib import Path
from time import sleep
from typing import Any

import pytest

from enterprise_crawler.contracts import Event
from enterprise_crawler.events import (
    DuplicateEventMessageError,
    EventClaimOwnershipError,
    EventNotClaimedError,
    EventQueueClosedError,
    EventQueueProtocol,
    EventQueueValidationError,
    SQLiteEventQueue,
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


def make_queue(
    tmp_path: Path,
    *,
    name: str = "default",
    lease_seconds: float = 30.0,
    clock: Any = None,
) -> SQLiteEventQueue:
    return SQLiteEventQueue(
        tmp_path
        / "events.sqlite3",
        name=name,
        lease_seconds=(
            lease_seconds
        ),
        clock=clock,
    )


# =============================================================================
# CONSTRUCTION
# =============================================================================
def test_sqlite_queue_creates_database(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        assert (
            queue.database_path.is_file()
        )

    finally:
        queue.close()


def test_sqlite_queue_satisfies_event_queue_protocol(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        assert isinstance(
            queue,
            EventQueueProtocol,
        )

    finally:
        queue.close()


def test_default_configuration(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        assert (
            queue.name
            == "default"
        )

        assert (
            queue.lease_seconds
            == 30.0
        )

        assert (
            queue.timeout_seconds
            == 15.0
        )

    finally:
        queue.close()


def test_custom_queue_name(
    tmp_path: Path,
) -> None:
    queue = SQLiteEventQueue(
        tmp_path
        / "events.sqlite3",
        name="  jobs  ",
    )

    try:
        assert (
            queue.name
            == "jobs"
        )

    finally:
        queue.close()


@pytest.mark.parametrize(
    "name",
    [
        "",
        " ",
        "\n\t",
    ],
)
def test_empty_name_is_rejected(
    tmp_path: Path,
    name: str,
) -> None:
    with pytest.raises(
        EventQueueValidationError
    ):
        SQLiteEventQueue(
            tmp_path
            / "events.sqlite3",
            name=name,
        )


@pytest.mark.parametrize(
    "value",
    [
        0,
        -1,
        True,
        "30",
        float("inf"),
        float("nan"),
    ],
)
def test_invalid_lease_seconds_is_rejected(
    tmp_path: Path,
    value: Any,
) -> None:
    with pytest.raises(
        EventQueueValidationError
    ):
        SQLiteEventQueue(
            tmp_path
            / "events.sqlite3",
            lease_seconds=value,
        )


@pytest.mark.parametrize(
    "value",
    [
        0,
        -1,
        True,
        "15",
    ],
)
def test_invalid_timeout_is_rejected(
    tmp_path: Path,
    value: Any,
) -> None:
    with pytest.raises(
        EventQueueValidationError
    ):
        SQLiteEventQueue(
            tmp_path
            / "events.sqlite3",
            timeout_seconds=value,
        )


def test_directory_cannot_be_database_path(
    tmp_path: Path,
) -> None:
    directory = (
        tmp_path
        / "database"
    )

    directory.mkdir()

    with pytest.raises(
        EventQueueValidationError
    ):
        SQLiteEventQueue(
            directory
        )


def test_parent_directory_is_created(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "nested"
        / "queue"
        / "events.sqlite3"
    )

    queue = SQLiteEventQueue(
        path
    )

    try:
        assert (
            path.is_file()
        )

    finally:
        queue.close()


def test_custom_clock_is_used(
    tmp_path: Path,
) -> None:
    clock = make_clock()

    queue = make_queue(
        tmp_path,
        clock=clock,
    )

    try:
        published = queue.publish(
            make_event()
        )

        assert (
            published.published_at
            == clock.current
        )

    finally:
        queue.close()


def test_non_callable_clock_is_rejected(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        EventQueueValidationError
    ):
        SQLiteEventQueue(
            tmp_path
            / "events.sqlite3",
            clock=123,  # type: ignore[arg-type]
        )


# =============================================================================
# SCHEMA / MIGRATION
# =============================================================================
def test_schema_version_is_two(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        snapshot = (
            queue.snapshot()
        )

        assert (
            snapshot[
                "schema_version"
            ]
            == 2
        )

    finally:
        queue.close()


def test_fresh_database_contains_next_attempt_at_column(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "events.sqlite3"
    )

    queue = SQLiteEventQueue(
        path
    )

    queue.close()

    connection = sqlite3.connect(
        path
    )

    try:
        rows = connection.execute(
            """
            PRAGMA table_info(
                event_queue_messages
            )
            """
        ).fetchall()

        columns = {
            row[
                1
            ]
            for row
            in rows
        }

        assert (
            "next_attempt_at"
            in columns
        )

    finally:
        connection.close()


def test_v1_database_is_migrated_to_v2(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "events.sqlite3"
    )

    connection = sqlite3.connect(
        path
    )

    try:
        connection.executescript(
            """
            CREATE TABLE event_queue_messages (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_name TEXT NOT NULL,
                message_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_timestamp TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                published_at TEXT NOT NULL,
                state TEXT NOT NULL
                    CHECK (state IN ('pending', 'claimed')),
                delivery_count INTEGER NOT NULL DEFAULT 0
                    CHECK (delivery_count >= 0),
                claim_token TEXT,
                claimed_at TEXT,
                lease_expires_at TEXT,
                UNIQUE(queue_name, message_id)
            );

            CREATE TABLE event_queue_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            INSERT INTO event_queue_meta (
                key,
                value
            )
            VALUES (
                'schema_version',
                '1'
            );
            """
        )

        connection.commit()

    finally:
        connection.close()

    queue = SQLiteEventQueue(
        path
    )

    queue.close()

    connection = sqlite3.connect(
        path
    )

    try:
        columns = {
            row[
                1
            ]
            for row
            in connection.execute(
                """
                PRAGMA table_info(
                    event_queue_messages
                )
                """
            ).fetchall()
        }

        assert (
            "next_attempt_at"
            in columns
        )

        row = connection.execute(
            """
            SELECT value
            FROM event_queue_meta
            WHERE key = 'schema_version'
            """
        ).fetchone()

        assert (
            row
            is not None
        )

        assert (
            row[
                0
            ]
            == "2"
        )

    finally:
        connection.close()


def test_v1_pending_event_survives_schema_migration(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "events.sqlite3"
    )

    timestamp = datetime(
        2026,
        8,
        19,
        12,
        0,
        tzinfo=UTC,
    ).isoformat()

    connection = sqlite3.connect(
        path
    )

    try:
        connection.executescript(
            """
            CREATE TABLE event_queue_messages (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_name TEXT NOT NULL,
                message_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_timestamp TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                published_at TEXT NOT NULL,
                state TEXT NOT NULL
                    CHECK (state IN ('pending', 'claimed')),
                delivery_count INTEGER NOT NULL DEFAULT 0
                    CHECK (delivery_count >= 0),
                claim_token TEXT,
                claimed_at TEXT,
                lease_expires_at TEXT,
                UNIQUE(queue_name, message_id)
            );

            CREATE TABLE event_queue_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            INSERT INTO event_queue_meta (
                key,
                value
            )
            VALUES (
                'schema_version',
                '1'
            );
            """
        )

        connection.execute(
            """
            INSERT INTO event_queue_messages (
                queue_name,
                message_id,
                event_type,
                event_timestamp,
                payload_json,
                metadata_json,
                published_at,
                state,
                delivery_count,
                claim_token,
                claimed_at,
                lease_expires_at
            )
            VALUES (
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                'pending',
                0,
                NULL,
                NULL,
                NULL
            )
            """,
            (
                "default",
                "legacy-message",
                "TEST_EVENT",
                timestamp,
                '{"value":123}',
                '{"source":"legacy"}',
                timestamp,
            ),
        )

        connection.commit()

    finally:
        connection.close()

    queue = SQLiteEventQueue(
        path
    )

    try:
        claimed = (
            queue.claim()
        )

        assert (
            claimed
            is not None
        )

        assert (
            claimed.message_id
            == "legacy-message"
        )

        assert (
            claimed.event.payload[
                "value"
            ]
            == 123
        )

        assert (
            claimed.delivery_count
            == 1
        )

    finally:
        queue.close()


# =============================================================================
# INITIAL STATE
# =============================================================================
def test_empty_queue(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
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
            queue.is_empty
            is True
        )

        assert (
            queue.claim()
            is None
        )

    finally:
        queue.close()


# =============================================================================
# PUBLISH
# =============================================================================
def test_publish_event(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        published = (
            queue.publish(
                make_event()
            )
        )

        assert (
            published.message_id
        )

        assert (
            published.event_type
            == "TEST_EVENT"
        )

        assert (
            queue.pending_count
            == 1
        )

        assert (
            queue.active_count
            == 1
        )

    finally:
        queue.close()


def test_explicit_message_id(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        published = (
            queue.publish(
                make_event(),
                message_id="message-1",
            )
        )

        assert (
            published.message_id
            == "message-1"
        )

        assert (
            queue.contains(
                "message-1"
            )
            is True
        )

    finally:
        queue.close()


def test_duplicate_message_id_is_rejected(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
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

    finally:
        queue.close()


def test_invalid_event_is_rejected(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        with pytest.raises(
            EventQueueValidationError
        ):
            queue.publish(
                object()  # type: ignore[arg-type]
            )

    finally:
        queue.close()


def test_non_json_serializable_payload_is_rejected(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    event = make_event()

    event.payload[
        "bad"
    ] = object()

    try:
        with pytest.raises(
            EventQueueValidationError
        ):
            queue.publish(
                event
            )

    finally:
        queue.close()


def test_non_finite_json_number_is_rejected(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    event = make_event()

    event.payload[
        "value"
    ] = float("nan")

    try:
        with pytest.raises(
            EventQueueValidationError
        ):
            queue.publish(
                event
            )

    finally:
        queue.close()


# =============================================================================
# DURABILITY
# =============================================================================
def test_event_survives_new_queue_instance(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "events.sqlite3"
    )

    first = SQLiteEventQueue(
        path
    )

    first.publish(
        make_event(
            value=123
        ),
        message_id="persistent",
    )

    first.close()

    second = SQLiteEventQueue(
        path
    )

    try:
        assert (
            second.pending_count
            == 1
        )

        claimed = (
            second.claim()
        )

        assert (
            claimed
            is not None
        )

        assert (
            claimed.message_id
            == "persistent"
        )

        assert (
            claimed.event.payload[
                "value"
            ]
            == 123
        )

    finally:
        second.close()


def test_delivery_count_survives_queue_reopen(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "events.sqlite3"
    )

    first = SQLiteEventQueue(
        path
    )

    first.publish(
        make_event(),
        message_id="persistent",
    )

    claimed = (
        first.claim()
    )

    assert (
        claimed
        is not None
    )

    first.nack(
        claimed.message_id,
        claimed.claim_token,
    )

    first.close()

    second = SQLiteEventQueue(
        path
    )

    try:
        redelivery = (
            second.claim()
        )

        assert (
            redelivery
            is not None
        )

        assert (
            redelivery.delivery_count
            == 2
        )

    finally:
        second.close()


# =============================================================================
# FIFO
# =============================================================================
def test_claim_is_fifo(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        for value in [
            1,
            2,
            3,
        ]:
            queue.publish(
                make_event(
                    value=value
                )
            )

        first = queue.claim()
        second = queue.claim()
        third = queue.claim()

        assert (
            first
            is not None
        )

        assert (
            second
            is not None
        )

        assert (
            third
            is not None
        )

        assert [
            first.event.payload[
                "value"
            ],
            second.event.payload[
                "value"
            ],
            third.event.payload[
                "value"
            ],
        ] == [
            1,
            2,
            3,
        ]

    finally:
        queue.close()


# =============================================================================
# CLAIM
# =============================================================================
def test_claim_sets_lease(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path,
        lease_seconds=10,
    )

    try:
        queue.publish(
            make_event()
        )

        claimed = (
            queue.claim()
        )

        assert (
            claimed
            is not None
        )

        assert (
            claimed.claim_token
        )

        assert (
            claimed.delivery_count
            == 1
        )

        assert (
            claimed.lease_expires_at
            is not None
        )

        assert (
            claimed.lease_expires_at
            > claimed.claimed_at
        )

        assert (
            queue.pending_count
            == 0
        )

        assert (
            queue.claimed_count
            == 1
        )

    finally:
        queue.close()


def test_claimed_event_is_not_immediately_claimable_again(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        queue.publish(
            make_event()
        )

        first = queue.claim()

        assert (
            first
            is not None
        )

        assert (
            queue.claim()
            is None
        )

    finally:
        queue.close()


# =============================================================================
# SCHEDULED RETRY
# =============================================================================
def test_delayed_requeue_is_not_immediately_claimable(
    tmp_path: Path,
) -> None:
    clock = make_clock()

    queue = make_queue(
        tmp_path,
        clock=clock,
    )

    try:
        queue.publish(
            make_event(),
            message_id="scheduled",
        )

        first = (
            queue.claim()
        )

        assert (
            first
            is not None
        )

        queue.nack(
            first.message_id,
            first.claim_token,
            retry_delay_seconds=30,
        )

        snapshot = (
            queue.message_snapshot(
                "scheduled"
            )
        )

        assert (
            snapshot[
                "state"
            ]
            == "pending"
        )

        assert (
            snapshot[
                "delivery_count"
            ]
            == 1
        )

        assert (
            snapshot[
                "next_attempt_at"
            ]
            is not None
        )

        assert (
            queue.claim()
            is None
        )

    finally:
        queue.close()


def test_delayed_requeue_becomes_claimable_when_due(
    tmp_path: Path,
) -> None:
    clock = make_clock()

    queue = make_queue(
        tmp_path,
        clock=clock,
    )

    try:
        queue.publish(
            make_event(),
            message_id="scheduled",
        )

        first = (
            queue.claim()
        )

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

        second = (
            queue.claim()
        )

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

        snapshot = (
            queue.message_snapshot(
                "scheduled"
            )
        )

        assert (
            snapshot[
                "next_attempt_at"
            ]
            is None
        )

    finally:
        queue.close()


def test_zero_retry_delay_preserves_immediate_requeue(
    tmp_path: Path,
) -> None:
    clock = make_clock()

    queue = make_queue(
        tmp_path,
        clock=clock,
    )

    try:
        queue.publish(
            make_event(),
            message_id="message",
        )

        first = (
            queue.claim()
        )

        assert (
            first
            is not None
        )

        queue.nack(
            first.message_id,
            first.claim_token,
            retry_delay_seconds=0,
        )

        snapshot = (
            queue.message_snapshot(
                "message"
            )
        )

        assert (
            snapshot[
                "next_attempt_at"
            ]
            is None
        )

        second = (
            queue.claim()
        )

        assert (
            second
            is not None
        )

        assert (
            second.delivery_count
            == 2
        )

    finally:
        queue.close()


def test_future_head_does_not_block_due_event(
    tmp_path: Path,
) -> None:
    clock = make_clock()

    queue = make_queue(
        tmp_path,
        clock=clock,
    )

    try:
        queue.publish(
            make_event(
                value="future"
            ),
            message_id="future",
        )

        future = (
            queue.claim()
        )

        assert (
            future
            is not None
        )

        queue.nack(
            future.message_id,
            future.claim_token,
            retry_delay_seconds=60,
        )

        queue.publish(
            make_event(
                value="due"
            ),
            message_id="due",
        )

        claimed = (
            queue.claim()
        )

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

    finally:
        queue.close()


def test_eligible_fifo_is_preserved_with_scheduled_head(
    tmp_path: Path,
) -> None:
    clock = make_clock()

    queue = make_queue(
        tmp_path,
        clock=clock,
    )

    try:
        queue.publish(
            make_event(
                value="future"
            ),
            message_id="future",
        )

        future = (
            queue.claim()
        )

        assert (
            future
            is not None
        )

        queue.nack(
            future.message_id,
            future.claim_token,
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

        first = (
            queue.claim()
        )

        second = (
            queue.claim()
        )

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
            "due-1",
            "due-2",
        ]

    finally:
        queue.close()


def test_original_fifo_is_restored_when_scheduled_event_becomes_due(
    tmp_path: Path,
) -> None:
    clock = make_clock()

    queue = make_queue(
        tmp_path,
        clock=clock,
    )

    try:
        queue.publish(
            make_event(
                value="future"
            ),
            message_id="future",
        )

        future = (
            queue.claim()
        )

        assert (
            future
            is not None
        )

        queue.nack(
            future.message_id,
            future.claim_token,
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

        first = (
            queue.claim()
        )

        second = (
            queue.claim()
        )

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

    finally:
        queue.close()


def test_scheduled_wait_does_not_increment_delivery_count(
    tmp_path: Path,
) -> None:
    clock = make_clock()

    queue = make_queue(
        tmp_path,
        clock=clock,
    )

    try:
        queue.publish(
            make_event(),
            message_id="message",
        )

        first = (
            queue.claim()
        )

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

        waiting = (
            queue.message_snapshot(
                "message"
            )
        )

        assert (
            waiting[
                "delivery_count"
            ]
            == 1
        )

        clock.advance(
            seconds=10
        )

        assert (
            queue.claim()
            is None
        )

        waiting_again = (
            queue.message_snapshot(
                "message"
            )
        )

        assert (
            waiting_again[
                "delivery_count"
            ]
            == 1
        )

        clock.advance(
            seconds=20
        )

        second = (
            queue.claim()
        )

        assert (
            second
            is not None
        )

        assert (
            second.delivery_count
            == 2
        )

    finally:
        queue.close()


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
    tmp_path: Path,
    value: Any,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        queue.publish(
            make_event()
        )

        claimed = (
            queue.claim()
        )

        assert (
            claimed
            is not None
        )

        with pytest.raises(
            EventQueueValidationError
        ):
            queue.nack(
                claimed.message_id,
                claimed.claim_token,
                retry_delay_seconds=value,  # type: ignore[arg-type]
            )

        assert (
            queue.claimed_count
            == 1
        )

    finally:
        queue.close()


def test_discard_rejects_nonzero_retry_delay(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        queue.publish(
            make_event()
        )

        claimed = (
            queue.claim()
        )

        assert (
            claimed
            is not None
        )

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

    finally:
        queue.close()


# =============================================================================
# SCHEDULED RETRY DURABILITY
# =============================================================================
def test_scheduled_retry_survives_queue_reopen(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "events.sqlite3"
    )

    clock = make_clock()

    first_queue = SQLiteEventQueue(
        path,
        clock=clock,
    )

    first_queue.publish(
        make_event(),
        message_id="scheduled",
    )

    claimed = (
        first_queue.claim()
    )

    assert (
        claimed
        is not None
    )

    first_queue.nack(
        claimed.message_id,
        claimed.claim_token,
        retry_delay_seconds=60,
    )

    before_close = (
        first_queue.message_snapshot(
            "scheduled"
        )
    )

    scheduled_time = (
        before_close[
            "next_attempt_at"
        ]
    )

    assert (
        scheduled_time
        is not None
    )

    first_queue.close()

    second_queue = SQLiteEventQueue(
        path,
        clock=clock,
    )

    try:
        after_reopen = (
            second_queue.message_snapshot(
                "scheduled"
            )
        )

        assert (
            after_reopen[
                "next_attempt_at"
            ]
            == scheduled_time
        )

        assert (
            second_queue.claim()
            is None
        )

        clock.advance(
            seconds=60
        )

        redelivery = (
            second_queue.claim()
        )

        assert (
            redelivery
            is not None
        )

        assert (
            redelivery.message_id
            == "scheduled"
        )

        assert (
            redelivery.delivery_count
            == 2
        )

    finally:
        second_queue.close()


def test_scheduled_retry_queue_name_isolation(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "events.sqlite3"
    )

    clock = make_clock()

    alpha = SQLiteEventQueue(
        path,
        name="alpha",
        clock=clock,
    )

    beta = SQLiteEventQueue(
        path,
        name="beta",
        clock=clock,
    )

    try:
        alpha.publish(
            make_event(
                value="alpha"
            ),
            message_id="same",
        )

        beta.publish(
            make_event(
                value="beta"
            ),
            message_id="same",
        )

        alpha_claim = (
            alpha.claim()
        )

        assert (
            alpha_claim
            is not None
        )

        alpha.nack(
            alpha_claim.message_id,
            alpha_claim.claim_token,
            retry_delay_seconds=60,
        )

        beta_claim = (
            beta.claim()
        )

        assert (
            beta_claim
            is not None
        )

        assert (
            beta_claim.event.payload[
                "value"
            ]
            == "beta"
        )

        assert (
            alpha.claim()
            is None
        )

    finally:
        alpha.close()
        beta.close()


# =============================================================================
# ACK
# =============================================================================
def test_ack_removes_event(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        queue.publish(
            make_event(),
            message_id="ack-me",
        )

        claimed = (
            queue.claim()
        )

        assert (
            claimed
            is not None
        )

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
            queue.contains(
                "ack-me"
            )
            is False
        )

    finally:
        queue.close()


def test_ack_wrong_token_is_rejected(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        queue.publish(
            make_event()
        )

        claimed = (
            queue.claim()
        )

        assert (
            claimed
            is not None
        )

        with pytest.raises(
            EventClaimOwnershipError
        ):
            queue.ack(
                claimed.message_id,
                "wrong-token",
            )

        assert (
            queue.claimed_count
            == 1
        )

    finally:
        queue.close()


def test_pending_event_cannot_be_acked(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        published = (
            queue.publish(
                make_event()
            )
        )

        with pytest.raises(
            EventNotClaimedError
        ):
            queue.ack(
                published.message_id,
                "token",
            )

    finally:
        queue.close()


def test_unknown_event_cannot_be_acked(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        with pytest.raises(
            UnknownEventMessageError
        ):
            queue.ack(
                "missing",
                "token",
            )

    finally:
        queue.close()


# =============================================================================
# NACK
# =============================================================================
def test_nack_requeues_event(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        queue.publish(
            make_event(),
            message_id="retry",
        )

        first = (
            queue.claim()
        )

        assert (
            first
            is not None
        )

        queue.nack(
            first.message_id,
            first.claim_token,
        )

        assert (
            queue.pending_count
            == 1
        )

        assert (
            queue.claimed_count
            == 0
        )

        second = (
            queue.claim()
        )

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

        assert (
            second.claim_token
            != first.claim_token
        )

    finally:
        queue.close()


def test_nack_can_discard_event(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        queue.publish(
            make_event()
        )

        claimed = (
            queue.claim()
        )

        assert (
            claimed
            is not None
        )

        queue.nack(
            claimed.message_id,
            claimed.claim_token,
            requeue=False,
        )

        assert (
            queue.active_count
            == 0
        )

    finally:
        queue.close()


def test_nack_wrong_token_is_rejected(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        queue.publish(
            make_event()
        )

        claimed = (
            queue.claim()
        )

        assert (
            claimed
            is not None
        )

        with pytest.raises(
            EventClaimOwnershipError
        ):
            queue.nack(
                claimed.message_id,
                "wrong",
            )

    finally:
        queue.close()


# =============================================================================
# MESSAGE SNAPSHOT
# =============================================================================
def test_message_snapshot_pending(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        queue.publish(
            make_event(),
            message_id="message",
        )

        snapshot = (
            queue.message_snapshot(
                "message"
            )
        )

        assert (
            snapshot[
                "state"
            ]
            == "pending"
        )

        assert (
            snapshot[
                "delivery_count"
            ]
            == 0
        )

        assert (
            snapshot[
                "claim_token"
            ]
            is None
        )

        assert (
            snapshot[
                "next_attempt_at"
            ]
            is None
        )

    finally:
        queue.close()


def test_message_snapshot_claimed(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    try:
        queue.publish(
            make_event(),
            message_id="message",
        )

        claimed = (
            queue.claim()
        )

        assert (
            claimed
            is not None
        )

        snapshot = (
            queue.message_snapshot(
                "message"
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
            snapshot[
                "claim_token"
            ]
            == claimed.claim_token
        )

        assert (
            snapshot[
                "lease_expires_at"
            ]
            is not None
        )

        assert (
            snapshot[
                "next_attempt_at"
            ]
            is None
        )

    finally:
        queue.close()


# =============================================================================
# STALE LEASE
# =============================================================================
def test_expired_lease_is_recovered_on_claim(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path,
        lease_seconds=0.02,
    )

    try:
        queue.publish(
            make_event(),
            message_id="lease-test",
        )

        first = (
            queue.claim()
        )

        assert (
            first
            is not None
        )

        sleep(
            0.04
        )

        second = (
            queue.claim()
        )

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

        assert (
            second.claim_token
            != first.claim_token
        )

    finally:
        queue.close()


def test_explicit_stale_lease_recovery(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path,
        lease_seconds=0.02,
    )

    try:
        queue.publish(
            make_event()
        )

        claimed = (
            queue.claim()
        )

        assert (
            claimed
            is not None
        )

        sleep(
            0.04
        )

        recovered = (
            queue.recover_stale_leases()
        )

        assert (
            recovered
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

    finally:
        queue.close()


def test_non_expired_lease_is_not_recovered(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path,
        lease_seconds=60,
    )

    try:
        queue.publish(
            make_event()
        )

        claimed = (
            queue.claim()
        )

        assert (
            claimed
            is not None
        )

        recovered = (
            queue.recover_stale_leases()
        )

        assert (
            recovered
            == 0
        )

        assert (
            queue.claimed_count
            == 1
        )

    finally:
        queue.close()


def test_stale_claim_token_cannot_ack_redelivery(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path,
        lease_seconds=0.02,
    )

    try:
        queue.publish(
            make_event()
        )

        first = (
            queue.claim()
        )

        assert (
            first
            is not None
        )

        sleep(
            0.04
        )

        second = (
            queue.claim()
        )

        assert (
            second
            is not None
        )

        with pytest.raises(
            EventClaimOwnershipError
        ):
            queue.ack(
                first.message_id,
                first.claim_token,
            )

        queue.ack(
            second.message_id,
            second.claim_token,
        )

        assert (
            queue.active_count
            == 0
        )

    finally:
        queue.close()


def test_stale_lease_recovery_does_not_create_retry_delay(
    tmp_path: Path,
) -> None:
    clock = make_clock()

    queue = make_queue(
        tmp_path,
        lease_seconds=10,
        clock=clock,
    )

    try:
        queue.publish(
            make_event(),
            message_id="message",
        )

        first = (
            queue.claim()
        )

        assert (
            first
            is not None
        )

        clock.advance(
            seconds=10
        )

        recovered = (
            queue.recover_stale_leases()
        )

        assert (
            recovered
            == 1
        )

        snapshot = (
            queue.message_snapshot(
                "message"
            )
        )

        assert (
            snapshot[
                "state"
            ]
            == "pending"
        )

        assert (
            snapshot[
                "next_attempt_at"
            ]
            is None
        )

        second = (
            queue.claim()
        )

        assert (
            second
            is not None
        )

        assert (
            second.delivery_count
            == 2
        )

    finally:
        queue.close()


# =============================================================================
# MULTIPLE QUEUE NAMES
# =============================================================================
def test_same_database_can_host_multiple_queue_names(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "events.sqlite3"
    )

    alpha = SQLiteEventQueue(
        path,
        name="alpha",
    )

    beta = SQLiteEventQueue(
        path,
        name="beta",
    )

    try:
        alpha.publish(
            make_event(
                value="alpha"
            ),
            message_id="same-id",
        )

        beta.publish(
            make_event(
                value="beta"
            ),
            message_id="same-id",
        )

        assert (
            alpha.active_count
            == 1
        )

        assert (
            beta.active_count
            == 1
        )

        alpha_claim = (
            alpha.claim()
        )

        beta_claim = (
            beta.claim()
        )

        assert (
            alpha_claim
            is not None
        )

        assert (
            beta_claim
            is not None
        )

        assert (
            alpha_claim.event.payload[
                "value"
            ]
            == "alpha"
        )

        assert (
            beta_claim.event.payload[
                "value"
            ]
            == "beta"
        )

    finally:
        alpha.close()
        beta.close()


# =============================================================================
# MULTIPLE INSTANCES / ATOMIC CLAIM
# =============================================================================
def test_two_instances_do_not_claim_same_message(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "events.sqlite3"
    )

    first = SQLiteEventQueue(
        path,
        name="jobs",
    )

    second = SQLiteEventQueue(
        path,
        name="jobs",
    )

    try:
        first.publish(
            make_event(),
            message_id="only-one",
        )

        results: list[
            Any
        ] = []

        lock = (
            threading.Lock()
        )

        barrier = (
            threading.Barrier(
                2
            )
        )

        def worker(
            queue: SQLiteEventQueue,
        ) -> None:
            barrier.wait(
                timeout=2
            )

            result = (
                queue.claim()
            )

            with lock:
                results.append(
                    result
                )

        thread_a = threading.Thread(
            target=worker,
            args=(
                first,
            ),
        )

        thread_b = threading.Thread(
            target=worker,
            args=(
                second,
            ),
        )

        thread_a.start()
        thread_b.start()

        thread_a.join(
            timeout=3
        )

        thread_b.join(
            timeout=3
        )

        claimed = [
            result
            for result
            in results
            if result
            is not None
        ]

        assert (
            len(
                claimed
            )
            == 1
        )

        assert (
            claimed[
                0
            ].message_id
            == "only-one"
        )

    finally:
        first.close()
        second.close()


def test_concurrent_instances_claim_each_message_once(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "events.sqlite3"
    )

    producer = SQLiteEventQueue(
        path,
        name="jobs",
    )

    first = SQLiteEventQueue(
        path,
        name="jobs",
    )

    second = SQLiteEventQueue(
        path,
        name="jobs",
    )

    event_count = 100

    try:
        for index in range(
            event_count
        ):
            producer.publish(
                make_event(
                    value=index
                ),
                message_id=(
                    f"message-{index}"
                ),
            )

        claimed_ids: list[
            str
        ] = []

        result_lock = (
            threading.Lock()
        )

        def consume(
            queue: SQLiteEventQueue,
        ) -> None:
            while True:
                claimed = (
                    queue.claim()
                )

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

        thread_a = threading.Thread(
            target=consume,
            args=(
                first,
            ),
        )

        thread_b = threading.Thread(
            target=consume,
            args=(
                second,
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
                claimed_ids
            )
            == event_count
        )

        assert (
            len(
                set(
                    claimed_ids
                )
            )
            == event_count
        )

        assert (
            producer.active_count
            == 0
        )

    finally:
        producer.close()
        first.close()
        second.close()


def test_two_instances_do_not_duplicate_due_scheduled_message(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "events.sqlite3"
    )

    clock = make_clock()

    producer = SQLiteEventQueue(
        path,
        name="jobs",
        clock=clock,
    )

    first = SQLiteEventQueue(
        path,
        name="jobs",
        clock=clock,
    )

    second = SQLiteEventQueue(
        path,
        name="jobs",
        clock=clock,
    )

    try:
        producer.publish(
            make_event(),
            message_id="scheduled",
        )

        initial = (
            producer.claim()
        )

        assert (
            initial
            is not None
        )

        producer.nack(
            initial.message_id,
            initial.claim_token,
            retry_delay_seconds=30,
        )

        assert (
            first.claim()
            is None
        )

        clock.advance(
            seconds=30
        )

        results: list[
            Any
        ] = []

        result_lock = (
            threading.Lock()
        )

        barrier = (
            threading.Barrier(
                2
            )
        )

        def consume(
            queue: SQLiteEventQueue,
        ) -> None:
            barrier.wait(
                timeout=2
            )

            result = (
                queue.claim()
            )

            with result_lock:
                results.append(
                    result
                )

        thread_a = threading.Thread(
            target=consume,
            args=(
                first,
            ),
        )

        thread_b = threading.Thread(
            target=consume,
            args=(
                second,
            ),
        )

        thread_a.start()
        thread_b.start()

        thread_a.join(
            timeout=3
        )

        thread_b.join(
            timeout=3
        )

        assert (
            thread_a.is_alive()
            is False
        )

        assert (
            thread_b.is_alive()
            is False
        )

        claimed = [
            result
            for result
            in results
            if result
            is not None
        ]

        assert (
            len(
                claimed
            )
            == 1
        )

        assert (
            claimed[
                0
            ].message_id
            == "scheduled"
        )

        assert (
            claimed[
                0
            ].delivery_count
            == 2
        )

    finally:
        producer.close()
        first.close()
        second.close()


# =============================================================================
# DATA INTEGRITY
# =============================================================================
def test_unicode_payload_survives_persistence(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    event = Event(
        event_type="UNICODE",
        timestamp=datetime.now(
            UTC
        ),
        payload={
            "şehir": "İstanbul",
            "mesaj": "Çağrı",
        },
        metadata={
            "kaynak": "İBB",
        },
    )

    try:
        queue.publish(
            event
        )

        claimed = (
            queue.claim()
        )

        assert (
            claimed
            is not None
        )

        assert (
            claimed.event.payload[
                "şehir"
            ]
            == "İstanbul"
        )

        assert (
            claimed.event.metadata[
                "kaynak"
            ]
            == "İBB"
        )

    finally:
        queue.close()


def test_nested_payload_survives_persistence(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    event = Event(
        event_type="NESTED",
        timestamp=datetime.now(
            UTC
        ),
        payload={
            "record": {
                "id": 1,
                "tags": [
                    "a",
                    "b",
                ],
            }
        },
        metadata={
            "trace": {
                "id": "abc",
            }
        },
    )

    try:
        queue.publish(
            event
        )

        claimed = (
            queue.claim()
        )

        assert (
            claimed
            is not None
        )

        assert (
            claimed.event.payload
            == event.payload
        )

        assert (
            claimed.event.metadata
            == event.metadata
        )

    finally:
        queue.close()


def test_source_mutation_after_publish_does_not_change_database(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    event = make_event(
        value="original"
    )

    try:
        queue.publish(
            event
        )

        event.payload[
            "value"
        ] = "mutated"

        claimed = (
            queue.claim()
        )

        assert (
            claimed
            is not None
        )

        assert (
            claimed.event.payload[
                "value"
            ]
            == "original"
        )

    finally:
        queue.close()


# =============================================================================
# SNAPSHOT
# =============================================================================
def test_snapshot_contains_configuration(
    tmp_path: Path,
) -> None:
    queue = SQLiteEventQueue(
        tmp_path
        / "events.sqlite3",
        name="jobs",
        lease_seconds=45,
        timeout_seconds=10,
    )

    try:
        snapshot = (
            queue.snapshot()
        )

        assert (
            snapshot[
                "name"
            ]
            == "jobs"
        )

        assert (
            snapshot[
                "backend"
            ]
            == "sqlite"
        )

        assert (
            snapshot[
                "schema_version"
            ]
            == 2
        )

        assert (
            snapshot[
                "lease_seconds"
            ]
            == 45.0
        )

        assert (
            snapshot[
                "timeout_seconds"
            ]
            == 10.0
        )

        assert (
            snapshot[
                "active_count"
            ]
            == 0
        )

    finally:
        queue.close()


# =============================================================================
# CLOSE
# =============================================================================
def test_close_is_idempotent(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    queue.close()
    queue.close()

    assert (
        queue.is_closed
        is True
    )


def test_closed_queue_rejects_publish(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    queue.close()

    with pytest.raises(
        EventQueueClosedError
    ):
        queue.publish(
            make_event()
        )


def test_closed_queue_rejects_claim(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
    )

    queue.close()

    with pytest.raises(
        EventQueueClosedError
    ):
        queue.claim()


def test_context_manager_closes_queue(
    tmp_path: Path,
) -> None:
    with SQLiteEventQueue(
        tmp_path
        / "events.sqlite3"
    ) as queue:
        assert (
            queue.is_closed
            is False
        )

    assert (
        queue.is_closed
        is True
    )


# =============================================================================
# REPRESENTATION
# =============================================================================
def test_repr_contains_configuration(
    tmp_path: Path,
) -> None:
    queue = SQLiteEventQueue(
        tmp_path
        / "events.sqlite3",
        name="jobs",
        lease_seconds=20,
    )

    try:
        rendered = repr(
            queue
        )

        assert (
            "SQLiteEventQueue"
            in rendered
        )

        assert (
            "jobs"
            in rendered
        )

        assert (
            "schema_version=2"
            in rendered
        )

        assert (
            "20.0"
            in rendered
        )

    finally:
        queue.close()