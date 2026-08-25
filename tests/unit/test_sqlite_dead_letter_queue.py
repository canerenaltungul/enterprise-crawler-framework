from __future__ import annotations

import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from enterprise_crawler.contracts import Event
from enterprise_crawler.events import (
    DeadLetterQueueProtocol,
    DeadLetterValidationError,
    DuplicateDeadLetterError,
    SQLiteDeadLetterQueue,
    SQLiteDeadLetterQueueClosedError,
    SQLiteDeadLetterQueueError,
    UnknownDeadLetterError,
)


UTC = timezone.utc


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
            "source": "sqlite-dlq-test",
        },
    )


def make_queue(
    database_path: Path,
    *,
    name: str = "dead-letter",
) -> SQLiteDeadLetterQueue:
    return SQLiteDeadLetterQueue(
        database_path,
        name=name,
        timeout_seconds=10,
    )


# =============================================================================
# CONSTRUCTION
# =============================================================================
def test_sqlite_dlq_creates_database(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "dlq.sqlite3"
    )

    queue = make_queue(
        database_path
    )

    try:
        assert (
            database_path.is_file()
        )

    finally:
        queue.close()


def test_sqlite_dlq_satisfies_protocol(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    try:
        assert isinstance(
            queue,
            DeadLetterQueueProtocol,
        )

    finally:
        queue.close()


def test_default_configuration(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "dlq.sqlite3"
    )

    queue = (
        SQLiteDeadLetterQueue(
            database_path
        )
    )

    try:
        assert (
            queue.name
            == "dead-letter"
        )

        assert (
            queue.timeout_seconds
            == 5.0
        )

    finally:
        queue.close()


def test_custom_queue_name(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3",
        name="  failed-events  ",
    )

    try:
        assert (
            queue.name
            == "failed-events"
        )

    finally:
        queue.close()


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        "\n\t",
    ],
)
def test_empty_name_is_rejected(
    tmp_path: Path,
    value: str,
) -> None:
    with pytest.raises(
        DeadLetterValidationError
    ):
        SQLiteDeadLetterQueue(
            tmp_path
            / "dlq.sqlite3",
            name=value,
        )


@pytest.mark.parametrize(
    "value",
    [
        0,
        -1,
        True,
        float("inf"),
        float("nan"),
        "5",
    ],
)
def test_invalid_timeout_is_rejected(
    tmp_path: Path,
    value: Any,
) -> None:
    with pytest.raises(
        DeadLetterValidationError
    ):
        SQLiteDeadLetterQueue(
            tmp_path
            / "dlq.sqlite3",
            timeout_seconds=value,  # type: ignore[arg-type]
        )


def test_directory_cannot_be_database_path(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        SQLiteDeadLetterQueueError
    ):
        SQLiteDeadLetterQueue(
            tmp_path
        )


def test_parent_directory_is_created(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "nested"
        / "state"
        / "dlq.sqlite3"
    )

    queue = make_queue(
        database_path
    )

    try:
        assert (
            database_path.is_file()
        )

    finally:
        queue.close()


# =============================================================================
# EMPTY STATE
# =============================================================================
def test_empty_queue(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    try:
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

    finally:
        queue.close()


# =============================================================================
# STORE
# =============================================================================
def test_store_dead_letter(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    try:
        record = queue.store(
            make_event(
                event_id=1
            ),
            message_id="message-1",
            delivery_count=3,
            error=RuntimeError(
                "boom"
            ),
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

    finally:
        queue.close()


def test_explicit_dead_letter_id(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    try:
        record = queue.store(
            make_event(),
            message_id="message",
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

    finally:
        queue.close()


def test_duplicate_dead_letter_id_is_rejected(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    try:
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

    finally:
        queue.close()


def test_source_queue_and_claim_token_survive(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    try:
        stored = queue.store(
            make_event(),
            message_id="message",
            delivery_count=2,
            error=RuntimeError(),
            source_queue="source-events",
            claim_token="claim-123",
        )

        loaded = queue.get(
            stored.dead_letter_id
        )

        assert (
            loaded.source_queue
            == "source-events"
        )

        assert (
            loaded.claim_token
            == "claim-123"
        )

    finally:
        queue.close()


def test_record_metadata_survives(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    try:
        stored = queue.store(
            make_event(),
            message_id="message",
            delivery_count=1,
            error=RuntimeError(),
            metadata={
                "reason": "exhausted",
                "retry": {
                    "count": 3,
                },
            },
        )

        loaded = queue.get(
            stored.dead_letter_id
        )

        assert (
            loaded.metadata
            == {
                "reason": "exhausted",
                "retry": {
                    "count": 3,
                },
            }
        )

    finally:
        queue.close()


# =============================================================================
# VALIDATION
# =============================================================================
def test_non_event_is_rejected(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    try:
        with pytest.raises(
            DeadLetterValidationError
        ):
            queue.store(
                object(),  # type: ignore[arg-type]
                message_id="message",
                delivery_count=1,
                error=RuntimeError(),
            )

    finally:
        queue.close()


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        "\n",
    ],
)
def test_empty_message_id_is_rejected(
    tmp_path: Path,
    value: str,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    try:
        with pytest.raises(
            DeadLetterValidationError
        ):
            queue.store(
                make_event(),
                message_id=value,
                delivery_count=1,
                error=RuntimeError(),
            )

    finally:
        queue.close()


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
    tmp_path: Path,
    value: Any,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    try:
        with pytest.raises(
            DeadLetterValidationError
        ):
            queue.store(
                make_event(),
                message_id="message",
                delivery_count=value,  # type: ignore[arg-type]
                error=RuntimeError(),
            )

    finally:
        queue.close()


def test_error_must_be_exception(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    try:
        with pytest.raises(
            DeadLetterValidationError
        ):
            queue.store(
                make_event(),
                message_id="message",
                delivery_count=1,
                error="boom",  # type: ignore[arg-type]
            )

    finally:
        queue.close()


def test_non_json_serializable_payload_is_rejected(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    event = make_event()

    event.payload[
        "bad"
    ] = object()

    try:
        with pytest.raises(
            DeadLetterValidationError
        ):
            queue.store(
                event,
                message_id="message",
                delivery_count=1,
                error=RuntimeError(),
            )

    finally:
        queue.close()


def test_non_finite_json_number_is_rejected(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    event = make_event()

    event.payload[
        "value"
    ] = float("nan")

    try:
        with pytest.raises(
            DeadLetterValidationError
        ):
            queue.store(
                event,
                message_id="message",
                delivery_count=1,
                error=RuntimeError(),
            )

    finally:
        queue.close()


def test_non_json_serializable_metadata_is_rejected(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    try:
        with pytest.raises(
            DeadLetterValidationError
        ):
            queue.store(
                make_event(),
                message_id="message",
                delivery_count=1,
                error=RuntimeError(),
                metadata={
                    "bad": object(),
                },
            )

    finally:
        queue.close()


# =============================================================================
# PERSISTENCE
# =============================================================================
def test_record_survives_queue_reopen(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "dlq.sqlite3"
    )

    first = make_queue(
        database_path
    )

    stored = first.store(
        make_event(
            event_id=55,
            value="persistent",
        ),
        message_id="message-55",
        delivery_count=3,
        error=RuntimeError(
            "failed"
        ),
        dead_letter_id="dead-55",
        metadata={
            "source": "test",
        },
    )

    first.close()

    second = make_queue(
        database_path
    )

    try:
        loaded = second.get(
            stored.dead_letter_id
        )

        assert (
            loaded.dead_letter_id
            == "dead-55"
        )

        assert (
            loaded.message_id
            == "message-55"
        )

        assert (
            loaded.delivery_count
            == 3
        )

        assert (
            loaded.event.payload[
                "event_id"
            ]
            == 55
        )

        assert (
            loaded.event.payload[
                "value"
            ]
            == "persistent"
        )

        assert (
            loaded.failure_message
            == "failed"
        )

        assert (
            loaded.metadata
            == {
                "source": "test",
            }
        )

    finally:
        second.close()


def test_event_timestamp_survives_reopen(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "dlq.sqlite3"
    )

    event = make_event()

    first = make_queue(
        database_path
    )

    record = first.store(
        event,
        message_id="message",
        delivery_count=1,
        error=RuntimeError(),
    )

    first.close()

    second = make_queue(
        database_path
    )

    try:
        loaded = second.get(
            record.dead_letter_id
        )

        assert (
            loaded.event.timestamp
            == event.timestamp
        )

    finally:
        second.close()


# =============================================================================
# COPY BOUNDARIES
# =============================================================================
def test_source_mutation_after_store_does_not_change_database(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    event = make_event(
        value={
            "nested": 1,
        }
    )

    metadata = {
        "record": {
            "value": 1,
        }
    }

    try:
        stored = queue.store(
            event,
            message_id="message",
            delivery_count=1,
            error=RuntimeError(),
            metadata=metadata,
        )

        event.payload[
            "value"
        ][
            "nested"
        ] = 99

        metadata[
            "record"
        ][
            "value"
        ] = 99

        loaded = queue.get(
            stored.dead_letter_id
        )

        assert (
            loaded.event.payload[
                "value"
            ][
                "nested"
            ]
            == 1
        )

        assert (
            loaded.metadata[
                "record"
            ][
                "value"
            ]
            == 1
        )

    finally:
        queue.close()


def test_get_returns_independent_copy(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    try:
        stored = queue.store(
            make_event(
                value={
                    "safe": True,
                }
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
        ][
            "safe"
        ] = False

        second = queue.get(
            stored.dead_letter_id
        )

        assert (
            second.event.payload[
                "value"
            ][
                "safe"
            ]
            is True
        )

    finally:
        queue.close()


# =============================================================================
# UNICODE / NESTED
# =============================================================================
def test_unicode_payload_survives_persistence(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "dlq.sqlite3"
    )

    first = make_queue(
        database_path
    )

    record = first.store(
        Event(
            event_type="İMAR_DEĞİŞİKLİĞİ",
            timestamp=datetime.now(
                UTC
            ),
            payload={
                "şehir": "İstanbul",
                "ilçe": "Beyoğlu",
            },
            metadata={
                "kaynak": "İBB",
            },
        ),
        message_id="unicode",
        delivery_count=1,
        error=RuntimeError(
            "başarısız"
        ),
    )

    first.close()

    second = make_queue(
        database_path
    )

    try:
        loaded = second.get(
            record.dead_letter_id
        )

        assert (
            loaded.event.event_type
            == "İMAR_DEĞİŞİKLİĞİ"
        )

        assert (
            loaded.event.payload[
                "şehir"
            ]
            == "İstanbul"
        )

        assert (
            loaded.event.metadata[
                "kaynak"
            ]
            == "İBB"
        )

        assert (
            loaded.failure_message
            == "başarısız"
        )

    finally:
        second.close()


def test_nested_payload_survives_persistence(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    try:
        stored = queue.store(
            Event(
                event_type="NESTED",
                timestamp=datetime.now(
                    UTC
                ),
                payload={
                    "a": {
                        "b": [
                            1,
                            2,
                            {
                                "c": True,
                            },
                        ]
                    }
                },
                metadata={
                    "x": {
                        "y": [
                            "a",
                            "b",
                        ]
                    }
                },
            ),
            message_id="nested",
            delivery_count=2,
            error=RuntimeError(),
        )

        loaded = queue.get(
            stored.dead_letter_id
        )

        assert (
            loaded.event.payload[
                "a"
            ][
                "b"
            ][
                2
            ][
                "c"
            ]
            is True
        )

        assert (
            loaded.event.metadata[
                "x"
            ][
                "y"
            ]
            == [
                "a",
                "b",
            ]
        )

    finally:
        queue.close()


# =============================================================================
# ORDER
# =============================================================================
def test_records_are_returned_in_insertion_order(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    try:
        for index in range(
            10
        ):
            queue.store(
                make_event(
                    event_id=index
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

        records = (
            queue.records()
        )

        assert [
            record.dead_letter_id
            for record
            in records
        ] == [
            f"dead-{index}"
            for index
            in range(
                10
            )
        ]

    finally:
        queue.close()


# =============================================================================
# QUEUE ISOLATION
# =============================================================================
def test_same_database_can_host_multiple_dlq_names(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "dlq.sqlite3"
    )

    alpha = make_queue(
        database_path,
        name="alpha",
    )

    beta = make_queue(
        database_path,
        name="beta",
    )

    try:
        alpha.store(
            make_event(
                event_id=1
            ),
            message_id="same-message",
            delivery_count=1,
            error=RuntimeError(),
            dead_letter_id="same-id",
        )

        beta.store(
            make_event(
                event_id=2
            ),
            message_id="same-message",
            delivery_count=1,
            error=RuntimeError(),
            dead_letter_id="same-id",
        )

        assert (
            alpha.count
            == 1
        )

        assert (
            beta.count
            == 1
        )

        assert (
            alpha.get(
                "same-id"
            ).event.payload[
                "event_id"
            ]
            == 1
        )

        assert (
            beta.get(
                "same-id"
            ).event.payload[
                "event_id"
            ]
            == 2
        )

    finally:
        alpha.close()
        beta.close()


# =============================================================================
# MULTIPLE INSTANCES
# =============================================================================
def test_two_instances_can_read_same_queue(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "dlq.sqlite3"
    )

    first = make_queue(
        database_path,
        name="shared",
    )

    second = make_queue(
        database_path,
        name="shared",
    )

    try:
        first.store(
            make_event(
                event_id=1
            ),
            message_id="message",
            delivery_count=1,
            error=RuntimeError(),
            dead_letter_id="dead",
        )

        loaded = second.get(
            "dead"
        )

        assert (
            loaded.event.payload[
                "event_id"
            ]
            == 1
        )

    finally:
        first.close()
        second.close()


# =============================================================================
# CONCURRENCY
# =============================================================================
def test_concurrent_stores_are_safe(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "dlq.sqlite3"
    )

    first = make_queue(
        database_path,
        name="shared",
    )

    second = make_queue(
        database_path,
        name="shared",
    )

    errors: list[
        BaseException
    ] = []

    errors_lock = (
        threading.Lock()
    )

    count = 100

    def store(
        index: int,
    ) -> None:
        queue = (
            first
            if index % 2 == 0
            else second
        )

        try:
            queue.store(
                make_event(
                    event_id=index
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

        except BaseException as exc:
            with errors_lock:
                errors.append(
                    exc
                )

    threads = [
        threading.Thread(
            target=store,
            args=(
                index,
            ),
        )
        for index
        in range(
            count
        )
    ]

    try:
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join(
                timeout=5
            )

            assert (
                thread.is_alive()
                is False
            )

        assert (
            errors
            == []
        )

        assert (
            first.count
            == count
        )

        assert (
            second.count
            == count
        )

        assert (
            len(
                first.records()
            )
            == count
        )

    finally:
        first.close()
        second.close()


def test_concurrent_duplicate_id_allows_only_one_store(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "dlq.sqlite3"
    )

    first = make_queue(
        database_path,
        name="shared",
    )

    second = make_queue(
        database_path,
        name="shared",
    )

    success_count = 0

    duplicate_count = 0

    lock = (
        threading.Lock()
    )

    def store(
        queue: SQLiteDeadLetterQueue,
    ) -> None:
        nonlocal success_count
        nonlocal duplicate_count

        try:
            queue.store(
                make_event(),
                message_id="message",
                delivery_count=1,
                error=RuntimeError(),
                dead_letter_id="same",
            )

        except DuplicateDeadLetterError:
            with lock:
                duplicate_count += 1

        else:
            with lock:
                success_count += 1

    threads = [
        threading.Thread(
            target=store,
            args=(
                first,
            ),
        ),
        threading.Thread(
            target=store,
            args=(
                second,
            ),
        ),
    ]

    try:
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join(
                timeout=5
            )

        assert (
            success_count
            == 1
        )

        assert (
            duplicate_count
            == 1
        )

        assert (
            first.count
            == 1
        )

    finally:
        first.close()
        second.close()


# =============================================================================
# LOOKUP / REMOVE
# =============================================================================
def test_unknown_dead_letter_is_rejected(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    try:
        with pytest.raises(
            UnknownDeadLetterError
        ):
            queue.get(
                "missing"
            )

    finally:
        queue.close()


def test_remove_dead_letter(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    try:
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

        assert (
            queue.contains(
                stored.dead_letter_id
            )
            is False
        )

    finally:
        queue.close()


def test_remove_unknown_dead_letter_is_rejected(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    try:
        with pytest.raises(
            UnknownDeadLetterError
        ):
            queue.remove(
                "missing"
            )

    finally:
        queue.close()


# =============================================================================
# SNAPSHOT
# =============================================================================
def test_snapshot_contains_configuration(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "dlq.sqlite3"
    )

    queue = make_queue(
        database_path,
        name="failed",
    )

    try:
        queue.store(
            make_event(),
            message_id="message",
            delivery_count=1,
            error=RuntimeError(),
            dead_letter_id="dead-1",
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
            == "sqlite"
        )

        assert (
            snapshot[
                "database_path"
            ]
            == str(
                database_path
            )
        )

        assert (
            snapshot[
                "count"
            ]
            == 1
        )

        assert (
            snapshot[
                "dead_letter_ids"
            ]
            == [
                "dead-1"
            ]
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
        / "dlq.sqlite3"
    )

    queue.close()
    queue.close()

    assert (
        queue.is_closed
        is True
    )


def test_closed_queue_rejects_store(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    queue.close()

    with pytest.raises(
        SQLiteDeadLetterQueueClosedError
    ):
        queue.store(
            make_event(),
            message_id="message",
            delivery_count=1,
            error=RuntimeError(),
        )


def test_closed_queue_rejects_get(
    tmp_path: Path,
) -> None:
    queue = make_queue(
        tmp_path
        / "dlq.sqlite3"
    )

    queue.close()

    with pytest.raises(
        SQLiteDeadLetterQueueClosedError
    ):
        queue.get(
            "dead"
        )


def test_context_manager_closes_queue(
    tmp_path: Path,
) -> None:
    database_path = (
        tmp_path
        / "dlq.sqlite3"
    )

    queue: SQLiteDeadLetterQueue

    with SQLiteDeadLetterQueue(
        database_path
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
    database_path = (
        tmp_path
        / "dlq.sqlite3"
    )

    queue = make_queue(
        database_path,
        name="failed",
    )

    try:
        rendered = repr(
            queue
        )

        assert (
            "SQLiteDeadLetterQueue"
            in rendered
        )

        assert (
            "failed"
            in rendered
        )

        assert (
    repr(
        str(
            database_path
        )
    )
    in rendered
)

    finally:
        queue.close()