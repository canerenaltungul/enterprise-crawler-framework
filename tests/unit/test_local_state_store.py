from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any

import pytest

from enterprise_crawler.exceptions import StorageError
from enterprise_crawler.storage.local_state_store import (
    LocalStateStore,
    SeenRecord,
    StateEntry,
)


# =============================================================================
# INITIALIZATION
# =============================================================================
def test_database_is_created(
    tmp_path: Path,
) -> None:
    db_path = (
        tmp_path
        / "state"
        / "crawler.db"
    )

    store = LocalStateStore(
        db_path
    )

    assert db_path.exists()
    assert db_path.is_file()

    store.close()


def test_directory_cannot_be_database_path(
    tmp_path: Path,
) -> None:
    directory = (
        tmp_path
        / "state"
    )

    directory.mkdir()

    with pytest.raises(
        StorageError,
        match="klasör",
    ):
        LocalStateStore(
            directory
        )


@pytest.mark.parametrize(
    "timeout",
    [
        0,
        -1,
        True,
        "15",
    ],
)
def test_invalid_timeout_is_rejected(
    tmp_path: Path,
    timeout: Any,
) -> None:
    with pytest.raises(
        StorageError,
    ):
        LocalStateStore(
            tmp_path / "state.db",
            timeout_seconds=timeout,
        )


# =============================================================================
# SQLITE CONFIGURATION
# =============================================================================
def test_database_uses_wal_mode(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    connection = sqlite3.connect(
        store.db_path
    )

    try:
        journal_mode = (
            connection.execute(
                "PRAGMA journal_mode;"
            )
            .fetchone()[0]
        )

        assert (
            str(journal_mode).lower()
            == "wal"
        )

    finally:
        connection.close()

    store.close()


def test_required_tables_exist(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    connection = sqlite3.connect(
        store.db_path
    )

    try:
        rows = connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table';
            """
        ).fetchall()

        table_names = {
            row[0]
            for row in rows
        }

        assert (
            "seen_records"
            in table_names
        )

        assert (
            "state_values"
            in table_names
        )

    finally:
        connection.close()

    store.close()


# =============================================================================
# IDEMPOTENCY
# =============================================================================
def test_mark_seen_returns_true_for_new_key(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    assert (
        store.mark_seen(
            "documents",
            "doc-1",
        )
        is True
    )

    assert (
        store.has_seen(
            "documents",
            "doc-1",
        )
        is True
    )

    store.close()


def test_mark_seen_returns_false_for_duplicate(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    assert (
        store.mark_seen(
            "documents",
            "doc-1",
        )
        is True
    )

    assert (
        store.mark_seen(
            "documents",
            "doc-1",
        )
        is False
    )

    assert (
        store.seen_count()
        == 1
    )

    store.close()


def test_same_key_in_different_namespaces_is_independent(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    assert (
        store.mark_seen(
            "source-a",
            "record-1",
        )
        is True
    )

    assert (
        store.mark_seen(
            "source-b",
            "record-1",
        )
        is True
    )

    assert (
        store.seen_count()
        == 2
    )

    store.close()


def test_seen_metadata_is_stored(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    store.mark_seen(
        "documents",
        "doc-1",
        metadata={
            "sha256": "abc",
            "status": "downloaded",
        },
    )

    record = store.get_seen(
        "documents",
        "doc-1",
    )

    assert isinstance(
        record,
        SeenRecord,
    )

    assert (
        record.namespace
        == "documents"
    )

    assert (
        record.key
        == "doc-1"
    )

    assert record.metadata == {
        "sha256": "abc",
        "status": "downloaded",
    }

    store.close()


def test_duplicate_seen_updates_metadata_but_preserves_first_seen(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    store.mark_seen(
        "documents",
        "doc-1",
        metadata={
            "version": 1,
        },
    )

    first = store.get_seen(
        "documents",
        "doc-1",
    )

    assert first is not None

    store.mark_seen(
        "documents",
        "doc-1",
        metadata={
            "version": 2,
        },
    )

    second = store.get_seen(
        "documents",
        "doc-1",
    )

    assert second is not None

    assert (
        second.first_seen_at
        == first.first_seen_at
    )

    assert (
        second.metadata["version"]
        == 2
    )

    store.close()


def test_get_seen_returns_none_for_unknown_key(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    assert (
        store.get_seen(
            "documents",
            "missing",
        )
        is None
    )

    store.close()


def test_forget_seen_removes_record(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    store.mark_seen(
        "documents",
        "doc-1",
    )

    assert (
        store.forget_seen(
            "documents",
            "doc-1",
        )
        is True
    )

    assert (
        store.has_seen(
            "documents",
            "doc-1",
        )
        is False
    )

    assert (
        store.forget_seen(
            "documents",
            "doc-1",
        )
        is False
    )

    store.close()


def test_seen_count_can_filter_namespace(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    store.mark_seen(
        "a",
        "1",
    )

    store.mark_seen(
        "a",
        "2",
    )

    store.mark_seen(
        "b",
        "1",
    )

    assert (
        store.seen_count()
        == 3
    )

    assert (
        store.seen_count(
            "a"
        )
        == 2
    )

    assert (
        store.seen_count(
            "b"
        )
        == 1
    )

    store.close()


# =============================================================================
# KEY / VALUE
# =============================================================================
def test_put_and_get_scalar_value(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    store.put(
        "crawler",
        "page",
        7,
    )

    assert (
        store.get(
            "crawler",
            "page",
        )
        == 7
    )

    store.close()


def test_put_and_get_json_object(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    value = {
        "page": 7,
        "cursor": "abc",
        "enabled": True,
    }

    store.put(
        "crawler",
        "checkpoint",
        value,
    )

    assert (
        store.get(
            "crawler",
            "checkpoint",
        )
        == value
    )

    store.close()


def test_put_replaces_existing_value(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    store.put(
        "crawler",
        "page",
        1,
    )

    store.put(
        "crawler",
        "page",
        2,
    )

    assert (
        store.get(
            "crawler",
            "page",
        )
        == 2
    )

    assert (
        store.count()
        == 1
    )

    store.close()


def test_get_returns_default_for_missing_key(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    marker = object()

    assert (
        store.get(
            "crawler",
            "missing",
            marker,
        )
        is marker
    )

    store.close()


def test_get_entry_returns_metadata(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    store.put(
        "crawler",
        "page",
        5,
    )

    entry = store.get_entry(
        "crawler",
        "page",
    )

    assert isinstance(
        entry,
        StateEntry,
    )

    assert (
        entry.namespace
        == "crawler"
    )

    assert entry.key == "page"
    assert entry.value == 5

    assert entry.created_at
    assert entry.updated_at

    store.close()


def test_delete_state_value(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    store.put(
        "crawler",
        "page",
        5,
    )

    assert (
        store.delete(
            "crawler",
            "page",
        )
        is True
    )

    assert (
        store.delete(
            "crawler",
            "page",
        )
        is False
    )

    assert (
        store.get(
            "crawler",
            "page",
        )
        is None
    )

    store.close()


def test_count_can_filter_namespace(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    store.put(
        "a",
        "one",
        1,
    )

    store.put(
        "a",
        "two",
        2,
    )

    store.put(
        "b",
        "one",
        1,
    )

    assert store.count() == 3

    assert (
        store.count("a")
        == 2
    )

    assert (
        store.count("b")
        == 1
    )

    store.close()


# =============================================================================
# NAMESPACE CLEAR
# =============================================================================
def test_clear_namespace_removes_seen_and_state(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    store.mark_seen(
        "source-a",
        "one",
    )

    store.mark_seen(
        "source-a",
        "two",
    )

    store.put(
        "source-a",
        "cursor",
        "abc",
    )

    store.put(
        "source-b",
        "cursor",
        "keep",
    )

    seen_deleted, state_deleted = (
        store.clear_namespace(
            "source-a"
        )
    )

    assert seen_deleted == 2
    assert state_deleted == 1

    assert (
        store.seen_count(
            "source-a"
        )
        == 0
    )

    assert (
        store.count(
            "source-a"
        )
        == 0
    )

    assert (
        store.get(
            "source-b",
            "cursor",
        )
        == "keep"
    )

    store.close()


# =============================================================================
# INPUT VALIDATION
# =============================================================================
@pytest.mark.parametrize(
    "namespace",
    [
        "",
        " ",
        None,
    ],
)
def test_empty_namespace_is_rejected(
    tmp_path: Path,
    namespace: Any,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    with pytest.raises(
        StorageError,
        match="namespace",
    ):
        store.put(
            namespace,
            "key",
            "value",
        )

    store.close()


@pytest.mark.parametrize(
    "key",
    [
        "",
        " ",
        None,
    ],
)
def test_empty_key_is_rejected(
    tmp_path: Path,
    key: Any,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    with pytest.raises(
        StorageError,
        match="key",
    ):
        store.put(
            "crawler",
            key,
            "value",
        )

    store.close()


def test_non_finite_value_is_rejected(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    with pytest.raises(
        StorageError,
        match="serialize",
    ):
        store.put(
            "crawler",
            "value",
            float("nan"),
        )

    store.close()


# =============================================================================
# THREAD SAFETY
# =============================================================================
def test_concurrent_mark_seen_creates_only_one_record(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    results: list[bool] = []

    results_lock = (
        threading.Lock()
    )

    def worker() -> None:
        result = store.mark_seen(
            "documents",
            "same-key",
        )

        with results_lock:
            results.append(
                result
            )

    threads = [
        threading.Thread(
            target=worker
        )
        for _ in range(10)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert (
        results.count(True)
        == 1
    )

    assert (
        results.count(False)
        == 9
    )

    assert (
        store.seen_count()
        == 1
    )

    store.close()


def test_concurrent_put_operations_are_safe(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    def worker(
        index: int,
    ) -> None:
        store.put(
            "workers",
            f"key-{index}",
            {
                "index": index,
            },
        )

    threads = [
        threading.Thread(
            target=worker,
            args=(index,),
        )
        for index in range(20)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert (
        store.count(
            "workers"
        )
        == 20
    )

    store.close()


# =============================================================================
# PERSISTENCE
# =============================================================================
def test_state_survives_new_store_instance(
    tmp_path: Path,
) -> None:
    db_path = (
        tmp_path
        / "state.db"
    )

    first = LocalStateStore(
        db_path
    )

    first.mark_seen(
        "documents",
        "doc-1",
    )

    first.put(
        "crawler",
        "page",
        9,
    )

    first.close()

    second = LocalStateStore(
        db_path
    )

    assert (
        second.has_seen(
            "documents",
            "doc-1",
        )
        is True
    )

    assert (
        second.get(
            "crawler",
            "page",
        )
        == 9
    )

    second.close()


# =============================================================================
# CLOSE
# =============================================================================
def test_close_is_idempotent(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    store.close()
    store.close()

    assert (
        store.is_closed
        is True
    )


def test_closed_store_rejects_operations(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    store.close()

    with pytest.raises(
        StorageError,
        match="Kapalı LocalStateStore",
    ):
        store.get(
            "crawler",
            "page",
        )


def test_context_manager_closes_store(
    tmp_path: Path,
) -> None:
    with LocalStateStore(
        tmp_path / "state.db"
    ) as store:
        store.put(
            "crawler",
            "page",
            1,
        )

        assert (
            store.is_closed
            is False
        )

    assert store.is_closed is True


# =============================================================================
# SNAPSHOT / REPR
# =============================================================================
def test_snapshot_reports_counts(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    store.mark_seen(
        "documents",
        "one",
    )

    store.put(
        "crawler",
        "page",
        1,
    )

    snapshot = (
        store.snapshot()
    )

    assert (
        snapshot["closed"]
        is False
    )

    assert (
        snapshot["seen_count"]
        == 1
    )

    assert (
        snapshot["state_count"]
        == 1
    )

    assert (
        snapshot["db_path"]
        == str(store.db_path)
    )

    store.close()


def test_repr_contains_database_path(
    tmp_path: Path,
) -> None:
    store = LocalStateStore(
        tmp_path / "state.db"
    )

    rendered = repr(
        store
    )

    assert (
        "LocalStateStore"
        in rendered
    )

    assert (
        str(store.db_path)
        in rendered
    )

    store.close()