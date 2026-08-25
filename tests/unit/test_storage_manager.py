from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from enterprise_crawler.exceptions import StorageError
from enterprise_crawler.storage import (
    AtomicFileWriter,
    LocalStateStore,
    LocalStorage,
    StorageManager,
)


# =============================================================================
# INITIALIZATION
# =============================================================================
def test_manager_creates_default_storage_stack(
    tmp_path: Path,
) -> None:
    root = (
        tmp_path
        / "storage"
    )

    manager = StorageManager(
        root
    )

    assert (
        manager.root
        == root.resolve()
    )

    assert isinstance(
        manager.writer,
        AtomicFileWriter,
    )

    assert isinstance(
        manager.files,
        LocalStorage,
    )

    assert isinstance(
        manager.state,
        LocalStateStore,
    )

    assert (
        manager.state_db_path
        == (
            root
            / ".state"
            / "local_state.db"
        ).resolve()
    )

    manager.close()


def test_manager_creates_root_directory(
    tmp_path: Path,
) -> None:
    root = (
        tmp_path
        / "nested"
        / "storage"
    )

    manager = StorageManager(
        root
    )

    assert root.exists()
    assert root.is_dir()

    manager.close()


def test_missing_root_rejected_when_creation_disabled(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        StorageError,
        match="root mevcut değil",
    ):
        StorageManager(
            tmp_path / "missing",
            create_root=False,
        )


def test_file_cannot_be_manager_root(
    tmp_path: Path,
) -> None:
    root = (
        tmp_path
        / "file.txt"
    )

    root.write_text(
        "hello",
        encoding="utf-8",
    )

    with pytest.raises(
        StorageError,
        match="root klasör",
    ):
        StorageManager(
            root
        )


# =============================================================================
# FILE STORAGE
# =============================================================================
def test_manager_save_and_load_bytes(
    tmp_path: Path,
) -> None:
    manager = StorageManager(
        tmp_path / "storage"
    )

    manager.save_bytes(
        "raw/file.bin",
        b"payload",
    )

    assert (
        manager.load_bytes(
            "raw/file.bin"
        )
        == b"payload"
    )

    manager.close()


def test_manager_save_and_load_text(
    tmp_path: Path,
) -> None:
    manager = StorageManager(
        tmp_path / "storage"
    )

    manager.save_text(
        "text/hello.txt",
        "Merhaba",
    )

    assert (
        manager.load_text(
            "text/hello.txt"
        )
        == "Merhaba"
    )

    manager.close()


def test_manager_save_and_load_json(
    tmp_path: Path,
) -> None:
    manager = StorageManager(
        tmp_path / "storage"
    )

    payload = {
        "name": "crawler",
        "version": 1,
    }

    manager.save_json(
        "records/item.json",
        payload,
    )

    assert (
        manager.load_json(
            "records/item.json"
        )
        == payload
    )

    manager.close()


def test_manager_exists_and_delete_file(
    tmp_path: Path,
) -> None:
    manager = StorageManager(
        tmp_path / "storage"
    )

    manager.save_text(
        "file.txt",
        "hello",
    )

    assert (
        manager.exists(
            "file.txt"
        )
        is True
    )

    assert (
        manager.delete_file(
            "file.txt"
        )
        is True
    )

    assert (
        manager.exists(
            "file.txt"
        )
        is False
    )

    manager.close()


# =============================================================================
# STATE
# =============================================================================
def test_manager_mark_seen_and_has_seen(
    tmp_path: Path,
) -> None:
    manager = StorageManager(
        tmp_path / "storage"
    )

    assert (
        manager.mark_seen(
            "documents",
            "doc-1",
        )
        is True
    )

    assert (
        manager.has_seen(
            "documents",
            "doc-1",
        )
        is True
    )

    assert (
        manager.mark_seen(
            "documents",
            "doc-1",
        )
        is False
    )

    manager.close()


def test_manager_state_is_persistent(
    tmp_path: Path,
) -> None:
    root = (
        tmp_path
        / "storage"
    )

    first = StorageManager(
        root
    )

    first.state.put(
        "crawler",
        "cursor",
        "abc",
    )

    first.close()

    second = StorageManager(
        root
    )

    assert (
        second.state.get(
            "crawler",
            "cursor",
        )
        == "abc"
    )

    second.close()


# =============================================================================
# CUSTOM STATE PATH
# =============================================================================
def test_relative_custom_state_path_is_resolved_under_root(
    tmp_path: Path,
) -> None:
    manager = StorageManager(
        tmp_path / "storage",
        state_db_path=(
            "runtime/state.db"
        ),
    )

    assert (
        manager.state_db_path
        == (
            manager.root
            / "runtime"
            / "state.db"
        ).resolve()
    )

    manager.close()


def test_absolute_custom_state_path_is_supported(
    tmp_path: Path,
) -> None:
    external = (
        tmp_path
        / "external"
        / "state.db"
    ).resolve()

    manager = StorageManager(
        tmp_path / "storage",
        state_db_path=external,
    )

    assert (
        manager.state_db_path
        == external
    )

    manager.close()


# =============================================================================
# DEPENDENCY INJECTION
# =============================================================================
def test_injected_writer_is_reused(
    tmp_path: Path,
) -> None:
    writer = AtomicFileWriter()

    manager = StorageManager(
        tmp_path / "storage",
        writer=writer,
    )

    assert (
        manager.writer
        is writer
    )

    assert (
        manager.files.writer
        is writer
    )

    snapshot = (
        manager.snapshot()
    )

    assert (
        snapshot["owns_writer"]
        is False
    )

    manager.close()


def test_injected_file_storage_is_reused(
    tmp_path: Path,
) -> None:
    files = LocalStorage(
        tmp_path / "files"
    )

    manager = StorageManager(
        tmp_path / "manager-root",
        files=files,
    )

    assert (
        manager.files
        is files
    )

    assert (
        manager.snapshot()[
            "owns_files"
        ]
        is False
    )

    manager.close()


def test_injected_state_store_is_not_closed(
    tmp_path: Path,
) -> None:
    state = LocalStateStore(
        tmp_path / "external.db"
    )

    manager = StorageManager(
        tmp_path / "storage",
        state=state,
    )

    assert (
        manager.state
        is state
    )

    assert (
        manager.snapshot()[
            "owns_state"
        ]
        is False
    )

    manager.close()

    assert (
        state.is_closed
        is False
    )

    state.put(
        "test",
        "key",
        "value",
    )

    assert (
        state.get(
            "test",
            "key",
        )
        == "value"
    )

    state.close()


# =============================================================================
# CLOSE
# =============================================================================
def test_owned_state_store_is_closed_with_manager(
    tmp_path: Path,
) -> None:
    manager = StorageManager(
        tmp_path / "storage"
    )

    state = manager.state

    assert (
        state.is_closed
        is False
    )

    manager.close()

    assert (
        state.is_closed
        is True
    )


def test_close_is_idempotent(
    tmp_path: Path,
) -> None:
    manager = StorageManager(
        tmp_path / "storage"
    )

    manager.close()
    manager.close()

    assert (
        manager.is_closed
        is True
    )


def test_closed_manager_rejects_convenience_operations(
    tmp_path: Path,
) -> None:
    manager = StorageManager(
        tmp_path / "storage"
    )

    manager.close()

    with pytest.raises(
        StorageError,
        match="Kapalı StorageManager",
    ):
        manager.save_text(
            "file.txt",
            "hello",
        )


def test_context_manager_closes_manager(
    tmp_path: Path,
) -> None:
    with StorageManager(
        tmp_path / "storage"
    ) as manager:
        manager.save_text(
            "file.txt",
            "hello",
        )

        assert (
            manager.is_closed
            is False
        )

    assert (
        manager.is_closed
        is True
    )


# =============================================================================
# SNAPSHOT
# =============================================================================
def test_snapshot_reports_storage_configuration(
    tmp_path: Path,
) -> None:
    manager = StorageManager(
        tmp_path / "storage"
    )

    manager.mark_seen(
        "documents",
        "one",
    )

    manager.state.put(
        "crawler",
        "page",
        1,
    )

    snapshot = (
        manager.snapshot()
    )

    assert (
        snapshot["root"]
        == str(manager.root)
    )

    assert (
        snapshot["state_db_path"]
        == str(
            manager.state_db_path
        )
    )

    assert (
        snapshot["closed"]
        is False
    )

    assert (
        snapshot["owns_writer"]
        is True
    )

    assert (
        snapshot["owns_files"]
        is True
    )

    assert (
        snapshot["owns_state"]
        is True
    )

    assert (
        snapshot["state"][
            "seen_count"
        ]
        == 1
    )

    assert (
        snapshot["state"][
            "state_count"
        ]
        == 1
    )

    manager.close()


def test_closed_snapshot_does_not_access_closed_state(
    tmp_path: Path,
) -> None:
    manager = StorageManager(
        tmp_path / "storage"
    )

    manager.close()

    snapshot = (
        manager.snapshot()
    )

    assert (
        snapshot["closed"]
        is True
    )

    assert (
        snapshot["state"]
        is None
    )


# =============================================================================
# REPRESENTATION
# =============================================================================
def test_repr_contains_storage_root(
    tmp_path: Path,
) -> None:
    manager = StorageManager(
        tmp_path / "storage"
    )

    rendered = repr(
        manager
    )

    assert (
        "StorageManager"
        in rendered
    )

    assert (
        str(manager.root)
        in rendered
    )

    manager.close()