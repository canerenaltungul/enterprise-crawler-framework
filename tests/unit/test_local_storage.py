from __future__ import annotations

import json
from pathlib import Path

import pytest

from enterprise_crawler.exceptions import StorageError
from enterprise_crawler.storage.atomic import (
    AtomicFileWriter,
)
from enterprise_crawler.storage.local import (
    LocalStorage,
)


# =============================================================================
# ROOT
# =============================================================================
def test_storage_root_is_created(
    tmp_path: Path,
) -> None:
    root = (
        tmp_path
        / "storage"
    )

    storage = LocalStorage(
        root
    )

    assert root.exists()
    assert root.is_dir()

    assert (
        storage.root
        == root.resolve()
    )


def test_missing_root_is_rejected_when_create_disabled(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        StorageError,
        match="root mevcut değil",
    ):
        LocalStorage(
            tmp_path / "missing",
            create_root=False,
        )


def test_file_cannot_be_used_as_root(
    tmp_path: Path,
) -> None:
    file_path = (
        tmp_path
        / "file.txt"
    )

    file_path.write_text(
        "hello",
        encoding="utf-8",
    )

    with pytest.raises(
        StorageError,
        match="root klasör",
    ):
        LocalStorage(
            file_path
        )


# =============================================================================
# PATH SECURITY
# =============================================================================
def test_relative_path_is_resolved_inside_root(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(
        tmp_path / "storage"
    )

    resolved = storage.resolve(
        "records/item.json"
    )

    assert (
        resolved
        == (
            storage.root
            / "records"
            / "item.json"
        ).resolve()
    )


@pytest.mark.parametrize(
    "path",
    [
        "../escape.txt",
        "../../escape.txt",
        "records/../../../escape.txt",
    ],
)
def test_path_traversal_is_rejected(
    tmp_path: Path,
    path: str,
) -> None:
    storage = LocalStorage(
        tmp_path / "storage"
    )

    with pytest.raises(
        StorageError,
        match="root dışına",
    ):
        storage.resolve(
            path
        )


def test_absolute_path_is_rejected(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(
        tmp_path / "storage"
    )

    absolute = (
        tmp_path
        / "outside.txt"
    ).resolve()

    with pytest.raises(
        StorageError,
        match="absolute path",
    ):
        storage.resolve(
            absolute
        )


# =============================================================================
# BYTES
# =============================================================================
def test_save_and_load_bytes(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(
        tmp_path / "storage"
    )

    result = storage.save_bytes(
        "raw/file.bin",
        b"payload",
    )

    assert result.size_bytes == 7

    assert (
        storage.load_bytes(
            "raw/file.bin"
        )
        == b"payload"
    )


def test_save_bytes_uses_atomic_writer(
    tmp_path: Path,
) -> None:
    writer = AtomicFileWriter()

    storage = LocalStorage(
        tmp_path / "storage",
        writer=writer,
    )

    result = storage.save_bytes(
        "file.bin",
        b"abc",
    )

    assert result.path.exists()

    assert result.path.parent == (
        storage.root
    )


# =============================================================================
# TEXT
# =============================================================================
def test_save_and_load_text(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(
        tmp_path / "storage"
    )

    storage.save_text(
        "text/hello.txt",
        "Merhaba Dünya",
    )

    assert (
        storage.load_text(
            "text/hello.txt"
        )
        == "Merhaba Dünya"
    )


# =============================================================================
# JSON
# =============================================================================
def test_save_and_load_json(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(
        tmp_path / "storage"
    )

    payload = {
        "name": "crawler",
        "count": 3,
    }

    storage.save_json(
        "records/item.json",
        payload,
    )

    assert (
        storage.load_json(
            "records/item.json"
        )
        == payload
    )


def test_invalid_json_is_rejected(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(
        tmp_path / "storage"
    )

    storage.save_text(
        "bad.json",
        "{invalid-json",
    )

    with pytest.raises(
        StorageError,
        match="JSON parse",
    ):
        storage.load_json(
            "bad.json"
        )


def test_json_root_must_be_object(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(
        tmp_path / "storage"
    )

    storage.save_text(
        "array.json",
        json.dumps(
            [1, 2, 3]
        ),
    )

    with pytest.raises(
        StorageError,
        match="root object",
    ):
        storage.load_json(
            "array.json"
        )


# =============================================================================
# EXISTS
# =============================================================================
def test_exists_reports_file_presence(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(
        tmp_path / "storage"
    )

    assert (
        storage.exists(
            "file.bin"
        )
        is False
    )

    storage.save_bytes(
        "file.bin",
        b"data",
    )

    assert (
        storage.exists(
            "file.bin"
        )
        is True
    )

    assert (
        storage.is_file(
            "file.bin"
        )
        is True
    )


# =============================================================================
# MISSING FILES
# =============================================================================
def test_missing_bytes_file_is_rejected(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(
        tmp_path / "storage"
    )

    with pytest.raises(
        StorageError,
        match="bulunamadı",
    ):
        storage.load_bytes(
            "missing.bin"
        )


def test_missing_text_file_is_rejected(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(
        tmp_path / "storage"
    )

    with pytest.raises(
        StorageError,
        match="bulunamadı",
    ):
        storage.load_text(
            "missing.txt"
        )


# =============================================================================
# OVERWRITE
# =============================================================================
def test_overwrite_false_preserves_existing_file(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(
        tmp_path / "storage"
    )

    storage.save_text(
        "file.txt",
        "original",
    )

    with pytest.raises(
        StorageError,
    ):
        storage.save_text(
            "file.txt",
            "replacement",
            overwrite=False,
        )

    assert (
        storage.load_text(
            "file.txt"
        )
        == "original"
    )


# =============================================================================
# DELETE
# =============================================================================
def test_delete_removes_file(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(
        tmp_path / "storage"
    )

    storage.save_text(
        "file.txt",
        "hello",
    )

    deleted = storage.delete(
        "file.txt"
    )

    assert deleted is True

    assert (
        storage.exists(
            "file.txt"
        )
        is False
    )


def test_delete_missing_file_is_rejected_by_default(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(
        tmp_path / "storage"
    )

    with pytest.raises(
        StorageError,
        match="bulunamadı",
    ):
        storage.delete(
            "missing.txt"
        )


def test_delete_missing_ok_returns_false(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(
        tmp_path / "storage"
    )

    assert (
        storage.delete(
            "missing.txt",
            missing_ok=True,
        )
        is False
    )


def test_delete_directory_is_rejected(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(
        tmp_path / "storage"
    )

    directory = storage.resolve(
        "directory"
    )

    directory.mkdir()

    with pytest.raises(
        StorageError,
        match="yalnız dosya",
    ):
        storage.delete(
            "directory"
        )


# =============================================================================
# REPRESENTATION
# =============================================================================
def test_repr_contains_storage_root(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(
        tmp_path / "storage"
    )

    rendered = repr(
        storage
    )

    assert (
        "LocalStorage"
        in rendered
    )

    assert (
        str(storage.root)
        in rendered
    )