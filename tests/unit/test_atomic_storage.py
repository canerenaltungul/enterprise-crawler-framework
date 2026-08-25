from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from enterprise_crawler.exceptions import AtomicWriteError
from enterprise_crawler.storage.atomic import (
    AtomicFileWriter,
    AtomicWriteResult,
)


# =============================================================================
# BYTES
# =============================================================================
def test_atomic_write_bytes_creates_complete_file(
    tmp_path: Path,
) -> None:
    writer = AtomicFileWriter()

    target = (
        tmp_path
        / "payload.bin"
    )

    result = writer.write_bytes(
        target,
        b"hello-world",
    )

    assert isinstance(
        result,
        AtomicWriteResult,
    )

    assert (
        target.read_bytes()
        == b"hello-world"
    )

    assert result.path == target
    assert result.size_bytes == 11

    assert (
        result.replaced_existing
        is False
    )


def test_atomic_write_empty_bytes(
    tmp_path: Path,
) -> None:
    writer = AtomicFileWriter()

    target = (
        tmp_path
        / "empty.bin"
    )

    result = writer.write_bytes(
        target,
        b"",
    )

    assert target.read_bytes() == b""
    assert result.size_bytes == 0


def test_parent_directories_are_created(
    tmp_path: Path,
) -> None:
    writer = AtomicFileWriter()

    target = (
        tmp_path
        / "a"
        / "b"
        / "c"
        / "file.bin"
    )

    writer.write_bytes(
        target,
        b"payload",
    )

    assert target.read_bytes() == (
        b"payload"
    )


def test_missing_parent_is_rejected_when_auto_create_disabled(
    tmp_path: Path,
) -> None:
    writer = AtomicFileWriter(
        create_parent_directories=False,
    )

    target = (
        tmp_path
        / "missing"
        / "file.bin"
    )

    with pytest.raises(
        AtomicWriteError,
        match="parent directory mevcut değil",
    ):
        writer.write_bytes(
            target,
            b"payload",
        )


# =============================================================================
# OVERWRITE
# =============================================================================
def test_existing_file_is_replaced_by_default(
    tmp_path: Path,
) -> None:
    target = (
        tmp_path
        / "file.bin"
    )

    target.write_bytes(
        b"old"
    )

    writer = AtomicFileWriter()

    result = writer.write_bytes(
        target,
        b"new",
    )

    assert target.read_bytes() == (
        b"new"
    )

    assert (
        result.replaced_existing
        is True
    )


def test_existing_file_is_preserved_when_overwrite_false(
    tmp_path: Path,
) -> None:
    target = (
        tmp_path
        / "file.bin"
    )

    target.write_bytes(
        b"original"
    )

    writer = AtomicFileWriter()

    with pytest.raises(
        AtomicWriteError,
        match="zaten mevcut",
    ):
        writer.write_bytes(
            target,
            b"replacement",
            overwrite=False,
        )

    assert target.read_bytes() == (
        b"original"
    )


# =============================================================================
# TEXT
# =============================================================================
def test_atomic_write_text(
    tmp_path: Path,
) -> None:
    writer = AtomicFileWriter()

    target = (
        tmp_path
        / "hello.txt"
    )

    result = writer.write_text(
        target,
        "Merhaba Dünya",
    )

    assert (
        target.read_text(
            encoding="utf-8"
        )
        == "Merhaba Dünya"
    )

    assert (
        result.size_bytes
        == len(
            "Merhaba Dünya".encode(
                "utf-8"
            )
        )
    )


def test_invalid_text_payload_is_rejected(
    tmp_path: Path,
) -> None:
    writer = AtomicFileWriter()

    with pytest.raises(
        AtomicWriteError,
        match="payload str",
    ):
        writer.write_text(
            tmp_path / "file.txt",
            b"bytes",  # type: ignore[arg-type]
        )


# =============================================================================
# JSON
# =============================================================================
def test_atomic_write_json(
    tmp_path: Path,
) -> None:
    writer = AtomicFileWriter()

    target = (
        tmp_path
        / "payload.json"
    )

    payload = {
        "name": "enterprise-crawler",
        "count": 3,
        "enabled": True,
    }

    writer.write_json(
        target,
        payload,
    )

    loaded = json.loads(
        target.read_text(
            encoding="utf-8"
        )
    )

    assert loaded == payload


def test_json_is_deterministically_sorted(
    tmp_path: Path,
) -> None:
    writer = AtomicFileWriter()

    target = (
        tmp_path
        / "payload.json"
    )

    writer.write_json(
        target,
        {
            "z": 1,
            "a": 2,
        },
        indent=None,
    )

    raw = target.read_text(
        encoding="utf-8"
    )

    assert raw.index('"a"') < (
        raw.index('"z"')
    )


def test_non_finite_json_number_is_rejected(
    tmp_path: Path,
) -> None:
    writer = AtomicFileWriter()

    with pytest.raises(
        AtomicWriteError,
        match="serialize",
    ):
        writer.write_json(
            tmp_path / "invalid.json",
            {
                "value": float("nan"),
            },
        )


def test_invalid_json_payload_type_is_rejected(
    tmp_path: Path,
) -> None:
    writer = AtomicFileWriter()

    with pytest.raises(
        AtomicWriteError,
        match="Mapping",
    ):
        writer.write_json(
            tmp_path / "invalid.json",
            [1, 2, 3],  # type: ignore[arg-type]
        )


# =============================================================================
# TARGET VALIDATION
# =============================================================================
def test_directory_target_is_rejected(
    tmp_path: Path,
) -> None:
    directory = (
        tmp_path
        / "directory"
    )

    directory.mkdir()

    writer = AtomicFileWriter()

    with pytest.raises(
        AtomicWriteError,
        match="klasör",
    ):
        writer.write_bytes(
            directory,
            b"payload",
        )


def test_invalid_bytes_payload_is_rejected(
    tmp_path: Path,
) -> None:
    writer = AtomicFileWriter()

    with pytest.raises(
        AtomicWriteError,
        match="payload bytes",
    ):
        writer.write_bytes(
            tmp_path / "file.bin",
            "not-bytes",  # type: ignore[arg-type]
        )


# =============================================================================
# TEMP FILE CLEANUP
# =============================================================================
def test_successful_write_leaves_no_temp_files(
    tmp_path: Path,
) -> None:
    writer = AtomicFileWriter()

    writer.write_bytes(
        tmp_path / "file.bin",
        b"payload",
    )

    temporary_files = [
        path
        for path in tmp_path.iterdir()
        if (
            path.name.startswith(
                ".file.bin."
            )
            and path.suffix == ".tmp"
        )
    ]

    assert temporary_files == []


# =============================================================================
# RESULT
# =============================================================================
def test_atomic_write_result_to_dict_serializes_path(
    tmp_path: Path,
) -> None:
    writer = AtomicFileWriter()

    result = writer.write_bytes(
        tmp_path / "file.bin",
        b"abc",
    )

    payload = result.to_dict()

    assert isinstance(
        payload["path"],
        str,
    )

    assert payload["size_bytes"] == 3

    assert (
        payload["replaced_existing"]
        is False
    )


# =============================================================================
# REPRESENTATION
# =============================================================================
def test_writer_repr_contains_configuration() -> None:
    writer = AtomicFileWriter(
        create_parent_directories=False,
    )

    rendered = repr(
        writer
    )

    assert (
        "AtomicFileWriter"
        in rendered
    )

    assert (
        "create_parent_directories=False"
        in rendered
    )