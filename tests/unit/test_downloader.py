from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest

from enterprise_crawler.core.downloader import (
    DownloadResult,
    Downloader,
)
from enterprise_crawler.exceptions import (
    DownloadError,
    NetworkError,
    ShutdownRequested,
)


# =============================================================================
# TEST DOUBLES
# =============================================================================
class FakeResponse:
    def __init__(
        self,
        chunks: list[bytes],
        *,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.headers = headers or {}

        self._chunks = chunks

        self.closed = False

        self.iter_chunk_size: int | None = None

    def iter_content(
        self,
        *,
        chunk_size: int,
    ):
        self.iter_chunk_size = (
            chunk_size
        )

        yield from self._chunks

    def close(self) -> None:
        self.closed = True


class FakeHttpClient:
    def __init__(
        self,
        response: FakeResponse | None = None,
    ) -> None:
        self.response = (
            response
            or FakeResponse(
                [b"hello"]
            )
        )

        self.calls: list[
            dict[str, Any]
        ] = []

        self.error: BaseException | None = (
            None
        )

    def get(
        self,
        url: str,
        **kwargs: Any,
    ) -> FakeResponse:
        self.calls.append(
            {
                "url": url,
                **kwargs,
            }
        )

        if self.error is not None:
            raise self.error

        return self.response


# =============================================================================
# SUCCESS
# =============================================================================
def test_download_writes_complete_file(
    tmp_path: Path,
) -> None:
    response = FakeResponse(
        [
            b"hello ",
            b"world",
        ],
        headers={
            "Content-Type": (
                "text/plain; charset=utf-8"
            ),
            "Content-Length": "11",
        },
    )

    client = FakeHttpClient(
        response
    )

    downloader = Downloader(
        client,  # type: ignore[arg-type]
        chunk_size=4,
    )

    target = (
        tmp_path
        / "hello.txt"
    )

    result = downloader.download(
        "https://example.com/hello.txt",
        target,
    )

    assert isinstance(
        result,
        DownloadResult,
    )

    assert target.read_bytes() == (
        b"hello world"
    )

    assert result.path == target

    assert result.size_bytes == 11

    assert result.sha256 == (
        hashlib.sha256(
            b"hello world"
        ).hexdigest()
    )

    assert (
        result.content_type
        == "text/plain"
    )

    assert (
        result.content_length
        == 11
    )

    assert result.chunk_count == 2

    assert response.closed is True


def test_download_result_to_dict_serializes_path(
    tmp_path: Path,
) -> None:
    result = Downloader(
        FakeHttpClient(),  # type: ignore[arg-type]
    ).download(
        "https://example.com/file",
        tmp_path / "file.bin",
    )

    payload = result.to_dict()

    assert isinstance(
        payload["path"],
        str,
    )

    assert (
        payload["size_bytes"]
        == 5
    )


def test_stream_true_is_forwarded_to_http_client(
    tmp_path: Path,
) -> None:
    client = FakeHttpClient()

    Downloader(
        client,  # type: ignore[arg-type]
    ).download(
        "https://example.com/file",
        tmp_path / "file.bin",
    )

    assert (
        client.calls[0]["stream"]
        is True
    )


def test_headers_are_forwarded(
    tmp_path: Path,
) -> None:
    client = FakeHttpClient()

    Downloader(
        client,  # type: ignore[arg-type]
    ).download(
        "https://example.com/file",
        tmp_path / "file.bin",
        headers={
            "Authorization": "Bearer test",
        },
    )

    assert (
        client.calls[0]["headers"]
        == {
            "Authorization": (
                "Bearer test"
            )
        }
    )


# =============================================================================
# SHA-256
# =============================================================================
def test_expected_sha256_is_verified(
    tmp_path: Path,
) -> None:
    payload = b"trusted-data"

    expected = hashlib.sha256(
        payload
    ).hexdigest()

    downloader = Downloader(
        FakeHttpClient(
            FakeResponse(
                [payload]
            )
        ),  # type: ignore[arg-type]
    )

    result = downloader.download(
        "https://example.com/file",
        tmp_path / "file.bin",
        expected_sha256=expected,
    )

    assert result.sha256 == expected


def test_sha256_mismatch_removes_temporary_file(
    tmp_path: Path,
) -> None:
    downloader = Downloader(
        FakeHttpClient(
            FakeResponse(
                [b"wrong-data"]
            )
        ),  # type: ignore[arg-type]
    )

    target = (
        tmp_path
        / "file.bin"
    )

    with pytest.raises(
        DownloadError,
        match="SHA-256",
    ):
        downloader.download(
            "https://example.com/file",
            target,
            expected_sha256=(
                hashlib.sha256(
                    b"expected-data"
                ).hexdigest()
            ),
        )

    assert target.exists() is False

    assert list(
        tmp_path.glob(
            "*.part"
        )
    ) == []

    assert list(
        tmp_path.glob(
            ".*.part"
        )
    ) == []


def test_invalid_expected_sha256_is_rejected(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        DownloadError,
        match="expected_sha256",
    ):
        Downloader(
            FakeHttpClient(),  # type: ignore[arg-type]
        ).download(
            "https://example.com/file",
            tmp_path / "file.bin",
            expected_sha256="abc",
        )


# =============================================================================
# SIZE LIMIT
# =============================================================================
def test_content_length_over_limit_is_rejected_before_write(
    tmp_path: Path,
) -> None:
    response = FakeResponse(
        [b"x" * 100],
        headers={
            "Content-Length": "100",
        },
    )

    downloader = Downloader(
        FakeHttpClient(
            response
        ),  # type: ignore[arg-type]
        max_download_bytes=50,
    )

    target = (
        tmp_path
        / "large.bin"
    )

    with pytest.raises(
        DownloadError,
        match="Content-Length",
    ):
        downloader.download(
            "https://example.com/large",
            target,
        )

    assert target.exists() is False


def test_streaming_limit_is_enforced_without_content_length(
    tmp_path: Path,
) -> None:
    response = FakeResponse(
        [
            b"12345",
            b"67890",
            b"X",
        ],
    )

    downloader = Downloader(
        FakeHttpClient(
            response
        ),  # type: ignore[arg-type]
        max_download_bytes=10,
    )

    target = (
        tmp_path
        / "large.bin"
    )

    with pytest.raises(
        DownloadError,
        match="maksimum boyutu",
    ):
        downloader.download(
            "https://example.com/large",
            target,
        )

    assert target.exists() is False


def test_per_download_max_bytes_overrides_default(
    tmp_path: Path,
) -> None:
    response = FakeResponse(
        [b"123456"],
    )

    downloader = Downloader(
        FakeHttpClient(
            response
        ),  # type: ignore[arg-type]
        max_download_bytes=100,
    )

    with pytest.raises(
        DownloadError,
    ):
        downloader.download(
            "https://example.com/file",
            tmp_path / "file.bin",
            max_bytes=5,
        )


@pytest.mark.parametrize(
    "value",
    [
        0,
        -1,
        True,
        1.5,
    ],
)
def test_invalid_max_bytes_is_rejected(
    tmp_path: Path,
    value: Any,
) -> None:
    downloader = Downloader(
        FakeHttpClient(),  # type: ignore[arg-type]
    )

    with pytest.raises(
        DownloadError,
    ):
        downloader.download(
            "https://example.com/file",
            tmp_path / "file.bin",
            max_bytes=value,
        )


# =============================================================================
# TARGET SAFETY
# =============================================================================
def test_existing_target_is_rejected_by_default(
    tmp_path: Path,
) -> None:
    target = (
        tmp_path
        / "existing.bin"
    )

    target.write_bytes(
        b"original"
    )

    downloader = Downloader(
        FakeHttpClient(),  # type: ignore[arg-type]
    )

    with pytest.raises(
        DownloadError,
        match="zaten mevcut",
    ):
        downloader.download(
            "https://example.com/file",
            target,
        )

    assert target.read_bytes() == (
        b"original"
    )


def test_existing_target_can_be_replaced_explicitly(
    tmp_path: Path,
) -> None:
    target = (
        tmp_path
        / "existing.bin"
    )

    target.write_bytes(
        b"old"
    )

    downloader = Downloader(
        FakeHttpClient(
            FakeResponse(
                [b"new"]
            )
        ),  # type: ignore[arg-type]
    )

    downloader.download(
        "https://example.com/file",
        target,
        overwrite=True,
    )

    assert target.read_bytes() == (
        b"new"
    )


def test_directory_target_is_rejected(
    tmp_path: Path,
) -> None:
    directory = (
        tmp_path
        / "directory"
    )

    directory.mkdir()

    with pytest.raises(
        DownloadError,
        match="klasör",
    ):
        Downloader(
            FakeHttpClient(),  # type: ignore[arg-type]
        ).download(
            "https://example.com/file",
            directory,
        )


# =============================================================================
# NETWORK FAILURE
# =============================================================================
def test_network_error_becomes_download_error(
    tmp_path: Path,
) -> None:
    client = FakeHttpClient()

    client.error = NetworkError(
        "transport failed"
    )

    with pytest.raises(
        DownloadError,
        match="HTTP download başarısız",
    ):
        Downloader(
            client,  # type: ignore[arg-type]
        ).download(
            "https://example.com/file",
            tmp_path / "file.bin",
        )


# =============================================================================
# CANCELLATION
# =============================================================================
def test_stop_before_request_propagates_shutdown(
    tmp_path: Path,
) -> None:
    client = FakeHttpClient()

    def stop_check() -> None:
        raise ShutdownRequested(
            "cancelled"
        )

    downloader = Downloader(
        client,  # type: ignore[arg-type]
        stop_check=stop_check,
    )

    with pytest.raises(
        ShutdownRequested,
    ):
        downloader.download(
            "https://example.com/file",
            tmp_path / "file.bin",
        )

    assert client.calls == []


def test_stop_during_stream_removes_partial_file(
    tmp_path: Path,
) -> None:
    response = FakeResponse(
        [
            b"first",
            b"second",
        ],
    )

    calls = 0

    def stop_check() -> None:
        nonlocal calls

        calls += 1

        if calls >= 3:
            raise ShutdownRequested(
                "stream cancelled"
            )

    target = (
        tmp_path
        / "file.bin"
    )

    downloader = Downloader(
        FakeHttpClient(
            response
        ),  # type: ignore[arg-type]
        stop_check=stop_check,
    )

    with pytest.raises(
        ShutdownRequested,
    ):
        downloader.download(
            "https://example.com/file",
            target,
        )

    assert target.exists() is False


# =============================================================================
# RESPONSE VALIDATION
# =============================================================================
def test_invalid_content_length_is_rejected(
    tmp_path: Path,
) -> None:
    response = FakeResponse(
        [b"hello"],
        headers={
            "Content-Length": "invalid",
        },
    )

    with pytest.raises(
        DownloadError,
        match="Content-Length",
    ):
        Downloader(
            FakeHttpClient(
                response
            ),  # type: ignore[arg-type]
        ).download(
            "https://example.com/file",
            tmp_path / "file.bin",
        )


def test_empty_chunks_are_ignored(
    tmp_path: Path,
) -> None:
    response = FakeResponse(
        [
            b"",
            b"hello",
            b"",
            b"world",
        ],
    )

    result = Downloader(
        FakeHttpClient(
            response
        ),  # type: ignore[arg-type]
    ).download(
        "https://example.com/file",
        tmp_path / "file.bin",
    )

    assert result.size_bytes == 10
    assert result.chunk_count == 2


# =============================================================================
# CONSTRUCTOR
# =============================================================================
@pytest.mark.parametrize(
    "chunk_size",
    [
        0,
        -1,
        True,
        1.5,
    ],
)
def test_invalid_chunk_size_is_rejected(
    chunk_size: Any,
) -> None:
    with pytest.raises(
        ValueError,
    ):
        Downloader(
            FakeHttpClient(),  # type: ignore[arg-type]
            chunk_size=chunk_size,
        )


@pytest.mark.parametrize(
    "max_download_bytes",
    [
        0,
        -1,
        True,
        1.5,
    ],
)
def test_invalid_default_max_download_bytes_is_rejected(
    max_download_bytes: Any,
) -> None:
    with pytest.raises(
        ValueError,
    ):
        Downloader(
            FakeHttpClient(),  # type: ignore[arg-type]
            max_download_bytes=(
                max_download_bytes
            ),
        )