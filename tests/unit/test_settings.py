from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from enterprise_crawler.config import (
    CrawlerSettings,
    DownloadSettings,
    HTTPSettings,
    StorageSettings,
)
from enterprise_crawler.exceptions import (
    ConfigurationError,
)


# =============================================================================
# HTTP DEFAULTS
# =============================================================================
def test_default_http_settings_are_valid() -> None:
    settings = HTTPSettings()

    assert (
        settings.timeout_seconds
        == 30.0
    )

    assert (
        settings.max_retries
        == 3
    )

    assert (
        settings.backoff_factor
        == 0.75
    )

    assert (
        settings.pool_connections
        == 10
    )

    assert (
        settings.pool_maxsize
        == 10
    )

    assert (
        settings.verify_tls
        is True
    )


def test_http_numeric_values_are_normalized() -> None:
    settings = HTTPSettings(
        timeout_seconds=15,
        backoff_factor=1,
    )

    assert (
        settings.timeout_seconds
        == 15.0
    )

    assert (
        settings.backoff_factor
        == 1.0
    )


@pytest.mark.parametrize(
    "value",
    [
        0,
        -1,
        True,
        "30",
    ],
)
def test_invalid_http_timeout_is_rejected(
    value: Any,
) -> None:
    with pytest.raises(
        ConfigurationError,
        match="timeout_seconds",
    ):
        HTTPSettings(
            timeout_seconds=value,
        )


@pytest.mark.parametrize(
    "value",
    [
        -1,
        True,
        1.5,
        "3",
    ],
)
def test_invalid_http_max_retries_is_rejected(
    value: Any,
) -> None:
    with pytest.raises(
        ConfigurationError,
        match="max_retries",
    ):
        HTTPSettings(
            max_retries=value,
        )


def test_zero_http_retries_is_allowed() -> None:
    settings = HTTPSettings(
        max_retries=0
    )

    assert (
        settings.max_retries
        == 0
    )


@pytest.mark.parametrize(
    "value",
    [
        -0.1,
        True,
        "0.75",
    ],
)
def test_invalid_http_backoff_is_rejected(
    value: Any,
) -> None:
    with pytest.raises(
        ConfigurationError,
        match="backoff_factor",
    ):
        HTTPSettings(
            backoff_factor=value,
        )


def test_zero_http_backoff_is_allowed() -> None:
    settings = HTTPSettings(
        backoff_factor=0
    )

    assert (
        settings.backoff_factor
        == 0.0
    )


@pytest.mark.parametrize(
    "field_name",
    [
        "pool_connections",
        "pool_maxsize",
    ],
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
def test_invalid_http_pool_sizes_are_rejected(
    field_name: str,
    value: Any,
) -> None:
    kwargs = {
        field_name: value
    }

    with pytest.raises(
        ConfigurationError,
    ):
        HTTPSettings(
            **kwargs
        )


def test_invalid_verify_tls_is_rejected() -> None:
    with pytest.raises(
        ConfigurationError,
        match="verify_tls",
    ):
        HTTPSettings(
            verify_tls="yes",  # type: ignore[arg-type]
        )


# =============================================================================
# DOWNLOAD
# =============================================================================
def test_default_download_settings_are_valid() -> None:
    settings = DownloadSettings()

    assert (
        settings.chunk_size
        == 64 * 1024
    )

    assert (
        settings.max_bytes
        == 256 * 1024 * 1024
    )


def test_download_max_bytes_can_be_unlimited() -> None:
    settings = DownloadSettings(
        max_bytes=None
    )

    assert (
        settings.max_bytes
        is None
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
def test_invalid_download_chunk_size_is_rejected(
    value: Any,
) -> None:
    with pytest.raises(
        ConfigurationError,
        match="chunk_size",
    ):
        DownloadSettings(
            chunk_size=value,
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
def test_invalid_download_max_bytes_is_rejected(
    value: Any,
) -> None:
    with pytest.raises(
        ConfigurationError,
        match="max_bytes",
    ):
        DownloadSettings(
            max_bytes=value,
        )


# =============================================================================
# STORAGE
# =============================================================================
def test_storage_is_disabled_by_default() -> None:
    settings = StorageSettings()

    assert (
        settings.enabled
        is False
    )

    assert (
        settings.root
        is None
    )

    assert (
        settings.state_path
        is None
    )

    assert (
        settings.sqlite_timeout_seconds
        == 15.0
    )


def test_enabled_storage_requires_root() -> None:
    with pytest.raises(
        ConfigurationError,
        match="root zorunludur",
    ):
        StorageSettings(
            enabled=True
        )


def test_enabled_storage_accepts_root() -> None:
    settings = StorageSettings(
        enabled=True,
        root="data/storage",
    )

    assert (
        settings.enabled
        is True
    )

    assert (
        settings.root
        == "data/storage"
    )


def test_storage_accepts_path_objects() -> None:
    settings = StorageSettings(
        enabled=True,
        root=Path(
            "data/storage"
        ),
        state_path=Path(
            ".state/state.db"
        ),
    )

    assert (
        settings.root
        == str(
            Path(
                "data/storage"
            )
        )
    )

    assert (
        settings.state_path
        == str(
            Path(
                ".state/state.db"
            )
        )
    )


def test_storage_root_can_be_preconfigured_while_disabled() -> None:
    settings = StorageSettings(
        enabled=False,
        root="data/storage",
    )

    assert (
        settings.enabled
        is False
    )

    assert (
        settings.root
        == "data/storage"
    )


@pytest.mark.parametrize(
    "field_name",
    [
        "root",
        "state_path",
    ],
)
@pytest.mark.parametrize(
    "value",
    [
        "",
        "   ",
    ],
)
def test_empty_storage_paths_are_rejected(
    field_name: str,
    value: str,
) -> None:
    kwargs = {
        field_name: value
    }

    with pytest.raises(
        ConfigurationError,
    ):
        StorageSettings(
            **kwargs
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
def test_invalid_sqlite_timeout_is_rejected(
    value: Any,
) -> None:
    with pytest.raises(
        ConfigurationError,
        match="sqlite_timeout_seconds",
    ):
        StorageSettings(
            sqlite_timeout_seconds=value,
        )


def test_invalid_storage_enabled_is_rejected() -> None:
    with pytest.raises(
        ConfigurationError,
        match="storage.enabled",
    ):
        StorageSettings(
            enabled=1,  # type: ignore[arg-type]
        )


# =============================================================================
# ROOT SETTINGS
# =============================================================================
def test_default_crawler_settings_are_valid() -> None:
    settings = CrawlerSettings()

    assert isinstance(
        settings.http,
        HTTPSettings,
    )

    assert isinstance(
        settings.download,
        DownloadSettings,
    )

    assert isinstance(
        settings.storage,
        StorageSettings,
    )

    assert (
        settings.storage_enabled
        is False
    )


def test_custom_nested_settings_are_preserved() -> None:
    http = HTTPSettings(
        timeout_seconds=12,
        max_retries=1,
    )

    download = DownloadSettings(
        chunk_size=1024,
        max_bytes=4096,
    )

    storage = StorageSettings(
        enabled=True,
        root="data",
    )

    settings = CrawlerSettings(
        http=http,
        download=download,
        storage=storage,
    )

    assert settings.http is http
    assert settings.download is download
    assert settings.storage is storage

    assert (
        settings.storage_enabled
        is True
    )


@pytest.mark.parametrize(
    (
        "field_name",
        "value",
    ),
    [
        (
            "http",
            {},
        ),
        (
            "download",
            {},
        ),
        (
            "storage",
            {},
        ),
    ],
)
def test_invalid_nested_setting_types_are_rejected(
    field_name: str,
    value: Any,
) -> None:
    kwargs = {
        field_name: value
    }

    with pytest.raises(
        ConfigurationError,
    ):
        CrawlerSettings(
            **kwargs
        )


# =============================================================================
# SERIALIZATION
# =============================================================================
def test_http_settings_to_dict() -> None:
    settings = HTTPSettings(
        timeout_seconds=15,
        max_retries=2,
    )

    payload = (
        settings.to_dict()
    )

    assert payload[
        "timeout_seconds"
    ] == 15.0

    assert payload[
        "max_retries"
    ] == 2


def test_download_settings_to_dict() -> None:
    settings = DownloadSettings(
        chunk_size=1024,
        max_bytes=None,
    )

    payload = (
        settings.to_dict()
    )

    assert (
        payload["chunk_size"]
        == 1024
    )

    assert (
        payload["max_bytes"]
        is None
    )


def test_storage_settings_to_dict() -> None:
    settings = StorageSettings(
        enabled=True,
        root="data",
        state_path="state.db",
    )

    payload = (
        settings.to_dict()
    )

    assert (
        payload["enabled"]
        is True
    )

    assert (
        payload["root"]
        == "data"
    )

    assert (
        payload["state_path"]
        == "state.db"
    )


def test_crawler_settings_to_dict_is_nested() -> None:
    settings = CrawlerSettings(
        storage=StorageSettings(
            enabled=True,
            root="data",
        )
    )

    payload = (
        settings.to_dict()
    )

    assert set(
        payload
    ) == {
        "http",
        "download",
        "storage",
    }

    assert (
        payload[
            "http"
        ][
            "timeout_seconds"
        ]
        == 30.0
    )

    assert (
        payload[
            "download"
        ][
            "chunk_size"
        ]
        == 64 * 1024
    )

    assert (
        payload[
            "storage"
        ][
            "enabled"
        ]
        is True
    )

    assert (
        payload[
            "storage"
        ][
            "root"
        ]
        == "data"
    )


# =============================================================================
# IMMUTABILITY
# =============================================================================
def test_settings_are_immutable() -> None:
    settings = HTTPSettings()

    with pytest.raises(
        AttributeError
    ):
        settings.timeout_seconds = 99  # type: ignore[misc]