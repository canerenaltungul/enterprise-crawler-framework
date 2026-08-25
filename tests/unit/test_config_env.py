from __future__ import annotations

from typing import Any

import pytest

from enterprise_crawler.config import (
    ConfigLoader,
    CrawlerSettings,
    HTTPSettings,
    StorageSettings,
)
from enterprise_crawler.exceptions import (
    ConfigurationError,
)


# =============================================================================
# EMPTY ENV
# =============================================================================
def test_empty_environment_produces_defaults() -> None:
    settings = (
        ConfigLoader.from_env(
            {}
        )
    )

    assert (
        settings.http.timeout_seconds
        == 30.0
    )

    assert (
        settings.http.max_retries
        == 3
    )

    assert (
        settings.storage.enabled
        is False
    )


def test_unrelated_environment_variables_are_ignored() -> None:
    settings = (
        ConfigLoader.from_env(
            {
                "PATH": "example",
                "HOME": "example",
                "SOME_OTHER_APP_VALUE": "123",
            }
        )
    )

    assert (
        settings.http.timeout_seconds
        == 30.0
    )


# =============================================================================
# HTTP
# =============================================================================
def test_http_environment_values_are_loaded() -> None:
    settings = (
        ConfigLoader.from_env(
            {
                "ENTERPRISE_CRAWLER_HTTP_TIMEOUT_SECONDS": "9.5",
                "ENTERPRISE_CRAWLER_HTTP_MAX_RETRIES": "7",
                "ENTERPRISE_CRAWLER_HTTP_BACKOFF_FACTOR": "0.25",
                "ENTERPRISE_CRAWLER_HTTP_POOL_CONNECTIONS": "4",
                "ENTERPRISE_CRAWLER_HTTP_POOL_MAXSIZE": "12",
                "ENTERPRISE_CRAWLER_HTTP_VERIFY_TLS": "false",
            }
        )
    )

    assert (
        settings.http.timeout_seconds
        == 9.5
    )

    assert (
        settings.http.max_retries
        == 7
    )

    assert (
        settings.http.backoff_factor
        == 0.25
    )

    assert (
        settings.http.pool_connections
        == 4
    )

    assert (
        settings.http.pool_maxsize
        == 12
    )

    assert (
        settings.http.verify_tls
        is False
    )


# =============================================================================
# BOOLEAN PARSING
# =============================================================================
@pytest.mark.parametrize(
    "value",
    [
        "true",
        "TRUE",
        "True",
        "1",
        "yes",
        "YES",
        "on",
        "ON",
    ],
)
def test_truthy_environment_values(
    value: str,
) -> None:
    settings = (
        ConfigLoader.from_env(
            {
                "ENTERPRISE_CRAWLER_STORAGE_ENABLED": value,
                "ENTERPRISE_CRAWLER_STORAGE_ROOT": "data",
            }
        )
    )

    assert (
        settings.storage.enabled
        is True
    )


@pytest.mark.parametrize(
    "value",
    [
        "false",
        "FALSE",
        "False",
        "0",
        "no",
        "NO",
        "off",
        "OFF",
    ],
)
def test_falsy_environment_values(
    value: str,
) -> None:
    settings = (
        ConfigLoader.from_env(
            {
                "ENTERPRISE_CRAWLER_STORAGE_ENABLED": value,
            }
        )
    )

    assert (
        settings.storage.enabled
        is False
    )


@pytest.mark.parametrize(
    "value",
    [
        "",
        "maybe",
        "truthy",
        "2",
        "-1",
    ],
)
def test_invalid_boolean_environment_value_is_rejected(
    value: str,
) -> None:
    with pytest.raises(
        ConfigurationError,
        match="boolean",
    ):
        ConfigLoader.from_env(
            {
                "ENTERPRISE_CRAWLER_HTTP_VERIFY_TLS": value,
            }
        )


# =============================================================================
# DOWNLOAD
# =============================================================================
def test_download_environment_values_are_loaded() -> None:
    settings = (
        ConfigLoader.from_env(
            {
                "ENTERPRISE_CRAWLER_DOWNLOAD_CHUNK_SIZE": "8192",
                "ENTERPRISE_CRAWLER_DOWNLOAD_MAX_BYTES": "500000",
            }
        )
    )

    assert (
        settings.download.chunk_size
        == 8192
    )

    assert (
        settings.download.max_bytes
        == 500_000
    )


@pytest.mark.parametrize(
    "value",
    [
        "null",
        "NULL",
        "none",
        "NONE",
    ],
)
def test_download_max_bytes_can_be_disabled_from_env(
    value: str,
) -> None:
    settings = (
        ConfigLoader.from_env(
            {
                "ENTERPRISE_CRAWLER_DOWNLOAD_MAX_BYTES": value,
            }
        )
    )

    assert (
        settings.download.max_bytes
        is None
    )


# =============================================================================
# STORAGE
# =============================================================================
def test_storage_environment_values_are_loaded() -> None:
    settings = (
        ConfigLoader.from_env(
            {
                "ENTERPRISE_CRAWLER_STORAGE_ENABLED": "true",
                "ENTERPRISE_CRAWLER_STORAGE_ROOT": "runtime-data",
                "ENTERPRISE_CRAWLER_STORAGE_STATE_PATH": ".state/custom.db",
                "ENTERPRISE_CRAWLER_STORAGE_SQLITE_TIMEOUT_SECONDS": "8.5",
            }
        )
    )

    assert (
        settings.storage.enabled
        is True
    )

    assert (
        settings.storage.root
        == "runtime-data"
    )

    assert (
        settings.storage.state_path
        == ".state/custom.db"
    )

    assert (
        settings.storage.sqlite_timeout_seconds
        == 8.5
    )


def test_env_enabled_storage_without_root_is_rejected() -> None:
    with pytest.raises(
        ConfigurationError,
        match="root zorunludur",
    ):
        ConfigLoader.from_env(
            {
                "ENTERPRISE_CRAWLER_STORAGE_ENABLED": "true",
            }
        )


# =============================================================================
# NUMERIC PARSING
# =============================================================================
@pytest.mark.parametrize(
    (
        "variable",
        "value",
    ),
    [
        (
            "ENTERPRISE_CRAWLER_HTTP_MAX_RETRIES",
            "abc",
        ),
        (
            "ENTERPRISE_CRAWLER_HTTP_POOL_CONNECTIONS",
            "1.5",
        ),
        (
            "ENTERPRISE_CRAWLER_DOWNLOAD_CHUNK_SIZE",
            "",
        ),
        (
            "ENTERPRISE_CRAWLER_DOWNLOAD_MAX_BYTES",
            "unlimited",
        ),
    ],
)
def test_invalid_integer_environment_values_are_rejected(
    variable: str,
    value: str,
) -> None:
    with pytest.raises(
        ConfigurationError,
        match="integer",
    ):
        ConfigLoader.from_env(
            {
                variable: value,
            }
        )


@pytest.mark.parametrize(
    (
        "variable",
        "value",
    ),
    [
        (
            "ENTERPRISE_CRAWLER_HTTP_TIMEOUT_SECONDS",
            "abc",
        ),
        (
            "ENTERPRISE_CRAWLER_HTTP_BACKOFF_FACTOR",
            "",
        ),
        (
            "ENTERPRISE_CRAWLER_STORAGE_SQLITE_TIMEOUT_SECONDS",
            "timeout",
        ),
    ],
)
def test_invalid_float_environment_values_are_rejected(
    variable: str,
    value: str,
) -> None:
    with pytest.raises(
        ConfigurationError,
        match="float",
    ):
        ConfigLoader.from_env(
            {
                variable: value,
            }
        )


def test_numeric_env_still_passes_settings_validation() -> None:
    with pytest.raises(
        ConfigurationError,
        match="timeout_seconds",
    ):
        ConfigLoader.from_env(
            {
                "ENTERPRISE_CRAWLER_HTTP_TIMEOUT_SECONDS": "0",
            }
        )


# =============================================================================
# STRING VALIDATION
# =============================================================================
@pytest.mark.parametrize(
    "variable",
    [
        "ENTERPRISE_CRAWLER_STORAGE_ROOT",
        "ENTERPRISE_CRAWLER_STORAGE_STATE_PATH",
    ],
)
def test_empty_string_environment_value_is_rejected(
    variable: str,
) -> None:
    with pytest.raises(
        ConfigurationError,
        match="boş",
    ):
        ConfigLoader.from_env(
            {
                variable: "   ",
            }
        )


# =============================================================================
# UNKNOWN VARIABLES
# =============================================================================
@pytest.mark.parametrize(
    "variable",
    [
        "ENTERPRISE_CRAWLER_UNKNOWN",
        "ENTERPRISE_CRAWLER_HTTP_TIMEOUT",
        "ENTERPRISE_CRAWLER_STORAGE_DATABASE",
        "ENTERPRISE_CRAWLER_DOWNLOAD_BUFFER_SIZE",
    ],
)
def test_unknown_framework_environment_variable_is_rejected(
    variable: str,
) -> None:
    with pytest.raises(
        ConfigurationError,
        match="Bilinmeyen",
    ):
        ConfigLoader.from_env(
            {
                variable: "1",
            }
        )


# =============================================================================
# BASE MAPPING
# =============================================================================
def test_environment_overrides_base_mapping() -> None:
    settings = (
        ConfigLoader.from_env(
            {
                "ENTERPRISE_CRAWLER_HTTP_TIMEOUT_SECONDS": "5",
                "ENTERPRISE_CRAWLER_HTTP_MAX_RETRIES": "1",
            },
            base={
                "http": {
                    "timeout_seconds": 30,
                    "max_retries": 8,
                    "pool_connections": 20,
                }
            },
        )
    )

    assert (
        settings.http.timeout_seconds
        == 5.0
    )

    assert (
        settings.http.max_retries
        == 1
    )

    # ENV'de olmadığı için base korunmalı.
    assert (
        settings.http.pool_connections
        == 20
    )


def test_environment_preserves_non_overridden_base_sections() -> None:
    settings = (
        ConfigLoader.from_env(
            {
                "ENTERPRISE_CRAWLER_HTTP_TIMEOUT_SECONDS": "6",
            },
            base={
                "download": {
                    "chunk_size": 1234,
                    "max_bytes": 9999,
                },
                "storage": {
                    "enabled": True,
                    "root": "base-storage",
                },
            },
        )
    )

    assert (
        settings.http.timeout_seconds
        == 6.0
    )

    assert (
        settings.download.chunk_size
        == 1234
    )

    assert (
        settings.download.max_bytes
        == 9999
    )

    assert (
        settings.storage.enabled
        is True
    )

    assert (
        settings.storage.root
        == "base-storage"
    )


# =============================================================================
# BASE CRAWLER SETTINGS
# =============================================================================
def test_environment_can_override_crawler_settings_instance() -> None:
    base = CrawlerSettings(
        http=HTTPSettings(
            timeout_seconds=20,
            max_retries=5,
        ),
        storage=StorageSettings(
            enabled=True,
            root="settings-storage",
        ),
    )

    settings = (
        ConfigLoader.from_env(
            {
                "ENTERPRISE_CRAWLER_HTTP_TIMEOUT_SECONDS": "3",
            },
            base=base,
        )
    )

    assert (
        settings.http.timeout_seconds
        == 3.0
    )

    assert (
        settings.http.max_retries
        == 5
    )

    assert (
        settings.storage.enabled
        is True
    )

    assert (
        settings.storage.root
        == "settings-storage"
    )


# =============================================================================
# BASE VALIDATION
# =============================================================================
def test_invalid_base_mapping_is_rejected_before_env_override() -> None:
    with pytest.raises(
        ConfigurationError,
        match="timeout_second",
    ):
        ConfigLoader.from_env(
            {
                "ENTERPRISE_CRAWLER_HTTP_TIMEOUT_SECONDS": "5",
            },
            base={
                "http": {
                    "timeout_second": 30,
                }
            },
        )


@pytest.mark.parametrize(
    "base",
    [
        "config",
        123,
        [],
        object(),
    ],
)
def test_invalid_base_type_is_rejected(
    base: Any,
) -> None:
    with pytest.raises(
        ConfigurationError,
        match="base",
    ):
        ConfigLoader.from_env(
            {},
            base=base,  # type: ignore[arg-type]
        )


# =============================================================================
# ENVIRONMENT CONTRACT
# =============================================================================
def test_non_mapping_environment_is_rejected() -> None:
    with pytest.raises(
        ConfigurationError,
        match="environment mapping",
    ):
        ConfigLoader.from_env(
            []  # type: ignore[arg-type]
        )


def test_non_string_environment_value_is_rejected() -> None:
    with pytest.raises(
        ConfigurationError,
        match="string olmalıdır",
    ):
        ConfigLoader.from_env(
            {
                "ENTERPRISE_CRAWLER_HTTP_MAX_RETRIES": 3,
            }  # type: ignore[dict-item]
        )


# =============================================================================
# INPUT IMMUTABILITY
# =============================================================================
def test_environment_mapping_is_not_mutated() -> None:
    environment = {
        "ENTERPRISE_CRAWLER_HTTP_TIMEOUT_SECONDS": "4",
    }

    original = dict(
        environment
    )

    ConfigLoader.from_env(
        environment
    )

    assert (
        environment
        == original
    )


def test_base_mapping_is_not_mutated() -> None:
    base = {
        "http": {
            "timeout_seconds": 30,
            "max_retries": 4,
        }
    }

    original = {
        "http": {
            "timeout_seconds": 30,
            "max_retries": 4,
        }
    }

    ConfigLoader.from_env(
        {
            "ENTERPRISE_CRAWLER_HTTP_TIMEOUT_SECONDS": "2",
        },
        base=base,
    )

    assert (
        base
        == original
    )