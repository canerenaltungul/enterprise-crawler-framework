from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from enterprise_crawler.config import (
    ConfigLoader,
    CrawlerSettings,
)
from enterprise_crawler.exceptions import (
    ConfigurationError,
)


# =============================================================================
# DEFAULT MAPPING
# =============================================================================
def test_empty_mapping_produces_default_settings() -> None:
    settings = (
        ConfigLoader.from_mapping(
            {}
        )
    )

    assert isinstance(
        settings,
        CrawlerSettings,
    )

    assert (
        settings.http.timeout_seconds
        == 30.0
    )

    assert (
        settings.download.chunk_size
        == 64 * 1024
    )

    assert (
        settings.storage.enabled
        is False
    )


# =============================================================================
# MAPPING
# =============================================================================
def test_mapping_loads_all_sections() -> None:
    settings = (
        ConfigLoader.from_mapping(
            {
                "http": {
                    "timeout_seconds": 12,
                    "max_retries": 1,
                    "backoff_factor": 0.25,
                    "pool_connections": 5,
                    "pool_maxsize": 20,
                    "verify_tls": True,
                },
                "download": {
                    "chunk_size": 4096,
                    "max_bytes": 100_000,
                },
                "storage": {
                    "enabled": True,
                    "root": "data/storage",
                    "state_path": (
                        ".state/custom.db"
                    ),
                    "sqlite_timeout_seconds": 9,
                },
            }
        )
    )

    assert (
        settings.http.timeout_seconds
        == 12.0
    )

    assert (
        settings.http.max_retries
        == 1
    )

    assert (
        settings.http.backoff_factor
        == 0.25
    )

    assert (
        settings.http.pool_connections
        == 5
    )

    assert (
        settings.http.pool_maxsize
        == 20
    )

    assert (
        settings.http.verify_tls
        is True
    )

    assert (
        settings.download.chunk_size
        == 4096
    )

    assert (
        settings.download.max_bytes
        == 100_000
    )

    assert (
        settings.storage.enabled
        is True
    )

    assert (
        settings.storage.root
        == "data/storage"
    )

    assert (
        settings.storage.state_path
        == ".state/custom.db"
    )

    assert (
        settings.storage.sqlite_timeout_seconds
        == 9.0
    )


def test_partial_mapping_uses_defaults_for_missing_sections() -> None:
    settings = (
        ConfigLoader.from_mapping(
            {
                "http": {
                    "max_retries": 0,
                }
            }
        )
    )

    assert (
        settings.http.max_retries
        == 0
    )

    assert (
        settings.http.timeout_seconds
        == 30.0
    )

    assert (
        settings.download.chunk_size
        == 64 * 1024
    )

    assert (
        settings.storage.enabled
        is False
    )


# =============================================================================
# STRICT ROOT VALIDATION
# =============================================================================
@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        "config",
        123,
    ],
)
def test_non_mapping_root_is_rejected(
    payload: Any,
) -> None:
    with pytest.raises(
        ConfigurationError,
        match="root",
    ):
        ConfigLoader.from_mapping(
            payload  # type: ignore[arg-type]
        )


def test_unknown_root_section_is_rejected() -> None:
    with pytest.raises(
        ConfigurationError,
        match="bilinmeyen field",
    ):
        ConfigLoader.from_mapping(
            {
                "http": {},
                "database": {},
            }
        )


def test_non_string_root_field_is_rejected() -> None:
    with pytest.raises(
        ConfigurationError,
        match="string",
    ):
        ConfigLoader.from_mapping(
            {
                123: {},
            }  # type: ignore[dict-item]
        )


# =============================================================================
# SECTION VALIDATION
# =============================================================================
@pytest.mark.parametrize(
    (
        "section",
        "value",
    ),
    [
        (
            "http",
            [],
        ),
        (
            "download",
            "invalid",
        ),
        (
            "storage",
            123,
        ),
    ],
)
def test_section_must_be_mapping(
    section: str,
    value: Any,
) -> None:
    with pytest.raises(
        ConfigurationError,
        match=section,
    ):
        ConfigLoader.from_mapping(
            {
                section: value,
            }
        )


def test_unknown_http_field_is_rejected() -> None:
    with pytest.raises(
        ConfigurationError,
        match="timeout_second",
    ):
        ConfigLoader.from_mapping(
            {
                "http": {
                    "timeout_second": 30,
                }
            }
        )


def test_unknown_download_field_is_rejected() -> None:
    with pytest.raises(
        ConfigurationError,
        match="buffer_size",
    ):
        ConfigLoader.from_mapping(
            {
                "download": {
                    "buffer_size": 4096,
                }
            }
        )


def test_unknown_storage_field_is_rejected() -> None:
    with pytest.raises(
        ConfigurationError,
        match="database_path",
    ):
        ConfigLoader.from_mapping(
            {
                "storage": {
                    "database_path": (
                        "state.db"
                    ),
                }
            }
        )


def test_non_string_section_field_is_rejected() -> None:
    with pytest.raises(
        ConfigurationError,
        match="string",
    ):
        ConfigLoader.from_mapping(
            {
                "http": {
                    123: 30,
                }
            }  # type: ignore[dict-item]
        )


# =============================================================================
# SETTINGS VALIDATION PROPAGATION
# =============================================================================
def test_invalid_http_setting_is_rejected() -> None:
    with pytest.raises(
        ConfigurationError,
        match="timeout_seconds",
    ):
        ConfigLoader.from_mapping(
            {
                "http": {
                    "timeout_seconds": 0,
                }
            }
        )


def test_invalid_download_setting_is_rejected() -> None:
    with pytest.raises(
        ConfigurationError,
        match="chunk_size",
    ):
        ConfigLoader.from_mapping(
            {
                "download": {
                    "chunk_size": 0,
                }
            }
        )


def test_enabled_storage_without_root_is_rejected() -> None:
    with pytest.raises(
        ConfigurationError,
        match="root zorunludur",
    ):
        ConfigLoader.from_mapping(
            {
                "storage": {
                    "enabled": True,
                }
            }
        )


# =============================================================================
# JSON STRING
# =============================================================================
def test_json_string_is_loaded() -> None:
    settings = (
        ConfigLoader.from_json(
            """
            {
                "http": {
                    "timeout_seconds": 8
                },
                "download": {
                    "max_bytes": null
                }
            }
            """
        )
    )

    assert (
        settings.http.timeout_seconds
        == 8.0
    )

    assert (
        settings.download.max_bytes
        is None
    )


def test_empty_json_string_is_rejected() -> None:
    with pytest.raises(
        ConfigurationError,
        match="boş",
    ):
        ConfigLoader.from_json(
            "   "
        )


def test_non_string_json_payload_is_rejected() -> None:
    with pytest.raises(
        ConfigurationError,
        match="str",
    ):
        ConfigLoader.from_json(
            {}  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "payload",
    [
        "{",
        '{"http": }',
        '{"http": {"timeout_seconds": 10}',
    ],
)
def test_malformed_json_is_rejected(
    payload: str,
) -> None:
    with pytest.raises(
        ConfigurationError,
        match="parse",
    ):
        ConfigLoader.from_json(
            payload
        )


@pytest.mark.parametrize(
    "payload",
    [
        "[]",
        '"hello"',
        "123",
        "null",
        "true",
    ],
)
def test_json_root_must_be_object(
    payload: str,
) -> None:
    with pytest.raises(
        ConfigurationError,
        match="root object",
    ):
        ConfigLoader.from_json(
            payload
        )


def test_json_unknown_field_is_rejected() -> None:
    with pytest.raises(
        ConfigurationError,
        match="unknown_option",
    ):
        ConfigLoader.from_json(
            """
            {
                "http": {
                    "unknown_option": true
                }
            }
            """
        )


# =============================================================================
# FILE
# =============================================================================
def test_json_file_is_loaded(
    tmp_path: Path,
) -> None:
    config_path = (
        tmp_path
        / "crawler.json"
    )

    config_path.write_text(
        json.dumps(
            {
                "http": {
                    "timeout_seconds": 11,
                },
                "storage": {
                    "enabled": True,
                    "root": "data",
                },
            }
        ),
        encoding="utf-8",
    )

    settings = (
        ConfigLoader.from_file(
            config_path
        )
    )

    assert (
        settings.http.timeout_seconds
        == 11.0
    )

    assert (
        settings.storage.enabled
        is True
    )

    assert (
        settings.storage.root
        == "data"
    )


def test_missing_file_is_rejected(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        ConfigurationError,
        match="bulunamadı",
    ):
        ConfigLoader.from_file(
            tmp_path
            / "missing.json"
        )


def test_directory_is_rejected_as_config_file(
    tmp_path: Path,
) -> None:
    directory = (
        tmp_path
        / "config"
    )

    directory.mkdir()

    with pytest.raises(
        ConfigurationError,
        match="dosya olmalıdır",
    ):
        ConfigLoader.from_file(
            directory
        )


@pytest.mark.parametrize(
    "filename",
    [
        "crawler.yaml",
        "crawler.yml",
        "crawler.toml",
        "crawler.txt",
        "crawler",
    ],
)
def test_unsupported_file_format_is_rejected(
    tmp_path: Path,
    filename: str,
) -> None:
    config_path = (
        tmp_path
        / filename
    )

    config_path.write_text(
        "{}",
        encoding="utf-8",
    )

    with pytest.raises(
        ConfigurationError,
        match="Desteklenmeyen",
    ):
        ConfigLoader.from_file(
            config_path
        )


def test_invalid_json_file_is_rejected(
    tmp_path: Path,
) -> None:
    config_path = (
        tmp_path
        / "crawler.json"
    )

    config_path.write_text(
        "{invalid-json",
        encoding="utf-8",
    )

    with pytest.raises(
        ConfigurationError,
        match="file geçersiz",
    ):
        ConfigLoader.from_file(
            config_path
        )


def test_invalid_configuration_inside_file_is_rejected(
    tmp_path: Path,
) -> None:
    config_path = (
        tmp_path
        / "crawler.json"
    )

    config_path.write_text(
        json.dumps(
            {
                "http": {
                    "timeout_second": 10,
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ConfigurationError,
        match="timeout_second",
    ):
        ConfigLoader.from_file(
            config_path
        )


# =============================================================================
# ROUNDTRIP
# =============================================================================
def test_settings_values_match_equivalent_mapping_and_json() -> None:
    mapping = {
        "http": {
            "timeout_seconds": 14,
            "max_retries": 2,
        },
        "download": {
            "chunk_size": 2048,
        },
        "storage": {
            "enabled": True,
            "root": "runtime-data",
        },
    }

    from_mapping = (
        ConfigLoader.from_mapping(
            mapping
        )
    )

    from_json = (
        ConfigLoader.from_json(
            json.dumps(
                mapping
            )
        )
    )

    assert (
        from_mapping.to_dict()
        == from_json.to_dict()
    )


def test_loader_does_not_mutate_input_mapping() -> None:
    payload = {
        "http": {
            "timeout_seconds": 10,
        }
    }

    original = {
        "http": {
            "timeout_seconds": 10,
        }
    }

    ConfigLoader.from_mapping(
        payload
    )

    assert payload == original