from __future__ import annotations

"""
Enterprise Crawler Framework - Config Loader

Configuration girdilerini canonical mapping yapısına dönüştürür ve
CrawlerSettings üretir.

Desteklenen kaynaklar
---------------------
* Python Mapping
* JSON string
* JSON file
* Environment variables

Temel prensip
-------------
Bütün giriş kaynakları sonunda aynı canonical yolu kullanır:

    input
      ↓
    mapping
      ↓
    from_mapping()
      ↓
    CrawlerSettings

Bu sayede JSON ve ENV için ayrı validation sistemleri oluşmaz.

Fail-closed davranışı
---------------------
* Bilinmeyen root section reddedilir.
* Bilinmeyen field reddedilir.
* Bilinmeyen ENTERPRISE_CRAWLER_* environment variable reddedilir.
* Yanlış tip reddedilir.
* Malformed JSON reddedilir.
* Geçersiz boolean/numeric ENV değeri reddedilir.

Bilerek henüz içermez
---------------------
* YAML
* .env file parser
* Remote configuration
* Secret manager
* Runtime component construction
"""

import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional

from enterprise_crawler.config.settings import (
    CrawlerSettings,
    DownloadSettings,
    HTTPSettings,
    StorageSettings,
)
from enterprise_crawler.exceptions import ConfigurationError


# =============================================================================
# CONFIG SCHEMA
# =============================================================================
_ALLOWED_ROOT_FIELDS = frozenset(
    {
        "http",
        "download",
        "storage",
    }
)

_ALLOWED_HTTP_FIELDS = frozenset(
    {
        "timeout_seconds",
        "max_retries",
        "backoff_factor",
        "pool_connections",
        "pool_maxsize",
        "verify_tls",
    }
)

_ALLOWED_DOWNLOAD_FIELDS = frozenset(
    {
        "chunk_size",
        "max_bytes",
    }
)

_ALLOWED_STORAGE_FIELDS = frozenset(
    {
        "enabled",
        "root",
        "state_path",
        "sqlite_timeout_seconds",
    }
)


# =============================================================================
# ENVIRONMENT SCHEMA
# =============================================================================
ENV_PREFIX = "ENTERPRISE_CRAWLER_"


_ENV_BINDINGS: dict[
    str,
    tuple[
        str,
        str,
        str,
    ],
] = {
    # HTTP
    "ENTERPRISE_CRAWLER_HTTP_TIMEOUT_SECONDS": (
        "http",
        "timeout_seconds",
        "float",
    ),
    "ENTERPRISE_CRAWLER_HTTP_MAX_RETRIES": (
        "http",
        "max_retries",
        "int",
    ),
    "ENTERPRISE_CRAWLER_HTTP_BACKOFF_FACTOR": (
        "http",
        "backoff_factor",
        "float",
    ),
    "ENTERPRISE_CRAWLER_HTTP_POOL_CONNECTIONS": (
        "http",
        "pool_connections",
        "int",
    ),
    "ENTERPRISE_CRAWLER_HTTP_POOL_MAXSIZE": (
        "http",
        "pool_maxsize",
        "int",
    ),
    "ENTERPRISE_CRAWLER_HTTP_VERIFY_TLS": (
        "http",
        "verify_tls",
        "bool",
    ),

    # Downloader
    "ENTERPRISE_CRAWLER_DOWNLOAD_CHUNK_SIZE": (
        "download",
        "chunk_size",
        "int",
    ),
    "ENTERPRISE_CRAWLER_DOWNLOAD_MAX_BYTES": (
        "download",
        "max_bytes",
        "nullable_int",
    ),

    # Storage
    "ENTERPRISE_CRAWLER_STORAGE_ENABLED": (
        "storage",
        "enabled",
        "bool",
    ),
    "ENTERPRISE_CRAWLER_STORAGE_ROOT": (
        "storage",
        "root",
        "str",
    ),
    "ENTERPRISE_CRAWLER_STORAGE_STATE_PATH": (
        "storage",
        "state_path",
        "str",
    ),
    "ENTERPRISE_CRAWLER_STORAGE_SQLITE_TIMEOUT_SECONDS": (
        "storage",
        "sqlite_timeout_seconds",
        "float",
    ),
}


# =============================================================================
# MAPPING HELPERS
# =============================================================================
def _ensure_mapping(
    value: Any,
    *,
    location: str,
) -> Mapping[str, Any]:
    if not isinstance(
        value,
        Mapping,
    ):
        raise ConfigurationError(
            f"{location} object/mapping olmalıdır."
        )

    return value


def _normalize_mapping_keys(
    mapping: Mapping[Any, Any],
    *,
    location: str,
) -> dict[str, Any]:
    normalized: dict[str, Any] = {}

    for raw_key, value in mapping.items():
        if not isinstance(
            raw_key,
            str,
        ):
            raise ConfigurationError(
                f"{location} field adları string olmalıdır."
            )

        key = raw_key.strip()

        if not key:
            raise ConfigurationError(
                f"{location} boş field adı içeremez."
            )

        if key in normalized:
            raise ConfigurationError(
                f"{location} duplicate field içeriyor "
                f"| field={key}"
            )

        normalized[key] = value

    return normalized


def _reject_unknown_fields(
    mapping: Mapping[str, Any],
    *,
    allowed: frozenset[str],
    location: str,
) -> None:
    unknown = sorted(
        set(mapping)
        - set(allowed)
    )

    if not unknown:
        return

    raise ConfigurationError(
        f"{location} bilinmeyen field içeriyor "
        f"| fields={', '.join(unknown)}"
    )


def _section_mapping(
    root: Mapping[str, Any],
    section_name: str,
) -> dict[str, Any]:
    raw_section = root.get(
        section_name,
        {},
    )

    section = _ensure_mapping(
        raw_section,
        location=section_name,
    )

    return _normalize_mapping_keys(
        section,
        location=section_name,
    )


def _deep_merge(
    base: Mapping[str, Any],
    override: Mapping[str, Any],
) -> dict[str, Any]:
    """
    Nested configuration mapping'lerini recursive merge eder.

    Override içindeki değerler base değerlerini kazanır.

    Input mapping'ler mutate edilmez.
    """

    result: dict[str, Any] = deepcopy(
        dict(base)
    )

    for key, value in override.items():
        existing = result.get(
            key
        )

        if (
            isinstance(existing, Mapping)
            and isinstance(value, Mapping)
        ):
            result[key] = _deep_merge(
                existing,
                value,
            )

        else:
            result[key] = deepcopy(
                value
            )

    return result


# =============================================================================
# ENVIRONMENT PARSING HELPERS
# =============================================================================
def _parse_env_bool(
    value: str,
    *,
    variable_name: str,
) -> bool:
    normalized = value.strip().lower()

    truthy = {
        "1",
        "true",
        "yes",
        "on",
    }

    falsy = {
        "0",
        "false",
        "no",
        "off",
    }

    if normalized in truthy:
        return True

    if normalized in falsy:
        return False

    raise ConfigurationError(
        "Environment variable boolean olarak parse edilemedi "
        f"| variable={variable_name} "
        f"| value={value!r}"
    )


def _parse_env_int(
    value: str,
    *,
    variable_name: str,
) -> int:
    normalized = value.strip()

    if not normalized:
        raise ConfigurationError(
            "Environment variable integer değeri boş olamaz "
            f"| variable={variable_name}"
        )

    try:
        return int(
            normalized,
            10,
        )

    except ValueError as exc:
        raise ConfigurationError(
            "Environment variable integer olarak parse edilemedi "
            f"| variable={variable_name} "
            f"| value={value!r}"
        ) from exc


def _parse_env_float(
    value: str,
    *,
    variable_name: str,
) -> float:
    normalized = value.strip()

    if not normalized:
        raise ConfigurationError(
            "Environment variable float değeri boş olamaz "
            f"| variable={variable_name}"
        )

    try:
        return float(
            normalized
        )

    except ValueError as exc:
        raise ConfigurationError(
            "Environment variable float olarak parse edilemedi "
            f"| variable={variable_name} "
            f"| value={value!r}"
        ) from exc


def _parse_env_string(
    value: str,
    *,
    variable_name: str,
) -> str:
    normalized = value.strip()

    if not normalized:
        raise ConfigurationError(
            "Environment variable string değeri boş olamaz "
            f"| variable={variable_name}"
        )

    return normalized


def _parse_nullable_env_int(
    value: str,
    *,
    variable_name: str,
) -> Optional[int]:
    normalized = value.strip().lower()

    if normalized in {
        "null",
        "none",
    }:
        return None

    return _parse_env_int(
        value,
        variable_name=variable_name,
    )


def _parse_env_value(
    value: Any,
    *,
    parser_kind: str,
    variable_name: str,
) -> Any:
    if not isinstance(
        value,
        str,
    ):
        raise ConfigurationError(
            "Environment variable değeri string olmalıdır "
            f"| variable={variable_name} "
            f"| actual={type(value).__name__}"
        )

    if parser_kind == "bool":
        return _parse_env_bool(
            value,
            variable_name=variable_name,
        )

    if parser_kind == "int":
        return _parse_env_int(
            value,
            variable_name=variable_name,
        )

    if parser_kind == "float":
        return _parse_env_float(
            value,
            variable_name=variable_name,
        )

    if parser_kind == "nullable_int":
        return _parse_nullable_env_int(
            value,
            variable_name=variable_name,
        )

    if parser_kind == "str":
        return _parse_env_string(
            value,
            variable_name=variable_name,
        )

    raise ConfigurationError(
        "Bilinmeyen internal ENV parser "
        f"| parser={parser_kind}"
    )


def _environment_to_mapping(
    environment: Mapping[str, str],
) -> dict[str, Any]:
    """
    ENTERPRISE_CRAWLER_* değerlerini nested canonical mapping'e çevirir.
    """

    result: dict[
        str,
        dict[str, Any],
    ] = {}

    crawler_variables = sorted(
        key
        for key in environment
        if key.startswith(
            ENV_PREFIX
        )
    )

    unknown = [
        key
        for key in crawler_variables
        if key not in _ENV_BINDINGS
    ]

    if unknown:
        raise ConfigurationError(
            "Bilinmeyen Enterprise Crawler environment variable "
            f"| variables={', '.join(unknown)}"
        )

    for variable_name in crawler_variables:
        section_name, field_name, parser_kind = (
            _ENV_BINDINGS[
                variable_name
            ]
        )

        parsed_value = (
            _parse_env_value(
                environment[
                    variable_name
                ],
                parser_kind=parser_kind,
                variable_name=(
                    variable_name
                ),
            )
        )

        section = result.setdefault(
            section_name,
            {},
        )

        section[
            field_name
        ] = parsed_value

    return result


# =============================================================================
# CONFIG LOADER
# =============================================================================
class ConfigLoader:
    """
    CrawlerSettings üretmek için merkezi configuration loader.

    Mapping::

        settings = ConfigLoader.from_mapping(
            {
                "http": {
                    "timeout_seconds": 10,
                },
            }
        )

    JSON::

        settings = ConfigLoader.from_json(
            '''
            {
              "http": {
                "max_retries": 2
              }
            }
            '''
        )

    File::

        settings = ConfigLoader.from_file(
            "crawler.json"
        )

    Environment::

        settings = ConfigLoader.from_env()

    Base configuration + ENV override::

        settings = ConfigLoader.from_env(
            base={
                "http": {
                    "timeout_seconds": 30
                }
            }
        )
    """

    # =========================================================================
    # MAPPING
    # =========================================================================
    @classmethod
    def from_mapping(
        cls,
        payload: Mapping[str, Any],
    ) -> CrawlerSettings:
        root = _ensure_mapping(
            payload,
            location="configuration root",
        )

        normalized_root = (
            _normalize_mapping_keys(
                root,
                location=(
                    "configuration root"
                ),
            )
        )

        _reject_unknown_fields(
            normalized_root,
            allowed=(
                _ALLOWED_ROOT_FIELDS
            ),
            location=(
                "configuration root"
            ),
        )

        # ---------------------------------------------------------------------
        # HTTP
        # ---------------------------------------------------------------------
        http_payload = (
            _section_mapping(
                normalized_root,
                "http",
            )
        )

        _reject_unknown_fields(
            http_payload,
            allowed=(
                _ALLOWED_HTTP_FIELDS
            ),
            location="http",
        )

        # ---------------------------------------------------------------------
        # DOWNLOAD
        # ---------------------------------------------------------------------
        download_payload = (
            _section_mapping(
                normalized_root,
                "download",
            )
        )

        _reject_unknown_fields(
            download_payload,
            allowed=(
                _ALLOWED_DOWNLOAD_FIELDS
            ),
            location="download",
        )

        # ---------------------------------------------------------------------
        # STORAGE
        # ---------------------------------------------------------------------
        storage_payload = (
            _section_mapping(
                normalized_root,
                "storage",
            )
        )

        _reject_unknown_fields(
            storage_payload,
            allowed=(
                _ALLOWED_STORAGE_FIELDS
            ),
            location="storage",
        )

        try:
            http = HTTPSettings(
                **http_payload
            )

            download = DownloadSettings(
                **download_payload
            )

            storage = StorageSettings(
                **storage_payload
            )

            return CrawlerSettings(
                http=http,
                download=download,
                storage=storage,
            )

        except ConfigurationError:
            raise

        except TypeError as exc:
            raise ConfigurationError(
                "Configuration model oluşturulamadı."
            ) from exc

    # =========================================================================
    # JSON STRING
    # =========================================================================
    @classmethod
    def from_json(
        cls,
        payload: str,
    ) -> CrawlerSettings:
        if not isinstance(
            payload,
            str,
        ):
            raise ConfigurationError(
                "JSON configuration str olmalıdır."
            )

        if not payload.strip():
            raise ConfigurationError(
                "JSON configuration boş olamaz."
            )

        try:
            decoded = json.loads(
                payload
            )

        except json.JSONDecodeError as exc:
            raise ConfigurationError(
                "JSON configuration parse edilemedi "
                f"| line={exc.lineno} "
                f"| column={exc.colno}"
            ) from exc

        if not isinstance(
            decoded,
            Mapping,
        ):
            raise ConfigurationError(
                "JSON configuration root object olmalıdır."
            )

        return cls.from_mapping(
            decoded
        )

    # =========================================================================
    # FILE
    # =========================================================================
    @classmethod
    def from_file(
        cls,
        path: str | Path,
        *,
        encoding: str = "utf-8",
    ) -> CrawlerSettings:
        config_path = Path(
            path
        ).expanduser()

        if not str(
            config_path
        ).strip():
            raise ConfigurationError(
                "Configuration file path boş olamaz."
            )

        if not config_path.exists():
            raise ConfigurationError(
                "Configuration file bulunamadı "
                f"| path={config_path}"
            )

        if not config_path.is_file():
            raise ConfigurationError(
                "Configuration path dosya olmalıdır "
                f"| path={config_path}"
            )

        suffix = (
            config_path.suffix.lower()
        )

        if suffix != ".json":
            raise ConfigurationError(
                "Desteklenmeyen configuration file formatı "
                f"| suffix={suffix or '<none>'} "
                "| supported=.json"
            )

        try:
            raw = config_path.read_text(
                encoding=encoding
            )

        except (
            OSError,
            UnicodeError,
            LookupError,
        ) as exc:
            raise ConfigurationError(
                "Configuration file okunamadı "
                f"| path={config_path}"
            ) from exc

        try:
            return cls.from_json(
                raw
            )

        except ConfigurationError as exc:
            raise ConfigurationError(
                "Configuration file geçersiz "
                f"| path={config_path} "
                f"| reason={exc}"
            ) from exc

    # =========================================================================
    # ENVIRONMENT
    # =========================================================================
    @classmethod
    def from_env(
        cls,
        environment: Optional[
            Mapping[str, str]
        ] = None,
        *,
        base: Optional[
            CrawlerSettings
            | Mapping[str, Any]
        ] = None,
    ) -> CrawlerSettings:
        """
        Environment variable'lardan CrawlerSettings üretir.

        ``environment`` verilmezse os.environ kullanılır.

        ``base`` verilirse ENV değerleri base configuration üzerine override
        olarak uygulanır.

        Precedence::

            defaults
                ↓
            base
                ↓
            environment

        Bu metot os.environ üzerinde mutation yapmaz.
        """

        if environment is None:
            resolved_environment: Mapping[
                str,
                str,
            ] = os.environ

        else:
            if not isinstance(
                environment,
                Mapping,
            ):
                raise ConfigurationError(
                    "environment mapping olmalıdır."
                )

            resolved_environment = (
                environment
            )

        # ---------------------------------------------------------------------
        # BASE
        # ---------------------------------------------------------------------
        if base is None:
            base_mapping: dict[
                str,
                Any,
            ] = {}

        elif isinstance(
            base,
            CrawlerSettings,
        ):
            base_mapping = (
                base.to_dict()
            )

        elif isinstance(
            base,
            Mapping,
        ):
            # Önce base'i validate ediyoruz.
            # Böylece ENV override, yanlış base configuration'ı gizleyemez.
            validated_base = (
                cls.from_mapping(
                    base
                )
            )

            base_mapping = (
                validated_base.to_dict()
            )

        else:
            raise ConfigurationError(
                "base CrawlerSettings veya Mapping olmalıdır."
            )

        # ---------------------------------------------------------------------
        # ENV → CANONICAL MAPPING
        # ---------------------------------------------------------------------
        env_mapping = (
            _environment_to_mapping(
                resolved_environment
            )
        )

        merged = _deep_merge(
            base_mapping,
            env_mapping,
        )

        # ---------------------------------------------------------------------
        # CANONICAL VALIDATION
        # ---------------------------------------------------------------------
        return cls.from_mapping(
            merged
        )