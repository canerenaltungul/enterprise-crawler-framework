from __future__ import annotations

"""
Enterprise Crawler Framework - Settings

Framework'ün kullanıcı-facing configuration modellerini tanımlar.

Sorumlulukları
--------------
* HTTP ayarlarını doğrulamak.
* Download ayarlarını doğrulamak.
* Storage ayarlarını doğrulamak.
* Bütün ayarları CrawlerSettings altında toplamak.
* Deterministic dict representation sağlamak.

Bilerek içermez
---------------
* YAML okuma.
* JSON dosyası okuma.
* Environment variable okuma.
* .env desteği.
* Dosya precedence kuralları.
* Runtime component oluşturma.
* SessionManager / HttpClient lifecycle.

Bunlar ConfigLoader veya daha üst composition katmanlarının sorumluluğudur.
"""

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

from enterprise_crawler.exceptions import ConfigurationError


# =============================================================================
# DEFAULTS
# =============================================================================
DEFAULT_HTTP_TIMEOUT_SECONDS = 30.0
DEFAULT_HTTP_MAX_RETRIES = 3
DEFAULT_HTTP_BACKOFF_FACTOR = 0.75
DEFAULT_HTTP_POOL_CONNECTIONS = 10
DEFAULT_HTTP_POOL_MAXSIZE = 10

DEFAULT_DOWNLOAD_CHUNK_SIZE = 64 * 1024
DEFAULT_DOWNLOAD_MAX_BYTES = 256 * 1024 * 1024

DEFAULT_SQLITE_TIMEOUT_SECONDS = 15.0


# =============================================================================
# VALIDATION HELPERS
# =============================================================================
def _positive_number(
    value: Any,
    *,
    field_name: str,
) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(
            value,
            (int, float),
        )
        or value <= 0
    ):
        raise ConfigurationError(
            f"{field_name} pozitif sayı olmalıdır."
        )

    return float(value)


def _non_negative_number(
    value: Any,
    *,
    field_name: str,
) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(
            value,
            (int, float),
        )
        or value < 0
    ):
        raise ConfigurationError(
            f"{field_name} negatif olmayan sayı olmalıdır."
        )

    return float(value)


def _positive_int(
    value: Any,
    *,
    field_name: str,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(
            value,
            int,
        )
        or value < 1
    ):
        raise ConfigurationError(
            f"{field_name} pozitif tam sayı olmalıdır."
        )

    return value


def _non_negative_int(
    value: Any,
    *,
    field_name: str,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(
            value,
            int,
        )
        or value < 0
    ):
        raise ConfigurationError(
            f"{field_name} negatif olmayan tam sayı olmalıdır."
        )

    return value


def _optional_path_string(
    value: Optional[
        str | Path
    ],
    *,
    field_name: str,
) -> Optional[str]:
    if value is None:
        return None

    normalized = str(
        value
    ).strip()

    if not normalized:
        raise ConfigurationError(
            f"{field_name} boş olamaz."
        )

    return normalized


# =============================================================================
# HTTP SETTINGS
# =============================================================================
@dataclass(
    frozen=True,
    slots=True,
)
class HTTPSettings:
    """
    HTTP runtime için kullanıcı-facing ayarlar.

    Bu model doğrudan HTTP request çalıştırmaz.
    """

    timeout_seconds: float = (
        DEFAULT_HTTP_TIMEOUT_SECONDS
    )

    max_retries: int = (
        DEFAULT_HTTP_MAX_RETRIES
    )

    backoff_factor: float = (
        DEFAULT_HTTP_BACKOFF_FACTOR
    )

    pool_connections: int = (
        DEFAULT_HTTP_POOL_CONNECTIONS
    )

    pool_maxsize: int = (
        DEFAULT_HTTP_POOL_MAXSIZE
    )

    verify_tls: bool = True

    def __post_init__(
        self,
    ) -> None:
        timeout = _positive_number(
            self.timeout_seconds,
            field_name=(
                "http.timeout_seconds"
            ),
        )

        max_retries = (
            _non_negative_int(
                self.max_retries,
                field_name=(
                    "http.max_retries"
                ),
            )
        )

        backoff_factor = (
            _non_negative_number(
                self.backoff_factor,
                field_name=(
                    "http.backoff_factor"
                ),
            )
        )

        pool_connections = (
            _positive_int(
                self.pool_connections,
                field_name=(
                    "http.pool_connections"
                ),
            )
        )

        pool_maxsize = (
            _positive_int(
                self.pool_maxsize,
                field_name=(
                    "http.pool_maxsize"
                ),
            )
        )

        if not isinstance(
            self.verify_tls,
            bool,
        ):
            raise ConfigurationError(
                "http.verify_tls boolean olmalıdır."
            )

        object.__setattr__(
            self,
            "timeout_seconds",
            timeout,
        )

        object.__setattr__(
            self,
            "max_retries",
            max_retries,
        )

        object.__setattr__(
            self,
            "backoff_factor",
            backoff_factor,
        )

        object.__setattr__(
            self,
            "pool_connections",
            pool_connections,
        )

        object.__setattr__(
            self,
            "pool_maxsize",
            pool_maxsize,
        )

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return asdict(
            self
        )


# =============================================================================
# DOWNLOAD SETTINGS
# =============================================================================
@dataclass(
    frozen=True,
    slots=True,
)
class DownloadSettings:
    """
    Streaming downloader ayarları.
    """

    chunk_size: int = (
        DEFAULT_DOWNLOAD_CHUNK_SIZE
    )

    max_bytes: Optional[int] = (
        DEFAULT_DOWNLOAD_MAX_BYTES
    )

    def __post_init__(
        self,
    ) -> None:
        chunk_size = (
            _positive_int(
                self.chunk_size,
                field_name=(
                    "download.chunk_size"
                ),
            )
        )

        if self.max_bytes is None:
            max_bytes = None

        else:
            max_bytes = (
                _positive_int(
                    self.max_bytes,
                    field_name=(
                        "download.max_bytes"
                    ),
                )
            )

        object.__setattr__(
            self,
            "chunk_size",
            chunk_size,
        )

        object.__setattr__(
            self,
            "max_bytes",
            max_bytes,
        )

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return asdict(
            self
        )


# =============================================================================
# STORAGE SETTINGS
# =============================================================================
@dataclass(
    frozen=True,
    slots=True,
)
class StorageSettings:
    """
    Local storage configuration.

    Storage varsayılan olarak kapalıdır.

    Etkinleştirildiğinde root zorunludur.
    """

    enabled: bool = False

    root: Optional[
        str | Path
    ] = None

    state_path: Optional[
        str | Path
    ] = None

    sqlite_timeout_seconds: float = (
        DEFAULT_SQLITE_TIMEOUT_SECONDS
    )

    def __post_init__(
        self,
    ) -> None:
        if not isinstance(
            self.enabled,
            bool,
        ):
            raise ConfigurationError(
                "storage.enabled boolean olmalıdır."
            )

        root = _optional_path_string(
            self.root,
            field_name="storage.root",
        )

        state_path = (
            _optional_path_string(
                self.state_path,
                field_name=(
                    "storage.state_path"
                ),
            )
        )

        timeout = _positive_number(
            self.sqlite_timeout_seconds,
            field_name=(
                "storage.sqlite_timeout_seconds"
            ),
        )

        if (
            self.enabled
            and root is None
        ):
            raise ConfigurationError(
                "storage.enabled=True iken "
                "storage.root zorunludur."
            )

        object.__setattr__(
            self,
            "root",
            root,
        )

        object.__setattr__(
            self,
            "state_path",
            state_path,
        )

        object.__setattr__(
            self,
            "sqlite_timeout_seconds",
            timeout,
        )

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return asdict(
            self
        )


# =============================================================================
# CRAWLER SETTINGS
# =============================================================================
@dataclass(
    frozen=True,
    slots=True,
)
class CrawlerSettings:
    """
    Framework runtime ayarlarının immutable root modeli.
    """

    http: HTTPSettings = field(
        default_factory=HTTPSettings
    )

    download: DownloadSettings = field(
        default_factory=DownloadSettings
    )

    storage: StorageSettings = field(
        default_factory=StorageSettings
    )

    def __post_init__(
        self,
    ) -> None:
        if not isinstance(
            self.http,
            HTTPSettings,
        ):
            raise ConfigurationError(
                "CrawlerSettings.http "
                "HTTPSettings olmalıdır."
            )

        if not isinstance(
            self.download,
            DownloadSettings,
        ):
            raise ConfigurationError(
                "CrawlerSettings.download "
                "DownloadSettings olmalıdır."
            )

        if not isinstance(
            self.storage,
            StorageSettings,
        ):
            raise ConfigurationError(
                "CrawlerSettings.storage "
                "StorageSettings olmalıdır."
            )

    @property
    def storage_enabled(
        self,
    ) -> bool:
        return self.storage.enabled

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "http": (
                self.http.to_dict()
            ),
            "download": (
                self.download.to_dict()
            ),
            "storage": (
                self.storage.to_dict()
            ),
        }