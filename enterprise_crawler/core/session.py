from __future__ import annotations

"""
Enterprise Crawler Framework - Session Manager

HTTP transport/session yaşam döngüsünü yönetir.

Sorumlulukları
--------------
* requests.Session oluşturmak.
* HTTPAdapter bağlamak.
* urllib3 Retry policy uygulamak.
* Connection pool boyutlarını yönetmek.
* Default session header'larını tanımlamak.
* Session ownership ve cleanup sağlamak.
* Session rebuild desteği sunmak.
* Runtime configuration snapshot üretmek.

Bilerek içermez
---------------
* URL validation.
* TLS policy.
* Proxy rotation.
* Circuit breaker.
* Request-level exception mapping.
* File downloading.
* Rate limiting.
* Authentication.
* Evidence / audit.

Bunlar HttpClient veya daha üst katmanların sorumluluğudur.
"""

import threading
from dataclasses import dataclass
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from enterprise_crawler.exceptions import NetworkError


# =============================================================================
# DEFAULTS
# =============================================================================
DEFAULT_POOL_CONNECTIONS = 10
DEFAULT_POOL_MAXSIZE = 10

DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_FACTOR = 0.75

DEFAULT_RETRY_STATUSES = (
    408,
    425,
    429,
    500,
    502,
    503,
    504,
)

DEFAULT_ALLOWED_METHODS = frozenset(
    {
        "GET",
        "HEAD",
        "OPTIONS",
        "POST",
        "PUT",
        "DELETE",
        "PATCH",
    }
)

DEFAULT_ACCEPT_HEADER = (
    "text/html,"
    "application/xhtml+xml,"
    "application/xml;q=0.9,"
    "application/json;q=0.9,"
    "*/*;q=0.8"
)

DEFAULT_ACCEPT_LANGUAGE = "en-US,en;q=0.9"

DEFAULT_HEADERS = {
    "Accept": DEFAULT_ACCEPT_HEADER,
    "Accept-Language": DEFAULT_ACCEPT_LANGUAGE,
}


# =============================================================================
# CONFIG
# =============================================================================
@dataclass(frozen=True, slots=True)
class SessionConfig:
    """
    HTTP transport yapılandırması.
    """

    max_retries: int = DEFAULT_MAX_RETRIES

    backoff_factor: float = DEFAULT_BACKOFF_FACTOR

    pool_connections: int = DEFAULT_POOL_CONNECTIONS

    pool_maxsize: int = DEFAULT_POOL_MAXSIZE

    retry_statuses: tuple[int, ...] = DEFAULT_RETRY_STATUSES

    allowed_methods: frozenset[str] = DEFAULT_ALLOWED_METHODS

    raise_on_status: bool = False

    respect_retry_after_header: bool = True

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_retries, bool)
            or not isinstance(self.max_retries, int)
            or self.max_retries < 0
        ):
            raise ValueError(
                "max_retries negatif olmayan tam sayı olmalıdır."
            )

        if (
            isinstance(self.backoff_factor, bool)
            or not isinstance(
                self.backoff_factor,
                (int, float),
            )
            or self.backoff_factor < 0
        ):
            raise ValueError(
                "backoff_factor negatif olmayan sayı olmalıdır."
            )

        if (
            isinstance(self.pool_connections, bool)
            or not isinstance(self.pool_connections, int)
            or self.pool_connections < 1
        ):
            raise ValueError(
                "pool_connections en az 1 olan tam sayı olmalıdır."
            )

        if (
            isinstance(self.pool_maxsize, bool)
            or not isinstance(self.pool_maxsize, int)
            or self.pool_maxsize < 1
        ):
            raise ValueError(
                "pool_maxsize en az 1 olan tam sayı olmalıdır."
            )

        if not self.allowed_methods:
            raise ValueError(
                "allowed_methods boş olamaz."
            )

        for method in self.allowed_methods:
            if not isinstance(method, str) or not method.strip():
                raise ValueError(
                    "allowed_methods yalnız geçerli HTTP method isimleri içermelidir."
                )

        for status in self.retry_statuses:
            if (
                isinstance(status, bool)
                or not isinstance(status, int)
                or status < 100
                or status > 599
            ):
                raise ValueError(
                    "retry_statuses geçerli HTTP status kodları içermelidir."
                )


# =============================================================================
# SESSION MANAGER
# =============================================================================
class SessionManager:
    """
    requests.Session lifecycle ve transport policy yöneticisi.
    """

    def __init__(
        self,
        *,
        config: Optional[SessionConfig] = None,
        headers: Optional[dict[str, str]] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.config = (
            config
            if config is not None
            else SessionConfig()
        )

        self._lock = threading.RLock()
        self._closed = False

        if session is None:
            self._session = self._build_session(
                headers=headers,
            )

            self._owns_session = True

        else:
            self._session = session
            self._owns_session = False

            self._apply_headers(
                self._session,
                headers=headers,
            )

    # =========================================================================
    # RETRY / ADAPTER
    # =========================================================================
    def _build_retry_policy(self) -> Retry:
        config = self.config

        return Retry(
            total=config.max_retries,
            connect=config.max_retries,
            read=config.max_retries,
            status=config.max_retries,
            redirect=config.max_retries,
            other=config.max_retries,
            backoff_factor=config.backoff_factor,
            status_forcelist=config.retry_statuses,
            allowed_methods=config.allowed_methods,
            raise_on_status=config.raise_on_status,
            respect_retry_after_header=(
                config.respect_retry_after_header
            ),
        )

    def _build_adapter(self) -> HTTPAdapter:
        return HTTPAdapter(
            max_retries=self._build_retry_policy(),
            pool_connections=self.config.pool_connections,
            pool_maxsize=self.config.pool_maxsize,
        )

    # =========================================================================
    # SESSION BUILD
    # =========================================================================
    def _build_session(
        self,
        *,
        headers: Optional[dict[str, str]],
    ) -> requests.Session:
        session = requests.Session()

        adapter = self._build_adapter()

        session.mount(
            "https://",
            adapter,
        )

        session.mount(
            "http://",
            adapter,
        )

        self._apply_headers(
            session,
            headers=headers,
        )

        return session

    # =========================================================================
    # HEADERS
    # =========================================================================
    @staticmethod
    def _normalize_headers(
        headers: Optional[dict[str, str]],
    ) -> dict[str, str]:
        normalized = dict(DEFAULT_HEADERS)

        if headers:
            normalized.update(
                {
                    str(key): str(value)
                    for key, value in headers.items()
                }
            )

        return normalized

    @classmethod
    def _apply_headers(
        cls,
        session: requests.Session,
        *,
        headers: Optional[dict[str, str]],
    ) -> None:
        session.headers.update(
            cls._normalize_headers(
                headers
            )
        )

    # =========================================================================
    # PUBLIC ACCESS
    # =========================================================================
    @property
    def session(self) -> requests.Session:
        with self._lock:
            if self._closed:
                raise NetworkError(
                    "Kapalı SessionManager session sağlayamaz."
                )

            return self._session

    @property
    def is_closed(self) -> bool:
        with self._lock:
            return self._closed

    @property
    def owns_session(self) -> bool:
        return self._owns_session

    # =========================================================================
    # REBUILD
    # =========================================================================
    def rebuild(self) -> requests.Session:
        """
        Framework-owned session'ı aynı config ile yeniden oluşturur.
        """

        with self._lock:
            if self._closed:
                raise NetworkError(
                    "Kapalı SessionManager rebuild edilemez."
                )

            if not self._owns_session:
                raise NetworkError(
                    "External session kullanan SessionManager rebuild edilemez."
                )

            previous_headers = dict(
                self._session.headers
            )

            try:
                self._session.close()
            except Exception as exc:
                raise NetworkError(
                    "HTTP session rebuild sırasında kapatılamadı."
                ) from exc

            self._session = self._build_session(
                headers=previous_headers,
            )

            return self._session

    # =========================================================================
    # SNAPSHOT
    # =========================================================================
    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "closed": self._closed,
                "owns_session": self._owns_session,
                "max_retries": self.config.max_retries,
                "backoff_factor": self.config.backoff_factor,
                "pool_connections": (
                    self.config.pool_connections
                ),
                "pool_maxsize": (
                    self.config.pool_maxsize
                ),
                "retry_statuses": list(
                    self.config.retry_statuses
                ),
                "allowed_methods": sorted(
                    self.config.allowed_methods
                ),
                "raise_on_status": (
                    self.config.raise_on_status
                ),
                "respect_retry_after_header": (
                    self.config.respect_retry_after_header
                ),
            }

    # =========================================================================
    # CLEANUP
    # =========================================================================
    def close(self) -> None:
        """
        Yalnız framework-owned session kapatılır.
        """

        with self._lock:
            if self._closed:
                return

            self._closed = True

            if not self._owns_session:
                return

            try:
                self._session.close()

            except Exception as exc:
                raise NetworkError(
                    "HTTP session kapatılamadı."
                ) from exc

    def __enter__(self) -> "SessionManager":
        if self.is_closed:
            raise NetworkError(
                "Kapalı SessionManager context manager olarak kullanılamaz."
            )

        return self

    def __exit__(
        self,
        exc_type: Any,
        exc: Any,
        traceback: Any,
    ) -> None:
        self.close()

    # =========================================================================
    # REPRESENTATION
    # =========================================================================
    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"max_retries={self.config.max_retries}, "
            f"pool_connections={self.config.pool_connections}, "
            f"pool_maxsize={self.config.pool_maxsize}, "
            f"closed={self.is_closed!r}"
            f")"
        )