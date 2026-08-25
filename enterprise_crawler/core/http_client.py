from __future__ import annotations

"""
Enterprise Crawler Framework - HTTP Client

Framework'ün provider-independent HTTP çalışma katmanı.

Sorumlulukları
--------------
* Merkezi HTTP request çalıştırmak.
* GET / POST yardımcıları sağlamak.
* Request timeout uygulamak.
* TLS doğrulamasını güvenli varsayılanla yönetmek.
* User-Agent rotasyonu uygulamak.
* Basit proxy seçimi sağlamak.
* Network hatalarını framework exception sözleşmesine çevirmek.
* Retry edilebilir HTTP durumlarını ayırt etmek.
* Domain bazlı circuit breaker uygulamak.
* External veya framework-managed requests.Session ile çalışmak.
* Cooperative shutdown callback'i desteklemek.

Bilerek içermez
---------------
* urllib3 Retry / HTTPAdapter konfigürasyonu.
* Connection pool policy.
* Cookie persistence policy.
* Downloader / dosya streaming.
* Rate limiting / concurrency budget.
* Source-specific authentication.
* Scheduler.
* Storage.
* Evidence / audit.

Session pooling ve transport-level retry davranışı ``core/session.py`` içinde
ayrı bir component olarak geliştirilecektir.
"""

import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Protocol
from urllib.parse import urlparse

import requests

from enterprise_crawler.exceptions import (
    CircuitBreakerOpenError,
    NetworkError,
    RetryableNetworkError,
)


# =============================================================================
# CONSTANTS
# =============================================================================
DEFAULT_TIMEOUT_SECONDS = 30.0

DEFAULT_USER_AGENTS = (
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.4 Safari/605.1.15"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
)

DEFAULT_ACCEPT_HEADER = (
    "text/html,"
    "application/xhtml+xml,"
    "application/xml;q=0.9,"
    "application/json;q=0.9,"
    "*/*;q=0.8"
)

DEFAULT_ACCEPT_LANGUAGE = (
    "en-US,en;q=0.9"
)

RETRYABLE_HTTP_STATUSES = frozenset(
    {
        408,
        425,
        429,
        500,
        502,
        503,
        504,
    }
)


# =============================================================================
# SESSION CONTRACT
# =============================================================================
class HTTPSessionProtocol(Protocol):
    """
    HttpClient'ın ihtiyaç duyduğu minimum session sözleşmesi.

    Böylece HttpClient doğrudan belirli SessionManager implementasyonuna
    bağımlı değildir.
    """

    headers: Any

    def request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> requests.Response:
        ...

    def close(self) -> None:
        ...


# =============================================================================
# CIRCUIT BREAKER
# =============================================================================
@dataclass(slots=True)
class CircuitState:
    """
    Tek domain için runtime circuit state.
    """

    consecutive_failures: int = 0

    opened_until_monotonic: float = 0.0

    last_error: Optional[str] = None


class DomainCircuitBreaker:
    """
    Domain bazlı hafif circuit breaker.

    Bu breaker process-local'dır.

    Distributed/global circuit state ileride Redis/PostgreSQL veya ayrı bir
    enterprise component ile sağlanabilir.
    """

    def __init__(
        self,
        *,
        failure_threshold: int = 4,
        recovery_timeout_seconds: float = 60.0,
    ) -> None:
        self.failure_threshold = max(
            1,
            int(failure_threshold),
        )

        self.recovery_timeout_seconds = max(
            0.1,
            float(recovery_timeout_seconds),
        )

        self._states: dict[str, CircuitState] = {}

        self._lock = threading.RLock()

    # -------------------------------------------------------------------------
    # INTERNAL
    # -------------------------------------------------------------------------
    def _state_for(
        self,
        domain: str,
    ) -> CircuitState:
        normalized_domain = str(
            domain or ""
        ).strip().lower()

        if not normalized_domain:
            raise ValueError(
                "Circuit breaker domain boş olamaz."
            )

        with self._lock:
            return self._states.setdefault(
                normalized_domain,
                CircuitState(),
            )

    # -------------------------------------------------------------------------
    # PUBLIC
    # -------------------------------------------------------------------------
    def allow_request(
        self,
        domain: str,
    ) -> bool:
        state = self._state_for(
            domain
        )

        now = time.monotonic()

        with self._lock:
            if (
                state.opened_until_monotonic
                <= 0
            ):
                return True

            if (
                now
                >= state.opened_until_monotonic
            ):
                # Basit half-open davranışı:
                # recovery süresi dolunca yeni request'e izin verilir.
                state.opened_until_monotonic = 0.0
                state.consecutive_failures = 0
                state.last_error = None

                return True

            return False

    def record_success(
        self,
        domain: str,
    ) -> None:
        if not domain:
            return

        state = self._state_for(
            domain
        )

        with self._lock:
            state.consecutive_failures = 0
            state.opened_until_monotonic = 0.0
            state.last_error = None

    def record_failure(
        self,
        domain: str,
        error: BaseException | str,
    ) -> None:
        if not domain:
            return

        state = self._state_for(
            domain
        )

        if isinstance(
            error,
            BaseException,
        ):
            message = (
                f"{error.__class__.__name__}: "
                f"{error}"
            )
        else:
            message = str(error)

        with self._lock:
            state.consecutive_failures += 1

            state.last_error = (
                message[:4_000]
            )

            if (
                state.consecutive_failures
                >= self.failure_threshold
            ):
                state.opened_until_monotonic = (
                    time.monotonic()
                    + self.recovery_timeout_seconds
                )

    def snapshot(
        self,
    ) -> dict[str, dict[str, Any]]:
        now = time.monotonic()

        with self._lock:
            return {
                domain: {
                    "consecutive_failures": (
                        state.consecutive_failures
                    ),
                    "is_open": (
                        state.opened_until_monotonic
                        > now
                    ),
                    "retry_after_seconds": round(
                        max(
                            0.0,
                            (
                                state.opened_until_monotonic
                                - now
                            ),
                        ),
                        6,
                    ),
                    "last_error": (
                        state.last_error
                    ),
                }
                for domain, state
                in self._states.items()
            }


# =============================================================================
# HTTP CLIENT
# =============================================================================
class HttpClient:
    """
    Generic synchronous HTTP client.

    Default olarak kendi ``requests.Session`` instance'ını oluşturur.

    İleride ``SessionManager`` geliştirildiğinde:

        HttpClient(session=session_manager.session)

    biçiminde dışarıdan session verilebilir.
    """

    def __init__(
        self,
        *,
        session: Optional[HTTPSessionProtocol] = None,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        proxies: Optional[list[str]] = None,
        user_agents: Optional[
            tuple[str, ...] | list[str]
        ] = None,
        allow_insecure_tls: bool = False,
        circuit_breaker: Optional[
            DomainCircuitBreaker
        ] = None,
        circuit_failure_threshold: int = 4,
        circuit_recovery_seconds: float = 60.0,
        stop_check: Optional[
            Callable[[], None]
        ] = None,
    ) -> None:
        self.timeout_seconds = max(
            0.1,
            float(timeout_seconds),
        )

        self.allow_insecure_tls = bool(
            allow_insecure_tls
        )

        self.proxies = [
            str(proxy).strip()
            for proxy in (
                proxies or []
            )
            if str(proxy).strip()
        ]

        normalized_agents = tuple(
            str(agent).strip()
            for agent in (
                user_agents
                or DEFAULT_USER_AGENTS
            )
            if str(agent).strip()
        )

        self.user_agents = (
            normalized_agents
            or DEFAULT_USER_AGENTS
        )

        self.stop_check = stop_check

        if session is None:
            internal_session = (
                requests.Session()
            )

            internal_session.headers.update(
                {
                    "Accept": (
                        DEFAULT_ACCEPT_HEADER
                    ),
                    "Accept-Language": (
                        DEFAULT_ACCEPT_LANGUAGE
                    ),
                }
            )

            self.session: HTTPSessionProtocol = (
                internal_session
            )

            self._owns_session = True

        else:
            self.session = session

            self._owns_session = False

        self.circuit_breaker = (
            circuit_breaker
            or DomainCircuitBreaker(
                failure_threshold=(
                    circuit_failure_threshold
                ),
                recovery_timeout_seconds=(
                    circuit_recovery_seconds
                ),
            )
        )

        self._closed = False

        self._close_lock = threading.Lock()

    # =========================================================================
    # URL / REQUEST HELPERS
    # =========================================================================
    @staticmethod
    def domain_from_url(
        url: str,
    ) -> str:
        raw_url = str(
            url or ""
        ).strip()

        if not raw_url:
            return ""

        try:
            parsed = urlparse(
                raw_url
            )
        except ValueError:
            return ""

        return (
            parsed.hostname
            or ""
        ).strip().lower()

    @staticmethod
    def _validate_url(
        url: str,
    ) -> str:
        normalized = str(
            url or ""
        ).strip()

        if not normalized:
            raise NetworkError(
                "HTTP URL boş olamaz."
            )

        try:
            parsed = urlparse(
                normalized
            )
        except ValueError as exc:
            raise NetworkError(
                f"Geçersiz URL: {url!r}"
            ) from exc

        if parsed.scheme.lower() not in {
            "http",
            "https",
        }:
            raise NetworkError(
                "HttpClient yalnız http:// "
                "ve https:// URL kabul eder."
            )

        if not parsed.hostname:
            raise NetworkError(
                f"URL hostname içermiyor: "
                f"{url!r}"
            )

        return normalized

    def _run_stop_check(
        self,
    ) -> None:
        callback = self.stop_check

        if callback is not None:
            callback()

    def _select_proxy(
        self,
    ) -> Optional[dict[str, str]]:
        if not self.proxies:
            return None

        proxy = random.choice(
            self.proxies
        )

        if "://" not in proxy:
            proxy = (
                f"http://{proxy}"
            )

        return {
            "http": proxy,
            "https": proxy,
        }

    def _request_headers(
        self,
        headers: Optional[
            Mapping[str, str]
        ],
    ) -> dict[str, str]:
        effective_headers = {
            str(key): str(value)
            for key, value
            in dict(
                headers or {}
            ).items()
        }

        effective_headers.setdefault(
            "User-Agent",
            random.choice(
                self.user_agents
            ),
        )

        return effective_headers

    # =========================================================================
    # REQUEST
    # =========================================================================
    def request(
        self,
        method: str,
        url: str,
        *,
        timeout_seconds: Optional[
            float
        ] = None,
        headers: Optional[
            Mapping[str, str]
        ] = None,
        verify: bool = True,
        allow_redirects: bool = True,
        raise_for_status: bool = True,
        **kwargs: Any,
    ) -> requests.Response:
        """
        Merkezi HTTP request metodu.

        Transport retry henüz burada uygulanmaz.

        Retry davranışı sonraki ``SessionManager`` component'i tarafından
        requests adapter seviyesinde sağlanacaktır.
        """

        if self._closed:
            raise NetworkError(
                "Kapalı HttpClient "
                "kullanılamaz."
            )

        self._run_stop_check()

        normalized_method = str(
            method or ""
        ).strip().upper()

        if not normalized_method:
            raise NetworkError(
                "HTTP method boş olamaz."
            )

        normalized_url = (
            self._validate_url(url)
        )

        if (
            verify is False
            and not self.allow_insecure_tls
        ):
            raise NetworkError(
                "TLS certificate verification "
                "kapatılamaz. "
                "allow_insecure_tls=True açıkça "
                "verilmedikçe verify=False "
                "reddedilir."
            )

        domain = self.domain_from_url(
            normalized_url
        )

        if (
            domain
            and not self.circuit_breaker.allow_request(
                domain
            )
        ):
            snapshot = (
                self.circuit_breaker
                .snapshot()
                .get(
                    domain,
                    {},
                )
            )

            retry_after = float(
                snapshot.get(
                    "retry_after_seconds",
                    0.0,
                )
                or 0.0
            )

            raise CircuitBreakerOpenError(
                "Circuit breaker is open "
                f"| domain={domain} "
                f"| retry_after_seconds="
                f"{retry_after:.3f}"
            )

        timeout = (
            self.timeout_seconds
            if timeout_seconds is None
            else max(
                0.1,
                float(
                    timeout_seconds
                ),
            )
        )

        effective_headers = (
            self._request_headers(
                headers
            )
        )

        explicit_proxies = kwargs.pop(
            "proxies",
            None,
        )

        proxies = (
            explicit_proxies
            if explicit_proxies is not None
            else self._select_proxy()
        )

        try:
            response = (
                self.session.request(
                    method=normalized_method,
                    url=normalized_url,
                    timeout=timeout,
                    headers=(
                        effective_headers
                    ),
                    proxies=proxies,
                    verify=verify,
                    allow_redirects=(
                        allow_redirects
                    ),
                    **kwargs,
                )
            )

        except requests.Timeout as exc:
            error = RetryableNetworkError(
                "HTTP request timed out "
                f"| method={normalized_method} "
                f"| url={normalized_url}"
            )

            self.circuit_breaker.record_failure(
                domain,
                error,
            )

            raise error from exc

        except requests.ConnectionError as exc:
            error = RetryableNetworkError(
                "HTTP connection failed "
                f"| method={normalized_method} "
                f"| url={normalized_url}"
            )

            self.circuit_breaker.record_failure(
                domain,
                error,
            )

            raise error from exc

        except requests.RequestException as exc:
            error = NetworkError(
                "HTTP request failed "
                f"| method={normalized_method} "
                f"| url={normalized_url} "
                f"| error={exc}"
            )

            raise error from exc

        except Exception as exc:
            error = NetworkError(
                "Unexpected HTTP transport failure "
                f"| method={normalized_method} "
                f"| url={normalized_url} "
                f"| error_type="
                f"{exc.__class__.__name__}"
            )

            raise error from exc

        self._run_stop_check()

        status_code = int(
            response.status_code
        )

        if (
            status_code
            in RETRYABLE_HTTP_STATUSES
        ):
            error = RetryableNetworkError(
                "Retryable HTTP response "
                f"| status={status_code} "
                f"| method={normalized_method} "
                f"| url={normalized_url}"
            )

            self.circuit_breaker.record_failure(
                domain,
                error,
            )

            if raise_for_status:
                raise error

            return response

        if status_code >= 400:
            # 4xx gibi caller/source hataları circuit breaker'ı açmaz.
            # Aynı domain'in tamamen kapatılması yerine hatayı çağırana
            # açıkça bildiririz.
            if raise_for_status:
                raise NetworkError(
                    "HTTP response failed "
                    f"| status={status_code} "
                    f"| method={normalized_method} "
                    f"| url={normalized_url}"
                )

            return response

        self.circuit_breaker.record_success(
            domain
        )

        return response

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================
    def get(
        self,
        url: str,
        **kwargs: Any,
    ) -> requests.Response:
        return self.request(
            "GET",
            url,
            **kwargs,
        )

    def post(
        self,
        url: str,
        **kwargs: Any,
    ) -> requests.Response:
        return self.request(
            "POST",
            url,
            **kwargs,
        )

    def head(
        self,
        url: str,
        **kwargs: Any,
    ) -> requests.Response:
        return self.request(
            "HEAD",
            url,
            **kwargs,
        )

    # =========================================================================
    # DIAGNOSTICS
    # =========================================================================
    def snapshot(
        self,
    ) -> dict[str, Any]:
        """
        Monitoring katmanı için dependency-free runtime görüntüsü.
        """

        return {
            "closed": self._closed,
            "timeout_seconds": (
                self.timeout_seconds
            ),
            "proxy_count": len(
                self.proxies
            ),
            "user_agent_count": len(
                self.user_agents
            ),
            "allow_insecure_tls": (
                self.allow_insecure_tls
            ),
            "owns_session": (
                self._owns_session
            ),
            "circuit_breaker": (
                self.circuit_breaker.snapshot()
            ),
        }

    # =========================================================================
    # CLEANUP
    # =========================================================================
    def close(
        self,
    ) -> None:
        """
        Yalnız HttpClient'ın kendisinin oluşturduğu session'ı kapatır.

        External session'ın lifecycle'ı onu oluşturan component'e aittir.
        """

        with self._close_lock:
            if self._closed:
                return

            self._closed = True

            if not self._owns_session:
                return

            try:
                self.session.close()

            except Exception as exc:
                raise NetworkError(
                    "HTTP session kapatılamadı."
                ) from exc

    def __enter__(
        self,
    ) -> "HttpClient":
        if self._closed:
            raise NetworkError(
                "Kapalı HttpClient context "
                "manager olarak kullanılamaz."
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
    def __repr__(
        self,
    ) -> str:
        return (
            f"{self.__class__.__name__}("
            f"timeout_seconds="
            f"{self.timeout_seconds!r}, "
            f"proxy_count="
            f"{len(self.proxies)}, "
            f"closed={self._closed!r}"
            f")"
        )