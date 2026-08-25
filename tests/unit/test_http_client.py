from __future__ import annotations

from typing import Any

import pytest
import requests

from enterprise_crawler.core.http_client import (
    DomainCircuitBreaker,
    HttpClient,
)
from enterprise_crawler.exceptions import (
    CircuitBreakerOpenError,
    NetworkError,
    RetryableNetworkError,
    ShutdownRequested,
)


# =============================================================================
# TEST DOUBLES
# =============================================================================
class FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        content: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(
                f"HTTP {self.status_code}"
            )


class FakeSession:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}

        self.calls: list[dict[str, Any]] = []

        self.response: FakeResponse = FakeResponse()

        self.error: BaseException | None = None

        self.closed = False

    def request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> FakeResponse:
        self.calls.append(
            {
                "method": method,
                "url": url,
                **kwargs,
            }
        )

        if self.error is not None:
            raise self.error

        return self.response

    def close(self) -> None:
        self.closed = True


# =============================================================================
# BASIC REQUEST BEHAVIOUR
# =============================================================================
def test_get_request_uses_expected_method_and_url() -> None:
    session = FakeSession()

    client = HttpClient(
        session=session,
    )

    response = client.get(
        "https://example.com/data"
    )

    assert response.status_code == 200

    assert len(session.calls) == 1

    call = session.calls[0]

    assert call["method"] == "GET"
    assert call["url"] == "https://example.com/data"


def test_post_request_uses_expected_method() -> None:
    session = FakeSession()

    client = HttpClient(
        session=session,
    )

    client.post(
        "https://example.com/api",
        json={
            "hello": "world",
        },
    )

    call = session.calls[0]

    assert call["method"] == "POST"

    assert call["json"] == {
        "hello": "world",
    }


def test_default_timeout_is_forwarded() -> None:
    session = FakeSession()

    client = HttpClient(
        session=session,
        timeout_seconds=12.5,
    )

    client.get(
        "https://example.com"
    )

    assert (
        session.calls[0]["timeout"]
        == 12.5
    )


def test_request_specific_timeout_overrides_default() -> None:
    session = FakeSession()

    client = HttpClient(
        session=session,
        timeout_seconds=30,
    )

    client.get(
        "https://example.com",
        timeout_seconds=4.5,
    )

    assert (
        session.calls[0]["timeout"]
        == 4.5
    )


# =============================================================================
# URL VALIDATION
# =============================================================================
@pytest.mark.parametrize(
    "url",
    [
        "",
        "example.com",
        "ftp://example.com/file",
        "file:///tmp/example",
    ],
)
def test_invalid_urls_are_rejected(
    url: str,
) -> None:
    client = HttpClient(
        session=FakeSession(),
    )

    with pytest.raises(NetworkError):
        client.get(url)


def test_domain_from_url_normalizes_hostname() -> None:
    assert (
        HttpClient.domain_from_url(
            "https://EXAMPLE.COM/path?q=1"
        )
        == "example.com"
    )


# =============================================================================
# TLS
# =============================================================================
def test_verify_false_is_rejected_by_default() -> None:
    client = HttpClient(
        session=FakeSession(),
    )

    with pytest.raises(
        NetworkError,
        match="TLS",
    ):
        client.get(
            "https://example.com",
            verify=False,
        )


def test_verify_false_is_allowed_when_explicitly_enabled() -> None:
    session = FakeSession()

    client = HttpClient(
        session=session,
        allow_insecure_tls=True,
    )

    client.get(
        "https://example.com",
        verify=False,
    )

    assert (
        session.calls[0]["verify"]
        is False
    )


# =============================================================================
# HEADERS / USER AGENT
# =============================================================================
def test_user_agent_is_added_when_missing() -> None:
    session = FakeSession()

    client = HttpClient(
        session=session,
        user_agents=[
            "EnterpriseCrawler-Test-UA",
        ],
    )

    client.get(
        "https://example.com"
    )

    headers = session.calls[0]["headers"]

    assert (
        headers["User-Agent"]
        == "EnterpriseCrawler-Test-UA"
    )


def test_explicit_user_agent_is_preserved() -> None:
    session = FakeSession()

    client = HttpClient(
        session=session,
        user_agents=[
            "Generated-UA",
        ],
    )

    client.get(
        "https://example.com",
        headers={
            "User-Agent": "Explicit-UA",
        },
    )

    headers = session.calls[0]["headers"]

    assert (
        headers["User-Agent"]
        == "Explicit-UA"
    )


# =============================================================================
# PROXY
# =============================================================================
def test_proxy_is_normalized_and_forwarded() -> None:
    session = FakeSession()

    client = HttpClient(
        session=session,
        proxies=[
            "127.0.0.1:8080",
        ],
    )

    client.get(
        "https://example.com"
    )

    assert (
        session.calls[0]["proxies"]
        == {
            "http": "http://127.0.0.1:8080",
            "https": "http://127.0.0.1:8080",
        }
    )


def test_explicit_request_proxy_overrides_client_proxy() -> None:
    session = FakeSession()

    client = HttpClient(
        session=session,
        proxies=[
            "127.0.0.1:8080",
        ],
    )

    explicit = {
        "http": "http://proxy-a:1234",
        "https": "http://proxy-a:1234",
    }

    client.get(
        "https://example.com",
        proxies=explicit,
    )

    assert (
        session.calls[0]["proxies"]
        == explicit
    )


# =============================================================================
# TRANSPORT ERRORS
# =============================================================================
def test_timeout_becomes_retryable_network_error() -> None:
    session = FakeSession()

    session.error = requests.Timeout(
        "timeout"
    )

    client = HttpClient(
        session=session,
    )

    with pytest.raises(
        RetryableNetworkError,
        match="timed out",
    ):
        client.get(
            "https://example.com"
        )


def test_connection_error_becomes_retryable_network_error() -> None:
    session = FakeSession()

    session.error = requests.ConnectionError(
        "connection refused"
    )

    client = HttpClient(
        session=session,
    )

    with pytest.raises(
        RetryableNetworkError,
        match="connection failed",
    ):
        client.get(
            "https://example.com"
        )


def test_generic_request_exception_becomes_network_error() -> None:
    session = FakeSession()

    session.error = requests.RequestException(
        "transport failure"
    )

    client = HttpClient(
        session=session,
    )

    with pytest.raises(
        NetworkError,
        match="HTTP request failed",
    ):
        client.get(
            "https://example.com"
        )


# =============================================================================
# HTTP STATUS
# =============================================================================
@pytest.mark.parametrize(
    "status_code",
    [
        408,
        425,
        429,
        500,
        502,
        503,
        504,
    ],
)
def test_retryable_http_status_raises_retryable_error(
    status_code: int,
) -> None:
    session = FakeSession()

    session.response = FakeResponse(
        status_code=status_code,
    )

    client = HttpClient(
        session=session,
    )

    with pytest.raises(
        RetryableNetworkError,
    ):
        client.get(
            "https://example.com"
        )


def test_retryable_status_can_be_returned_without_raise() -> None:
    session = FakeSession()

    session.response = FakeResponse(
        status_code=503,
    )

    client = HttpClient(
        session=session,
    )

    response = client.get(
        "https://example.com",
        raise_for_status=False,
    )

    assert response.status_code == 503


def test_non_retryable_404_raises_network_error() -> None:
    session = FakeSession()

    session.response = FakeResponse(
        status_code=404,
    )

    client = HttpClient(
        session=session,
    )

    with pytest.raises(
        NetworkError,
    ):
        client.get(
            "https://example.com/missing"
        )


def test_non_retryable_404_does_not_increment_circuit_failure() -> None:
    session = FakeSession()

    session.response = FakeResponse(
        status_code=404,
    )

    breaker = DomainCircuitBreaker(
        failure_threshold=1,
    )

    client = HttpClient(
        session=session,
        circuit_breaker=breaker,
    )

    with pytest.raises(NetworkError):
        client.get(
            "https://example.com/missing"
        )

    snapshot = breaker.snapshot()

    assert (
        snapshot.get(
            "example.com",
            {},
        ).get(
            "consecutive_failures",
            0,
        )
        == 0
    )


# =============================================================================
# CIRCUIT BREAKER
# =============================================================================
def test_circuit_breaker_opens_after_threshold() -> None:
    session = FakeSession()

    session.error = requests.Timeout(
        "timeout"
    )

    breaker = DomainCircuitBreaker(
        failure_threshold=2,
        recovery_timeout_seconds=60,
    )

    client = HttpClient(
        session=session,
        circuit_breaker=breaker,
    )

    for _ in range(2):
        with pytest.raises(
            RetryableNetworkError,
        ):
            client.get(
                "https://example.com"
            )

    snapshot = breaker.snapshot()

    assert (
        snapshot["example.com"][
            "consecutive_failures"
        ]
        == 2
    )

    assert (
        snapshot["example.com"][
            "is_open"
        ]
        is True
    )


def test_open_circuit_blocks_request_before_session_call() -> None:
    session = FakeSession()

    breaker = DomainCircuitBreaker(
        failure_threshold=1,
        recovery_timeout_seconds=60,
    )

    breaker.record_failure(
        "example.com",
        RuntimeError("failure"),
    )

    client = HttpClient(
        session=session,
        circuit_breaker=breaker,
    )

    with pytest.raises(
        CircuitBreakerOpenError,
    ):
        client.get(
            "https://example.com"
        )

    assert session.calls == []


def test_success_resets_circuit_failure_count() -> None:
    session = FakeSession()

    breaker = DomainCircuitBreaker(
        failure_threshold=3,
    )

    breaker.record_failure(
        "example.com",
        RuntimeError("failure"),
    )

    client = HttpClient(
        session=session,
        circuit_breaker=breaker,
    )

    client.get(
        "https://example.com"
    )

    snapshot = breaker.snapshot()

    assert (
        snapshot["example.com"][
            "consecutive_failures"
        ]
        == 0
    )

    assert (
        snapshot["example.com"][
            "is_open"
        ]
        is False
    )


# =============================================================================
# STOP CHECK
# =============================================================================
def test_stop_check_runs_before_request() -> None:
    session = FakeSession()

    calls: list[str] = []

    def stop_check() -> None:
        calls.append("stop")

    client = HttpClient(
        session=session,
        stop_check=stop_check,
    )

    client.get(
        "https://example.com"
    )

    assert calls == [
        "stop",
        "stop",
    ]


def test_stop_check_can_cancel_before_transport() -> None:
    session = FakeSession()

    def stop_check() -> None:
        raise ShutdownRequested(
            "stop requested"
        )

    client = HttpClient(
        session=session,
        stop_check=stop_check,
    )

    with pytest.raises(
        ShutdownRequested,
    ):
        client.get(
            "https://example.com"
        )

    assert session.calls == []


# =============================================================================
# SESSION OWNERSHIP / CLEANUP
# =============================================================================
def test_external_session_is_not_closed_by_client() -> None:
    session = FakeSession()

    client = HttpClient(
        session=session,
    )

    client.close()

    assert session.closed is False

    assert (
        client.snapshot()[
            "owns_session"
        ]
        is False
    )


def test_close_is_idempotent() -> None:
    client = HttpClient(
        session=FakeSession(),
    )

    client.close()
    client.close()

    assert (
        client.snapshot()["closed"]
        is True
    )


def test_closed_client_rejects_requests() -> None:
    client = HttpClient(
        session=FakeSession(),
    )

    client.close()

    with pytest.raises(
        NetworkError,
        match="Kapalı HttpClient",
    ):
        client.get(
            "https://example.com"
        )


# =============================================================================
# SNAPSHOT
# =============================================================================
def test_http_client_snapshot_contains_runtime_configuration() -> None:
    client = HttpClient(
        session=FakeSession(),
        timeout_seconds=15,
        proxies=[
            "proxy.example:8080",
        ],
        user_agents=[
            "UA-1",
            "UA-2",
        ],
    )

    snapshot = client.snapshot()

    assert snapshot["closed"] is False
    assert snapshot["timeout_seconds"] == 15.0
    assert snapshot["proxy_count"] == 1
    assert snapshot["user_agent_count"] == 2
    assert snapshot["owns_session"] is False