from __future__ import annotations

from typing import Any

import pytest
import requests
from requests.adapters import HTTPAdapter

from enterprise_crawler.core.session import (
    DEFAULT_ACCEPT_HEADER,
    DEFAULT_ACCEPT_LANGUAGE,
    SessionConfig,
    SessionManager,
)
from enterprise_crawler.exceptions import NetworkError


# =============================================================================
# TEST DOUBLE
# =============================================================================
class FakeSession(requests.Session):
    def __init__(self) -> None:
        super().__init__()

        self.close_called = 0

    def close(self) -> None:
        self.close_called += 1

        super().close()


# =============================================================================
# CONFIG
# =============================================================================
def test_default_session_config_is_valid() -> None:
    config = SessionConfig()

    assert config.max_retries == 3
    assert config.backoff_factor == 0.75

    assert config.pool_connections == 10
    assert config.pool_maxsize == 10

    assert 429 in config.retry_statuses
    assert 500 in config.retry_statuses

    assert "GET" in config.allowed_methods
    assert "POST" in config.allowed_methods


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("max_retries", -1),
        ("backoff_factor", -0.1),
        ("pool_connections", 0),
        ("pool_maxsize", 0),
    ],
)
def test_invalid_numeric_config_is_rejected(
    field_name: str,
    value: Any,
) -> None:
    kwargs = {
        field_name: value,
    }

    with pytest.raises(ValueError):
        SessionConfig(**kwargs)


def test_empty_allowed_methods_is_rejected() -> None:
    with pytest.raises(
        ValueError,
        match="allowed_methods",
    ):
        SessionConfig(
            allowed_methods=frozenset(),
        )


@pytest.mark.parametrize(
    "status",
    [
        99,
        600,
        True,
        "500",
    ],
)
def test_invalid_retry_status_is_rejected(
    status: Any,
) -> None:
    with pytest.raises(ValueError):
        SessionConfig(
            retry_statuses=(status,),
        )


# =============================================================================
# SESSION CREATION
# =============================================================================
def test_manager_creates_requests_session() -> None:
    manager = SessionManager()

    assert isinstance(
        manager.session,
        requests.Session,
    )

    assert manager.owns_session is True

    manager.close()


def test_default_headers_are_applied() -> None:
    manager = SessionManager()

    session = manager.session

    assert (
        session.headers["Accept"]
        == DEFAULT_ACCEPT_HEADER
    )

    assert (
        session.headers["Accept-Language"]
        == DEFAULT_ACCEPT_LANGUAGE
    )

    manager.close()


def test_custom_headers_override_defaults() -> None:
    manager = SessionManager(
        headers={
            "Accept": "application/json",
            "X-Test": "yes",
        }
    )

    session = manager.session

    assert (
        session.headers["Accept"]
        == "application/json"
    )

    assert session.headers["X-Test"] == "yes"

    assert (
        session.headers["Accept-Language"]
        == DEFAULT_ACCEPT_LANGUAGE
    )

    manager.close()


# =============================================================================
# ADAPTER
# =============================================================================
def test_http_and_https_adapters_are_mounted() -> None:
    manager = SessionManager()

    session = manager.session

    http_adapter = session.get_adapter(
        "http://example.com"
    )

    https_adapter = session.get_adapter(
        "https://example.com"
    )

    assert isinstance(
        http_adapter,
        HTTPAdapter,
    )

    assert isinstance(
        https_adapter,
        HTTPAdapter,
    )

    manager.close()


def test_adapter_uses_configured_retry_policy() -> None:
    config = SessionConfig(
        max_retries=5,
        backoff_factor=1.25,
    )

    manager = SessionManager(
        config=config,
    )

    adapter = manager.session.get_adapter(
        "https://example.com"
    )

    retry = adapter.max_retries

    assert retry.total == 5
    assert retry.connect == 5
    assert retry.read == 5
    assert retry.status == 5

    assert retry.backoff_factor == 1.25

    assert 429 in retry.status_forcelist
    assert 503 in retry.status_forcelist

    assert "GET" in retry.allowed_methods

    manager.close()


def test_adapter_uses_configured_pool_sizes() -> None:
    manager = SessionManager(
        config=SessionConfig(
            pool_connections=7,
            pool_maxsize=23,
        )
    )

    adapter = manager.session.get_adapter(
        "https://example.com"
    )

    assert (
        adapter._pool_connections
        == 7
    )

    assert (
        adapter._pool_maxsize
        == 23
    )

    manager.close()


# =============================================================================
# EXTERNAL SESSION
# =============================================================================
def test_external_session_is_reused() -> None:
    session = FakeSession()

    manager = SessionManager(
        session=session,
    )

    assert manager.session is session

    assert manager.owns_session is False

    manager.close()

    assert session.close_called == 0


def test_headers_are_applied_to_external_session() -> None:
    session = FakeSession()

    manager = SessionManager(
        session=session,
        headers={
            "X-Framework": "test",
        },
    )

    assert (
        session.headers["X-Framework"]
        == "test"
    )

    manager.close()


# =============================================================================
# CLOSE
# =============================================================================
def test_closed_manager_rejects_session_access() -> None:
    manager = SessionManager()

    manager.close()

    with pytest.raises(
        NetworkError,
        match="Kapalı SessionManager",
    ):
        _ = manager.session


def test_close_is_idempotent() -> None:
    manager = SessionManager()

    manager.close()
    manager.close()

    assert manager.is_closed is True


def test_context_manager_closes_owned_session() -> None:
    with SessionManager() as manager:
        assert manager.is_closed is False

    assert manager.is_closed is True


# =============================================================================
# REBUILD
# =============================================================================
def test_owned_session_can_be_rebuilt() -> None:
    manager = SessionManager()

    first = manager.session

    second = manager.rebuild()

    assert second is manager.session
    assert second is not first

    assert manager.is_closed is False

    manager.close()


def test_rebuild_preserves_existing_headers() -> None:
    manager = SessionManager(
        headers={
            "X-Test": "persist",
        }
    )

    rebuilt = manager.rebuild()

    assert (
        rebuilt.headers["X-Test"]
        == "persist"
    )

    manager.close()


def test_external_session_cannot_be_rebuilt() -> None:
    manager = SessionManager(
        session=FakeSession(),
    )

    with pytest.raises(
        NetworkError,
        match="External session",
    ):
        manager.rebuild()

    manager.close()


def test_closed_manager_cannot_be_rebuilt() -> None:
    manager = SessionManager()

    manager.close()

    with pytest.raises(
        NetworkError,
        match="Kapalı SessionManager",
    ):
        manager.rebuild()


# =============================================================================
# SNAPSHOT
# =============================================================================
def test_snapshot_exposes_transport_configuration() -> None:
    manager = SessionManager(
        config=SessionConfig(
            max_retries=6,
            backoff_factor=2.0,
            pool_connections=4,
            pool_maxsize=17,
        )
    )

    snapshot = manager.snapshot()

    assert snapshot["closed"] is False
    assert snapshot["owns_session"] is True

    assert snapshot["max_retries"] == 6
    assert snapshot["backoff_factor"] == 2.0

    assert snapshot["pool_connections"] == 4
    assert snapshot["pool_maxsize"] == 17

    assert 429 in snapshot["retry_statuses"]

    assert "GET" in snapshot["allowed_methods"]

    manager.close()