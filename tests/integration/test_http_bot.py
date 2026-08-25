from __future__ import annotations

from typing import Any

from enterprise_crawler.contracts import ExecutionResult
from enterprise_crawler.contracts.enums import ExecutionStatus
from enterprise_crawler.core.base_bot import BaseBot
from enterprise_crawler.core.crawler import Crawler
from enterprise_crawler.core.http_client import HttpClient
from enterprise_crawler.core.session import SessionManager


# =============================================================================
# TEST DOUBLES
# =============================================================================
class FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
    ) -> None:
        self.status_code = (
            status_code
        )

        self.headers: dict[
            str,
            str,
        ] = {
            "Content-Type": (
                "application/json"
            )
        }


class FakeSession:
    def __init__(
        self,
    ) -> None:
        self.headers: dict[
            str,
            str,
        ] = {}

        self.calls: list[
            dict[str, Any]
        ] = []

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

        return FakeResponse(
            status_code=200
        )

    def close(
        self,
    ) -> None:
        self.closed = True


# =============================================================================
# BOT
# =============================================================================
class HttpBot(
    BaseBot
):
    def execute(
        self,
    ) -> ExecutionResult:
        response = self.http.get(
            "https://example.com/api"
        )

        self.mark_record_processed()

        return ExecutionResult(
            status=(
                ExecutionStatus.COMPLETED
            ),
            records_processed=1,
            metadata={
                "http_status": (
                    response.status_code
                ),
            },
        )


# =============================================================================
# INTEGRATION
# =============================================================================
def test_crawler_basebot_http_composition() -> None:
    fake_session = (
        FakeSession()
    )

    session_manager = (
        SessionManager(
            session=fake_session,  # type: ignore[arg-type]
        )
    )

    http_client = HttpClient(
        session=(
            session_manager.session
        ),
    )

    bot = HttpBot(
        session_manager=(
            session_manager
        ),
        http_client=(
            http_client
        ),
    )

    crawler = Crawler(
        bot
    )

    result = crawler.run()

    assert (
        result.status
        is ExecutionStatus.COMPLETED
    )

    assert (
        result.records_processed
        == 1
    )

    assert (
        result.metadata[
            "http_status"
        ]
        == 200
    )

    assert len(
        fake_session.calls
    ) == 1

    call = (
        fake_session.calls[0]
    )

    assert (
        call["method"]
        == "GET"
    )

    assert (
        call["url"]
        == "https://example.com/api"
    )

    assert (
        call["timeout"]
        == 30.0
    )

    assert (
        bot.run_count
        == 1
    )

    assert (
        crawler.run_count
        == 1
    )

    # Inject edilmiş external resource'ları BaseBot kapatmamalı.
    bot.close()

    assert (
        fake_session.closed
        is False
    )

    http_client.close()
    session_manager.close()


def test_http_bot_can_run_twice_with_same_resources() -> None:
    fake_session = (
        FakeSession()
    )

    session_manager = (
        SessionManager(
            session=fake_session,  # type: ignore[arg-type]
        )
    )

    http_client = HttpClient(
        session=(
            session_manager.session
        ),
    )

    bot = HttpBot(
        session_manager=(
            session_manager
        ),
        http_client=(
            http_client
        ),
    )

    crawler = Crawler(
        bot
    )

    first = crawler.run()
    second = crawler.run()

    assert (
        first.status
        is ExecutionStatus.COMPLETED
    )

    assert (
        second.status
        is ExecutionStatus.COMPLETED
    )

    assert (
        bot.run_count
        == 2
    )

    assert (
        crawler.run_count
        == 2
    )

    assert len(
        fake_session.calls
    ) == 2

    http_client.close()
    session_manager.close()


def test_basebot_creates_http_stack_automatically() -> None:
    bot = HttpBot()

    try:
        assert isinstance(
            bot.session_manager,
            SessionManager,
        )

        assert isinstance(
            bot.http,
            HttpClient,
        )

        assert (
            bot.downloader
            is not None
        )

        snapshot = (
            bot.runtime_snapshot()
        )

        assert (
            snapshot["http"]
            is not None
        )

        assert (
            snapshot["session"]
            is not None
        )

    finally:
        bot.close()