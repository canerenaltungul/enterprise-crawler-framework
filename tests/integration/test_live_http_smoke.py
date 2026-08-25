from __future__ import annotations

import os

import pytest

from enterprise_crawler.contracts import ExecutionResult
from enterprise_crawler.contracts.enums import ExecutionStatus
from enterprise_crawler.core.base_bot import BaseBot
from enterprise_crawler.core.crawler import Crawler, CrawlerState


LIVE_TEST_ENV = "ENTERPRISE_CRAWLER_LIVE_TESTS"

LIVE_URL = "https://example.com"


def live_tests_enabled() -> bool:
    value = os.getenv(
        LIVE_TEST_ENV,
        "",
    )

    return value.strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


pytestmark = pytest.mark.skipif(
    not live_tests_enabled(),
    reason=(
        "Live HTTP tests disabled. "
        f"Set {LIVE_TEST_ENV}=1 to enable."
    ),
)


class LiveHttpBot(BaseBot):
    """
    Enterprise Crawler Framework gerçek HTTPS smoke botu.

    Amaç provider-specific veri toplamak değil;
    Crawler → BaseBot → HttpClient → SessionManager zincirinin
    gerçek ağ üzerinde çalıştığını doğrulamaktır.
    """

    def execute(self) -> ExecutionResult:
        response = self.http.get(
            LIVE_URL,
            timeout_seconds=15.0,
        )

        body = response.content

        if not body:
            raise RuntimeError(
                "Live endpoint boş response body döndürdü."
            )

        self.mark_record_processed()

        return ExecutionResult(
            status=ExecutionStatus.COMPLETED,
            records_processed=1,
            errors=0,
            warnings=0,
            metadata={
                "live_http": {
                    "url": LIVE_URL,
                    "status_code": response.status_code,
                    "body_size_bytes": len(body),
                    "content_type": response.headers.get(
                        "Content-Type"
                    ),
                }
            },
        )


def test_live_http_bot_end_to_end() -> None:
    bot = LiveHttpBot(
        request_timeout_seconds=15.0,
    )

    crawler = Crawler(
        bot
    )

    try:
        result = crawler.run()

        assert isinstance(
            result,
            ExecutionResult,
        )

        assert (
            result.status
            is ExecutionStatus.COMPLETED
        )

        assert result.records_processed == 1
        assert result.errors == 0

        live_metadata = (
            result.metadata["live_http"]
        )

        assert (
            live_metadata["url"]
            == LIVE_URL
        )

        assert (
            live_metadata["status_code"]
            == 200
        )

        assert (
            live_metadata["body_size_bytes"]
            > 0
        )

        assert (
            crawler.state
            is CrawlerState.FINISHED
        )

        assert crawler.run_count == 1
        assert bot.run_count == 1

    finally:
        bot.close()


def test_live_http_bot_can_reuse_session() -> None:
    """
    Aynı bot ve aynı HTTP stack ardışık iki gerçek request'te
    tekrar kullanılabiliyor mu?
    """

    bot = LiveHttpBot(
        request_timeout_seconds=15.0,
    )

    crawler = Crawler(
        bot
    )

    try:
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

        assert first.records_processed == 1
        assert second.records_processed == 1

        assert bot.run_count == 2
        assert crawler.run_count == 2

        assert (
            crawler.state
            is CrawlerState.FINISHED
        )

    finally:
        bot.close()