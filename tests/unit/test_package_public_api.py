from __future__ import annotations

import enterprise_crawler

from enterprise_crawler import (
    BaseBot,
    Crawler,
    ExecutionResult,
    ExecutionStatus,
)


# =============================================================================
# TOP-LEVEL PUBLIC API
# =============================================================================
def test_top_level_exports_core_runtime() -> None:
    assert (
        enterprise_crawler.BaseBot
        is BaseBot
    )

    assert (
        enterprise_crawler.Crawler
        is Crawler
    )


def test_top_level_exports_execution_contract() -> None:
    assert (
        enterprise_crawler.ExecutionResult
        is ExecutionResult
    )

    assert (
        enterprise_crawler.ExecutionStatus
        is ExecutionStatus
    )


def test_top_level_exports_version_metadata() -> None:
    assert isinstance(
        enterprise_crawler.__version__,
        str,
    )

    assert (
        enterprise_crawler.__version__.strip()
    )

    assert isinstance(
        enterprise_crawler.__title__,
        str,
    )

    assert (
        enterprise_crawler.__title__.strip()
    )

    assert isinstance(
        enterprise_crawler.FRAMEWORK_NAME,
        str,
    )

    assert (
        enterprise_crawler.FRAMEWORK_NAME.strip()
    )


def test_top_level_all_matches_supported_surface() -> None:
    assert (
        enterprise_crawler.__all__
        == [
            "__version__",
            "__title__",
            "FRAMEWORK_NAME",
            "BaseBot",
            "Crawler",
            "ExecutionResult",
            "ExecutionStatus",
        ]
    )


# =============================================================================
# REAL PUBLIC WORKFLOW
# =============================================================================
class HelloBot(
    BaseBot
):
    def execute(
        self,
    ) -> None:
        self.mark_record_processed()


def test_top_level_public_api_can_run_real_bot() -> None:
    with HelloBot(
        bot_name="public-api-test"
    ) as bot:
        crawler = Crawler(
            bot
        )

        result = (
            crawler.run()
        )

    assert isinstance(
        result,
        ExecutionResult,
    )

    assert (
        result.status
        is ExecutionStatus.COMPLETED
    )

    assert (
        result.records_processed
        == 1
    )

    assert (
        result.errors
        == 0
    )

    assert (
        crawler.last_result
        is result
    )