from __future__ import annotations

import threading

import pytest

from enterprise_crawler.contracts import ExecutionResult
from enterprise_crawler.contracts.enums import ExecutionStatus
from enterprise_crawler.core.base_bot import BaseBot
from enterprise_crawler.core.crawler import Crawler, CrawlerState
from enterprise_crawler.exceptions import AlreadyRunningError


# =============================================================================
# TEST BOTS
# =============================================================================
class HelloWorldBot(BaseBot):
    def execute(self):
        self.set_runtime_metadata("message", "hello-world")
        print("Hello World")


class ResultBot(BaseBot):
    def execute(self):
        self.mark_record_processed(3)
        self.mark_warning()

        return ExecutionResult(
            status=ExecutionStatus.COMPLETED,
            records_processed=3,
            errors=0,
            warnings=1,
            metadata={
                "source": "unit-test",
            },
        )


class MappingResultBot(BaseBot):
    def execute(self):
        return {
            "status": "completed",
            "records_processed": 2,
            "errors": 0,
            "warnings": 1,
            "metadata": {
                "mode": "mapping",
            },
        }


class FailingBot(BaseBot):
    def execute(self):
        raise RuntimeError("intentional-test-failure")


class CleanupFailingBot(BaseBot):
    def execute(self):
        return ExecutionResult(
            status=ExecutionStatus.COMPLETED,
        )

    def cleanup(self):
        raise RuntimeError("cleanup-failure")


class BlockingBot(BaseBot):
    def __init__(
        self,
        *,
        entered_event: threading.Event,
        release_event: threading.Event,
    ) -> None:
        super().__init__()

        self.entered_event = entered_event
        self.release_event = release_event

    def execute(self):
        self.entered_event.set()

        if not self.release_event.wait(timeout=5):
            raise RuntimeError(
                "BlockingBot test release signal timed out."
            )

        return ExecutionResult(
            status=ExecutionStatus.COMPLETED,
        )


class HookBot(BaseBot):
    def __init__(self) -> None:
        super().__init__()

        self.calls: list[str] = []

    def initialize(self) -> None:
        self.calls.append("initialize")

    def before_run(self) -> None:
        self.calls.append("before_run")

    def execute(self):
        self.calls.append("execute")

        return ExecutionResult(
            status=ExecutionStatus.COMPLETED,
        )

    def after_run(
        self,
        result: ExecutionResult,
    ):
        self.calls.append("after_run")

        result.metadata["after_run"] = True

        return result

    def cleanup(self) -> None:
        self.calls.append("cleanup")


# =============================================================================
# BASEBOT
# =============================================================================
def test_hello_world_bot_runs_successfully(
    capsys: pytest.CaptureFixture[str],
) -> None:
    bot = HelloWorldBot()

    result = bot.run()

    captured = capsys.readouterr()

    assert "Hello World" in captured.out

    assert isinstance(result, ExecutionResult)

    assert result.status is ExecutionStatus.COMPLETED

    assert result.records_processed == 0
    assert result.errors == 0
    assert result.warnings == 0

    assert result.metadata["runtime"]["message"] == "hello-world"

    assert result.metadata["bot"]["bot_name"] == "HelloWorldBot"
    assert result.metadata["bot"]["run_count"] == 1

    assert bot.is_running is False
    assert bot.run_count == 1
    assert bot.last_result is result


def test_execution_result_is_preserved() -> None:
    bot = ResultBot()

    result = bot.run()

    assert result.status is ExecutionStatus.COMPLETED

    assert result.records_processed == 3
    assert result.errors == 0
    assert result.warnings == 1

    assert result.metadata["source"] == "unit-test"

    assert result.metadata["bot"]["bot_name"] == "ResultBot"


def test_mapping_result_is_normalized() -> None:
    bot = MappingResultBot()

    result = bot.run()

    assert isinstance(result, ExecutionResult)

    assert result.status is ExecutionStatus.COMPLETED

    assert result.records_processed == 2
    assert result.errors == 0
    assert result.warnings == 1

    assert result.metadata["mode"] == "mapping"


def test_execute_exception_becomes_failed_result() -> None:
    bot = FailingBot()

    result = bot.run()

    assert result.status is ExecutionStatus.FAILED

    assert result.records_processed == 0
    assert result.errors == 1

    assert (
        result.metadata["failure"]["exception_type"]
        == "RuntimeError"
    )

    assert (
        result.metadata["failure"]["message"]
        == "intentional-test-failure"
    )

    assert bot.is_running is False


def test_cleanup_failure_degrades_successful_run() -> None:
    bot = CleanupFailingBot()

    result = bot.run()

    assert result.status is ExecutionStatus.DEGRADED

    assert result.errors == 1

    assert (
        result.metadata["cleanup_failure"]["exception_type"]
        == "RuntimeError"
    )

    assert (
        result.metadata["cleanup_failure"]["message"]
        == "cleanup-failure"
    )


def test_stop_requested_before_run_returns_cancelled() -> None:
    bot = HelloWorldBot()

    bot.request_stop()

    result = bot.run()

    assert result.status is ExecutionStatus.CANCELLED

    assert result.errors == 0

    assert (
        result.metadata["cancellation"]["exception_type"]
        == "ShutdownRequested"
    )

    assert bot.is_running is False


def test_stop_request_can_be_reset_between_runs() -> None:
    bot = HelloWorldBot()

    bot.request_stop()

    first_result = bot.run()

    assert first_result.status is ExecutionStatus.CANCELLED

    bot.reset_stop_request()

    second_result = bot.run()

    assert second_result.status is ExecutionStatus.COMPLETED

    assert bot.run_count == 2


def test_lifecycle_hook_order_is_deterministic() -> None:
    bot = HookBot()

    result = bot.run()

    assert result.status is ExecutionStatus.COMPLETED

    assert bot.calls == [
        "initialize",
        "before_run",
        "execute",
        "after_run",
        "cleanup",
    ]

    assert result.metadata["after_run"] is True


def test_provider_cannot_override_run() -> None:
    with pytest.raises(
        TypeError,
        match=r"run\(\) metodunu override edemez",
    ):

        class InvalidBot(BaseBot):
            def run(self):
                return None

            def execute(self):
                return None


def test_same_bot_instance_cannot_run_concurrently() -> None:
    entered = threading.Event()
    release = threading.Event()

    bot = BlockingBot(
        entered_event=entered,
        release_event=release,
    )

    result_holder: list[ExecutionResult] = []

    def first_run() -> None:
        result_holder.append(
            bot.run()
        )

    worker = threading.Thread(
        target=first_run,
        daemon=True,
    )

    worker.start()

    assert entered.wait(timeout=2)

    with pytest.raises(AlreadyRunningError):
        bot.run()

    release.set()

    worker.join(timeout=3)

    assert worker.is_alive() is False

    assert len(result_holder) == 1

    assert (
        result_holder[0].status
        is ExecutionStatus.COMPLETED
    )

    assert bot.is_running is False


# =============================================================================
# CRAWLER + BASEBOT
# =============================================================================
def test_crawler_runs_base_bot() -> None:
    bot = HelloWorldBot()

    crawler = Crawler(bot)

    result = crawler.run()

    assert isinstance(result, ExecutionResult)

    assert result.status is ExecutionStatus.COMPLETED

    assert crawler.last_result is result

    assert crawler.run_count == 1

    assert crawler.state is CrawlerState.FINISHED


def test_crawler_preserves_bot_result() -> None:
    bot = ResultBot()

    crawler = Crawler(bot)

    result = crawler.run()

    assert result.status is ExecutionStatus.COMPLETED

    assert result.records_processed == 3
    assert result.errors == 0
    assert result.warnings == 1

    assert result.metadata["source"] == "unit-test"


def test_crawler_receives_failed_bot_result() -> None:
    crawler = Crawler(
        FailingBot()
    )

    result = crawler.run()

    assert result.status is ExecutionStatus.FAILED

    assert result.errors == 1

    assert (
        result.metadata["failure"]["exception_type"]
        == "RuntimeError"
    )


def test_crawler_receives_cancelled_bot_result() -> None:
    bot = HelloWorldBot()

    bot.request_stop()

    crawler = Crawler(bot)

    result = crawler.run()

    assert result.status is ExecutionStatus.CANCELLED

    assert crawler.state is CrawlerState.FINISHED


def test_crawler_request_stop_is_forwarded_to_bot() -> None:
    bot = HelloWorldBot()

    crawler = Crawler(bot)

    crawler.request_stop()

    assert crawler.stop_requested is True
    assert bot.should_stop() is True

    result = crawler.run()

    assert result.status is ExecutionStatus.CANCELLED


def test_crawler_runtime_snapshot() -> None:
    crawler = Crawler(
        ResultBot()
    )

    result = crawler.run()

    snapshot = crawler.snapshot()

    assert result.status is ExecutionStatus.COMPLETED

    assert snapshot.state is CrawlerState.FINISHED

    assert snapshot.run_count == 1

    assert snapshot.started_at is not None
    assert snapshot.finished_at is not None

    assert snapshot.last_status is ExecutionStatus.COMPLETED

    assert snapshot.stop_requested is False


def test_same_crawler_instance_cannot_run_concurrently() -> None:
    entered = threading.Event()
    release = threading.Event()

    crawler = Crawler(
        BlockingBot(
            entered_event=entered,
            release_event=release,
        )
    )

    result_holder: list[ExecutionResult] = []

    def first_run() -> None:
        result_holder.append(
            crawler.run()
        )

    worker = threading.Thread(
        target=first_run,
        daemon=True,
    )

    worker.start()

    assert entered.wait(timeout=2)

    with pytest.raises(AlreadyRunningError):
        crawler.run()

    release.set()

    worker.join(timeout=3)

    assert worker.is_alive() is False

    assert len(result_holder) == 1

    assert (
        result_holder[0].status
        is ExecutionStatus.COMPLETED
    )

    assert crawler.state is CrawlerState.FINISHED


# =============================================================================
# RUNTIME REUSE
# =============================================================================
def test_bot_and_crawler_can_run_more_than_once_sequentially() -> None:
    bot = ResultBot()

    crawler = Crawler(bot)

    first = crawler.run()
    second = crawler.run()

    assert first.status is ExecutionStatus.COMPLETED
    assert second.status is ExecutionStatus.COMPLETED

    assert bot.run_count == 2
    assert crawler.run_count == 2

    assert crawler.state is CrawlerState.FINISHED