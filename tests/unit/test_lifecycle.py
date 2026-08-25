from __future__ import annotations

from typing import Any, Optional

import pytest

from enterprise_crawler.contracts import ExecutionResult
from enterprise_crawler.contracts.enums import ExecutionStatus
from enterprise_crawler.core.lifecycle import LifecycleRunner
from enterprise_crawler.exceptions import ShutdownRequested


# =============================================================================
# TEST DOUBLE
# =============================================================================
class LifecycleTestBot:
    """
    LifecycleRunner'ı BaseBot'tan bağımsız test etmek için minimal bot double'ı.
    """

    def __init__(self) -> None:
        self.bot_name = "LifecycleTestBot"

        self.calls: list[str] = []

        self.stop_requested = False

        self.execute_result: Any = ExecutionResult(
            status=ExecutionStatus.COMPLETED,
        )

        self.execute_error: Optional[BaseException] = None
        self.cleanup_error: Optional[BaseException] = None

        self.after_result: Optional[ExecutionResult] = None

        self.begin_called = 0
        self.finish_called = 0
        self.release_called = 0

    # -------------------------------------------------------------------------
    # LIFECYCLE
    # -------------------------------------------------------------------------
    def raise_if_stopping(self) -> None:
        self.calls.append("raise_if_stopping")

        if self.stop_requested:
            raise ShutdownRequested(
                "test shutdown requested"
            )

    def initialize(self) -> None:
        self.calls.append("initialize")

    def before_run(self) -> None:
        self.calls.append("before_run")

    def execute(self) -> Any:
        self.calls.append("execute")

        if self.execute_error is not None:
            raise self.execute_error

        return self.execute_result

    def after_run(
        self,
        result: ExecutionResult,
    ) -> Optional[ExecutionResult]:
        self.calls.append("after_run")

        return self.after_result

    def cleanup(self) -> None:
        self.calls.append("cleanup")

        if self.cleanup_error is not None:
            raise self.cleanup_error

    # -------------------------------------------------------------------------
    # RESULT NORMALIZATION
    # -------------------------------------------------------------------------
    def _normalize_execution_result(
        self,
        result: Any,
    ) -> ExecutionResult:
        self.calls.append("normalize")

        if isinstance(result, ExecutionResult):
            return result

        if result is None:
            return ExecutionResult(
                status=ExecutionStatus.COMPLETED,
            )

        if isinstance(result, dict):
            return ExecutionResult(
                status=ExecutionStatus(
                    str(
                        result.get(
                            "status",
                            "completed",
                        )
                    ).lower()
                ),
                records_processed=int(
                    result.get(
                        "records_processed",
                        0,
                    )
                ),
                errors=int(
                    result.get(
                        "errors",
                        0,
                    )
                ),
                warnings=int(
                    result.get(
                        "warnings",
                        0,
                    )
                ),
                metadata=dict(
                    result.get("metadata") or {}
                ),
            )

        raise TypeError(
            "unsupported execution result"
        )

    # -------------------------------------------------------------------------
    # RESULT BUILDERS
    # -------------------------------------------------------------------------
    def _cancelled_result(
        self,
        error: BaseException,
    ) -> ExecutionResult:
        self.calls.append("cancelled_result")

        return ExecutionResult(
            status=ExecutionStatus.CANCELLED,
            metadata={
                "cancellation": {
                    "exception_type": (
                        error.__class__.__name__
                    ),
                    "message": str(error),
                }
            },
        )

    def _failed_result(
        self,
        error: BaseException,
    ) -> ExecutionResult:
        self.calls.append("failed_result")

        return ExecutionResult(
            status=ExecutionStatus.FAILED,
            errors=1,
            metadata={
                "failure": {
                    "exception_type": (
                        error.__class__.__name__
                    ),
                    "message": str(error),
                }
            },
        )

    # -------------------------------------------------------------------------
    # RUN STATE
    # -------------------------------------------------------------------------
    def _begin_run(self) -> float:
        self.calls.append("begin_run")

        self.begin_called += 1

        return 100.0

    def _finish_run_state(
        self,
        result: ExecutionResult,
        *,
        started_monotonic: float,
    ) -> ExecutionResult:
        self.calls.append("finish_run_state")

        self.finish_called += 1

        result.metadata = {
            **dict(result.metadata or {}),
            "test_runtime": {
                "started_monotonic": (
                    started_monotonic
                ),
            },
        }

        return result

    def _release_run_lock(self) -> None:
        self.calls.append("release_run_lock")

        self.release_called += 1


# =============================================================================
# NORMAL EXECUTION
# =============================================================================
def test_lifecycle_runner_executes_hooks_in_order() -> None:
    bot = LifecycleTestBot()

    runner = LifecycleRunner()

    result = runner.run(bot)

    assert result.status is ExecutionStatus.COMPLETED

    assert bot.calls == [
        "begin_run",
        "raise_if_stopping",
        "initialize",
        "raise_if_stopping",
        "before_run",
        "raise_if_stopping",
        "execute",
        "normalize",
        "after_run",
        "cleanup",
        "finish_run_state",
        "release_run_lock",
    ]

    assert bot.begin_called == 1
    assert bot.finish_called == 1
    assert bot.release_called == 1


def test_after_run_can_replace_execution_result() -> None:
    bot = LifecycleTestBot()

    bot.after_result = ExecutionResult(
        status=ExecutionStatus.DEGRADED,
        records_processed=5,
        errors=1,
        warnings=2,
        metadata={
            "source": "after-run",
        },
    )

    result = LifecycleRunner().run(bot)

    assert result.status is ExecutionStatus.DEGRADED

    assert result.records_processed == 5
    assert result.errors == 1
    assert result.warnings == 2

    assert result.metadata["source"] == "after-run"

    assert bot.calls.count("normalize") == 2


# =============================================================================
# FAILURE
# =============================================================================
def test_execution_exception_becomes_failed_result() -> None:
    bot = LifecycleTestBot()

    bot.execute_error = RuntimeError(
        "execution exploded"
    )

    result = LifecycleRunner().run(bot)

    assert result.status is ExecutionStatus.FAILED
    assert result.errors == 1

    assert (
        result.metadata["failure"]["exception_type"]
        == "RuntimeError"
    )

    assert (
        result.metadata["failure"]["message"]
        == "execution exploded"
    )

    assert "cleanup" in bot.calls

    assert bot.finish_called == 1
    assert bot.release_called == 1


def test_cleanup_failure_degrades_successful_result() -> None:
    bot = LifecycleTestBot()

    bot.cleanup_error = RuntimeError(
        "cleanup exploded"
    )

    result = LifecycleRunner().run(bot)

    assert result.status is ExecutionStatus.DEGRADED
    assert result.errors == 1

    assert (
        result.metadata["cleanup_failure"][
            "exception_type"
        ]
        == "RuntimeError"
    )

    assert (
        result.metadata["cleanup_failure"][
            "message"
        ]
        == "cleanup exploded"
    )

    assert bot.release_called == 1


def test_cleanup_failure_does_not_mask_execution_failure() -> None:
    bot = LifecycleTestBot()

    bot.execute_error = ValueError(
        "primary execution failure"
    )

    bot.cleanup_error = RuntimeError(
        "secondary cleanup failure"
    )

    result = LifecycleRunner().run(bot)

    assert result.status is ExecutionStatus.FAILED

    assert result.errors == 1

    assert (
        result.metadata["failure"]["exception_type"]
        == "ValueError"
    )

    assert (
        result.metadata["failure"]["message"]
        == "primary execution failure"
    )

    assert (
        result.metadata["cleanup_failure"][
            "exception_type"
        ]
        == "RuntimeError"
    )

    assert (
        result.metadata["cleanup_failure"][
            "message"
        ]
        == "secondary cleanup failure"
    )

    assert bot.release_called == 1


# =============================================================================
# CANCELLATION
# =============================================================================
def test_shutdown_before_initialize_returns_cancelled() -> None:
    bot = LifecycleTestBot()

    bot.stop_requested = True

    result = LifecycleRunner().run(bot)

    assert result.status is ExecutionStatus.CANCELLED

    assert (
        result.metadata["cancellation"][
            "exception_type"
        ]
        == "ShutdownRequested"
    )

    assert "initialize" not in bot.calls
    assert "execute" not in bot.calls

    assert "cleanup" in bot.calls

    assert bot.finish_called == 1
    assert bot.release_called == 1


class StopDuringInitializeBot(LifecycleTestBot):
    def initialize(self) -> None:
        super().initialize()

        self.stop_requested = True


def test_shutdown_between_hooks_prevents_execute() -> None:
    bot = StopDuringInitializeBot()

    result = LifecycleRunner().run(bot)

    assert result.status is ExecutionStatus.CANCELLED

    assert "initialize" in bot.calls
    assert "before_run" not in bot.calls
    assert "execute" not in bot.calls

    assert "cleanup" in bot.calls

    assert bot.release_called == 1


# =============================================================================
# CONTRACT VALIDATION
# =============================================================================
def test_lifecycle_runner_rejects_none_bot() -> None:
    runner = LifecycleRunner()

    with pytest.raises(
        TypeError,
        match="bot=None",
    ):
        runner.run(None)  # type: ignore[arg-type]


def test_lifecycle_runner_rejects_incomplete_bot_contract() -> None:
    class InvalidBot:
        bot_name = "InvalidBot"

        def execute(self):
            return None

    runner = LifecycleRunner()

    with pytest.raises(
        TypeError,
        match="Bot lifecycle contract eksik",
    ):
        runner.run(InvalidBot())  # type: ignore[arg-type]


# =============================================================================
# LOCK / FINALIZATION GUARANTEES
# =============================================================================
class FinishStateFailingBot(LifecycleTestBot):
    def _finish_run_state(
        self,
        result: ExecutionResult,
        *,
        started_monotonic: float,
    ) -> ExecutionResult:
        self.calls.append("finish_run_state")

        self.finish_called += 1

        raise RuntimeError(
            "finish state failure"
        )


def test_run_lock_is_released_even_if_finish_state_fails() -> None:
    bot = FinishStateFailingBot()

    runner = LifecycleRunner()

    with pytest.raises(
        RuntimeError,
        match="finish state failure",
    ):
        runner.run(bot)

    assert bot.finish_called == 1
    assert bot.release_called == 1

    assert (
        bot.calls[-1]
        == "release_run_lock"
    )