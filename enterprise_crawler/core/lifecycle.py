from __future__ import annotations

"""
Enterprise Crawler Framework - Lifecycle Runtime

Bu modül bot yaşam döngüsünün orkestrasyonundan sorumludur.

Akış
----
    initialize()
        ↓
    before_run()
        ↓
    execute()
        ↓
    after_run()
        ↓
    cleanup()

LifecycleRunner bot iş mantığını bilmez. HTTP, storage, scheduler, event,
plugin veya monitoring sorumluluğu taşımaz.

BaseBot dışarıdan aynı ``run()`` API'sini korur; fakat lifecycle yürütmesini
bu bileşene devreder.
"""

from dataclasses import dataclass
from typing import Any, Optional, Protocol, runtime_checkable

from enterprise_crawler.contracts import ExecutionResult
from enterprise_crawler.contracts.enums import ExecutionStatus
from enterprise_crawler.exceptions import ShutdownRequested


# =============================================================================
# BOT CONTRACT
# =============================================================================
@runtime_checkable
class LifecycleBot(Protocol):
    """
    LifecycleRunner'ın ihtiyaç duyduğu minimum bot sözleşmesi.

    BaseBot import edilmez. Böylece circular import oluşmaz.
    """

    bot_name: str

    def raise_if_stopping(self) -> None:
        ...

    def initialize(self) -> None:
        ...

    def before_run(self) -> None:
        ...

    def execute(self) -> Any:
        ...

    def after_run(
        self,
        result: ExecutionResult,
    ) -> Optional[ExecutionResult]:
        ...

    def cleanup(self) -> None:
        ...

    def _normalize_execution_result(
        self,
        result: Any,
    ) -> ExecutionResult:
        ...

    def _cancelled_result(
        self,
        error: BaseException,
    ) -> ExecutionResult:
        ...

    def _failed_result(
        self,
        error: BaseException,
    ) -> ExecutionResult:
        ...

    def _begin_run(self) -> float:
        ...

    def _finish_run_state(
        self,
        result: ExecutionResult,
        *,
        started_monotonic: float,
    ) -> ExecutionResult:
        ...

    def _release_run_lock(self) -> None:
        ...


# =============================================================================
# LIFECYCLE MODELS
# =============================================================================
@dataclass(frozen=True, slots=True)
class LifecycleExecution:
    """
    Tek lifecycle çalışmasının iç diagnostic sonucu.

    Public ``ExecutionResult`` contract'ının yerine geçmez.
    """

    result: ExecutionResult
    cleanup_failed: bool = False


# =============================================================================
# LIFECYCLE RUNNER
# =============================================================================
class LifecycleRunner:
    """
    Bir bot instance'ının merkezi yaşam döngüsünü yürütür.

    Runner kendi runtime state'ini tutmaz. Aynı LifecycleRunner instance'ı
    farklı botlar için yeniden kullanılabilir.
    """

    def run(
        self,
        bot: LifecycleBot,
    ) -> ExecutionResult:
        """
        Bot lifecycle'ını tam olarak bir kez çalıştırır.

        Başarı:
            execute() sonucu normalize edilir.

        ShutdownRequested:
            CANCELLED sonucu üretilir.

        Diğer exception:
            FAILED sonucu üretilir.

        cleanup() her durumda çağrılır.

        cleanup başarısız olursa:
        * ana çalışma FAILED ise FAILED korunur,
        * aksi halde sonuç DEGRADED olur.
        """

        self._validate_bot(bot)

        started_monotonic = bot._begin_run()

        result: Optional[ExecutionResult] = None

        try:
            result = self._execute_lifecycle(bot)

        except ShutdownRequested as cancellation:
            result = bot._cancelled_result(
                cancellation
            )

        except Exception as execution_error:
            result = bot._failed_result(
                execution_error
            )

        finally:
            result = self._finalize_lifecycle(
                bot,
                result=result,
            )

            try:
                result = bot._finish_run_state(
                    result,
                    started_monotonic=started_monotonic,
                )

            finally:
                bot._release_run_lock()

        return result

    # =========================================================================
    # EXECUTION
    # =========================================================================
    @staticmethod
    def _execute_lifecycle(
        bot: LifecycleBot,
    ) -> ExecutionResult:
        bot.raise_if_stopping()

        bot.initialize()

        bot.raise_if_stopping()

        bot.before_run()

        bot.raise_if_stopping()

        raw_result = bot.execute()

        result = bot._normalize_execution_result(
            raw_result
        )

        after_result = bot.after_run(
            result
        )

        if after_result is not None:
            result = bot._normalize_execution_result(
                after_result
            )

        return result

    # =========================================================================
    # CLEANUP
    # =========================================================================
    @staticmethod
    def _finalize_lifecycle(
        bot: LifecycleBot,
        *,
        result: Optional[ExecutionResult],
    ) -> ExecutionResult:
        """
        cleanup() garantisini uygular.

        cleanup hatası ana execution hatasını maskelemez.
        """

        try:
            bot.cleanup()

        except Exception as cleanup_error:
            if result is None:
                return bot._failed_result(
                    cleanup_error
                )

            metadata = dict(
                result.metadata or {}
            )

            metadata["cleanup_failure"] = {
                "exception_type": (
                    cleanup_error.__class__.__name__
                ),
                "message": (
                    LifecycleRunner._safe_exception_message(
                        cleanup_error
                    )
                ),
            }

            result.metadata = metadata

            if result.status is ExecutionStatus.FAILED:
                return result

            result.status = ExecutionStatus.DEGRADED
            result.errors = max(
                1,
                result.errors,
            )

            return result

        if result is None:
            return bot._failed_result(
                RuntimeError(
                    "Bot lifecycle sonuç üretmeden tamamlandı."
                )
            )

        return result

    # =========================================================================
    # VALIDATION
    # =========================================================================
    @staticmethod
    def _validate_bot(
        bot: Any,
    ) -> None:
        if bot is None:
            raise TypeError(
                "LifecycleRunner bot=None kabul etmez."
            )

        required_methods = (
            "raise_if_stopping",
            "initialize",
            "before_run",
            "execute",
            "after_run",
            "cleanup",
            "_normalize_execution_result",
            "_cancelled_result",
            "_failed_result",
            "_begin_run",
            "_finish_run_state",
            "_release_run_lock",
        )

        missing = [
            method_name
            for method_name in required_methods
            if not callable(
                getattr(
                    bot,
                    method_name,
                    None,
                )
            )
        ]

        if missing:
            raise TypeError(
                "Bot lifecycle contract eksik: "
                + ", ".join(missing)
            )

    # =========================================================================
    # HELPERS
    # =========================================================================
    @staticmethod
    def _safe_exception_message(
        error: BaseException,
    ) -> str:
        message = str(error).strip()

        if not message:
            message = error.__class__.__name__

        return message[:8_000]