from __future__ import annotations

"""
Enterprise Crawler Framework - Crawler Runtime

``Crawler`` framework'ün en üst seviye tek-bot çalıştırma facade'ıdır.

Sorumlulukları
--------------
* Çalıştırılabilir bir bot instance'ını kabul etmek.
* Aynı Crawler instance'ının eşzamanlı ikinci kez çalıştırılmasını engellemek.
* Bot'un ``run()`` metodunu çağırmak.
* Dönen değerin framework ``ExecutionResult`` sözleşmesine uymasını sağlamak.
* Beklenmeyen bot hatalarını standart FAILED sonucuna çevirmek.
* Shutdown/cancellation durumlarını standart CANCELLED sonucuna çevirmek.
* Son çalışma sonucunu ve temel runtime durumunu saklamak.

Bilerek içermez
---------------
* Bot lifecycle implementasyonu.
* HTTP istemcisi.
* Retry / circuit breaker.
* Storage.
* Scheduler.
* Event queue.
* Plugin yönetimi.
* Monitoring / metrics backend.
* Orchestrator.

Bu sorumluluklar ayrı modüllerde kalır.

Temel kullanım
--------------
    bot = MyBot()
    crawler = Crawler(bot)
    result = crawler.run()
"""

import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Optional, Protocol, runtime_checkable

from enterprise_crawler.contracts import ExecutionResult
from enterprise_crawler.contracts.enums import ExecutionStatus
from enterprise_crawler.exceptions import (
    AlreadyRunningError,
    ContractValidationError,
    EnterpriseCrawlerError,
    ShutdownRequested,
)


UTC = timezone.utc


# =============================================================================
# RUNTIME CONTRACTS
# =============================================================================
@runtime_checkable
class RunnableBot(Protocol):
    """
    Crawler'ın çalıştırabileceği minimum bot sözleşmesi.

    Crawler özellikle BaseBot import etmez. Bu sayede:

        core/crawler.py
            ↓
        protocol

    ilişkisi korunur ve BaseBot ile circular import oluşmaz.

    Framework BaseBot bu protokolü doğal olarak sağlayacaktır.
    """

    def run(self) -> ExecutionResult:
        ...


class CrawlerState(str, Enum):
    """
    Crawler facade'ın kendi runtime durumu.

    Bu enum bot'un ExecutionStatus değerinden ayrıdır.

    Örneğin:
        CrawlerState.IDLE
            Crawler henüz çalışmıyor.

        ExecutionStatus.COMPLETED
            Son bot çalışması başarıyla tamamlandı.
    """

    IDLE = "idle"
    RUNNING = "running"
    STOPPING = "stopping"
    FINISHED = "finished"


@dataclass(frozen=True, slots=True)
class CrawlerRuntimeSnapshot:
    """
    Crawler runtime durumunun immutable görüntüsü.

    Monitoring katmanı ileride bu modeli doğrudan kullanabilir veya kendi
    metriğine dönüştürebilir.
    """

    state: CrawlerState
    run_count: int
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    last_status: Optional[ExecutionStatus]
    stop_requested: bool


# =============================================================================
# HELPERS
# =============================================================================
def utc_now() -> datetime:
    return datetime.now(UTC)


def _safe_exception_message(error: BaseException) -> str:
    """
    Exception mesajını metadata için sınırlar.

    Traceback burada saklanmaz. Traceback/loglama ileride merkezi logging
    katmanının sorumluluğudur.
    """

    message = str(error).strip()

    if not message:
        message = error.__class__.__name__

    return message[:4_000]


def _copy_metadata(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}

    return dict(value)


# =============================================================================
# CRAWLER
# =============================================================================
class Crawler:
    """
    Tek bir bot instance'ını güvenli şekilde çalıştıran framework facade'ı.

    Mimari kural
    ------------
    ``Crawler`` bot iş mantığını bilmez.

    Bot:
        nasıl veri toplanacağını bilir.

    BaseBot:
        bot lifecycle'ını bilir.

    Crawler:
        bot instance'ının ne zaman ve nasıl çağrılacağını bilir.

    Bu ayrım özellikle ileride Scheduler ve Orchestrator eklendiğinde önemlidir.
    """

    def __init__(
        self,
        bot: RunnableBot,
        *,
        propagate_exceptions: bool = False,
    ) -> None:
        """
        Parameters
        ----------
        bot:
            ``run() -> ExecutionResult`` sözleşmesini sağlayan bot instance'ı.

        propagate_exceptions:
            False:
                Beklenmeyen bot hataları FAILED ExecutionResult'a çevrilir.

            True:
                Beklenmeyen hata result üretildikten sonra tekrar raise edilir.

            Framework lifecycle exceptionları kendi anlamlarına göre ayrıca
            ele alınır.
        """

        self._validate_bot(bot)

        self._bot = bot
        self._propagate_exceptions = bool(propagate_exceptions)

        self._state = CrawlerState.IDLE
        self._run_count = 0

        self._started_at: Optional[datetime] = None
        self._finished_at: Optional[datetime] = None
        self._last_result: Optional[ExecutionResult] = None

        self._stop_requested = False

        self._state_lock = threading.RLock()

    # =========================================================================
    # PUBLIC PROPERTIES
    # =========================================================================
    @property
    def bot(self) -> RunnableBot:
        return self._bot

    @property
    def state(self) -> CrawlerState:
        with self._state_lock:
            return self._state

    @property
    def is_running(self) -> bool:
        return self.state is CrawlerState.RUNNING

    @property
    def stop_requested(self) -> bool:
        with self._state_lock:
            return self._stop_requested

    @property
    def run_count(self) -> int:
        with self._state_lock:
            return self._run_count

    @property
    def last_result(self) -> Optional[ExecutionResult]:
        with self._state_lock:
            return self._last_result

    # =========================================================================
    # VALIDATION
    # =========================================================================
    @staticmethod
    def _validate_bot(bot: Any) -> None:
        if bot is None:
            raise ContractValidationError(
                "Crawler bot instance'ı None olamaz."
            )

        run_method = getattr(bot, "run", None)

        if not callable(run_method):
            raise ContractValidationError(
                "Crawler bot'u callable run() metodu sağlamalıdır."
            )

    @staticmethod
    def _validate_result(result: Any) -> ExecutionResult:
        """
        BaseBot ile Crawler arasındaki public contract sınırı.

        Sessiz dict/tuple/None dönüşümlerine özellikle izin verilmez.
        Bir framework bot'u daima ExecutionResult döndürmelidir.
        """

        if not isinstance(result, ExecutionResult):
            raise ContractValidationError(
                "Bot.run() ExecutionResult döndürmelidir; "
                f"actual={type(result).__name__}."
            )

        if not isinstance(result.status, ExecutionStatus):
            raise ContractValidationError(
                "ExecutionResult.status ExecutionStatus olmalıdır."
            )

        for field_name in (
            "records_processed",
            "errors",
            "warnings",
        ):
            value = getattr(result, field_name)

            if isinstance(value, bool) or not isinstance(value, int):
                raise ContractValidationError(
                    f"ExecutionResult.{field_name} tam sayı olmalıdır."
                )

            if value < 0:
                raise ContractValidationError(
                    f"ExecutionResult.{field_name} negatif olamaz."
                )

        if not isinstance(result.metadata, dict):
            raise ContractValidationError(
                "ExecutionResult.metadata dict olmalıdır."
            )

        return result

    # =========================================================================
    # EXECUTION
    # =========================================================================
    def run(self) -> ExecutionResult:
        """
        Bot'u bir kez çalıştırır ve standart ExecutionResult döndürür.

        Aynı Crawler instance'ı başka bir thread tarafından hâlihazırda
        çalıştırılıyorsa ikinci run reddedilir.
        """

        self._begin_run()

        try:
            if self.stop_requested:
                result = self._cancelled_result(
                    reason="stop_requested_before_bot_run",
                )
            else:
                result = self._execute_bot()

            self._store_result(result)

            return result

        finally:
            self._finish_run()

    def _begin_run(self) -> None:
        with self._state_lock:
            if self._state is CrawlerState.RUNNING:
                raise AlreadyRunningError(
                    "Bu Crawler instance'ı zaten çalışıyor."
                )

            if self._state is CrawlerState.STOPPING:
                raise AlreadyRunningError(
                    "Crawler durdurulma aşamasındayken yeniden başlatılamaz."
                )

            self._state = CrawlerState.RUNNING
            self._started_at = utc_now()
            self._finished_at = None
            self._run_count += 1

    def _execute_bot(self) -> ExecutionResult:
        try:
            raw_result = self._bot.run()

            return self._validate_result(raw_result)

        except ShutdownRequested as error:
            return self._cancelled_result(
                reason="shutdown_requested",
                error=error,
            )

        except ContractValidationError as error:
            result = self._failed_result(
                error,
                failure_kind="contract_validation",
            )

            if self._propagate_exceptions:
                self._store_result(result)
                raise

            return result

        except EnterpriseCrawlerError as error:
            result = self._failed_result(
                error,
                failure_kind="framework_error",
            )

            if self._propagate_exceptions:
                self._store_result(result)
                raise

            return result

        except Exception as error:
            result = self._failed_result(
                error,
                failure_kind="unhandled_bot_error",
            )

            if self._propagate_exceptions:
                self._store_result(result)
                raise

            return result

    # =========================================================================
    # STOP
    # =========================================================================
    def request_stop(self) -> None:
        """
        Cooperative stop isteği gönderir.

        Crawler bot'un iç thread/process'ini zorla öldürmez.

        Bot ``request_stop()`` sağlıyorsa istek ona da iletilir. Gerçek
        cooperative cancellation davranışı BaseBot tarafından uygulanacaktır.
        """

        with self._state_lock:
            self._stop_requested = True

            if self._state is CrawlerState.RUNNING:
                self._state = CrawlerState.STOPPING

        bot_request_stop = getattr(self._bot, "request_stop", None)

        if callable(bot_request_stop):
            bot_request_stop()

    def reset_stop_request(self) -> None:
        """
        Yeni bir manuel run öncesinde stop bayrağını temizler.

        Çalışan crawler üzerinde çağrı yapılamaz.
        """

        with self._state_lock:
            if self._state in {
                CrawlerState.RUNNING,
                CrawlerState.STOPPING,
            }:
                raise AlreadyRunningError(
                    "Çalışan Crawler'ın stop isteği resetlenemez."
                )

            self._stop_requested = False

    # =========================================================================
    # RESULT BUILDERS
    # =========================================================================
    def _cancelled_result(
        self,
        *,
        reason: str,
        error: Optional[BaseException] = None,
    ) -> ExecutionResult:
        metadata: dict[str, Any] = {
            "crawler": {
                "reason": reason,
            }
        }

        if error is not None:
            metadata["crawler"]["exception_type"] = (
                error.__class__.__name__
            )
            metadata["crawler"]["exception_message"] = (
                _safe_exception_message(error)
            )

        return ExecutionResult(
            status=ExecutionStatus.CANCELLED,
            records_processed=0,
            errors=0,
            warnings=0,
            metadata=metadata,
        )

    def _failed_result(
        self,
        error: BaseException,
        *,
        failure_kind: str,
    ) -> ExecutionResult:
        return ExecutionResult(
            status=ExecutionStatus.FAILED,
            records_processed=0,
            errors=1,
            warnings=0,
            metadata={
                "crawler": {
                    "failure_kind": failure_kind,
                    "exception_type": error.__class__.__name__,
                    "exception_message": _safe_exception_message(error),
                }
            },
        )

    # =========================================================================
    # STATE
    # =========================================================================
    def _store_result(self, result: ExecutionResult) -> None:
        with self._state_lock:
            self._last_result = result

    def _finish_run(self) -> None:
        with self._state_lock:
            self._finished_at = utc_now()
            self._state = CrawlerState.FINISHED

    def snapshot(self) -> CrawlerRuntimeSnapshot:
        """
        Thread-safe runtime snapshot döndürür.
        """

        with self._state_lock:
            last_status = (
                self._last_result.status
                if self._last_result is not None
                else None
            )

            return CrawlerRuntimeSnapshot(
                state=self._state,
                run_count=self._run_count,
                started_at=self._started_at,
                finished_at=self._finished_at,
                last_status=last_status,
                stop_requested=self._stop_requested,
            )

    # =========================================================================
    # REPRESENTATION
    # =========================================================================
    def __repr__(self) -> str:
        bot_name = getattr(
            self._bot,
            "bot_name",
            self._bot.__class__.__name__,
        )

        return (
            f"{self.__class__.__name__}("
            f"bot={bot_name!r}, "
            f"state={self.state.value!r}, "
            f"run_count={self.run_count}"
            f")"
        )