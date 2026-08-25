from __future__ import annotations

"""
Enterprise Crawler Framework - Event Worker

Queue ve dispatcher katmanlarını uzun yaşayan worker execution modeli altında
birleştirir.

Worker belirli bir queue backend'ine bağlı değildir.

Desteklenen queue contract::

    EventQueueProtocol

Dolayısıyla aynı worker:

- InMemoryEventQueue
- SQLiteEventQueue

ile çalışabilir.

Failure Routing
---------------
Worker iki çalışma modunu destekler.

Legacy mode::

    retry_policy=None

Bu durumda mevcut davranış korunur:

    handler failure
        ↓
    requeue_on_error=True
        → nack(requeue=True)

    requeue_on_error=False
        → nack(requeue=False)

Policy mode::

    retry_policy=RetryPolicy(...)

Bu durumda failure routing RetryPolicy tarafından belirlenir:

    handler failure
        ↓
    RetryPolicy.decide()
        ↓
        ├── RETRY
        │     └── nack(
        │             requeue=True,
        │             retry_delay_seconds=decision.retry_delay_seconds,
        │         )
        │
        ├── DISCARD
        │     └── nack(requeue=False)
        │
        └── DEAD_LETTER
              ↓
           DLQ.store()
              ↓
         yalnız store başarılıysa
              ↓
        nack(requeue=False)

Fail-Closed DLQ
---------------
Dead-letter yazımı başarısız olursa source event ack/nack edilmez.

Event claimed durumda bırakılır. Durable queue kullanılıyorsa lease expiration
sonrası tekrar recover edilebilir.

Bu sıra event'in DLQ failure sırasında sessizce kaybolmasını engeller.
"""

import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from numbers import Real
from time import monotonic
from typing import Any, Optional

from enterprise_crawler.events.dead_letter import (
    DeadLetterQueueProtocol,
)
from enterprise_crawler.events.dispatcher import (
    DispatchResult,
    EventDispatcher,
    EventHandlerExecutionError,
)
from enterprise_crawler.events.queue import (
    ClaimedEvent,
    EventQueueProtocol,
)
from enterprise_crawler.events.retry import (
    RetryAction,
    RetryDecision,
    RetryPolicy,
)


UTC = timezone.utc


# =============================================================================
# EXCEPTIONS
# =============================================================================
class EventWorkerError(RuntimeError):
    """
    Event worker hatalarının temel sınıfı.
    """


class EventWorkerValidationError(
    EventWorkerError
):
    """
    EventWorker configuration / argument contract hatası.
    """


class EventWorkerAlreadyRunningError(
    EventWorkerError
):
    """
    Aynı worker instance eşzamanlı çalıştırılmaya çalışıldığında.
    """


class EventWorkerClosedError(
    EventWorkerError
):
    """
    Kapatılmış worker çalıştırılmaya çalışıldığında.
    """


class EventWorkerDeadLetterError(
    EventWorkerError
):
    """
    DEAD_LETTER kararı üretildiği halde DLQ kullanılamadığında.

    Source event bu hata sırasında ack/nack edilmez.
    """


# =============================================================================
# HELPERS
# =============================================================================
def utc_now() -> datetime:
    return datetime.now(
        UTC
    )


def _normalize_name(
    value: Any,
    *,
    field_name: str,
) -> str:
    if not isinstance(
        value,
        str,
    ):
        raise EventWorkerValidationError(
            f"{field_name} str olmalıdır."
        )

    normalized = (
        value.strip()
    )

    if not normalized:
        raise EventWorkerValidationError(
            f"{field_name} boş olamaz."
        )

    return normalized


def _normalize_boolean(
    value: Any,
    *,
    field_name: str,
) -> bool:
    if not isinstance(
        value,
        bool,
    ):
        raise EventWorkerValidationError(
            f"{field_name} bool olmalıdır."
        )

    return value


def _normalize_non_negative_float(
    value: Any,
    *,
    field_name: str,
) -> float:
    if (
        isinstance(
            value,
            bool,
        )
        or not isinstance(
            value,
            Real,
        )
    ):
        raise EventWorkerValidationError(
            f"{field_name} sayı olmalıdır."
        )

    normalized = float(
        value
    )

    if normalized < 0:
        raise EventWorkerValidationError(
            f"{field_name} negatif olamaz."
        )

    return normalized


def _normalize_optional_positive_int(
    value: Any,
    *,
    field_name: str,
) -> Optional[int]:
    if value is None:
        return None

    if (
        isinstance(
            value,
            bool,
        )
        or not isinstance(
            value,
            int,
        )
    ):
        raise EventWorkerValidationError(
            f"{field_name} int veya None olmalıdır."
        )

    if value <= 0:
        raise EventWorkerValidationError(
            f"{field_name} sıfırdan büyük olmalıdır."
        )

    return value


def _validate_queue(
    queue: Any,
) -> EventQueueProtocol:
    if not isinstance(
        queue,
        EventQueueProtocol,
    ):
        raise EventWorkerValidationError(
            "queue EventQueueProtocol "
            "sözleşmesini sağlamalıdır "
            f"| actual={type(queue).__name__}"
        )

    return queue


def _validate_retry_policy(
    value: Any,
) -> Optional[
    RetryPolicy
]:
    if value is None:
        return None

    if not isinstance(
        value,
        RetryPolicy,
    ):
        raise EventWorkerValidationError(
            "retry_policy RetryPolicy "
            "veya None olmalıdır."
        )

    return value


def _validate_dead_letter_queue(
    value: Any,
) -> Optional[
    DeadLetterQueueProtocol
]:
    if value is None:
        return None

    if not isinstance(
        value,
        DeadLetterQueueProtocol,
    ):
        raise EventWorkerValidationError(
            "dead_letter_queue "
            "DeadLetterQueueProtocol "
            "sözleşmesini sağlamalıdır."
        )

    return value


def _policy_error(
    error: BaseException,
) -> BaseException:
    """
    EventHandlerExecutionError wrapper'ının altındaki gerçek handler
    exception'ını RetryPolicy'ye verir.

    Böylece kullanıcı örneğin::

        retry_exceptions=(TimeoutError,)

    tanımladığında wrapper class yerine gerçek TimeoutError sınıflandırılır.
    """

    if isinstance(
        error,
        EventHandlerExecutionError,
    ):
        cause = getattr(
            error,
            "cause",
            None,
        )

        if isinstance(
            cause,
            BaseException,
        ):
            return cause

    return error


# =============================================================================
# RUN SUMMARY
# =============================================================================
@dataclass(
    slots=True,
    frozen=True,
)
class WorkerRunSummary:
    mode: str

    processed_count: int

    failure_count: int

    started_at: datetime

    finished_at: datetime

    duration_seconds: float

    stop_requested: bool

    queue_empty: bool

    max_events_reached: bool = False

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "mode": (
                self.mode
            ),
            "processed_count": (
                self.processed_count
            ),
            "failure_count": (
                self.failure_count
            ),
            "started_at": (
                self.started_at.isoformat()
            ),
            "finished_at": (
                self.finished_at.isoformat()
            ),
            "duration_seconds": (
                self.duration_seconds
            ),
            "stop_requested": (
                self.stop_requested
            ),
            "queue_empty": (
                self.queue_empty
            ),
            "max_events_reached": (
                self.max_events_reached
            ),
        }


# =============================================================================
# WORKER
# =============================================================================
class EventWorker:
    """
    Backend-independent event worker runtime.

    Geriye uyumluluk
    ----------------
    retry_policy verilmezse legacy ``requeue_on_error`` davranışı kullanılır.

    Yeni failure policy
    ------------------
    retry_policy verilirse worker failure kararını policy üzerinden üretir.

    ``dead_letter_queue`` yalnız DEAD_LETTER kararı çıktığında zorunludur.
    """

    def __init__(
        self,
        *,
        queue: EventQueueProtocol,
        dispatcher: EventDispatcher,
        name: str = "event-worker",
        stop_event: Optional[
            Any
        ] = None,
        requeue_on_error: bool = True,
        stop_on_error: bool = False,
        idle_sleep_seconds: float = 0.05,
        retry_policy: Optional[
            RetryPolicy
        ] = None,
        dead_letter_queue: Optional[
            DeadLetterQueueProtocol
        ] = None,
    ) -> None:
        self.queue = (
            _validate_queue(
                queue
            )
        )

        if not isinstance(
            dispatcher,
            EventDispatcher,
        ):
            raise EventWorkerValidationError(
                "dispatcher EventDispatcher olmalıdır."
            )

        self.dispatcher = (
            dispatcher
        )

        self.name = (
            _normalize_name(
                name,
                field_name="name",
            )
        )

        self.requeue_on_error = (
            _normalize_boolean(
                requeue_on_error,
                field_name=(
                    "requeue_on_error"
                ),
            )
        )

        self.stop_on_error = (
            _normalize_boolean(
                stop_on_error,
                field_name="stop_on_error",
            )
        )

        self.idle_sleep_seconds = (
            _normalize_non_negative_float(
                idle_sleep_seconds,
                field_name=(
                    "idle_sleep_seconds"
                ),
            )
        )

        self.retry_policy = (
            _validate_retry_policy(
                retry_policy
            )
        )

        self.dead_letter_queue = (
            _validate_dead_letter_queue(
                dead_letter_queue
            )
        )

        self.stop_event = (
            stop_event
            if stop_event is not None
            else threading.Event()
        )

        self._run_lock = (
            threading.Lock()
        )

        self._state_lock = (
            threading.RLock()
        )

        self._is_running = False

        self._is_closed = False

        self._run_count = 0

        self._total_processed = 0

        self._total_failures = 0

        self._retry_count = 0

        self._discard_count = 0

        self._dead_letter_count = 0

        self._last_dispatch: Optional[
            DispatchResult
        ] = None

        self._last_error: Optional[
            BaseException
        ] = None

        self._last_retry_decision: Optional[
            RetryDecision
        ] = None

        self._last_summary: Optional[
            WorkerRunSummary
        ] = None

        self._run_started_at: Optional[
            datetime
        ] = None

        self._run_finished_at: Optional[
            datetime
        ] = None

    # =========================================================================
    # PUBLIC STATE
    # =========================================================================
    @property
    def is_running(
        self,
    ) -> bool:
        with self._state_lock:
            return (
                self._is_running
            )

    @property
    def is_closed(
        self,
    ) -> bool:
        with self._state_lock:
            return (
                self._is_closed
            )

    @property
    def run_count(
        self,
    ) -> int:
        with self._state_lock:
            return (
                self._run_count
            )

    @property
    def total_processed(
        self,
    ) -> int:
        with self._state_lock:
            return (
                self._total_processed
            )

    @property
    def total_failures(
        self,
    ) -> int:
        with self._state_lock:
            return (
                self._total_failures
            )

    @property
    def retry_count(
        self,
    ) -> int:
        with self._state_lock:
            return (
                self._retry_count
            )

    @property
    def discard_count(
        self,
    ) -> int:
        with self._state_lock:
            return (
                self._discard_count
            )

    @property
    def dead_letter_count(
        self,
    ) -> int:
        with self._state_lock:
            return (
                self._dead_letter_count
            )

    @property
    def last_dispatch(
        self,
    ) -> Optional[
        DispatchResult
    ]:
        with self._state_lock:
            return (
                self._last_dispatch
            )

    @property
    def last_error(
        self,
    ) -> Optional[
        BaseException
    ]:
        with self._state_lock:
            return (
                self._last_error
            )

    @property
    def last_retry_decision(
        self,
    ) -> Optional[
        RetryDecision
    ]:
        with self._state_lock:
            return (
                self._last_retry_decision
            )

    @property
    def last_summary(
        self,
    ) -> Optional[
        WorkerRunSummary
    ]:
        with self._state_lock:
            return (
                self._last_summary
            )

    # =========================================================================
    # STOP SIGNAL
    # =========================================================================
    def should_stop(
        self,
    ) -> bool:
        event = (
            self.stop_event
        )

        is_set = getattr(
            event,
            "is_set",
            None,
        )

        if callable(
            is_set
        ):
            return bool(
                is_set()
            )

        return bool(
            event
        )

    def request_stop(
        self,
    ) -> None:
        setter = getattr(
            self.stop_event,
            "set",
            None,
        )

        if callable(
            setter
        ):
            setter()

            return

        self.stop_event = True

    def reset_stop_request(
        self,
    ) -> None:
        if self.is_running:
            raise EventWorkerAlreadyRunningError(
                "Çalışan worker'ın stop sinyali "
                "resetlenemez "
                f"| worker={self.name}"
            )

        clearer = getattr(
            self.stop_event,
            "clear",
            None,
        )

        if callable(
            clearer
        ):
            clearer()

        else:
            self.stop_event = False

    # =========================================================================
    # RUN STATE
    # =========================================================================
    def _ensure_open(
        self,
    ) -> None:
        if self.is_closed:
            raise EventWorkerClosedError(
                "EventWorker kapalı "
                f"| worker={self.name}"
            )

    def _begin_run(
        self,
    ) -> tuple[
        datetime,
        float,
    ]:
        self._ensure_open()

        acquired = (
            self._run_lock.acquire(
                blocking=False
            )
        )

        if not acquired:
            raise EventWorkerAlreadyRunningError(
                "EventWorker zaten çalışıyor "
                f"| worker={self.name}"
            )

        try:
            with self._state_lock:
                if self._is_closed:
                    raise EventWorkerClosedError(
                        "EventWorker kapalı "
                        f"| worker={self.name}"
                    )

                if self._is_running:
                    raise EventWorkerAlreadyRunningError(
                        "EventWorker zaten çalışıyor "
                        f"| worker={self.name}"
                    )

                started_at = (
                    utc_now()
                )

                self._is_running = True

                self._run_count += 1

                self._run_started_at = (
                    started_at
                )

                self._run_finished_at = None

                self._last_error = None

                self._last_retry_decision = None

                self._last_summary = None

            return (
                started_at,
                monotonic(),
            )

        except BaseException:
            try:
                self._run_lock.release()

            except RuntimeError:
                pass

            raise

    def _finish_run(
        self,
        *,
        mode: str,
        started_at: datetime,
        started_monotonic: float,
        processed_count: int,
        failure_count: int,
        max_events_reached: bool = False,
    ) -> WorkerRunSummary:
        finished_at = (
            utc_now()
        )

        duration_seconds = max(
            0.0,
            monotonic()
            - started_monotonic,
        )

        summary = (
            WorkerRunSummary(
                mode=mode,
                processed_count=(
                    processed_count
                ),
                failure_count=(
                    failure_count
                ),
                started_at=(
                    started_at
                ),
                finished_at=(
                    finished_at
                ),
                duration_seconds=round(
                    duration_seconds,
                    6,
                ),
                stop_requested=(
                    self.should_stop()
                ),
                queue_empty=(
                    self.queue.active_count
                    == 0
                ),
                max_events_reached=(
                    max_events_reached
                ),
            )
        )

        with self._state_lock:
            self._run_finished_at = (
                finished_at
            )

            self._last_summary = (
                summary
            )

            self._is_running = False

        return summary

    def _release_run_lock(
        self,
    ) -> None:
        try:
            self._run_lock.release()

        except RuntimeError:
            pass

    # =========================================================================
    # RESULT NORMALIZATION
    # =========================================================================
    @staticmethod
    def _bind_claim_to_dispatch_result(
        result: DispatchResult,
        claimed: ClaimedEvent,
    ) -> DispatchResult:
        return DispatchResult(
            event_type=(
                result.event_type
            ),
            handler_name=(
                result.handler_name
            ),
            value=(
                result.value
            ),
            duration_seconds=(
                result.duration_seconds
            ),
            message_id=(
                claimed.message_id
            ),
            delivery_count=(
                claimed.delivery_count
            ),
        )

    # =========================================================================
    # POLICY FAILURE ROUTING
    # =========================================================================
    def _route_policy_failure(
        self,
        claimed: ClaimedEvent,
        error: BaseException,
    ) -> RetryDecision:
        policy = (
            self.retry_policy
        )

        if policy is None:
            raise EventWorkerError(
                "Internal error: retry policy bulunamadı."
            )

        classified_error = (
            _policy_error(
                error
            )
        )

        decision = (
            policy.decide(
                claimed.delivery_count,
                classified_error,
            )
        )

        with self._state_lock:
            self._last_retry_decision = (
                decision
            )

        # ---------------------------------------------------------------------
        # RETRY
        # ---------------------------------------------------------------------
        if (
            decision.action
            is RetryAction.RETRY
        ):
            self.queue.nack(
                claimed.message_id,
                claimed.claim_token,
                requeue=True,
                retry_delay_seconds=(
                    decision.retry_delay_seconds
                ),
            )

            with self._state_lock:
                self._retry_count += 1

            return decision

        # ---------------------------------------------------------------------
        # DISCARD
        # ---------------------------------------------------------------------
        if (
            decision.action
            is RetryAction.DISCARD
        ):
            self.queue.nack(
                claimed.message_id,
                claimed.claim_token,
                requeue=False,
            )

            with self._state_lock:
                self._discard_count += 1

            return decision

        # ---------------------------------------------------------------------
        # DEAD LETTER
        #
        # Fail-closed ordering:
        #
        #   DLQ.store()
        #       ↓
        #   only after success
        #       ↓
        #   source nack(requeue=False)
        # ---------------------------------------------------------------------
        if (
            decision.action
            is RetryAction.DEAD_LETTER
        ):
            dlq = (
                self.dead_letter_queue
            )

            if dlq is None:
                raise EventWorkerDeadLetterError(
                    "RetryPolicy DEAD_LETTER kararı üretti "
                    "fakat dead_letter_queue yapılandırılmamış "
                    f"| worker={self.name} "
                    f"| message_id={claimed.message_id!r}"
                )

            dlq.store(
                claimed.event,
                message_id=(
                    claimed.message_id
                ),
                delivery_count=(
                    claimed.delivery_count
                ),
                error=(
                    classified_error
                ),
                source_queue=(
                    getattr(
                        self.queue,
                        "name",
                        None,
                    )
                ),
                claim_token=(
                    claimed.claim_token
                ),
                metadata={
                    "worker": (
                        self.name
                    ),
                    "retry_decision": (
                        decision.to_dict()
                    ),
                },
            )

            # DLQ store başarılı olmadan bu satıra gelinmez.
            self.queue.nack(
                claimed.message_id,
                claimed.claim_token,
                requeue=False,
            )

            with self._state_lock:
                self._dead_letter_count += 1

            return decision

        raise EventWorkerError(
            "Bilinmeyen retry action "
            f"| action={decision.action!r}"
        )

    # =========================================================================
    # LEGACY PROCESSING
    # =========================================================================
    def _process_once_legacy(
        self,
        claimed: ClaimedEvent,
    ) -> DispatchResult:
        return (
            self.dispatcher.dispatch_claimed(
                self.queue,
                claimed,
                requeue_on_error=(
                    self.requeue_on_error
                ),
            )
        )

    # =========================================================================
    # POLICY PROCESSING
    # =========================================================================
    def _process_once_with_policy(
        self,
        claimed: ClaimedEvent,
    ) -> DispatchResult:
        try:
            dispatch_result = (
                self.dispatcher.dispatch(
                    claimed.event
                )
            )

        except BaseException as exc:
            self._route_policy_failure(
                claimed,
                exc,
            )

            raise

        self.queue.ack(
            claimed.message_id,
            claimed.claim_token,
        )

        return (
            self._bind_claim_to_dispatch_result(
                dispatch_result,
                claimed,
            )
        )

    # =========================================================================
    # PROCESSING
    # =========================================================================
    def _process_once(
        self,
    ) -> Optional[
        DispatchResult
    ]:
        if self.should_stop():
            return None

        claimed = (
            self.queue.claim()
        )

        if claimed is None:
            return None

        try:
            if (
                self.retry_policy
                is None
            ):
                result = (
                    self._process_once_legacy(
                        claimed
                    )
                )

            else:
                result = (
                    self._process_once_with_policy(
                        claimed
                    )
                )

        except BaseException as exc:
            with self._state_lock:
                self._total_failures += 1

                self._last_error = (
                    exc
                )

            raise

        with self._state_lock:
            self._total_processed += 1

            self._last_dispatch = (
                result
            )

            self._last_error = None

            self._last_retry_decision = None

        return result

    # =========================================================================
    # FAILURE LOOP DECISION
    # =========================================================================
    def _should_break_after_failure(
        self,
    ) -> bool:
        """
        run_until_empty poison-event hot-loop koruması.

        Legacy:
            requeue=True ise aynı event'i aynı run'da tekrar claim etme.

        Policy:
            RETRY ise aynı event'i aynı run'da tekrar claim etme.
            DISCARD / DEAD_LETTER ise sıradaki event'e devam edilebilir.
        """

        if self.retry_policy is None:
            return (
                self.requeue_on_error
            )

        decision = (
            self.last_retry_decision
        )

        if decision is None:
            # Failure routing tamamlanamadı.
            # Örn. DLQ store failure.
            #
            # Fail-safe olarak aynı run içinde devam etmiyoruz.
            return True

        return (
            decision.action
            is RetryAction.RETRY
        )

    # =========================================================================
    # RUN ONCE
    # =========================================================================
    def run_once(
        self,
    ) -> Optional[
        DispatchResult
    ]:
        (
            started_at,
            started_monotonic,
        ) = self._begin_run()

        processed_count = 0

        failure_count = 0

        try:
            try:
                result = (
                    self._process_once()
                )

            except BaseException:
                failure_count = 1

                raise

            if result is not None:
                processed_count = 1

            return result

        finally:
            self._finish_run(
                mode="once",
                started_at=(
                    started_at
                ),
                started_monotonic=(
                    started_monotonic
                ),
                processed_count=(
                    processed_count
                ),
                failure_count=(
                    failure_count
                ),
            )

            self._release_run_lock()

    # =========================================================================
    # RUN UNTIL EMPTY
    # =========================================================================
    def run_until_empty(
        self,
        *,
        max_events: Optional[
            int
        ] = None,
    ) -> WorkerRunSummary:
        resolved_max_events = (
            _normalize_optional_positive_int(
                max_events,
                field_name="max_events",
            )
        )

        (
            started_at,
            started_monotonic,
        ) = self._begin_run()

        processed_count = 0

        failure_count = 0

        attempted_count = 0

        max_events_reached = False

        summary: Optional[
            WorkerRunSummary
        ] = None

        try:
            while not self.should_stop():
                if (
                    resolved_max_events
                    is not None
                    and attempted_count
                    >= resolved_max_events
                ):
                    max_events_reached = True

                    break

                try:
                    result = (
                        self._process_once()
                    )

                except BaseException:
                    attempted_count += 1

                    failure_count += 1

                    if self.stop_on_error:
                        raise

                    if (
                        self._should_break_after_failure()
                    ):
                        break

                    continue

                if result is None:
                    break

                attempted_count += 1

                processed_count += 1

            summary = self._finish_run(
                mode="until_empty",
                started_at=(
                    started_at
                ),
                started_monotonic=(
                    started_monotonic
                ),
                processed_count=(
                    processed_count
                ),
                failure_count=(
                    failure_count
                ),
                max_events_reached=(
                    max_events_reached
                ),
            )

            return summary

        finally:
            if summary is None:
                self._finish_run(
                    mode="until_empty",
                    started_at=(
                        started_at
                    ),
                    started_monotonic=(
                        started_monotonic
                    ),
                    processed_count=(
                        processed_count
                    ),
                    failure_count=(
                        failure_count
                    ),
                    max_events_reached=(
                        max_events_reached
                    ),
                )

            self._release_run_lock()

    # =========================================================================
    # RUN FOREVER
    # =========================================================================
    def run_forever(
        self,
        *,
        max_events: Optional[
            int
        ] = None,
    ) -> WorkerRunSummary:
        resolved_max_events = (
            _normalize_optional_positive_int(
                max_events,
                field_name="max_events",
            )
        )

        (
            started_at,
            started_monotonic,
        ) = self._begin_run()

        processed_count = 0

        failure_count = 0

        attempted_count = 0

        max_events_reached = False

        summary: Optional[
            WorkerRunSummary
        ] = None

        try:
            while not self.should_stop():
                if (
                    resolved_max_events
                    is not None
                    and attempted_count
                    >= resolved_max_events
                ):
                    max_events_reached = True

                    break

                try:
                    result = (
                        self._process_once()
                    )

                except BaseException:
                    attempted_count += 1

                    failure_count += 1

                    if self.stop_on_error:
                        raise

                    self._idle_wait()

                    continue

                if result is None:
                    self._idle_wait()

                    continue

                attempted_count += 1

                processed_count += 1

            summary = self._finish_run(
                mode="forever",
                started_at=(
                    started_at
                ),
                started_monotonic=(
                    started_monotonic
                ),
                processed_count=(
                    processed_count
                ),
                failure_count=(
                    failure_count
                ),
                max_events_reached=(
                    max_events_reached
                ),
            )

            return summary

        finally:
            if summary is None:
                self._finish_run(
                    mode="forever",
                    started_at=(
                        started_at
                    ),
                    started_monotonic=(
                        started_monotonic
                    ),
                    processed_count=(
                        processed_count
                    ),
                    failure_count=(
                        failure_count
                    ),
                    max_events_reached=(
                        max_events_reached
                    ),
                )

            self._release_run_lock()

    # =========================================================================
    # IDLE WAIT
    # =========================================================================
    def _idle_wait(
        self,
    ) -> None:
        timeout = (
            self.idle_sleep_seconds
        )

        if timeout <= 0:
            return

        waiter = getattr(
            self.stop_event,
            "wait",
            None,
        )

        if callable(
            waiter
        ):
            waiter(
                timeout
            )

            return

        threading.Event().wait(
            timeout
        )

    # =========================================================================
    # CLOSE
    # =========================================================================
    def close(
        self,
    ) -> None:
        with self._state_lock:
            if self._is_closed:
                return

            if self._is_running:
                raise EventWorkerAlreadyRunningError(
                    "Çalışan EventWorker kapatılamaz "
                    f"| worker={self.name}"
                )

            self._is_closed = True

    def __enter__(
        self,
    ) -> "EventWorker":
        self._ensure_open()

        return self

    def __exit__(
        self,
        exc_type: Any,
        exc: Any,
        traceback: Any,
    ) -> None:
        self.close()

    # =========================================================================
    # SNAPSHOT
    # =========================================================================
    def snapshot(
        self,
    ) -> dict[str, Any]:
        with self._state_lock:
            return {
                "name": (
                    self.name
                ),
                "is_running": (
                    self._is_running
                ),
                "is_closed": (
                    self._is_closed
                ),
                "run_count": (
                    self._run_count
                ),
                "stop_requested": (
                    self.should_stop()
                ),
                "requeue_on_error": (
                    self.requeue_on_error
                ),
                "stop_on_error": (
                    self.stop_on_error
                ),
                "idle_sleep_seconds": (
                    self.idle_sleep_seconds
                ),
                "retry_policy_enabled": (
                    self.retry_policy
                    is not None
                ),
                "retry_policy": (
                    self.retry_policy.snapshot()
                    if self.retry_policy
                    is not None
                    else None
                ),
                "dead_letter_queue_enabled": (
                    self.dead_letter_queue
                    is not None
                ),
                "dead_letter_queue": (
                    self.dead_letter_queue.snapshot()
                    if self.dead_letter_queue
                    is not None
                    else None
                ),
                "total_processed": (
                    self._total_processed
                ),
                "total_failures": (
                    self._total_failures
                ),
                "retry_count": (
                    self._retry_count
                ),
                "discard_count": (
                    self._discard_count
                ),
                "dead_letter_count": (
                    self._dead_letter_count
                ),
                "started_at": (
                    self._run_started_at.isoformat()
                    if self._run_started_at
                    is not None
                    else None
                ),
                "finished_at": (
                    self._run_finished_at.isoformat()
                    if self._run_finished_at
                    is not None
                    else None
                ),
                "last_dispatch": (
                    self._last_dispatch.to_dict()
                    if self._last_dispatch
                    is not None
                    else None
                ),
                "last_error_type": (
                    self._last_error.__class__.__name__
                    if self._last_error
                    is not None
                    else None
                ),
                "last_retry_decision": (
                    self._last_retry_decision.to_dict()
                    if self._last_retry_decision
                    is not None
                    else None
                ),
                "last_summary": (
                    self._last_summary.to_dict()
                    if self._last_summary
                    is not None
                    else None
                ),
                "queue": (
                    self.queue.snapshot()
                ),
                "dispatcher": (
                    self.dispatcher.snapshot()
                ),
            }

    # =========================================================================
    # REPRESENTATION
    # =========================================================================
    def __repr__(
        self,
    ) -> str:
        return (
            f"{self.__class__.__name__}("
            f"name={self.name!r}, "
            f"is_running={self.is_running}, "
            f"run_count={self.run_count}, "
            f"total_processed={self.total_processed}, "
            f"total_failures={self.total_failures}, "
            f"retry_policy_enabled="
            f"{self.retry_policy is not None}, "
            f"dead_letter_queue_enabled="
            f"{self.dead_letter_queue is not None}, "
            f"closed={self.is_closed}"
            f")"
        )