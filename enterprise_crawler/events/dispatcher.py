from __future__ import annotations

"""
Enterprise Crawler Framework - Event Dispatcher

Event type → handler routing katmanı.

Akış::

    Event
      ↓
    EventDispatcher
      ↓
    registered handler
      ↓
    result

Queue integration::

    queue.claim()
      ↓
    dispatcher.dispatch_claimed(...)
      ↓
    handler(event)
      ↓
    success -> queue.ack()
    failure -> queue.nack()

Dispatcher belirli bir queue backend'ine bağımlı değildir.

Desteklenen queue contract::

    EventQueueProtocol
"""

import threading
from dataclasses import dataclass
from time import monotonic
from typing import Any, Callable, Optional

from enterprise_crawler.contracts import Event
from enterprise_crawler.events.queue import (
    ClaimedEvent,
    EventQueueProtocol,
)


# =============================================================================
# EXCEPTIONS
# =============================================================================
class EventDispatcherError(RuntimeError):
    """
    Event dispatcher temel hatası.
    """


class EventDispatcherValidationError(
    EventDispatcherError
):
    """
    Dispatcher input contract hatası.
    """


class DuplicateEventHandlerError(
    EventDispatcherError
):
    """
    Aynı event type için ikinci handler kaydedildiğinde.
    """


class EventHandlerNotFoundError(
    EventDispatcherError
):
    """
    Event type için handler bulunamadığında.
    """


class EventHandlerExecutionError(
    EventDispatcherError
):
    """
    Handler exception ürettiğinde.
    """

    def __init__(
        self,
        message: str,
        *,
        event_type: str,
        handler_name: str,
        cause: BaseException,
    ) -> None:
        super().__init__(
            message
        )

        self.event_type = (
            event_type
        )

        self.handler_name = (
            handler_name
        )

        self.cause = cause


# =============================================================================
# HELPERS
# =============================================================================
def _normalize_event_type(
    value: Any,
) -> str:
    if not isinstance(
        value,
        str,
    ):
        raise EventDispatcherValidationError(
            "event_type str olmalıdır."
        )

    normalized = (
        value.strip()
    )

    if not normalized:
        raise EventDispatcherValidationError(
            "event_type boş olamaz."
        )

    return normalized


def _handler_name(
    handler: Any,
) -> str:
    explicit_name = getattr(
        handler,
        "__name__",
        None,
    )

    if (
        isinstance(
            explicit_name,
            str,
        )
        and explicit_name.strip()
    ):
        return (
            explicit_name.strip()
        )

    return (
        handler.__class__.__name__
    )


def _safe_exception_message(
    error: BaseException,
) -> str:
    message = (
        str(
            error
        ).strip()
    )

    if not message:
        message = (
            error.__class__.__name__
        )

    return message[:8_000]


def _validate_queue(
    queue: Any,
) -> EventQueueProtocol:
    if not isinstance(
        queue,
        EventQueueProtocol,
    ):
        raise EventDispatcherValidationError(
            "queue EventQueueProtocol "
            "sözleşmesini sağlamalıdır "
            f"| actual={type(queue).__name__}"
        )

    return queue


# =============================================================================
# RESULT
# =============================================================================
@dataclass(
    slots=True,
    frozen=True,
)
class DispatchResult:
    """
    Başarılı event dispatch sonucu.
    """

    event_type: str

    handler_name: str

    value: Any

    duration_seconds: float

    message_id: Optional[
        str
    ] = None

    delivery_count: Optional[
        int
    ] = None

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "event_type": (
                self.event_type
            ),
            "handler_name": (
                self.handler_name
            ),
            "value": (
                self.value
            ),
            "duration_seconds": (
                self.duration_seconds
            ),
            "message_id": (
                self.message_id
            ),
            "delivery_count": (
                self.delivery_count
            ),
        }


# =============================================================================
# DISPATCHER
# =============================================================================
class EventDispatcher:
    """
    Thread-safe event type → handler registry ve dispatcher.

    Event type lookup case-insensitive'dir.
    """

    def __init__(
        self,
        *,
        name: str = "default",
    ) -> None:
        self.name = (
            _normalize_event_type(
                name
            )
        )

        self._lock = (
            threading.RLock()
        )

        self._handlers: dict[
            str,
            tuple[
                str,
                Callable[
                    [Event],
                    Any,
                ],
            ],
        ] = {}

        self._dispatch_count = 0
        self._failure_count = 0

    # =========================================================================
    # REGISTRATION
    # =========================================================================
    def register(
        self,
        event_type: str,
        handler: Callable[
            [Event],
            Any,
        ],
        *,
        replace: bool = False,
    ) -> None:
        normalized_type = (
            _normalize_event_type(
                event_type
            )
        )

        if not callable(
            handler
        ):
            raise EventDispatcherValidationError(
                "handler callable olmalıdır."
            )

        if not isinstance(
            replace,
            bool,
        ):
            raise EventDispatcherValidationError(
                "replace bool olmalıdır."
            )

        key = (
            normalized_type.casefold()
        )

        with self._lock:
            if (
                key
                in self._handlers
                and not replace
            ):
                raise DuplicateEventHandlerError(
                    "Event handler zaten kayıtlı "
                    f"| event_type={normalized_type!r}"
                )

            self._handlers[
                key
            ] = (
                normalized_type,
                handler,
            )

    def unregister(
        self,
        event_type: str,
    ) -> Callable[
        [Event],
        Any,
    ]:
        normalized_type = (
            _normalize_event_type(
                event_type
            )
        )

        key = (
            normalized_type.casefold()
        )

        with self._lock:
            try:
                _canonical, handler = (
                    self._handlers.pop(
                        key
                    )
                )

            except KeyError as exc:
                raise EventHandlerNotFoundError(
                    "Event handler bulunamadı "
                    f"| event_type={normalized_type!r}"
                ) from exc

        return handler

    # =========================================================================
    # LOOKUP
    # =========================================================================
    def contains(
        self,
        event_type: str,
    ) -> bool:
        if not isinstance(
            event_type,
            str,
        ):
            return False

        normalized = (
            event_type.strip()
        )

        if not normalized:
            return False

        key = (
            normalized.casefold()
        )

        with self._lock:
            return (
                key
                in self._handlers
            )

    def __contains__(
        self,
        event_type: object,
    ) -> bool:
        if not isinstance(
            event_type,
            str,
        ):
            return False

        return self.contains(
            event_type
        )

    def handler_for(
        self,
        event_type: str,
    ) -> Callable[
        [Event],
        Any,
    ]:
        normalized_type = (
            _normalize_event_type(
                event_type
            )
        )

        key = (
            normalized_type.casefold()
        )

        with self._lock:
            try:
                _canonical, handler = (
                    self._handlers[
                        key
                    ]
                )

            except KeyError as exc:
                raise EventHandlerNotFoundError(
                    "Event handler bulunamadı "
                    f"| event_type={normalized_type!r}"
                ) from exc

        return handler

    def event_types(
        self,
    ) -> list[str]:
        with self._lock:
            values = [
                canonical
                for (
                    canonical,
                    _handler,
                )
                in self._handlers.values()
            ]

        return sorted(
            values,
            key=str.casefold,
        )

    # =========================================================================
    # DISPATCH
    # =========================================================================
    def dispatch(
        self,
        event: Event,
    ) -> DispatchResult:
        if not isinstance(
            event,
            Event,
        ):
            raise EventDispatcherValidationError(
                "event Event olmalıdır "
                f"| actual={type(event).__name__}"
            )

        event_type = (
            _normalize_event_type(
                event.event_type
            )
        )

        handler = (
            self.handler_for(
                event_type
            )
        )

        handler_name = (
            _handler_name(
                handler
            )
        )

        started = (
            monotonic()
        )

        try:
            value = handler(
                event
            )

        except Exception as exc:
            with self._lock:
                self._failure_count += 1

            raise EventHandlerExecutionError(
                "Event handler başarısız "
                f"| event_type={event_type!r} "
                f"| handler={handler_name!r} "
                f"| error={_safe_exception_message(exc)}",
                event_type=event_type,
                handler_name=handler_name,
                cause=exc,
            ) from exc

        duration = max(
            0.0,
            monotonic()
            - started,
        )

        with self._lock:
            self._dispatch_count += 1

        return DispatchResult(
            event_type=(
                event_type
            ),
            handler_name=(
                handler_name
            ),
            value=value,
            duration_seconds=round(
                duration,
                6,
            ),
        )

    # =========================================================================
    # CLAIMED DISPATCH
    # =========================================================================
    def dispatch_claimed(
        self,
        queue: EventQueueProtocol,
        claimed: ClaimedEvent,
        *,
        requeue_on_error: bool = True,
    ) -> DispatchResult:
        resolved_queue = (
            _validate_queue(
                queue
            )
        )

        if not isinstance(
            claimed,
            ClaimedEvent,
        ):
            raise EventDispatcherValidationError(
                "claimed ClaimedEvent olmalıdır."
            )

        if not isinstance(
            requeue_on_error,
            bool,
        ):
            raise EventDispatcherValidationError(
                "requeue_on_error bool olmalıdır."
            )

        try:
            result = (
                self.dispatch(
                    claimed.event
                )
            )

        except BaseException:
            resolved_queue.nack(
                claimed.message_id,
                claimed.claim_token,
                requeue=(
                    requeue_on_error
                ),
            )

            raise

        resolved_queue.ack(
            claimed.message_id,
            claimed.claim_token,
        )

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

    def dispatch_next(
        self,
        queue: EventQueueProtocol,
        *,
        requeue_on_error: bool = True,
    ) -> Optional[
        DispatchResult
    ]:
        resolved_queue = (
            _validate_queue(
                queue
            )
        )

        claimed = (
            resolved_queue.claim()
        )

        if claimed is None:
            return None

        return self.dispatch_claimed(
            resolved_queue,
            claimed,
            requeue_on_error=(
                requeue_on_error
            ),
        )

    # =========================================================================
    # SNAPSHOT
    # =========================================================================
    def snapshot(
        self,
    ) -> dict[str, Any]:
        with self._lock:
            return {
                "name": (
                    self.name
                ),
                "handler_count": (
                    len(
                        self._handlers
                    )
                ),
                "event_types": (
                    self.event_types()
                ),
                "dispatch_count": (
                    self._dispatch_count
                ),
                "failure_count": (
                    self._failure_count
                ),
            }

    # =========================================================================
    # REPRESENTATION
    # =========================================================================
    def __len__(
        self,
    ) -> int:
        with self._lock:
            return len(
                self._handlers
            )

    def __repr__(
        self,
    ) -> str:
        return (
            f"{self.__class__.__name__}("
            f"name={self.name!r}, "
            f"handler_count={len(self)}"
            f")"
        )