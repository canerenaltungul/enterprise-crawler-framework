from __future__ import annotations

"""
Enterprise Crawler Framework - Event Queue Contracts and In-Memory Queue

Bu modül iki sorumluluk taşır:

1. Event queue implementasyonlarının ortak runtime sözleşmesini tanımlar.
2. Framework'ün process-local InMemoryEventQueue implementasyonunu sağlar.

Queue Semantiği
---------------
Event
    ↓
publish()
    ↓
pending
    ↓
claim()
    ↓
ClaimedEvent
    ↓
ack() / nack()

Concrete implementation bağımsız contract:

    EventQueueProtocol

Bu protocol sayesinde EventDispatcher ve EventWorker belirli bir queue
implementasyonuna bağımlı değildir.

Desteklenen implementasyonlar:

- InMemoryEventQueue
- SQLiteEventQueue

Event contract'ı runtime state ile kirletilmez.

Queue runtime alanları:

- message_id
- claim_token
- delivery_count
- published_at
- claimed_at
- lease_expires_at [persistent queue implementasyonlarında optional]
- next_attempt_at [pending scheduled retry state]

Scheduled Retry
---------------
``nack(requeue=True, retry_delay_seconds=N)`` event'i tekrar pending duruma
alır ancak N > 0 ise event yalnız belirtilen delay geçtikten sonra yeniden
claim edilebilir.

Retry delay ile claim lease birbirinden farklı kavramlardır:

- lease_expires_at:
    claimed event ownership deadline

- next_attempt_at:
    pending event'in yeniden claim edilebileceği en erken zaman

Eligible event seçimi:

    pending
    AND
    (
        next_attempt_at IS NULL
        OR next_attempt_at <= now
    )

şeklindedir.

Henüz zamanı gelmemiş eski bir event, daha sonra publish edilmiş fakat due olan
event'leri bloke etmez. Eligible event'ler kendi aralarında FIFO semantiğini
korur.
"""

import copy
import math
import threading
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from numbers import Real
from typing import (
    Any,
    Callable,
    Optional,
    Protocol,
    runtime_checkable,
)

from enterprise_crawler.contracts import Event


UTC = timezone.utc


# =============================================================================
# EXCEPTIONS
# =============================================================================
class EventQueueError(RuntimeError):
    """
    Event queue hatalarının temel sınıfı.
    """


class EventQueueValidationError(
    EventQueueError
):
    """
    Queue input contract doğrulaması başarısız olduğunda.
    """


class DuplicateEventMessageError(
    EventQueueError
):
    """
    Aynı message_id ikinci kez publish edilmeye çalışıldığında.
    """


class UnknownEventMessageError(
    EventQueueError
):
    """
    Queue içinde bulunmayan message_id kullanıldığında.
    """


class EventNotClaimedError(
    EventQueueError
):
    """
    Pending event ack/nack edilmeye çalışıldığında.
    """


class EventClaimOwnershipError(
    EventQueueError
):
    """
    Yanlış veya stale claim_token kullanıldığında.
    """


class EventQueueClosedError(
    EventQueueError
):
    """
    Kapatılmış persistent queue kullanılmaya çalışıldığında.
    """


# =============================================================================
# HELPERS
# =============================================================================
def utc_now() -> datetime:
    return datetime.now(
        UTC
    )


def _normalize_non_empty_string(
    value: Any,
    *,
    field_name: str,
) -> str:
    if not isinstance(
        value,
        str,
    ):
        raise EventQueueValidationError(
            f"{field_name} str olmalıdır."
        )

    normalized = (
        value.strip()
    )

    if not normalized:
        raise EventQueueValidationError(
            f"{field_name} boş olamaz."
        )

    return normalized


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
        raise EventQueueValidationError(
            f"{field_name} sayı olmalıdır."
        )

    normalized = float(
        value
    )

    if (
        not math.isfinite(
            normalized
        )
        or normalized < 0
    ):
        raise EventQueueValidationError(
            f"{field_name} negatif olmayan "
            "sonlu sayı olmalıdır."
        )

    return normalized


def _normalize_clock(
    value: Any,
) -> Callable[
    [],
    datetime,
]:
    if not callable(
        value
    ):
        raise EventQueueValidationError(
            "clock callable olmalıdır."
        )

    return value


def _read_clock(
    clock: Callable[
        [],
        datetime,
    ],
) -> datetime:
    value = (
        clock()
    )

    if not isinstance(
        value,
        datetime,
    ):
        raise EventQueueValidationError(
            "clock datetime döndürmelidir."
        )

    if value.tzinfo is None:
        raise EventQueueValidationError(
            "clock timezone-aware datetime döndürmelidir."
        )

    return value.astimezone(
        UTC
    )


def _clone_event(
    event: Event,
) -> Event:
    return Event(
        event_type=event.event_type,
        timestamp=event.timestamp,
        payload=copy.deepcopy(
            event.payload
        ),
        metadata=copy.deepcopy(
            event.metadata
        ),
    )


def _validate_event(
    event: Any,
) -> Event:
    if not isinstance(
        event,
        Event,
    ):
        raise EventQueueValidationError(
            "event Event olmalıdır "
            f"| actual={type(event).__name__}"
        )

    event_type = (
        _normalize_non_empty_string(
            event.event_type,
            field_name="event.event_type",
        )
    )

    if not isinstance(
        event.timestamp,
        datetime,
    ):
        raise EventQueueValidationError(
            "event.timestamp datetime olmalıdır."
        )

    if not isinstance(
        event.payload,
        dict,
    ):
        raise EventQueueValidationError(
            "event.payload dict olmalıdır."
        )

    if not isinstance(
        event.metadata,
        dict,
    ):
        raise EventQueueValidationError(
            "event.metadata dict olmalıdır."
        )

    normalized_event = (
        _clone_event(
            event
        )
    )

    normalized_event.event_type = (
        event_type
    )

    return normalized_event


# =============================================================================
# PUBLIC RESULT CONTRACTS
# =============================================================================
@dataclass(
    slots=True,
    frozen=True,
)
class PublishedEvent:
    """
    Başarılı publish operasyonunun sonucu.
    """

    message_id: str

    event_type: str

    published_at: datetime

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "message_id": (
                self.message_id
            ),
            "event_type": (
                self.event_type
            ),
            "published_at": (
                self.published_at.isoformat()
            ),
        }


@dataclass(
    slots=True,
    frozen=True,
)
class ClaimedEvent:
    """
    Consumer tarafından sahiplenilmiş event.

    claim_token:
        Queue ownership token'ı.

    lease_expires_at:
        Durable queue implementasyonlarında claim lease deadline.

        InMemoryEventQueue lease kullanmadığı için None'dır.
    """

    message_id: str

    claim_token: str

    event: Event

    delivery_count: int

    published_at: datetime

    claimed_at: datetime

    lease_expires_at: Optional[
        datetime
    ] = None

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "message_id": (
                self.message_id
            ),
            "claim_token": (
                self.claim_token
            ),
            "event_type": (
                self.event.event_type
            ),
            "delivery_count": (
                self.delivery_count
            ),
            "published_at": (
                self.published_at.isoformat()
            ),
            "claimed_at": (
                self.claimed_at.isoformat()
            ),
            "lease_expires_at": (
                self.lease_expires_at.isoformat()
                if self.lease_expires_at
                is not None
                else None
            ),
        }


# =============================================================================
# QUEUE PROTOCOL
# =============================================================================
@runtime_checkable
class EventQueueProtocol(
    Protocol
):
    """
    EventWorker / EventDispatcher tarafından kullanılan ortak queue contract.

    Implementasyonun storage teknolojisi önemli değildir.

    Scheduled retry backend-independent contract'ın parçasıdır.
    """

    @property
    def active_count(
        self,
    ) -> int:
        ...

    @property
    def pending_count(
        self,
    ) -> int:
        ...

    @property
    def claimed_count(
        self,
    ) -> int:
        ...

    def publish(
        self,
        event: Event,
        *,
        message_id: Optional[
            str
        ] = None,
    ) -> PublishedEvent:
        ...

    def claim(
        self,
    ) -> Optional[
        ClaimedEvent
    ]:
        ...

    def ack(
        self,
        message_id: str,
        claim_token: str,
    ) -> Event:
        ...

    def nack(
        self,
        message_id: str,
        claim_token: str,
        *,
        requeue: bool = True,
        retry_delay_seconds: float = 0.0,
    ) -> Event:
        ...

    def snapshot(
        self,
    ) -> dict[str, Any]:
        ...


# =============================================================================
# IN-MEMORY INTERNAL ENTRY
# =============================================================================
@dataclass(slots=True)
class _QueueEntry:
    message_id: str

    event: Event

    published_at: datetime

    delivery_count: int = 0

    claimed: bool = False

    claim_token: Optional[
        str
    ] = None

    claimed_at: Optional[
        datetime
    ] = None

    next_attempt_at: Optional[
        datetime
    ] = None


# =============================================================================
# IN-MEMORY QUEUE
# =============================================================================
class InMemoryEventQueue:
    """
    Thread-safe FIFO event queue.

    Bu sınıf yalnız process memory'sinde yaşar.

    Özellikler:

    - FIFO pending order
    - explicit publish
    - atomic claim
    - claim ownership token
    - ack
    - nack + immediate requeue
    - nack + delayed requeue
    - nack + discard
    - duplicate message_id protection
    - delivery counting
    - deterministic runtime snapshot
    - injectable clock
    """

    def __init__(
        self,
        *,
        name: str = "default",
        clock: Optional[
            Callable[
                [],
                datetime,
            ]
        ] = None,
    ) -> None:
        self.name = (
            _normalize_non_empty_string(
                name,
                field_name="name",
            )
        )

        self._clock = (
            _normalize_clock(
                clock
            )
            if clock is not None
            else utc_now
        )

        self._lock = (
            threading.RLock()
        )

        self._pending: deque[
            str
        ] = deque()

        self._entries: dict[
            str,
            _QueueEntry,
        ] = {}

        self._published_count = 0
        self._claim_count = 0
        self._acked_count = 0
        self._nacked_count = 0
        self._requeued_count = 0
        self._discarded_count = 0

    # =========================================================================
    # CLOCK
    # =========================================================================
    def _now(
        self,
    ) -> datetime:
        return _read_clock(
            self._clock
        )

    # =========================================================================
    # COUNTERS
    # =========================================================================
    @property
    def pending_count(
        self,
    ) -> int:
        with self._lock:
            return len(
                self._pending
            )

    @property
    def claimed_count(
        self,
    ) -> int:
        with self._lock:
            return sum(
                1
                for entry
                in self._entries.values()
                if entry.claimed
            )

    @property
    def active_count(
        self,
    ) -> int:
        with self._lock:
            return len(
                self._entries
            )

    @property
    def is_empty(
        self,
    ) -> bool:
        return (
            self.active_count
            == 0
        )

    # =========================================================================
    # PUBLISH
    # =========================================================================
    def publish(
        self,
        event: Event,
        *,
        message_id: Optional[
            str
        ] = None,
    ) -> PublishedEvent:
        normalized_event = (
            _validate_event(
                event
            )
        )

        if message_id is None:
            resolved_message_id = (
                uuid.uuid4().hex
            )

        else:
            resolved_message_id = (
                _normalize_non_empty_string(
                    message_id,
                    field_name="message_id",
                )
            )

        published_at = (
            self._now()
        )

        with self._lock:
            if (
                resolved_message_id
                in self._entries
            ):
                raise DuplicateEventMessageError(
                    "message_id zaten queue içinde "
                    f"| message_id={resolved_message_id!r}"
                )

            entry = _QueueEntry(
                message_id=(
                    resolved_message_id
                ),
                event=(
                    normalized_event
                ),
                published_at=(
                    published_at
                ),
            )

            self._entries[
                resolved_message_id
            ] = entry

            self._pending.append(
                resolved_message_id
            )

            self._published_count += 1

        return PublishedEvent(
            message_id=(
                resolved_message_id
            ),
            event_type=(
                normalized_event.event_type
            ),
            published_at=(
                published_at
            ),
        )

    # =========================================================================
    # CLAIM
    # =========================================================================
    def claim(
        self,
    ) -> Optional[
        ClaimedEvent
    ]:
        with self._lock:
            now = (
                self._now()
            )

            selected_message_id: Optional[
                str
            ] = None

            selected_entry: Optional[
                _QueueEntry
            ] = None

            # Deque rotate edilmez.
            #
            # Böylece due olmayan eski bir event atlanırken geri kalan
            # pending event'lerin relative FIFO sırası değişmez.
            for message_id in tuple(
                self._pending
            ):
                entry = (
                    self._entries.get(
                        message_id
                    )
                )

                if entry is None:
                    continue

                if entry.claimed:
                    continue

                next_attempt_at = (
                    entry.next_attempt_at
                )

                if (
                    next_attempt_at
                    is not None
                    and next_attempt_at
                    > now
                ):
                    continue

                selected_message_id = (
                    message_id
                )

                selected_entry = (
                    entry
                )

                break

            if (
                selected_message_id
                is None
                or selected_entry
                is None
            ):
                return None

            try:
                self._pending.remove(
                    selected_message_id
                )

            except ValueError:
                # Internal state değişmişse fail-closed davran.
                return None

            claim_token = (
                uuid.uuid4().hex
            )

            claimed_at = (
                now
            )

            selected_entry.claimed = True

            selected_entry.claim_token = (
                claim_token
            )

            selected_entry.claimed_at = (
                claimed_at
            )

            # Artık scheduled waiting state'inde değil.
            selected_entry.next_attempt_at = None

            selected_entry.delivery_count += 1

            self._claim_count += 1

            return ClaimedEvent(
                message_id=(
                    selected_entry.message_id
                ),
                claim_token=(
                    claim_token
                ),
                event=_clone_event(
                    selected_entry.event
                ),
                delivery_count=(
                    selected_entry.delivery_count
                ),
                published_at=(
                    selected_entry.published_at
                ),
                claimed_at=(
                    claimed_at
                ),
                lease_expires_at=None,
            )

    # =========================================================================
    # ACK
    # =========================================================================
    def ack(
        self,
        message_id: str,
        claim_token: str,
    ) -> Event:
        resolved_message_id = (
            _normalize_non_empty_string(
                message_id,
                field_name="message_id",
            )
        )

        resolved_claim_token = (
            _normalize_non_empty_string(
                claim_token,
                field_name="claim_token",
            )
        )

        with self._lock:
            entry = self._require_entry(
                resolved_message_id
            )

            self._require_claim_owner(
                entry,
                resolved_claim_token,
            )

            removed = (
                self._entries.pop(
                    resolved_message_id
                )
            )

            self._acked_count += 1

            return _clone_event(
                removed.event
            )

    # =========================================================================
    # NACK
    # =========================================================================
    def nack(
        self,
        message_id: str,
        claim_token: str,
        *,
        requeue: bool = True,
        retry_delay_seconds: float = 0.0,
    ) -> Event:
        resolved_message_id = (
            _normalize_non_empty_string(
                message_id,
                field_name="message_id",
            )
        )

        resolved_claim_token = (
            _normalize_non_empty_string(
                claim_token,
                field_name="claim_token",
            )
        )

        if not isinstance(
            requeue,
            bool,
        ):
            raise EventQueueValidationError(
                "requeue bool olmalıdır."
            )

        resolved_retry_delay = (
            _normalize_non_negative_float(
                retry_delay_seconds,
                field_name=(
                    "retry_delay_seconds"
                ),
            )
        )

        if (
            not requeue
            and resolved_retry_delay
            != 0.0
        ):
            raise EventQueueValidationError(
                "retry_delay_seconds yalnız "
                "requeue=True ile kullanılabilir."
            )

        with self._lock:
            entry = self._require_entry(
                resolved_message_id
            )

            self._require_claim_owner(
                entry,
                resolved_claim_token,
            )

            self._nacked_count += 1

            event_copy = (
                _clone_event(
                    entry.event
                )
            )

            if requeue:
                entry.claimed = False

                entry.claim_token = None

                entry.claimed_at = None

                if (
                    resolved_retry_delay
                    > 0.0
                ):
                    entry.next_attempt_at = (
                        self._now()
                        + timedelta(
                            seconds=(
                                resolved_retry_delay
                            )
                        )
                    )

                else:
                    entry.next_attempt_at = None

                self._pending.append(
                    resolved_message_id
                )

                self._requeued_count += 1

            else:
                self._entries.pop(
                    resolved_message_id
                )

                self._discarded_count += 1

            return event_copy

    # =========================================================================
    # LOOKUP
    # =========================================================================
    def contains(
        self,
        message_id: str,
    ) -> bool:
        if not isinstance(
            message_id,
            str,
        ):
            return False

        normalized = (
            message_id.strip()
        )

        if not normalized:
            return False

        with self._lock:
            return (
                normalized
                in self._entries
            )

    def __contains__(
        self,
        message_id: object,
    ) -> bool:
        if not isinstance(
            message_id,
            str,
        ):
            return False

        return self.contains(
            message_id
        )

    # =========================================================================
    # INTERNAL VALIDATION
    # =========================================================================
    def _require_entry(
        self,
        message_id: str,
    ) -> _QueueEntry:
        try:
            return self._entries[
                message_id
            ]

        except KeyError as exc:
            raise UnknownEventMessageError(
                "Event message bulunamadı "
                f"| message_id={message_id!r}"
            ) from exc

    @staticmethod
    def _require_claim_owner(
        entry: _QueueEntry,
        claim_token: str,
    ) -> None:
        if not entry.claimed:
            raise EventNotClaimedError(
                "Event claim edilmemiş "
                f"| message_id={entry.message_id!r}"
            )

        if (
            entry.claim_token
            != claim_token
        ):
            raise EventClaimOwnershipError(
                "Claim token eşleşmiyor "
                f"| message_id={entry.message_id!r}"
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
                "backend": (
                    "memory"
                ),
                "pending_count": (
                    len(
                        self._pending
                    )
                ),
                "claimed_count": sum(
                    1
                    for entry
                    in self._entries.values()
                    if entry.claimed
                ),
                "active_count": (
                    len(
                        self._entries
                    )
                ),
                "published_count": (
                    self._published_count
                ),
                "claim_count": (
                    self._claim_count
                ),
                "acked_count": (
                    self._acked_count
                ),
                "nacked_count": (
                    self._nacked_count
                ),
                "requeued_count": (
                    self._requeued_count
                ),
                "discarded_count": (
                    self._discarded_count
                ),
            }

    # =========================================================================
    # REPRESENTATION
    # =========================================================================
    def __len__(
        self,
    ) -> int:
        return (
            self.active_count
        )

    def __repr__(
        self,
    ) -> str:
        return (
            f"{self.__class__.__name__}("
            f"name={self.name!r}, "
            f"pending_count={self.pending_count}, "
            f"claimed_count={self.claimed_count}"
            f")"
        )