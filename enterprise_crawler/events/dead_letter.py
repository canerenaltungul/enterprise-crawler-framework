from __future__ import annotations

"""
Enterprise Crawler Framework - Dead Letter Queue

Başarısız event delivery'lerinin normal event queue'dan ayrıldıktan sonra
saklanacağı backend-independent DLQ sözleşmesini ve in-memory
implementasyonunu sağlar.

Akış
----
EventWorker
    ↓
RetryPolicy
    ↓
DEAD_LETTER
    ↓
DeadLetterQueueProtocol.store(...)
    ↓
DeadLetterRecord

Bu modül iki katman sağlar:

    DeadLetterQueueProtocol
        ↓
    InMemoryDeadLetterQueue

``DeadLetterQueue`` public convenience alias'i
``InMemoryDeadLetterQueue`` sınıfına işaret eder.

Daha sonra SQLiteDeadLetterQueue aynı protocol'ü implement ederek worker'a
takılabilir.
"""

import copy
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import (
    Any,
    Mapping,
    Optional,
    Protocol,
    runtime_checkable,
)

from enterprise_crawler.contracts import Event


UTC = timezone.utc


# =============================================================================
# EXCEPTIONS
# =============================================================================
class DeadLetterQueueError(RuntimeError):
    """
    Dead-letter queue hatalarının temel sınıfı.
    """


class DeadLetterValidationError(
    DeadLetterQueueError
):
    """
    Dead-letter input contract hatası.
    """


class DuplicateDeadLetterError(
    DeadLetterQueueError
):
    """
    Aynı dead_letter_id ikinci kez saklanmak istendiğinde.
    """


class UnknownDeadLetterError(
    DeadLetterQueueError
):
    """
    Olmayan dead-letter kaydı istendiğinde.
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
        raise DeadLetterValidationError(
            f"{field_name} str olmalıdır."
        )

    normalized = (
        value.strip()
    )

    if not normalized:
        raise DeadLetterValidationError(
            f"{field_name} boş olamaz."
        )

    return normalized


def _normalize_optional_string(
    value: Any,
    *,
    field_name: str,
) -> Optional[str]:
    if value is None:
        return None

    return _normalize_non_empty_string(
        value,
        field_name=(
            field_name
        ),
    )


def _normalize_positive_int(
    value: Any,
    *,
    field_name: str,
) -> int:
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
        raise DeadLetterValidationError(
            f"{field_name} int olmalıdır."
        )

    if value <= 0:
        raise DeadLetterValidationError(
            f"{field_name} sıfırdan büyük olmalıdır."
        )

    return value


def _clone_event(
    event: Event,
) -> Event:
    return Event(
        event_type=(
            event.event_type
        ),
        timestamp=(
            event.timestamp
        ),
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
        raise DeadLetterValidationError(
            "event Event olmalıdır "
            f"| actual={type(event).__name__}"
        )

    event_type = (
        _normalize_non_empty_string(
            event.event_type,
            field_name=(
                "event.event_type"
            ),
        )
    )

    if not isinstance(
        event.timestamp,
        datetime,
    ):
        raise DeadLetterValidationError(
            "event.timestamp datetime olmalıdır."
        )

    if not isinstance(
        event.payload,
        dict,
    ):
        raise DeadLetterValidationError(
            "event.payload dict olmalıdır."
        )

    if not isinstance(
        event.metadata,
        dict,
    ):
        raise DeadLetterValidationError(
            "event.metadata dict olmalıdır."
        )

    cloned = (
        _clone_event(
            event
        )
    )

    cloned.event_type = (
        event_type
    )

    return cloned


def _safe_exception_message(
    error: BaseException,
) -> str:
    message = str(
        error
    ).strip()

    if not message:
        message = (
            error.__class__.__name__
        )

    return message[:8_000]


def _clone_record(
    record: "DeadLetterRecord",
) -> "DeadLetterRecord":
    return DeadLetterRecord(
        dead_letter_id=(
            record.dead_letter_id
        ),
        message_id=(
            record.message_id
        ),
        event=(
            _clone_event(
                record.event
            )
        ),
        delivery_count=(
            record.delivery_count
        ),
        failure_type=(
            record.failure_type
        ),
        failure_message=(
            record.failure_message
        ),
        failed_at=(
            record.failed_at
        ),
        source_queue=(
            record.source_queue
        ),
        claim_token=(
            record.claim_token
        ),
        metadata=copy.deepcopy(
            record.metadata
        ),
    )


# =============================================================================
# RECORD
# =============================================================================
@dataclass(
    slots=True,
    frozen=True,
)
class DeadLetterRecord:
    """
    Tek dead-letter kaydı.
    """

    dead_letter_id: str

    message_id: str

    event: Event

    delivery_count: int

    failure_type: str

    failure_message: str

    failed_at: datetime

    source_queue: Optional[
        str
    ] = None

    claim_token: Optional[
        str
    ] = None

    metadata: dict[
        str,
        Any
    ] = None  # type: ignore[assignment]

    def __post_init__(
        self,
    ) -> None:
        if self.metadata is None:
            object.__setattr__(
                self,
                "metadata",
                {},
            )

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "dead_letter_id": (
                self.dead_letter_id
            ),
            "message_id": (
                self.message_id
            ),
            "event": {
                "event_type": (
                    self.event.event_type
                ),
                "timestamp": (
                    self.event.timestamp.isoformat()
                ),
                "payload": copy.deepcopy(
                    self.event.payload
                ),
                "metadata": copy.deepcopy(
                    self.event.metadata
                ),
            },
            "delivery_count": (
                self.delivery_count
            ),
            "failure_type": (
                self.failure_type
            ),
            "failure_message": (
                self.failure_message
            ),
            "failed_at": (
                self.failed_at.isoformat()
            ),
            "source_queue": (
                self.source_queue
            ),
            "claim_token": (
                self.claim_token
            ),
            "metadata": copy.deepcopy(
                self.metadata
            ),
        }


# =============================================================================
# PROTOCOL
# =============================================================================
@runtime_checkable
class DeadLetterQueueProtocol(
    Protocol
):
    @property
    def count(
        self,
    ) -> int:
        ...

    def store(
        self,
        event: Event,
        *,
        message_id: str,
        delivery_count: int,
        error: BaseException,
        dead_letter_id: Optional[
            str
        ] = None,
        source_queue: Optional[
            str
        ] = None,
        claim_token: Optional[
            str
        ] = None,
        metadata: Optional[
            Mapping[str, Any]
        ] = None,
    ) -> DeadLetterRecord:
        ...

    def get(
        self,
        dead_letter_id: str,
    ) -> DeadLetterRecord:
        ...

    def remove(
        self,
        dead_letter_id: str,
    ) -> DeadLetterRecord:
        ...

    def snapshot(
        self,
    ) -> dict[str, Any]:
        ...


# =============================================================================
# IN-MEMORY DLQ
# =============================================================================
class InMemoryDeadLetterQueue:
    """
    Thread-safe process-local dead-letter queue.
    """

    def __init__(
        self,
        *,
        name: str = "dead-letter",
    ) -> None:
        self.name = (
            _normalize_non_empty_string(
                name,
                field_name="name",
            )
        )

        self._lock = (
            threading.RLock()
        )

        self._records: dict[
            str,
            DeadLetterRecord,
        ] = {}

        self._order: list[
            str
        ] = []

        self._stored_count = 0

        self._removed_count = 0

    # =========================================================================
    # STATE
    # =========================================================================
    @property
    def count(
        self,
    ) -> int:
        with self._lock:
            return len(
                self._records
            )

    @property
    def is_empty(
        self,
    ) -> bool:
        return (
            self.count
            == 0
        )

    # =========================================================================
    # STORE
    # =========================================================================
    def store(
        self,
        event: Event,
        *,
        message_id: str,
        delivery_count: int,
        error: BaseException,
        dead_letter_id: Optional[
            str
        ] = None,
        source_queue: Optional[
            str
        ] = None,
        claim_token: Optional[
            str
        ] = None,
        metadata: Optional[
            Mapping[str, Any]
        ] = None,
    ) -> DeadLetterRecord:
        normalized_event = (
            _validate_event(
                event
            )
        )

        resolved_message_id = (
            _normalize_non_empty_string(
                message_id,
                field_name=(
                    "message_id"
                ),
            )
        )

        resolved_delivery_count = (
            _normalize_positive_int(
                delivery_count,
                field_name=(
                    "delivery_count"
                ),
            )
        )

        if not isinstance(
            error,
            BaseException,
        ):
            raise DeadLetterValidationError(
                "error BaseException olmalıdır."
            )

        if dead_letter_id is None:
            resolved_dead_letter_id = (
                uuid.uuid4().hex
            )

        else:
            resolved_dead_letter_id = (
                _normalize_non_empty_string(
                    dead_letter_id,
                    field_name=(
                        "dead_letter_id"
                    ),
                )
            )

        resolved_source_queue = (
            _normalize_optional_string(
                source_queue,
                field_name=(
                    "source_queue"
                ),
            )
        )

        resolved_claim_token = (
            _normalize_optional_string(
                claim_token,
                field_name=(
                    "claim_token"
                ),
            )
        )

        if metadata is None:
            resolved_metadata: dict[
                str,
                Any
            ] = {}

        else:
            if not isinstance(
                metadata,
                Mapping,
            ):
                raise DeadLetterValidationError(
                    "metadata Mapping olmalıdır."
                )

            resolved_metadata = (
                copy.deepcopy(
                    dict(
                        metadata
                    )
                )
            )

        record = DeadLetterRecord(
            dead_letter_id=(
                resolved_dead_letter_id
            ),
            message_id=(
                resolved_message_id
            ),
            event=(
                normalized_event
            ),
            delivery_count=(
                resolved_delivery_count
            ),
            failure_type=(
                error.__class__.__name__
            ),
            failure_message=(
                _safe_exception_message(
                    error
                )
            ),
            failed_at=(
                utc_now()
            ),
            source_queue=(
                resolved_source_queue
            ),
            claim_token=(
                resolved_claim_token
            ),
            metadata=(
                resolved_metadata
            ),
        )

        with self._lock:
            if (
                resolved_dead_letter_id
                in self._records
            ):
                raise DuplicateDeadLetterError(
                    "dead_letter_id zaten kayıtlı "
                    f"| dead_letter_id="
                    f"{resolved_dead_letter_id!r}"
                )

            self._records[
                resolved_dead_letter_id
            ] = record

            self._order.append(
                resolved_dead_letter_id
            )

            self._stored_count += 1

        return _clone_record(
            record
        )

    # =========================================================================
    # GET
    # =========================================================================
    def get(
        self,
        dead_letter_id: str,
    ) -> DeadLetterRecord:
        resolved = (
            _normalize_non_empty_string(
                dead_letter_id,
                field_name=(
                    "dead_letter_id"
                ),
            )
        )

        with self._lock:
            try:
                record = (
                    self._records[
                        resolved
                    ]
                )

            except KeyError as exc:
                raise UnknownDeadLetterError(
                    "Dead-letter kaydı bulunamadı "
                    f"| dead_letter_id={resolved!r}"
                ) from exc

            return _clone_record(
                record
            )

    # =========================================================================
    # REMOVE
    # =========================================================================
    def remove(
        self,
        dead_letter_id: str,
    ) -> DeadLetterRecord:
        resolved = (
            _normalize_non_empty_string(
                dead_letter_id,
                field_name=(
                    "dead_letter_id"
                ),
            )
        )

        with self._lock:
            try:
                record = (
                    self._records.pop(
                        resolved
                    )
                )

            except KeyError as exc:
                raise UnknownDeadLetterError(
                    "Dead-letter kaydı bulunamadı "
                    f"| dead_letter_id={resolved!r}"
                ) from exc

            try:
                self._order.remove(
                    resolved
                )

            except ValueError:
                pass

            self._removed_count += 1

            return _clone_record(
                record
            )

    # =========================================================================
    # LIST
    # =========================================================================
    def records(
        self,
    ) -> list[
        DeadLetterRecord
    ]:
        with self._lock:
            return [
                _clone_record(
                    self._records[
                        dead_letter_id
                    ]
                )
                for dead_letter_id
                in self._order
                if dead_letter_id
                in self._records
            ]

    # =========================================================================
    # CONTAINS
    # =========================================================================
    def contains(
        self,
        dead_letter_id: str,
    ) -> bool:
        if not isinstance(
            dead_letter_id,
            str,
        ):
            return False

        normalized = (
            dead_letter_id.strip()
        )

        if not normalized:
            return False

        with self._lock:
            return (
                normalized
                in self._records
            )

    def __contains__(
        self,
        dead_letter_id: object,
    ) -> bool:
        if not isinstance(
            dead_letter_id,
            str,
        ):
            return False

        return self.contains(
            dead_letter_id
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
                "count": (
                    len(
                        self._records
                    )
                ),
                "stored_count": (
                    self._stored_count
                ),
                "removed_count": (
                    self._removed_count
                ),
                "dead_letter_ids": list(
                    self._order
                ),
            }

    # =========================================================================
    # REPRESENTATION
    # =========================================================================
    def __len__(
        self,
    ) -> int:
        return (
            self.count
        )

    def __repr__(
        self,
    ) -> str:
        return (
            f"{self.__class__.__name__}("
            f"name={self.name!r}, "
            f"count={self.count}"
            f")"
        )


# Public convenience name.
DeadLetterQueue = (
    InMemoryDeadLetterQueue
)