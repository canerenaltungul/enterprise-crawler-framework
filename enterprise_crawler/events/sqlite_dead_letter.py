from __future__ import annotations

"""
Enterprise Crawler Framework - SQLite Dead Letter Queue

DeadLetterQueueProtocol için durable SQLite backend.

Amaç
----
InMemoryDeadLetterQueue process-local olduğu için process kapanınca kayıtlar
kaybolur.

SQLiteDeadLetterQueue ise dead-letter kayıtlarını kalıcı olarak saklar.

Desteklenen davranışlar
-----------------------
- persistent dead-letter storage
- reopen sonrası kayıtların korunması
- deterministic insertion order
- duplicate dead_letter_id protection
- JSON-safe event payload / metadata
- Unicode desteği
- queue name isolation
- thread-safe erişim
- multiple SQLiteDeadLetterQueue instance desteği
- explicit close lifecycle

Schema
------
Aynı SQLite database birden fazla logical dead-letter queue barındırabilir.

Primary identity:

    (queue_name, dead_letter_id)
"""

import copy
import json
import math
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    Mapping,
    Optional,
)

from enterprise_crawler.contracts import Event
from enterprise_crawler.events.dead_letter import (
    DeadLetterQueueError,
    DeadLetterQueueProtocol,
    DeadLetterRecord,
    DeadLetterValidationError,
    DuplicateDeadLetterError,
    UnknownDeadLetterError,
)


UTC = timezone.utc


# =============================================================================
# EXCEPTIONS
# =============================================================================
class SQLiteDeadLetterQueueError(
    DeadLetterQueueError
):
    """
    SQLite dead-letter backend hatası.
    """


class SQLiteDeadLetterQueueClosedError(
    SQLiteDeadLetterQueueError
):
    """
    Kapatılmış SQLiteDeadLetterQueue kullanılmaya çalışıldığında.
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
        field_name=field_name,
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


def _normalize_positive_float(
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
            (
                int,
                float,
            ),
        )
    ):
        raise DeadLetterValidationError(
            f"{field_name} sayı olmalıdır."
        )

    normalized = float(
        value
    )

    if (
        not math.isfinite(
            normalized
        )
        or normalized <= 0
    ):
        raise DeadLetterValidationError(
            f"{field_name} pozitif ve finite olmalıdır."
        )

    return normalized


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


def _clone_record(
    record: DeadLetterRecord,
) -> DeadLetterRecord:
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


def _json_dumps(
    value: Any,
    *,
    field_name: str,
) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(
                ",",
                ":",
            ),
            sort_keys=True,
        )

    except (
        TypeError,
        ValueError,
    ) as exc:
        raise DeadLetterValidationError(
            f"{field_name} JSON serializable olmalıdır."
        ) from exc


def _json_loads(
    value: str,
) -> Any:
    try:
        return json.loads(
            value
        )

    except json.JSONDecodeError as exc:
        raise SQLiteDeadLetterQueueError(
            "SQLite dead-letter JSON decode başarısız."
        ) from exc


def _datetime_to_text(
    value: datetime,
) -> str:
    return value.isoformat()


def _datetime_from_text(
    value: str,
) -> datetime:
    try:
        return datetime.fromisoformat(
            value
        )

    except ValueError as exc:
        raise SQLiteDeadLetterQueueError(
            "SQLite dead-letter datetime decode başarısız "
            f"| value={value!r}"
        ) from exc


# =============================================================================
# SQLITE DLQ
# =============================================================================
class SQLiteDeadLetterQueue:
    """
    Durable SQLite dead-letter queue.

    Örnek::

        dlq = SQLiteDeadLetterQueue(
            "events.sqlite3",
            name="failed-events",
        )

        dlq.store(
            event,
            message_id="message-1",
            delivery_count=3,
            error=RuntimeError("boom"),
        )
    """

    def __init__(
        self,
        database_path: str | Path,
        *,
        name: str = "dead-letter",
        timeout_seconds: float = 5.0,
    ) -> None:
        if not isinstance(
            database_path,
            (
                str,
                Path,
            ),
        ):
            raise DeadLetterValidationError(
                "database_path str veya Path olmalıdır."
            )

        self.database_path = (
            Path(
                database_path
            )
        )

        self.name = (
            _normalize_non_empty_string(
                name,
                field_name="name",
            )
        )

        self.timeout_seconds = (
            _normalize_positive_float(
                timeout_seconds,
                field_name=(
                    "timeout_seconds"
                ),
            )
        )

        if (
            self.database_path.exists()
            and self.database_path.is_dir()
        ):
            raise SQLiteDeadLetterQueueError(
                "database_path directory olamaz "
                f"| path={self.database_path}"
            )

        parent = (
            self.database_path.parent
        )

        parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._lock = (
            threading.RLock()
        )

        self._closed = False

        try:
            self._connection = (
                sqlite3.connect(
                    str(
                        self.database_path
                    ),
                    timeout=(
                        self.timeout_seconds
                    ),
                    isolation_level=None,
                    check_same_thread=False,
                )
            )

        except sqlite3.Error as exc:
            raise SQLiteDeadLetterQueueError(
                "SQLite bağlantısı açılamadı "
                f"| path={self.database_path} "
                f"| error={exc}"
            ) from exc

        try:
            with self._lock:
                self._connection.execute(
                    "PRAGMA journal_mode=WAL"
                )

                self._connection.execute(
                    "PRAGMA synchronous=NORMAL"
                )

                self._connection.execute(
                    "PRAGMA foreign_keys=ON"
                )

                self._initialize_schema()

        except Exception:
            try:
                self._connection.close()

            except Exception:
                pass

            raise

    # =========================================================================
    # SCHEMA
    # =========================================================================
    def _initialize_schema(
        self,
    ) -> None:
        try:
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS dead_letter_messages (
                    queue_name TEXT NOT NULL,
                    dead_letter_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,

                    event_type TEXT NOT NULL,
                    event_timestamp TEXT NOT NULL,
                    event_payload_json TEXT NOT NULL,
                    event_metadata_json TEXT NOT NULL,

                    delivery_count INTEGER NOT NULL,

                    failure_type TEXT NOT NULL,
                    failure_message TEXT NOT NULL,
                    failed_at TEXT NOT NULL,

                    source_queue TEXT,
                    claim_token TEXT,
                    record_metadata_json TEXT NOT NULL,

                    sequence_id INTEGER PRIMARY KEY AUTOINCREMENT,

                    UNIQUE (
                        queue_name,
                        dead_letter_id
                    )
                )
                """
            )

            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_dead_letter_messages_queue_sequence
                ON dead_letter_messages (
                    queue_name,
                    sequence_id
                )
                """
            )

            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_dead_letter_messages_queue_message
                ON dead_letter_messages (
                    queue_name,
                    message_id
                )
                """
            )

        except sqlite3.Error as exc:
            raise SQLiteDeadLetterQueueError(
                "SQLite dead-letter schema oluşturulamadı "
                f"| error={exc}"
            ) from exc

    # =========================================================================
    # STATE
    # =========================================================================
    @property
    def is_closed(
        self,
    ) -> bool:
        with self._lock:
            return (
                self._closed
            )

    @property
    def count(
        self,
    ) -> int:
        self._ensure_open()

        with self._lock:
            try:
                row = (
                    self._connection.execute(
                        """
                        SELECT COUNT(*)
                        FROM dead_letter_messages
                        WHERE queue_name = ?
                        """,
                        (
                            self.name,
                        ),
                    ).fetchone()
                )

            except sqlite3.Error as exc:
                raise SQLiteDeadLetterQueueError(
                    "Dead-letter count okunamadı "
                    f"| error={exc}"
                ) from exc

        return int(
            row[
                0
            ]
            if row
            else 0
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
    # GUARD
    # =========================================================================
    def _ensure_open(
        self,
    ) -> None:
        if self.is_closed:
            raise SQLiteDeadLetterQueueClosedError(
                "SQLiteDeadLetterQueue kapalı "
                f"| queue={self.name}"
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
        self._ensure_open()

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

        event_payload_json = (
            _json_dumps(
                normalized_event.payload,
                field_name=(
                    "event.payload"
                ),
            )
        )

        event_metadata_json = (
            _json_dumps(
                normalized_event.metadata,
                field_name=(
                    "event.metadata"
                ),
            )
        )

        record_metadata_json = (
            _json_dumps(
                resolved_metadata,
                field_name="metadata",
            )
        )

        failed_at = (
            utc_now()
        )

        failure_type = (
            error.__class__.__name__
        )

        failure_message = (
            _safe_exception_message(
                error
            )
        )

        with self._lock:
            try:
                self._connection.execute(
                    "BEGIN IMMEDIATE"
                )

                try:
                    self._connection.execute(
                        """
                        INSERT INTO dead_letter_messages (
                            queue_name,
                            dead_letter_id,
                            message_id,
                            event_type,
                            event_timestamp,
                            event_payload_json,
                            event_metadata_json,
                            delivery_count,
                            failure_type,
                            failure_message,
                            failed_at,
                            source_queue,
                            claim_token,
                            record_metadata_json
                        )
                        VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        )
                        """,
                        (
                            self.name,
                            resolved_dead_letter_id,
                            resolved_message_id,
                            normalized_event.event_type,
                            _datetime_to_text(
                                normalized_event.timestamp
                            ),
                            event_payload_json,
                            event_metadata_json,
                            resolved_delivery_count,
                            failure_type,
                            failure_message,
                            _datetime_to_text(
                                failed_at
                            ),
                            resolved_source_queue,
                            resolved_claim_token,
                            record_metadata_json,
                        ),
                    )

                except sqlite3.IntegrityError as exc:
                    self._connection.execute(
                        "ROLLBACK"
                    )

                    raise DuplicateDeadLetterError(
                        "dead_letter_id zaten kayıtlı "
                        f"| queue={self.name!r} "
                        f"| dead_letter_id="
                        f"{resolved_dead_letter_id!r}"
                    ) from exc

                except sqlite3.Error:
                    self._connection.execute(
                        "ROLLBACK"
                    )

                    raise

                self._connection.execute(
                    "COMMIT"
                )

            except DuplicateDeadLetterError:
                raise

            except sqlite3.Error as exc:
                raise SQLiteDeadLetterQueueError(
                    "Dead-letter kayıt edilemedi "
                    f"| queue={self.name} "
                    f"| dead_letter_id="
                    f"{resolved_dead_letter_id!r} "
                    f"| error={exc}"
                ) from exc

        return DeadLetterRecord(
            dead_letter_id=(
                resolved_dead_letter_id
            ),
            message_id=(
                resolved_message_id
            ),
            event=(
                _clone_event(
                    normalized_event
                )
            ),
            delivery_count=(
                resolved_delivery_count
            ),
            failure_type=(
                failure_type
            ),
            failure_message=(
                failure_message
            ),
            failed_at=(
                failed_at
            ),
            source_queue=(
                resolved_source_queue
            ),
            claim_token=(
                resolved_claim_token
            ),
            metadata=copy.deepcopy(
                resolved_metadata
            ),
        )

    # =========================================================================
    # ROW DECODING
    # =========================================================================
    @staticmethod
    def _row_to_record(
        row: sqlite3.Row | tuple[Any, ...],
    ) -> DeadLetterRecord:
        (
            dead_letter_id,
            message_id,
            event_type,
            event_timestamp,
            event_payload_json,
            event_metadata_json,
            delivery_count,
            failure_type,
            failure_message,
            failed_at,
            source_queue,
            claim_token,
            record_metadata_json,
        ) = row

        payload = (
            _json_loads(
                event_payload_json
            )
        )

        event_metadata = (
            _json_loads(
                event_metadata_json
            )
        )

        record_metadata = (
            _json_loads(
                record_metadata_json
            )
        )

        if not isinstance(
            payload,
            dict,
        ):
            raise SQLiteDeadLetterQueueError(
                "Stored event payload dict değil."
            )

        if not isinstance(
            event_metadata,
            dict,
        ):
            raise SQLiteDeadLetterQueueError(
                "Stored event metadata dict değil."
            )

        if not isinstance(
            record_metadata,
            dict,
        ):
            raise SQLiteDeadLetterQueueError(
                "Stored record metadata dict değil."
            )

        return DeadLetterRecord(
            dead_letter_id=(
                str(
                    dead_letter_id
                )
            ),
            message_id=(
                str(
                    message_id
                )
            ),
            event=Event(
                event_type=(
                    str(
                        event_type
                    )
                ),
                timestamp=(
                    _datetime_from_text(
                        str(
                            event_timestamp
                        )
                    )
                ),
                payload=payload,
                metadata=(
                    event_metadata
                ),
            ),
            delivery_count=(
                int(
                    delivery_count
                )
            ),
            failure_type=(
                str(
                    failure_type
                )
            ),
            failure_message=(
                str(
                    failure_message
                )
            ),
            failed_at=(
                _datetime_from_text(
                    str(
                        failed_at
                    )
                )
            ),
            source_queue=(
                str(
                    source_queue
                )
                if source_queue
                is not None
                else None
            ),
            claim_token=(
                str(
                    claim_token
                )
                if claim_token
                is not None
                else None
            ),
            metadata=(
                record_metadata
            ),
        )

    # =========================================================================
    # GET
    # =========================================================================
    def get(
        self,
        dead_letter_id: str,
    ) -> DeadLetterRecord:
        self._ensure_open()

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
                row = (
                    self._connection.execute(
                        """
                        SELECT
                            dead_letter_id,
                            message_id,
                            event_type,
                            event_timestamp,
                            event_payload_json,
                            event_metadata_json,
                            delivery_count,
                            failure_type,
                            failure_message,
                            failed_at,
                            source_queue,
                            claim_token,
                            record_metadata_json
                        FROM dead_letter_messages
                        WHERE
                            queue_name = ?
                            AND dead_letter_id = ?
                        """,
                        (
                            self.name,
                            resolved,
                        ),
                    ).fetchone()
                )

            except sqlite3.Error as exc:
                raise SQLiteDeadLetterQueueError(
                    "Dead-letter kaydı okunamadı "
                    f"| error={exc}"
                ) from exc

        if row is None:
            raise UnknownDeadLetterError(
                "Dead-letter kaydı bulunamadı "
                f"| queue={self.name!r} "
                f"| dead_letter_id={resolved!r}"
            )

        return (
            _clone_record(
                self._row_to_record(
                    row
                )
            )
        )

    # =========================================================================
    # REMOVE
    # =========================================================================
    def remove(
        self,
        dead_letter_id: str,
    ) -> DeadLetterRecord:
        self._ensure_open()

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
                self._connection.execute(
                    "BEGIN IMMEDIATE"
                )

                try:
                    row = (
                        self._connection.execute(
                            """
                            SELECT
                                dead_letter_id,
                                message_id,
                                event_type,
                                event_timestamp,
                                event_payload_json,
                                event_metadata_json,
                                delivery_count,
                                failure_type,
                                failure_message,
                                failed_at,
                                source_queue,
                                claim_token,
                                record_metadata_json
                            FROM dead_letter_messages
                            WHERE
                                queue_name = ?
                                AND dead_letter_id = ?
                            """,
                            (
                                self.name,
                                resolved,
                            ),
                        ).fetchone()
                    )

                    if row is None:
                        self._connection.execute(
                            "ROLLBACK"
                        )

                        raise UnknownDeadLetterError(
                            "Dead-letter kaydı bulunamadı "
                            f"| queue={self.name!r} "
                            f"| dead_letter_id={resolved!r}"
                        )

                    self._connection.execute(
                        """
                        DELETE FROM dead_letter_messages
                        WHERE
                            queue_name = ?
                            AND dead_letter_id = ?
                        """,
                        (
                            self.name,
                            resolved,
                        ),
                    )

                except UnknownDeadLetterError:
                    raise

                except sqlite3.Error:
                    self._connection.execute(
                        "ROLLBACK"
                    )

                    raise

                self._connection.execute(
                    "COMMIT"
                )

            except UnknownDeadLetterError:
                raise

            except sqlite3.Error as exc:
                raise SQLiteDeadLetterQueueError(
                    "Dead-letter kaydı silinemedi "
                    f"| error={exc}"
                ) from exc

        return (
            _clone_record(
                self._row_to_record(
                    row
                )
            )
        )

    # =========================================================================
    # LIST
    # =========================================================================
    def records(
        self,
    ) -> list[
        DeadLetterRecord
    ]:
        self._ensure_open()

        with self._lock:
            try:
                rows = (
                    self._connection.execute(
                        """
                        SELECT
                            dead_letter_id,
                            message_id,
                            event_type,
                            event_timestamp,
                            event_payload_json,
                            event_metadata_json,
                            delivery_count,
                            failure_type,
                            failure_message,
                            failed_at,
                            source_queue,
                            claim_token,
                            record_metadata_json
                        FROM dead_letter_messages
                        WHERE queue_name = ?
                        ORDER BY sequence_id ASC
                        """,
                        (
                            self.name,
                        ),
                    ).fetchall()
                )

            except sqlite3.Error as exc:
                raise SQLiteDeadLetterQueueError(
                    "Dead-letter kayıtları listelenemedi "
                    f"| error={exc}"
                ) from exc

        return [
            (
                _clone_record(
                    self._row_to_record(
                        row
                    )
                )
            )
            for row in rows
        ]

    # =========================================================================
    # CONTAINS
    # =========================================================================
    def contains(
        self,
        dead_letter_id: str,
    ) -> bool:
        self._ensure_open()

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
            try:
                row = (
                    self._connection.execute(
                        """
                        SELECT 1
                        FROM dead_letter_messages
                        WHERE
                            queue_name = ?
                            AND dead_letter_id = ?
                        LIMIT 1
                        """,
                        (
                            self.name,
                            normalized,
                        ),
                    ).fetchone()
                )

            except sqlite3.Error as exc:
                raise SQLiteDeadLetterQueueError(
                    "Dead-letter contains kontrolü başarısız "
                    f"| error={exc}"
                ) from exc

        return (
            row
            is not None
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

        try:
            return self.contains(
                dead_letter_id
            )

        except SQLiteDeadLetterQueueClosedError:
            return False

    # =========================================================================
    # SNAPSHOT
    # =========================================================================
    def snapshot(
        self,
    ) -> dict[str, Any]:
        self._ensure_open()

        records = (
            self.records()
        )

        return {
            "name": (
                self.name
            ),
            "backend": (
                "sqlite"
            ),
            "database_path": (
                str(
                    self.database_path
                )
            ),
            "timeout_seconds": (
                self.timeout_seconds
            ),
            "count": (
                len(
                    records
                )
            ),
            "dead_letter_ids": [
                (
                    record.dead_letter_id
                )
                for record
                in records
            ],
        }

    # =========================================================================
    # CLOSE
    # =========================================================================
    def close(
        self,
    ) -> None:
        with self._lock:
            if self._closed:
                return

            try:
                self._connection.close()

            except sqlite3.Error as exc:
                raise SQLiteDeadLetterQueueError(
                    "SQLiteDeadLetterQueue kapatılamadı "
                    f"| error={exc}"
                ) from exc

            self._closed = True

    def __enter__(
        self,
    ) -> "SQLiteDeadLetterQueue":
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
            f"database_path="
            f"{str(self.database_path)!r}, "
            f"count="
            f"{self.count if not self.is_closed else 'closed'}"
            f")"
        )