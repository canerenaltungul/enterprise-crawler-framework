from __future__ import annotations

"""
Enterprise Crawler Framework - SQLite Event Queue

Durable, process-safe SQLite event queue.

Temel özellikler
----------------
- durable publish
- deterministic FIFO among eligible events
- atomic claim
- claim ownership token
- lease timeout
- stale lease recovery
- persistent delivery_count
- ack
- nack + immediate requeue
- nack + delayed requeue
- nack + discard
- durable next_attempt_at
- WAL
- multiple queue instances over same database
- schema v1 -> v2 migration

Atomic Claim
------------
Claim transaction:

    BEGIN IMMEDIATE
        ↓
    stale lease recovery
        ↓
    oldest eligible pending row SELECT
        ↓
    UPDATE state=claimed
        ↓
    COMMIT

SQLite write lock nedeniyle iki farklı SQLiteEventQueue instance aynı
message'ı aynı anda claim edemez.

Lease
-----
Claim edilen event:

    state = claimed
    claim_token = UUID
    claimed_at = now
    lease_expires_at = now + lease_seconds

Lease süresi geçmiş event claim() sırasında veya
recover_stale_leases() ile yeniden pending duruma alınır.

Scheduled Retry
---------------
Retry nedeniyle yeniden queue'ya alınan event:

    state = pending
    next_attempt_at = now + retry_delay_seconds

next_attempt_at gelecekteyse event claim edilemez.

Claim sorgusu önce eligible event'leri filtreler, sonra bunlar arasında
sequence ASC ile deterministic FIFO uygular.

Dolayısıyla eski fakat henüz due olmayan bir event, daha yeni fakat due olan
bir event'i bloke etmez.

Lease recovery retry scheduling değildir. Stale lease recover edildiğinde
next_attempt_at temizlenir ve event normal pending/due hale gelir.

Persistence
-----------
ack edilmiş veya discard edilmiş event aktif queue tablosundan silinir.

delivery_count ve next_attempt_at database'de kalıcıdır.

Schema
------
Schema version 1:
    original durable event queue

Schema version 2:
    next_attempt_at column added

Mevcut v1 database açıldığında migration otomatik ve idempotent uygulanır.
"""

import copy
import json
import math
import sqlite3
import threading
import uuid
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from numbers import Real
from pathlib import Path
from typing import (
    Any,
    Callable,
    Optional,
)

from enterprise_crawler.contracts import Event
from enterprise_crawler.events.queue import (
    ClaimedEvent,
    DuplicateEventMessageError,
    EventClaimOwnershipError,
    EventNotClaimedError,
    EventQueueClosedError,
    EventQueueError,
    EventQueueValidationError,
    PublishedEvent,
    UnknownEventMessageError,
    _normalize_clock,
    _normalize_non_empty_string,
    _normalize_non_negative_float,
    _read_clock,
    _validate_event,
)


UTC = timezone.utc


# =============================================================================
# SQLITE-SPECIFIC EXCEPTION
# =============================================================================
class SQLiteEventQueueError(
    EventQueueError
):
    """
    SQLite storage/runtime hatası.
    """


# =============================================================================
# HELPERS
# =============================================================================
def utc_now() -> datetime:
    return datetime.now(
        UTC
    )


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
        or normalized <= 0
    ):
        raise EventQueueValidationError(
            f"{field_name} sıfırdan büyük "
            "sonlu sayı olmalıdır."
        )

    return normalized


def _normalize_timeout(
    value: Any,
) -> float:
    return _normalize_positive_float(
        value,
        field_name="timeout_seconds",
    )


def _json_dump(
    value: dict[str, Any],
    *,
    field_name: str,
) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(
                ",",
                ":",
            ),
            allow_nan=False,
        )

    except (
        TypeError,
        ValueError,
    ) as exc:
        raise EventQueueValidationError(
            f"{field_name} JSON serializable olmalıdır."
        ) from exc


def _json_load_object(
    value: str,
    *,
    field_name: str,
) -> dict[str, Any]:
    try:
        decoded = json.loads(
            value
        )

    except json.JSONDecodeError as exc:
        raise SQLiteEventQueueError(
            f"Stored {field_name} JSON bozuk."
        ) from exc

    if not isinstance(
        decoded,
        dict,
    ):
        raise SQLiteEventQueueError(
            f"Stored {field_name} JSON object olmalıdır."
        )

    return decoded


def _datetime_to_storage(
    value: datetime,
) -> str:
    return value.isoformat()


def _datetime_from_storage(
    value: str,
    *,
    field_name: str,
) -> datetime:
    try:
        decoded = (
            datetime.fromisoformat(
                value
            )
        )

    except (
        TypeError,
        ValueError,
    ) as exc:
        raise SQLiteEventQueueError(
            f"Stored {field_name} datetime bozuk."
        ) from exc

    if decoded.tzinfo is None:
        raise SQLiteEventQueueError(
            f"Stored {field_name} datetime timezone-aware olmalıdır."
        )

    return decoded.astimezone(
        UTC
    )


# =============================================================================
# SQLITE QUEUE
# =============================================================================
class SQLiteEventQueue:
    """
    SQLite-backed durable event queue.
    """

    SCHEMA_VERSION = 2

    def __init__(
        self,
        database_path: str | Path,
        *,
        name: str = "default",
        lease_seconds: float = 30.0,
        timeout_seconds: float = 15.0,
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

        self.lease_seconds = (
            _normalize_positive_float(
                lease_seconds,
                field_name="lease_seconds",
            )
        )

        self.timeout_seconds = (
            _normalize_timeout(
                timeout_seconds
            )
        )

        self._clock = (
            _normalize_clock(
                clock
            )
            if clock is not None
            else utc_now
        )

        if not isinstance(
            database_path,
            (
                str,
                Path,
            ),
        ):
            raise EventQueueValidationError(
                "database_path str veya Path olmalıdır."
            )

        raw_path = str(
            database_path
        ).strip()

        if not raw_path:
            raise EventQueueValidationError(
                "database_path boş olamaz."
            )

        self.database_path = (
            Path(
                raw_path
            )
        )

        if (
            self.database_path.exists()
            and self.database_path.is_dir()
        ):
            raise EventQueueValidationError(
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
                    self.database_path,
                    timeout=(
                        self.timeout_seconds
                    ),
                    isolation_level=None,
                    check_same_thread=False,
                )
            )

            self._connection.row_factory = (
                sqlite3.Row
            )

            self._configure_database()
            self._initialize_schema()

        except Exception as exc:
            try:
                connection = getattr(
                    self,
                    "_connection",
                    None,
                )

                if connection is not None:
                    connection.close()

            except Exception:
                pass

            if isinstance(
                exc,
                EventQueueError,
            ):
                raise

            raise SQLiteEventQueueError(
                "SQLite event queue açılamadı "
                f"| path={self.database_path} "
                f"| error={exc}"
            ) from exc

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
    # DATABASE SETUP
    # =========================================================================
    def _configure_database(
        self,
    ) -> None:
        cursor = (
            self._connection.cursor()
        )

        try:
            cursor.execute(
                "PRAGMA journal_mode=WAL"
            )

            cursor.execute(
                "PRAGMA synchronous=NORMAL"
            )

            cursor.execute(
                "PRAGMA foreign_keys=ON"
            )

            cursor.execute(
                (
                    "PRAGMA busy_timeout="
                    f"{int(self.timeout_seconds * 1000)}"
                )
            )

        finally:
            cursor.close()

    def _initialize_schema(
        self,
    ) -> None:
        cursor = (
            self._connection.cursor()
        )

        try:
            cursor.executescript(
                """
                CREATE TABLE IF NOT EXISTS event_queue_messages (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    queue_name TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_timestamp TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    published_at TEXT NOT NULL,
                    state TEXT NOT NULL
                        CHECK (state IN ('pending', 'claimed')),
                    delivery_count INTEGER NOT NULL DEFAULT 0
                        CHECK (delivery_count >= 0),
                    claim_token TEXT,
                    claimed_at TEXT,
                    lease_expires_at TEXT,
                    next_attempt_at TEXT,
                    UNIQUE(queue_name, message_id)
                );

                CREATE TABLE IF NOT EXISTS event_queue_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )

            columns = {
                str(
                    row[
                        "name"
                    ]
                )
                for row
                in cursor.execute(
                    """
                    PRAGMA table_info(
                        event_queue_messages
                    )
                    """
                ).fetchall()
            }

            if (
                "next_attempt_at"
                not in columns
            ):
                cursor.execute(
                    """
                    ALTER TABLE event_queue_messages
                    ADD COLUMN next_attempt_at TEXT
                    """
                )

            cursor.executescript(
                """
                CREATE INDEX IF NOT EXISTS
                    idx_event_queue_pending
                ON event_queue_messages (
                    queue_name,
                    state,
                    sequence
                );

                CREATE INDEX IF NOT EXISTS
                    idx_event_queue_due
                ON event_queue_messages (
                    queue_name,
                    state,
                    next_attempt_at,
                    sequence
                );

                CREATE INDEX IF NOT EXISTS
                    idx_event_queue_lease
                ON event_queue_messages (
                    queue_name,
                    state,
                    lease_expires_at
                );
                """
            )

            cursor.execute(
                """
                INSERT INTO event_queue_meta (
                    key,
                    value
                )
                VALUES (
                    'schema_version',
                    ?
                )
                ON CONFLICT(key)
                DO UPDATE SET
                    value = excluded.value
                """,
                (
                    str(
                        self.SCHEMA_VERSION
                    ),
                ),
            )

        except sqlite3.Error as exc:
            raise SQLiteEventQueueError(
                "SQLite event queue schema oluşturulamadı "
                "veya migrate edilemedi."
            ) from exc

        finally:
            cursor.close()

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

    def _ensure_open(
        self,
    ) -> None:
        if self.is_closed:
            raise EventQueueClosedError(
                "SQLiteEventQueue kapalı "
                f"| queue={self.name}"
            )

    # =========================================================================
    # TRANSACTION HELPERS
    # =========================================================================
    def _begin_immediate(
        self,
    ) -> None:
        try:
            self._connection.execute(
                "BEGIN IMMEDIATE"
            )

        except sqlite3.Error as exc:
            raise SQLiteEventQueueError(
                "SQLite transaction başlatılamadı."
            ) from exc

    def _commit(
        self,
    ) -> None:
        try:
            self._connection.execute(
                "COMMIT"
            )

        except sqlite3.Error as exc:
            raise SQLiteEventQueueError(
                "SQLite transaction commit edilemedi."
            ) from exc

    def _rollback_safely(
        self,
    ) -> None:
        try:
            self._connection.execute(
                "ROLLBACK"
            )

        except sqlite3.Error:
            pass

    # =========================================================================
    # ROW CONVERSION
    # =========================================================================
    @staticmethod
    def _row_to_event(
        row: sqlite3.Row,
    ) -> Event:
        return Event(
            event_type=(
                row[
                    "event_type"
                ]
            ),
            timestamp=(
                _datetime_from_storage(
                    row[
                        "event_timestamp"
                    ],
                    field_name=(
                        "event_timestamp"
                    ),
                )
            ),
            payload=(
                _json_load_object(
                    row[
                        "payload_json"
                    ],
                    field_name="payload",
                )
            ),
            metadata=(
                _json_load_object(
                    row[
                        "metadata_json"
                    ],
                    field_name="metadata",
                )
            ),
        )

    # =========================================================================
    # COUNTS
    # =========================================================================
    def _count_state(
        self,
        state: Optional[
            str
        ] = None,
    ) -> int:
        self._ensure_open()

        with self._lock:
            try:
                if state is None:
                    row = (
                        self._connection.execute(
                            """
                            SELECT COUNT(*) AS count
                            FROM event_queue_messages
                            WHERE queue_name = ?
                            """,
                            (
                                self.name,
                            ),
                        ).fetchone()
                    )

                else:
                    row = (
                        self._connection.execute(
                            """
                            SELECT COUNT(*) AS count
                            FROM event_queue_messages
                            WHERE queue_name = ?
                              AND state = ?
                            """,
                            (
                                self.name,
                                state,
                            ),
                        ).fetchone()
                    )

            except sqlite3.Error as exc:
                raise SQLiteEventQueueError(
                    "Queue count okunamadı."
                ) from exc

        if row is None:
            return 0

        return int(
            row[
                "count"
            ]
        )

    @property
    def active_count(
        self,
    ) -> int:
        return self._count_state()

    @property
    def pending_count(
        self,
    ) -> int:
        return self._count_state(
            "pending"
        )

    @property
    def claimed_count(
        self,
    ) -> int:
        return self._count_state(
            "claimed"
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
        self._ensure_open()

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

        payload_json = (
            _json_dump(
                normalized_event.payload,
                field_name=(
                    "event.payload"
                ),
            )
        )

        metadata_json = (
            _json_dump(
                normalized_event.metadata,
                field_name=(
                    "event.metadata"
                ),
            )
        )

        published_at = (
            self._now()
        )

        with self._lock:
            try:
                self._connection.execute(
                    """
                    INSERT INTO event_queue_messages (
                        queue_name,
                        message_id,
                        event_type,
                        event_timestamp,
                        payload_json,
                        metadata_json,
                        published_at,
                        state,
                        delivery_count,
                        claim_token,
                        claimed_at,
                        lease_expires_at,
                        next_attempt_at
                    )
                    VALUES (
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        'pending',
                        0,
                        NULL,
                        NULL,
                        NULL,
                        NULL
                    )
                    """,
                    (
                        self.name,
                        resolved_message_id,
                        normalized_event.event_type,
                        _datetime_to_storage(
                            normalized_event.timestamp
                        ),
                        payload_json,
                        metadata_json,
                        _datetime_to_storage(
                            published_at
                        ),
                    ),
                )

            except sqlite3.IntegrityError as exc:
                raise DuplicateEventMessageError(
                    "message_id zaten queue içinde "
                    f"| queue={self.name!r} "
                    f"| message_id={resolved_message_id!r}"
                ) from exc

            except sqlite3.Error as exc:
                raise SQLiteEventQueueError(
                    "Event SQLite queue'ya yazılamadı."
                ) from exc

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
    # STALE LEASE RECOVERY
    # =========================================================================
    def _recover_stale_leases_locked(
        self,
        now: datetime,
    ) -> int:
        cursor = (
            self._connection.execute(
                """
                UPDATE event_queue_messages
                SET
                    state = 'pending',
                    claim_token = NULL,
                    claimed_at = NULL,
                    lease_expires_at = NULL,
                    next_attempt_at = NULL
                WHERE queue_name = ?
                  AND state = 'claimed'
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at <= ?
                """,
                (
                    self.name,
                    _datetime_to_storage(
                        now
                    ),
                ),
            )
        )

        return max(
            0,
            int(
                cursor.rowcount
            ),
        )

    def recover_stale_leases(
        self,
    ) -> int:
        self._ensure_open()

        with self._lock:
            self._begin_immediate()

            try:
                count = (
                    self._recover_stale_leases_locked(
                        self._now()
                    )
                )

                self._commit()

                return count

            except BaseException:
                self._rollback_safely()

                raise

    # =========================================================================
    # CLAIM
    # =========================================================================
    def claim(
        self,
    ) -> Optional[
        ClaimedEvent
    ]:
        self._ensure_open()

        with self._lock:
            self._begin_immediate()

            try:
                now = (
                    self._now()
                )

                self._recover_stale_leases_locked(
                    now
                )

                now_storage = (
                    _datetime_to_storage(
                        now
                    )
                )

                row = (
                    self._connection.execute(
                        """
                        SELECT
                            sequence,
                            queue_name,
                            message_id,
                            event_type,
                            event_timestamp,
                            payload_json,
                            metadata_json,
                            published_at,
                            state,
                            delivery_count,
                            claim_token,
                            claimed_at,
                            lease_expires_at,
                            next_attempt_at
                        FROM event_queue_messages
                        WHERE queue_name = ?
                          AND state = 'pending'
                          AND (
                              next_attempt_at IS NULL
                              OR next_attempt_at <= ?
                          )
                        ORDER BY sequence ASC
                        LIMIT 1
                        """,
                        (
                            self.name,
                            now_storage,
                        ),
                    ).fetchone()
                )

                if row is None:
                    self._commit()
                    return None

                claim_token = (
                    uuid.uuid4().hex
                )

                claimed_at = (
                    now
                )

                lease_expires_at = (
                    now
                    + timedelta(
                        seconds=(
                            self.lease_seconds
                        )
                    )
                )

                new_delivery_count = (
                    int(
                        row[
                            "delivery_count"
                        ]
                    )
                    + 1
                )

                cursor = (
                    self._connection.execute(
                        """
                        UPDATE event_queue_messages
                        SET
                            state = 'claimed',
                            delivery_count = ?,
                            claim_token = ?,
                            claimed_at = ?,
                            lease_expires_at = ?,
                            next_attempt_at = NULL
                        WHERE sequence = ?
                          AND queue_name = ?
                          AND state = 'pending'
                          AND (
                              next_attempt_at IS NULL
                              OR next_attempt_at <= ?
                          )
                        """,
                        (
                            new_delivery_count,
                            claim_token,
                            _datetime_to_storage(
                                claimed_at
                            ),
                            _datetime_to_storage(
                                lease_expires_at
                            ),
                            row[
                                "sequence"
                            ],
                            self.name,
                            now_storage,
                        ),
                    )
                )

                if (
                    cursor.rowcount
                    != 1
                ):
                    raise SQLiteEventQueueError(
                        "Atomic event claim başarısız."
                    )

                self._commit()

                return ClaimedEvent(
                    message_id=(
                        row[
                            "message_id"
                        ]
                    ),
                    claim_token=(
                        claim_token
                    ),
                    event=(
                        self._row_to_event(
                            row
                        )
                    ),
                    delivery_count=(
                        new_delivery_count
                    ),
                    published_at=(
                        _datetime_from_storage(
                            row[
                                "published_at"
                            ],
                            field_name=(
                                "published_at"
                            ),
                        )
                    ),
                    claimed_at=(
                        claimed_at
                    ),
                    lease_expires_at=(
                        lease_expires_at
                    ),
                )

            except BaseException:
                self._rollback_safely()
                raise

    # =========================================================================
    # ENTRY LOOKUP
    # =========================================================================
    def _require_row(
        self,
        message_id: str,
    ) -> sqlite3.Row:
        row = (
            self._connection.execute(
                """
                SELECT
                    sequence,
                    queue_name,
                    message_id,
                    event_type,
                    event_timestamp,
                    payload_json,
                    metadata_json,
                    published_at,
                    state,
                    delivery_count,
                    claim_token,
                    claimed_at,
                    lease_expires_at,
                    next_attempt_at
                FROM event_queue_messages
                WHERE queue_name = ?
                  AND message_id = ?
                """,
                (
                    self.name,
                    message_id,
                ),
            ).fetchone()
        )

        if row is None:
            raise UnknownEventMessageError(
                "Event message bulunamadı "
                f"| queue={self.name!r} "
                f"| message_id={message_id!r}"
            )

        return row

    @staticmethod
    def _require_claim_owner(
        row: sqlite3.Row,
        claim_token: str,
    ) -> None:
        if (
            row[
                "state"
            ]
            != "claimed"
        ):
            raise EventNotClaimedError(
                "Event claim edilmemiş "
                f"| message_id={row['message_id']!r}"
            )

        if (
            row[
                "claim_token"
            ]
            != claim_token
        ):
            raise EventClaimOwnershipError(
                "Claim token eşleşmiyor "
                f"| message_id={row['message_id']!r}"
            )

    # =========================================================================
    # ACK
    # =========================================================================
    def ack(
        self,
        message_id: str,
        claim_token: str,
    ) -> Event:
        self._ensure_open()

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
            self._begin_immediate()

            try:
                row = (
                    self._require_row(
                        resolved_message_id
                    )
                )

                self._require_claim_owner(
                    row,
                    resolved_claim_token,
                )

                event = (
                    self._row_to_event(
                        row
                    )
                )

                cursor = (
                    self._connection.execute(
                        """
                        DELETE FROM event_queue_messages
                        WHERE queue_name = ?
                          AND message_id = ?
                          AND state = 'claimed'
                          AND claim_token = ?
                        """,
                        (
                            self.name,
                            resolved_message_id,
                            resolved_claim_token,
                        ),
                    )
                )

                if (
                    cursor.rowcount
                    != 1
                ):
                    raise EventClaimOwnershipError(
                        "Event ack ownership değişti "
                        f"| message_id={resolved_message_id!r}"
                    )

                self._commit()

                return copy.deepcopy(
                    event
                )

            except BaseException:
                self._rollback_safely()
                raise

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
        self._ensure_open()

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
            self._begin_immediate()

            try:
                row = (
                    self._require_row(
                        resolved_message_id
                    )
                )

                self._require_claim_owner(
                    row,
                    resolved_claim_token,
                )

                event = (
                    self._row_to_event(
                        row
                    )
                )

                if requeue:
                    if (
                        resolved_retry_delay
                        > 0.0
                    ):
                        next_attempt_at = (
                            self._now()
                            + timedelta(
                                seconds=(
                                    resolved_retry_delay
                                )
                            )
                        )

                        next_attempt_storage = (
                            _datetime_to_storage(
                                next_attempt_at
                            )
                        )

                    else:
                        next_attempt_storage = None

                    cursor = (
                        self._connection.execute(
                            """
                            UPDATE event_queue_messages
                            SET
                                state = 'pending',
                                claim_token = NULL,
                                claimed_at = NULL,
                                lease_expires_at = NULL,
                                next_attempt_at = ?
                            WHERE queue_name = ?
                              AND message_id = ?
                              AND state = 'claimed'
                              AND claim_token = ?
                            """,
                            (
                                next_attempt_storage,
                                self.name,
                                resolved_message_id,
                                resolved_claim_token,
                            ),
                        )
                    )

                else:
                    cursor = (
                        self._connection.execute(
                            """
                            DELETE FROM event_queue_messages
                            WHERE queue_name = ?
                              AND message_id = ?
                              AND state = 'claimed'
                              AND claim_token = ?
                            """,
                            (
                                self.name,
                                resolved_message_id,
                                resolved_claim_token,
                            ),
                        )
                    )

                if (
                    cursor.rowcount
                    != 1
                ):
                    raise EventClaimOwnershipError(
                        "Event nack ownership değişti "
                        f"| message_id={resolved_message_id!r}"
                    )

                self._commit()

                return copy.deepcopy(
                    event
                )

            except BaseException:
                self._rollback_safely()
                raise

    # =========================================================================
    # LOOKUP
    # =========================================================================
    def contains(
        self,
        message_id: str,
    ) -> bool:
        self._ensure_open()

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
            try:
                row = (
                    self._connection.execute(
                        """
                        SELECT 1
                        FROM event_queue_messages
                        WHERE queue_name = ?
                          AND message_id = ?
                        LIMIT 1
                        """,
                        (
                            self.name,
                            normalized,
                        ),
                    ).fetchone()
                )

            except sqlite3.Error as exc:
                raise SQLiteEventQueueError(
                    "Queue lookup başarısız."
                ) from exc

        return (
            row
            is not None
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
    # MESSAGE INSPECTION
    # =========================================================================
    def message_snapshot(
        self,
        message_id: str,
    ) -> dict[str, Any]:
        self._ensure_open()

        resolved = (
            _normalize_non_empty_string(
                message_id,
                field_name="message_id",
            )
        )

        with self._lock:
            row = (
                self._require_row(
                    resolved
                )
            )

            return {
                "message_id": (
                    row[
                        "message_id"
                    ]
                ),
                "event_type": (
                    row[
                        "event_type"
                    ]
                ),
                "state": (
                    row[
                        "state"
                    ]
                ),
                "delivery_count": (
                    int(
                        row[
                            "delivery_count"
                        ]
                    )
                ),
                "published_at": (
                    row[
                        "published_at"
                    ]
                ),
                "claim_token": (
                    row[
                        "claim_token"
                    ]
                ),
                "claimed_at": (
                    row[
                        "claimed_at"
                    ]
                ),
                "lease_expires_at": (
                    row[
                        "lease_expires_at"
                    ]
                ),
                "next_attempt_at": (
                    row[
                        "next_attempt_at"
                    ]
                ),
            }

    # =========================================================================
    # SNAPSHOT
    # =========================================================================
    def snapshot(
        self,
    ) -> dict[str, Any]:
        self._ensure_open()

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
            "schema_version": (
                self.SCHEMA_VERSION
            ),
            "lease_seconds": (
                self.lease_seconds
            ),
            "timeout_seconds": (
                self.timeout_seconds
            ),
            "pending_count": (
                self.pending_count
            ),
            "claimed_count": (
                self.claimed_count
            ),
            "active_count": (
                self.active_count
            ),
            "closed": False,
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
                raise SQLiteEventQueueError(
                    "SQLite event queue kapatılamadı."
                ) from exc

            finally:
                self._closed = True

    def __enter__(
        self,
    ) -> "SQLiteEventQueue":
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
            self.active_count
        )

    def __repr__(
        self,
    ) -> str:
        return (
            f"{self.__class__.__name__}("
            f"name={self.name!r}, "
            f"database_path={str(self.database_path)!r}, "
            f"schema_version={self.SCHEMA_VERSION}, "
            f"lease_seconds={self.lease_seconds}, "
            f"closed={self.is_closed}"
            f")"
        )