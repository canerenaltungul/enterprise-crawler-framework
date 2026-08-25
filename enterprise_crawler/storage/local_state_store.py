from __future__ import annotations

"""
Enterprise Crawler Framework - Local State Store

Hafif, process-local / machine-local SQLite state ve idempotency deposu.

Amaç
----
* Record-level idempotency sağlamak.
* Küçük JSON-serializable state değerlerini saklamak.
* SQLite WAL ile güvenli yerel kullanım sunmak.
* Her transaction sonunda fiziksel connection'ı kapatmak.
* Thread-safe kullanım sağlamak.

Bilerek içermez
---------------
* Source checkpoint.
* Distributed locks.
* Event queue.
* Dataset lineage.
* Evidence chain.
* Audit archive.
* Distributed/global idempotency.
* PostgreSQL state.

Bu store yüksek ölçekli distributed coordination yerine geçmez.
"""

import json
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional

from enterprise_crawler.exceptions import StorageError


UTC = timezone.utc


# =============================================================================
# HELPERS
# =============================================================================
def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _normalize_namespace(
    namespace: str,
) -> str:
    value = str(
        namespace or ""
    ).strip()

    if not value:
        raise StorageError(
            "State namespace boş olamaz."
        )

    if len(value) > 255:
        raise StorageError(
            "State namespace en fazla 255 karakter olabilir."
        )

    return value


def _normalize_key(
    key: str,
) -> str:
    value = str(
        key or ""
    ).strip()

    if not value:
        raise StorageError(
            "State key boş olamaz."
        )

    if len(value) > 2_048:
        raise StorageError(
            "State key en fazla 2048 karakter olabilir."
        )

    return value


def _serialize_json(
    value: Any,
) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
            allow_nan=False,
        )

    except (
        TypeError,
        ValueError,
    ) as exc:
        raise StorageError(
            "State value JSON serialize edilemedi."
        ) from exc


def _deserialize_json(
    value: str,
) -> Any:
    try:
        return json.loads(
            value
        )

    except json.JSONDecodeError as exc:
        raise StorageError(
            "State value JSON parse edilemedi."
        ) from exc


# =============================================================================
# MODELS
# =============================================================================
@dataclass(
    frozen=True,
    slots=True,
)
class SeenRecord:
    namespace: str
    key: str

    first_seen_at: str
    last_seen_at: str

    metadata: dict[str, Any]


@dataclass(
    frozen=True,
    slots=True,
)
class StateEntry:
    namespace: str
    key: str

    value: Any

    created_at: str
    updated_at: str


# =============================================================================
# LOCAL STATE STORE
# =============================================================================
class LocalStateStore:
    """
    SQLite tabanlı local state store.

    Örnek::

        store = LocalStateStore(
            "data/state/crawler.db"
        )

        if store.mark_seen(
            "documents",
            "document-123",
        ):
            process_document()

        store.put(
            "crawler",
            "last_page",
            7,
        )

        page = store.get(
            "crawler",
            "last_page",
        )
    """

    def __init__(
        self,
        db_path: str | Path,
        *,
        timeout_seconds: float = 15.0,
    ) -> None:
        path = Path(
            db_path
        ).expanduser()

        if not str(
            path
        ).strip():
            raise StorageError(
                "LocalStateStore db_path boş olamaz."
            )

        if (
            isinstance(
                timeout_seconds,
                bool,
            )
            or not isinstance(
                timeout_seconds,
                (int, float),
            )
            or timeout_seconds <= 0
        ):
            raise StorageError(
                "timeout_seconds pozitif sayı olmalıdır."
            )

        try:
            path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            self.db_path = (
                path.resolve()
            )

        except OSError as exc:
            raise StorageError(
                "LocalStateStore database path hazırlanamadı."
            ) from exc

        if (
            self.db_path.exists()
            and self.db_path.is_dir()
        ):
            raise StorageError(
                "LocalStateStore db_path klasör olamaz."
            )

        self.timeout_seconds = float(
            timeout_seconds
        )

        self._lock = (
            threading.RLock()
        )

        self._closed = False

        self._initialize()

    # =========================================================================
    # STATE
    # =========================================================================
    @property
    def is_closed(
        self,
    ) -> bool:
        with self._lock:
            return self._closed

    def _ensure_open(
        self,
    ) -> None:
        if self._closed:
            raise StorageError(
                "Kapalı LocalStateStore kullanılamaz."
            )

    # =========================================================================
    # CONNECTION
    # =========================================================================
    def _connect(
        self,
    ) -> sqlite3.Connection:
        self._ensure_open()

        try:
            connection = (
                sqlite3.connect(
                    self.db_path,
                    timeout=(
                        self.timeout_seconds
                    ),
                    check_same_thread=False,
                )
            )

            connection.row_factory = (
                sqlite3.Row
            )

            connection.execute(
                "PRAGMA journal_mode=WAL;"
            )

            connection.execute(
                "PRAGMA synchronous=NORMAL;"
            )

            connection.execute(
                "PRAGMA foreign_keys=ON;"
            )

            connection.execute(
                (
                    "PRAGMA busy_timeout="
                    f"{int(self.timeout_seconds * 1000)};"
                )
            )

            return connection

        except sqlite3.Error as exc:
            raise StorageError(
                "SQLite bağlantısı açılamadı "
                f"| path={self.db_path}"
            ) from exc

    @contextmanager
    def _connection(
        self,
    ) -> Iterator[
        sqlite3.Connection
    ]:
        """
        Transaction ve physical connection lifecycle'ını birlikte yönetir.

        Başarı:
            commit

        Hata:
            rollback

        Her durumda:
            close
        """

        connection = self._connect()

        try:
            yield connection

            connection.commit()

        except Exception:
            try:
                connection.rollback()
            except sqlite3.Error:
                pass

            raise

        finally:
            try:
                connection.close()
            except sqlite3.Error:
                pass

    # =========================================================================
    # INITIALIZATION
    # =========================================================================
    def _initialize(
        self,
    ) -> None:
        with self._lock:
            try:
                with self._connection() as connection:
                    connection.executescript(
                        """
                        CREATE TABLE IF NOT EXISTS seen_records (
                            namespace TEXT NOT NULL,
                            record_key TEXT NOT NULL,
                            first_seen_at TEXT NOT NULL,
                            last_seen_at TEXT NOT NULL,
                            metadata_json TEXT NOT NULL,
                            PRIMARY KEY (
                                namespace,
                                record_key
                            )
                        );

                        CREATE INDEX IF NOT EXISTS
                            idx_seen_records_namespace
                        ON seen_records (
                            namespace
                        );

                        CREATE TABLE IF NOT EXISTS state_values (
                            namespace TEXT NOT NULL,
                            state_key TEXT NOT NULL,
                            value_json TEXT NOT NULL,
                            created_at TEXT NOT NULL,
                            updated_at TEXT NOT NULL,
                            PRIMARY KEY (
                                namespace,
                                state_key
                            )
                        );

                        CREATE INDEX IF NOT EXISTS
                            idx_state_values_namespace
                        ON state_values (
                            namespace
                        );
                        """
                    )

            except sqlite3.Error as exc:
                raise StorageError(
                    "LocalStateStore schema oluşturulamadı."
                ) from exc

    # =========================================================================
    # IDEMPOTENCY
    # =========================================================================
    def has_seen(
        self,
        namespace: str,
        key: str,
    ) -> bool:
        normalized_namespace = (
            _normalize_namespace(
                namespace
            )
        )

        normalized_key = (
            _normalize_key(
                key
            )
        )

        with self._lock:
            self._ensure_open()

            try:
                with self._connection() as connection:
                    row = connection.execute(
                        """
                        SELECT 1
                        FROM seen_records
                        WHERE namespace = ?
                          AND record_key = ?
                        LIMIT 1;
                        """,
                        (
                            normalized_namespace,
                            normalized_key,
                        ),
                    ).fetchone()

                    return row is not None

            except sqlite3.Error as exc:
                raise StorageError(
                    "LocalStateStore has_seen başarısız."
                ) from exc

    def mark_seen(
        self,
        namespace: str,
        key: str,
        *,
        metadata: Optional[
            Mapping[str, Any]
        ] = None,
    ) -> bool:
        """
        Key ilk kez görülüyorsa True döndürür.

        Daha önce görülmüşse:
        * False döndürür.
        * last_seen_at güncellenir.
        * verilen metadata mevcut metadata'nın yerine yazılır.
        """

        normalized_namespace = (
            _normalize_namespace(
                namespace
            )
        )

        normalized_key = (
            _normalize_key(
                key
            )
        )

        serialized_metadata = (
            _serialize_json(
                dict(
                    metadata or {}
                )
            )
        )

        now = utc_now_iso()

        with self._lock:
            self._ensure_open()

            try:
                with self._connection() as connection:
                    cursor = connection.execute(
                        """
                        INSERT OR IGNORE
                        INTO seen_records (
                            namespace,
                            record_key,
                            first_seen_at,
                            last_seen_at,
                            metadata_json
                        )
                        VALUES (?, ?, ?, ?, ?);
                        """,
                        (
                            normalized_namespace,
                            normalized_key,
                            now,
                            now,
                            serialized_metadata,
                        ),
                    )

                    inserted = (
                        cursor.rowcount == 1
                    )

                    if inserted:
                        return True

                    connection.execute(
                        """
                        UPDATE seen_records
                        SET last_seen_at = ?,
                            metadata_json = ?
                        WHERE namespace = ?
                          AND record_key = ?;
                        """,
                        (
                            now,
                            serialized_metadata,
                            normalized_namespace,
                            normalized_key,
                        ),
                    )

                    return False

            except sqlite3.Error as exc:
                raise StorageError(
                    "LocalStateStore mark_seen başarısız."
                ) from exc

    def get_seen(
        self,
        namespace: str,
        key: str,
    ) -> Optional[
        SeenRecord
    ]:
        normalized_namespace = (
            _normalize_namespace(
                namespace
            )
        )

        normalized_key = (
            _normalize_key(
                key
            )
        )

        with self._lock:
            self._ensure_open()

            try:
                with self._connection() as connection:
                    row = connection.execute(
                        """
                        SELECT
                            namespace,
                            record_key,
                            first_seen_at,
                            last_seen_at,
                            metadata_json
                        FROM seen_records
                        WHERE namespace = ?
                          AND record_key = ?
                        LIMIT 1;
                        """,
                        (
                            normalized_namespace,
                            normalized_key,
                        ),
                    ).fetchone()

            except sqlite3.Error as exc:
                raise StorageError(
                    "LocalStateStore get_seen başarısız."
                ) from exc

        if row is None:
            return None

        metadata = (
            _deserialize_json(
                row["metadata_json"]
            )
        )

        if not isinstance(
            metadata,
            dict,
        ):
            raise StorageError(
                "Seen record metadata object olmalıdır."
            )

        return SeenRecord(
            namespace=(
                row["namespace"]
            ),
            key=row["record_key"],
            first_seen_at=(
                row["first_seen_at"]
            ),
            last_seen_at=(
                row["last_seen_at"]
            ),
            metadata=metadata,
        )

    def forget_seen(
        self,
        namespace: str,
        key: str,
    ) -> bool:
        normalized_namespace = (
            _normalize_namespace(
                namespace
            )
        )

        normalized_key = (
            _normalize_key(
                key
            )
        )

        with self._lock:
            self._ensure_open()

            try:
                with self._connection() as connection:
                    cursor = connection.execute(
                        """
                        DELETE FROM seen_records
                        WHERE namespace = ?
                          AND record_key = ?;
                        """,
                        (
                            normalized_namespace,
                            normalized_key,
                        ),
                    )

                    return (
                        cursor.rowcount > 0
                    )

            except sqlite3.Error as exc:
                raise StorageError(
                    "LocalStateStore forget_seen başarısız."
                ) from exc

    def seen_count(
        self,
        namespace: Optional[
            str
        ] = None,
    ) -> int:
        with self._lock:
            self._ensure_open()

            try:
                with self._connection() as connection:
                    if namespace is None:
                        row = connection.execute(
                            """
                            SELECT COUNT(*) AS total
                            FROM seen_records;
                            """
                        ).fetchone()

                    else:
                        normalized_namespace = (
                            _normalize_namespace(
                                namespace
                            )
                        )

                        row = connection.execute(
                            """
                            SELECT COUNT(*) AS total
                            FROM seen_records
                            WHERE namespace = ?;
                            """,
                            (
                                normalized_namespace,
                            ),
                        ).fetchone()

                    return int(
                        row["total"]
                    )

            except sqlite3.Error as exc:
                raise StorageError(
                    "LocalStateStore seen_count başarısız."
                ) from exc

    # =========================================================================
    # KEY / VALUE STATE
    # =========================================================================
    def put(
        self,
        namespace: str,
        key: str,
        value: Any,
    ) -> None:
        normalized_namespace = (
            _normalize_namespace(
                namespace
            )
        )

        normalized_key = (
            _normalize_key(
                key
            )
        )

        serialized_value = (
            _serialize_json(
                value
            )
        )

        now = utc_now_iso()

        with self._lock:
            self._ensure_open()

            try:
                with self._connection() as connection:
                    connection.execute(
                        """
                        INSERT INTO state_values (
                            namespace,
                            state_key,
                            value_json,
                            created_at,
                            updated_at
                        )
                        VALUES (?, ?, ?, ?, ?)

                        ON CONFLICT(
                            namespace,
                            state_key
                        )
                        DO UPDATE SET
                            value_json =
                                excluded.value_json,
                            updated_at =
                                excluded.updated_at;
                        """,
                        (
                            normalized_namespace,
                            normalized_key,
                            serialized_value,
                            now,
                            now,
                        ),
                    )

            except sqlite3.Error as exc:
                raise StorageError(
                    "LocalStateStore put başarısız."
                ) from exc

    def get(
        self,
        namespace: str,
        key: str,
        default: Any = None,
    ) -> Any:
        normalized_namespace = (
            _normalize_namespace(
                namespace
            )
        )

        normalized_key = (
            _normalize_key(
                key
            )
        )

        with self._lock:
            self._ensure_open()

            try:
                with self._connection() as connection:
                    row = connection.execute(
                        """
                        SELECT value_json
                        FROM state_values
                        WHERE namespace = ?
                          AND state_key = ?
                        LIMIT 1;
                        """,
                        (
                            normalized_namespace,
                            normalized_key,
                        ),
                    ).fetchone()

            except sqlite3.Error as exc:
                raise StorageError(
                    "LocalStateStore get başarısız."
                ) from exc

        if row is None:
            return default

        return _deserialize_json(
            row["value_json"]
        )

    def get_entry(
        self,
        namespace: str,
        key: str,
    ) -> Optional[
        StateEntry
    ]:
        normalized_namespace = (
            _normalize_namespace(
                namespace
            )
        )

        normalized_key = (
            _normalize_key(
                key
            )
        )

        with self._lock:
            self._ensure_open()

            try:
                with self._connection() as connection:
                    row = connection.execute(
                        """
                        SELECT
                            namespace,
                            state_key,
                            value_json,
                            created_at,
                            updated_at
                        FROM state_values
                        WHERE namespace = ?
                          AND state_key = ?
                        LIMIT 1;
                        """,
                        (
                            normalized_namespace,
                            normalized_key,
                        ),
                    ).fetchone()

            except sqlite3.Error as exc:
                raise StorageError(
                    "LocalStateStore get_entry başarısız."
                ) from exc

        if row is None:
            return None

        return StateEntry(
            namespace=(
                row["namespace"]
            ),
            key=row["state_key"],
            value=_deserialize_json(
                row["value_json"]
            ),
            created_at=(
                row["created_at"]
            ),
            updated_at=(
                row["updated_at"]
            ),
        )

    def delete(
        self,
        namespace: str,
        key: str,
    ) -> bool:
        normalized_namespace = (
            _normalize_namespace(
                namespace
            )
        )

        normalized_key = (
            _normalize_key(
                key
            )
        )

        with self._lock:
            self._ensure_open()

            try:
                with self._connection() as connection:
                    cursor = connection.execute(
                        """
                        DELETE FROM state_values
                        WHERE namespace = ?
                          AND state_key = ?;
                        """,
                        (
                            normalized_namespace,
                            normalized_key,
                        ),
                    )

                    return (
                        cursor.rowcount > 0
                    )

            except sqlite3.Error as exc:
                raise StorageError(
                    "LocalStateStore delete başarısız."
                ) from exc

    def count(
        self,
        namespace: Optional[
            str
        ] = None,
    ) -> int:
        with self._lock:
            self._ensure_open()

            try:
                with self._connection() as connection:
                    if namespace is None:
                        row = connection.execute(
                            """
                            SELECT COUNT(*) AS total
                            FROM state_values;
                            """
                        ).fetchone()

                    else:
                        normalized_namespace = (
                            _normalize_namespace(
                                namespace
                            )
                        )

                        row = connection.execute(
                            """
                            SELECT COUNT(*) AS total
                            FROM state_values
                            WHERE namespace = ?;
                            """,
                            (
                                normalized_namespace,
                            ),
                        ).fetchone()

                    return int(
                        row["total"]
                    )

            except sqlite3.Error as exc:
                raise StorageError(
                    "LocalStateStore count başarısız."
                ) from exc

    # =========================================================================
    # CLEAR
    # =========================================================================
    def clear_namespace(
        self,
        namespace: str,
    ) -> tuple[int, int]:
        """
        Namespace içindeki hem idempotency hem KV state kayıtlarını siler.

        Returns
        -------
        tuple[int, int]
            (seen_deleted, state_deleted)
        """

        normalized_namespace = (
            _normalize_namespace(
                namespace
            )
        )

        with self._lock:
            self._ensure_open()

            try:
                with self._connection() as connection:
                    seen_cursor = (
                        connection.execute(
                            """
                            DELETE FROM seen_records
                            WHERE namespace = ?;
                            """,
                            (
                                normalized_namespace,
                            ),
                        )
                    )

                    state_cursor = (
                        connection.execute(
                            """
                            DELETE FROM state_values
                            WHERE namespace = ?;
                            """,
                            (
                                normalized_namespace,
                            ),
                        )
                    )

                    return (
                        max(
                            0,
                            seen_cursor.rowcount,
                        ),
                        max(
                            0,
                            state_cursor.rowcount,
                        ),
                    )

            except sqlite3.Error as exc:
                raise StorageError(
                    "LocalStateStore clear_namespace başarısız."
                ) from exc

    # =========================================================================
    # DIAGNOSTICS
    # =========================================================================
    def snapshot(
        self,
    ) -> dict[str, Any]:
        return {
            "db_path": str(
                self.db_path
            ),
            "closed": self.is_closed,
            "timeout_seconds": (
                self.timeout_seconds
            ),
            "seen_count": (
                0
                if self.is_closed
                else self.seen_count()
            ),
            "state_count": (
                0
                if self.is_closed
                else self.count()
            ),
        }

    # =========================================================================
    # CLEANUP
    # =========================================================================
    def close(
        self,
    ) -> None:
        """
        Store'u yeni operasyonlara kapatır.

        Kalıcı connection tutulmadığı için kapatılacak SQLite descriptor yoktur.
        """

        with self._lock:
            self._closed = True

    def __enter__(
        self,
    ) -> "LocalStateStore":
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
    def __repr__(
        self,
    ) -> str:
        return (
            f"{self.__class__.__name__}("
            f"db_path='{self.db_path}', "
            f"closed={self.is_closed}"
            f")"
        )