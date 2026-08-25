from __future__ import annotations

"""
Enterprise Crawler Framework - Storage Manager

Framework storage bileşenlerini tek composition noktası altında toplar.

Sorumlulukları
--------------
* AtomicFileWriter lifecycle erişimi sağlamak.
* LocalStorage oluşturmak veya inject edilmiş instance kullanmak.
* LocalStateStore oluşturmak veya inject edilmiş instance kullanmak.
* Storage root ve state database konfigürasyonunu merkezileştirmek.
* Resource ownership kurallarını uygulamak.
* Runtime snapshot üretmek.
* Context manager desteği sağlamak.

Bilerek içermez
---------------
* Dosya yazma implementasyonu.
* SQLite sorgu implementasyonu.
* RawArchive.
* Evidence chain.
* Cloud storage.
* Dataset lineage.
* Checkpoint semantics.
* Event queue.

Bu sınıf bir facade/composition root'tur.
"""

import threading
from pathlib import Path
from typing import Any, Optional

from enterprise_crawler.exceptions import StorageError
from enterprise_crawler.storage.atomic import AtomicFileWriter
from enterprise_crawler.storage.local import LocalStorage
from enterprise_crawler.storage.local_state_store import LocalStateStore


# =============================================================================
# STORAGE MANAGER
# =============================================================================
class StorageManager:
    """
    Framework storage bileşenlerinin merkezi erişim noktası.

    Varsayılan yapı::

        StorageManager(
            root="data/storage"
        )

    şu bileşenleri oluşturur::

        manager.writer
        manager.files
        manager.state

    Varsayılan state database::

        <root>/.state/local_state.db

    Örnek::

        with StorageManager(
            root="data/storage"
        ) as storage:

            storage.files.save_json(
                "records/item.json",
                {"id": 1},
            )

            storage.state.mark_seen(
                "records",
                "item-1",
            )
    """

    def __init__(
        self,
        root: str | Path,
        *,
        state_db_path: Optional[
            str | Path
        ] = None,
        create_root: bool = True,
        state_timeout_seconds: float = 15.0,

        # Dependency injection
        writer: Optional[
            AtomicFileWriter
        ] = None,
        files: Optional[
            LocalStorage
        ] = None,
        state: Optional[
            LocalStateStore
        ] = None,
    ) -> None:
        self._lock = threading.RLock()
        self._closed = False

        # ---------------------------------------------------------------------
        # ROOT
        # ---------------------------------------------------------------------
        raw_root = Path(
            root
        )

        if not str(
            raw_root
        ).strip():
            raise StorageError(
                "StorageManager root boş olamaz."
            )

        try:
            resolved_root = (
                raw_root.expanduser().resolve()
            )
        except OSError as exc:
            raise StorageError(
                "StorageManager root çözümlenemedi."
            ) from exc

        if resolved_root.exists():
            if not resolved_root.is_dir():
                raise StorageError(
                    "StorageManager root klasör olmalıdır "
                    f"| path={resolved_root}"
                )

        elif create_root:
            try:
                resolved_root.mkdir(
                    parents=True,
                    exist_ok=True,
                )
            except OSError as exc:
                raise StorageError(
                    "StorageManager root oluşturulamadı "
                    f"| path={resolved_root}"
                ) from exc

        else:
            raise StorageError(
                "StorageManager root mevcut değil "
                f"| path={resolved_root}"
            )

        self.root = resolved_root

        # ---------------------------------------------------------------------
        # WRITER
        # ---------------------------------------------------------------------
        if writer is None:
            self.writer = AtomicFileWriter()

            self._owns_writer = True
        else:
            self.writer = writer

            self._owns_writer = False

        # ---------------------------------------------------------------------
        # FILE STORAGE
        # ---------------------------------------------------------------------
        if files is None:
            self.files = LocalStorage(
                self.root,
                create_root=False,
                writer=self.writer,
            )

            self._owns_files = True
        else:
            self.files = files

            self._owns_files = False

        # ---------------------------------------------------------------------
        # STATE DATABASE PATH
        # ---------------------------------------------------------------------
        if state_db_path is None:
            resolved_state_db_path = (
                self.root
                / ".state"
                / "local_state.db"
            )
        else:
            raw_state_db = Path(
                state_db_path
            )

            if raw_state_db.is_absolute():
                resolved_state_db_path = (
                    raw_state_db
                    .expanduser()
                    .resolve()
                )
            else:
                resolved_state_db_path = (
                    self.root
                    / raw_state_db
                ).resolve()

        self.state_db_path = (
            resolved_state_db_path
        )

        # ---------------------------------------------------------------------
        # STATE STORE
        # ---------------------------------------------------------------------
        if state is None:
            self.state = LocalStateStore(
                self.state_db_path,
                timeout_seconds=(
                    state_timeout_seconds
                ),
            )

            self._owns_state = True
        else:
            self.state = state

            self._owns_state = False

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
                "Kapalı StorageManager kullanılamaz."
            )

    # =========================================================================
    # CONVENIENCE ACCESS
    # =========================================================================
    def save_bytes(
        self,
        relative_path: str | Path,
        payload: bytes,
        *,
        overwrite: bool = True,
    ):
        self._ensure_open()

        return self.files.save_bytes(
            relative_path,
            payload,
            overwrite=overwrite,
        )

    def save_text(
        self,
        relative_path: str | Path,
        payload: str,
        *,
        encoding: str = "utf-8",
        overwrite: bool = True,
    ):
        self._ensure_open()

        return self.files.save_text(
            relative_path,
            payload,
            encoding=encoding,
            overwrite=overwrite,
        )

    def save_json(
        self,
        relative_path: str | Path,
        payload: dict[str, Any],
        *,
        encoding: str = "utf-8",
        indent: Optional[int] = 2,
        sort_keys: bool = True,
        ensure_ascii: bool = False,
        overwrite: bool = True,
    ):
        self._ensure_open()

        return self.files.save_json(
            relative_path,
            payload,
            encoding=encoding,
            indent=indent,
            sort_keys=sort_keys,
            ensure_ascii=ensure_ascii,
            overwrite=overwrite,
        )

    def load_bytes(
        self,
        relative_path: str | Path,
    ) -> bytes:
        self._ensure_open()

        return self.files.load_bytes(
            relative_path
        )

    def load_text(
        self,
        relative_path: str | Path,
        *,
        encoding: str = "utf-8",
    ) -> str:
        self._ensure_open()

        return self.files.load_text(
            relative_path,
            encoding=encoding,
        )

    def load_json(
        self,
        relative_path: str | Path,
        *,
        encoding: str = "utf-8",
    ) -> dict[str, Any]:
        self._ensure_open()

        return self.files.load_json(
            relative_path,
            encoding=encoding,
        )

    def exists(
        self,
        relative_path: str | Path,
    ) -> bool:
        self._ensure_open()

        return self.files.exists(
            relative_path
        )

    def delete_file(
        self,
        relative_path: str | Path,
        *,
        missing_ok: bool = False,
    ) -> bool:
        self._ensure_open()

        return self.files.delete(
            relative_path,
            missing_ok=missing_ok,
        )

    # =========================================================================
    # IDEMPOTENCY CONVENIENCE
    # =========================================================================
    def has_seen(
        self,
        namespace: str,
        key: str,
    ) -> bool:
        self._ensure_open()

        return self.state.has_seen(
            namespace,
            key,
        )

    def mark_seen(
        self,
        namespace: str,
        key: str,
        *,
        metadata: Optional[
            dict[str, Any]
        ] = None,
    ) -> bool:
        self._ensure_open()

        return self.state.mark_seen(
            namespace,
            key,
            metadata=metadata,
        )

    # =========================================================================
    # DIAGNOSTICS
    # =========================================================================
    def snapshot(
        self,
    ) -> dict[str, Any]:
        with self._lock:
            return {
                "root": str(
                    self.root
                ),
                "closed": self._closed,
                "state_db_path": str(
                    self.state_db_path
                ),
                "owns_writer": (
                    self._owns_writer
                ),
                "owns_files": (
                    self._owns_files
                ),
                "owns_state": (
                    self._owns_state
                ),
                "state": (
                    self.state.snapshot()
                    if not self._closed
                    else None
                ),
            }

    # =========================================================================
    # CLEANUP
    # =========================================================================
    def close(
        self,
    ) -> None:
        """
        Manager-owned kaynakları kapatır.

        AtomicFileWriter ve LocalStorage kalıcı resource tutmadığı için
        fiziksel close işlemi gerekmez.

        LocalStateStore manager tarafından oluşturulduysa kapatılır.

        Inject edilmiş resource'ların lifecycle'ı çağırana aittir.
        """

        with self._lock:
            if self._closed:
                return

            self._closed = True

            errors: list[
                BaseException
            ] = []

            if self._owns_state:
                try:
                    self.state.close()
                except Exception as exc:
                    errors.append(
                        exc
                    )

            if errors:
                raise StorageError(
                    "StorageManager cleanup başarısız "
                    f"| error_count={len(errors)} "
                    f"| first_error={errors[0]}"
                )

    def __enter__(
        self,
    ) -> "StorageManager":
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
            f"root='{self.root}', "
            f"closed={self.is_closed}"
            f")"
        )