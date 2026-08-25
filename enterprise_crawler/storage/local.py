from __future__ import annotations

"""
Enterprise Crawler Framework - Local Storage

Storage root altında güvenli yerel dosya erişimi sağlar.

Sorumlulukları
--------------
* Root-relative path çözümlemek.
* Path traversal'ı engellemek.
* AtomicFileWriter üzerinden güvenli write yapmak.
* bytes / text / JSON save ve load sağlamak.
* exists() ve delete() işlemleri sunmak.
* Root dışındaki hiçbir path'e erişmemek.

Bilerek içermez
---------------
* SQLite state.
* Metadata database.
* RawArchive.
* Evidence chain.
* Cloud storage.
* Versioning.
* Lock manager.
* Dataset lineage.

Bunlar daha üst storage katmanlarının sorumluluğudur.
"""

import json
from pathlib import Path
from typing import Any, Mapping, Optional

from enterprise_crawler.exceptions import StorageError
from enterprise_crawler.storage.atomic import (
    AtomicFileWriter,
    AtomicWriteResult,
)


# =============================================================================
# HELPERS
# =============================================================================
def _is_relative_to(
    path: Path,
    root: Path,
) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


# =============================================================================
# LOCAL STORAGE
# =============================================================================
class LocalStorage:
    """
    Güvenli root-scoped local storage.

    Örnek::

        storage = LocalStorage(
            "data/storage"
        )

        storage.save_json(
            "records/item.json",
            {"id": 1},
        )

        payload = storage.load_json(
            "records/item.json"
        )
    """

    def __init__(
        self,
        root: str | Path,
        *,
        create_root: bool = True,
        writer: Optional[
            AtomicFileWriter
        ] = None,
    ) -> None:
        raw_root = Path(
            root
        )

        if not str(
            raw_root
        ).strip():
            raise StorageError(
                "LocalStorage root boş olamaz."
            )

        try:
            resolved_root = (
                raw_root.expanduser().resolve()
            )
        except OSError as exc:
            raise StorageError(
                "LocalStorage root çözümlenemedi."
            ) from exc

        if resolved_root.exists():
            if not resolved_root.is_dir():
                raise StorageError(
                    "LocalStorage root klasör olmalıdır "
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
                    "LocalStorage root oluşturulamadı "
                    f"| path={resolved_root}"
                ) from exc

        else:
            raise StorageError(
                "LocalStorage root mevcut değil "
                f"| path={resolved_root}"
            )

        self.root = (
            resolved_root
        )

        self.writer = (
            writer
            if writer is not None
            else AtomicFileWriter()
        )

    # =========================================================================
    # PATH RESOLUTION
    # =========================================================================
    def resolve(
        self,
        relative_path: str | Path,
    ) -> Path:
        """
        Root-relative path'i güvenli mutlak path'e dönüştürür.

        Absolute path reddedilir.
        ``..`` traversal root dışına çıkıyorsa reddedilir.
        """

        raw_path = Path(
            relative_path
        )

        if raw_path.is_absolute():
            raise StorageError(
                "LocalStorage absolute path kabul etmez."
            )

        if not str(
            raw_path
        ).strip():
            raise StorageError(
                "LocalStorage path boş olamaz."
            )

        try:
            candidate = (
                self.root
                / raw_path
            ).resolve()
        except OSError as exc:
            raise StorageError(
                "LocalStorage path çözümlenemedi."
            ) from exc

        if not _is_relative_to(
            candidate,
            self.root,
        ):
            raise StorageError(
                "LocalStorage path storage root dışına çıkamaz "
                f"| root={self.root} "
                f"| requested={relative_path}"
            )

        return candidate

    # =========================================================================
    # EXISTS
    # =========================================================================
    def exists(
        self,
        relative_path: str | Path,
    ) -> bool:
        return self.resolve(
            relative_path
        ).exists()

    def is_file(
        self,
        relative_path: str | Path,
    ) -> bool:
        return self.resolve(
            relative_path
        ).is_file()

    # =========================================================================
    # SAVE
    # =========================================================================
    def save_bytes(
        self,
        relative_path: str | Path,
        payload: bytes,
        *,
        overwrite: bool = True,
    ) -> AtomicWriteResult:
        target = self.resolve(
            relative_path
        )

        try:
            return self.writer.write_bytes(
                target,
                payload,
                overwrite=overwrite,
            )

        except Exception as exc:
            if isinstance(
                exc,
                StorageError,
            ):
                raise

            raise StorageError(
                "LocalStorage bytes save başarısız "
                f"| path={relative_path}"
            ) from exc

    def save_text(
        self,
        relative_path: str | Path,
        payload: str,
        *,
        encoding: str = "utf-8",
        overwrite: bool = True,
    ) -> AtomicWriteResult:
        target = self.resolve(
            relative_path
        )

        try:
            return self.writer.write_text(
                target,
                payload,
                encoding=encoding,
                overwrite=overwrite,
            )

        except Exception as exc:
            if isinstance(
                exc,
                StorageError,
            ):
                raise

            raise StorageError(
                "LocalStorage text save başarısız "
                f"| path={relative_path}"
            ) from exc

    def save_json(
        self,
        relative_path: str | Path,
        payload: Mapping[str, Any],
        *,
        encoding: str = "utf-8",
        indent: Optional[int] = 2,
        sort_keys: bool = True,
        ensure_ascii: bool = False,
        overwrite: bool = True,
    ) -> AtomicWriteResult:
        target = self.resolve(
            relative_path
        )

        try:
            return self.writer.write_json(
                target,
                payload,
                encoding=encoding,
                indent=indent,
                sort_keys=sort_keys,
                ensure_ascii=ensure_ascii,
                overwrite=overwrite,
            )

        except Exception as exc:
            if isinstance(
                exc,
                StorageError,
            ):
                raise

            raise StorageError(
                "LocalStorage JSON save başarısız "
                f"| path={relative_path}"
            ) from exc

    # =========================================================================
    # LOAD
    # =========================================================================
    def load_bytes(
        self,
        relative_path: str | Path,
    ) -> bytes:
        path = self.resolve(
            relative_path
        )

        if not path.exists():
            raise StorageError(
                "LocalStorage dosya bulunamadı "
                f"| path={relative_path}"
            )

        if not path.is_file():
            raise StorageError(
                "LocalStorage path dosya değil "
                f"| path={relative_path}"
            )

        try:
            return path.read_bytes()

        except OSError as exc:
            raise StorageError(
                "LocalStorage bytes load başarısız "
                f"| path={relative_path}"
            ) from exc

    def load_text(
        self,
        relative_path: str | Path,
        *,
        encoding: str = "utf-8",
    ) -> str:
        path = self.resolve(
            relative_path
        )

        if not path.exists():
            raise StorageError(
                "LocalStorage dosya bulunamadı "
                f"| path={relative_path}"
            )

        if not path.is_file():
            raise StorageError(
                "LocalStorage path dosya değil "
                f"| path={relative_path}"
            )

        try:
            return path.read_text(
                encoding=encoding
            )

        except (
            OSError,
            UnicodeError,
            LookupError,
        ) as exc:
            raise StorageError(
                "LocalStorage text load başarısız "
                f"| path={relative_path}"
            ) from exc

    def load_json(
        self,
        relative_path: str | Path,
        *,
        encoding: str = "utf-8",
    ) -> dict[str, Any]:
        raw = self.load_text(
            relative_path,
            encoding=encoding,
        )

        try:
            parsed = json.loads(
                raw
            )

        except json.JSONDecodeError as exc:
            raise StorageError(
                "LocalStorage JSON parse başarısız "
                f"| path={relative_path}"
            ) from exc

        if not isinstance(
            parsed,
            dict,
        ):
            raise StorageError(
                "LocalStorage JSON root object olmalıdır "
                f"| path={relative_path}"
            )

        return parsed

    # =========================================================================
    # DELETE
    # =========================================================================
    def delete(
        self,
        relative_path: str | Path,
        *,
        missing_ok: bool = False,
    ) -> bool:
        path = self.resolve(
            relative_path
        )

        if not path.exists():
            if missing_ok:
                return False

            raise StorageError(
                "LocalStorage silinecek dosya bulunamadı "
                f"| path={relative_path}"
            )

        if not path.is_file():
            raise StorageError(
                "LocalStorage delete yalnız dosya silebilir "
                f"| path={relative_path}"
            )

        try:
            path.unlink()

        except OSError as exc:
            raise StorageError(
                "LocalStorage delete başarısız "
                f"| path={relative_path}"
            ) from exc

        return True

    # =========================================================================
    # REPRESENTATION
    # =========================================================================
    def __repr__(
        self,
    ) -> str:
        """
        İnsan tarafından okunabilir storage root gösterimi.

        Path ``!r`` ile render edilmez; özellikle Windows'ta backslash'ların
        çift escape edilmesini istemiyoruz.
        """

        return (
            f"{self.__class__.__name__}("
            f"root='{self.root}'"
            f")"
        )