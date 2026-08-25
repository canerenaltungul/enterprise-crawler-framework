from __future__ import annotations

"""
Enterprise Crawler Framework - Atomic File Writer

Yerel dosyaları güvenli ve atomik biçimde yazar.

Akış
----
payload
   ↓
temporary file
   ↓
write
   ↓
flush
   ↓
fsync(file)
   ↓
os.replace()
   ↓
fsync(directory)   [POSIX]
   ↓
final file

Garanti
-------
* Başarılı işlemde hedef dosya bütünüyle görünür.
* Yazım yarıda kesilirse hedef dosyada yarım içerik bırakılmaz.
* Geçici dosya hata durumunda temizlenir.
* overwrite=False ise mevcut hedef korunur.
* bytes, text ve JSON için ortak atomik altyapı kullanılır.

Bilerek içermez
---------------
* Cloud storage.
* Versioning.
* RawArchive.
* Evidence manifest.
* Checksum registry.
* Idempotency.
* Metadata database.
* Directory abstraction.

Bunlar daha üst storage katmanlarının sorumluluğudur.
"""

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional

from enterprise_crawler.exceptions import AtomicWriteError


# =============================================================================
# RESULT
# =============================================================================
@dataclass(frozen=True, slots=True)
class AtomicWriteResult:
    """
    Başarılı atomik yazım sonucu.
    """

    path: Path
    size_bytes: int
    replaced_existing: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "size_bytes": self.size_bytes,
            "replaced_existing": self.replaced_existing,
        }


# =============================================================================
# HELPERS
# =============================================================================
def _fsync_directory(
    directory: Path,
) -> None:
    """
    POSIX sistemlerde directory metadata'sını fsync eder.

    Windows'ta directory handle fsync davranışı taşınabilir olmadığı için
    bilinçli olarak no-op uygulanır.
    """

    if os.name == "nt":
        return

    try:
        descriptor = os.open(
            str(directory),
            os.O_RDONLY,
        )
    except OSError:
        return

    try:
        os.fsync(
            descriptor
        )
    except OSError:
        # Directory fsync best-effort'tur.
        pass
    finally:
        os.close(
            descriptor
        )


def _validate_target(
    target: str | Path,
) -> Path:
    target_path = Path(
        target
    )

    if not target_path.name:
        raise AtomicWriteError(
            "Atomic write target dosya adı içermelidir."
        )

    if (
        target_path.exists()
        and target_path.is_dir()
    ):
        raise AtomicWriteError(
            "Atomic write target bir klasör olamaz."
        )

    return target_path


# =============================================================================
# WRITER
# =============================================================================
class AtomicFileWriter:
    """
    Dependency-free local atomic file writer.

    Örnek::

        writer = AtomicFileWriter()

        writer.write_bytes(
            "data/raw.bin",
            b"payload",
        )

        writer.write_text(
            "data/example.txt",
            "hello",
        )

        writer.write_json(
            "data/example.json",
            {"hello": "world"},
        )
    """

    def __init__(
        self,
        *,
        create_parent_directories: bool = True,
    ) -> None:
        self.create_parent_directories = bool(
            create_parent_directories
        )

    # =========================================================================
    # BYTES
    # =========================================================================
    def write_bytes(
        self,
        target: str | Path,
        payload: bytes,
        *,
        overwrite: bool = True,
    ) -> AtomicWriteResult:
        if not isinstance(
            payload,
            bytes,
        ):
            raise AtomicWriteError(
                "write_bytes payload bytes olmalıdır."
            )

        target_path = _validate_target(
            target
        )

        parent = (
            target_path.parent
        )

        if self.create_parent_directories:
            try:
                parent.mkdir(
                    parents=True,
                    exist_ok=True,
                )
            except OSError as exc:
                raise AtomicWriteError(
                    "Atomic write parent directory oluşturulamadı "
                    f"| path={parent}"
                ) from exc

        elif not parent.exists():
            raise AtomicWriteError(
                "Atomic write parent directory mevcut değil "
                f"| path={parent}"
            )

        if not parent.is_dir():
            raise AtomicWriteError(
                "Atomic write parent path klasör değil "
                f"| path={parent}"
            )

        existed_before = (
            target_path.exists()
        )

        if (
            existed_before
            and not overwrite
        ):
            raise AtomicWriteError(
                "Atomic write target zaten mevcut "
                f"| path={target_path}"
            )

        temporary_path: Optional[
            Path
        ] = None

        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=parent,
                prefix=(
                    f".{target_path.name}."
                ),
                suffix=".tmp",
                delete=False,
            ) as handle:
                temporary_path = Path(
                    handle.name
                )

                handle.write(
                    payload
                )

                handle.flush()

                os.fsync(
                    handle.fileno()
                )

            # Race guard:
            # overwrite=False iken hedef write sırasında başka biri tarafından
            # oluşturulduysa üzerine yazmıyoruz.
            if (
                not overwrite
                and target_path.exists()
            ):
                raise AtomicWriteError(
                    "Atomic write target işlem sırasında oluşturuldu "
                    f"| path={target_path}"
                )

            os.replace(
                temporary_path,
                target_path,
            )

            temporary_path = None

            _fsync_directory(
                parent
            )

            return AtomicWriteResult(
                path=target_path,
                size_bytes=len(
                    payload
                ),
                replaced_existing=(
                    existed_before
                ),
            )

        except AtomicWriteError:
            raise

        except Exception as exc:
            raise AtomicWriteError(
                "Atomic file write başarısız "
                f"| path={target_path} "
                f"| error_type={exc.__class__.__name__}"
            ) from exc

        finally:
            if (
                temporary_path is not None
                and temporary_path.exists()
            ):
                try:
                    temporary_path.unlink()
                except OSError:
                    pass

    # =========================================================================
    # TEXT
    # =========================================================================
    def write_text(
        self,
        target: str | Path,
        payload: str,
        *,
        encoding: str = "utf-8",
        overwrite: bool = True,
    ) -> AtomicWriteResult:
        if not isinstance(
            payload,
            str,
        ):
            raise AtomicWriteError(
                "write_text payload str olmalıdır."
            )

        try:
            encoded = payload.encode(
                encoding
            )
        except (
            LookupError,
            UnicodeError,
        ) as exc:
            raise AtomicWriteError(
                "Text payload encode edilemedi "
                f"| encoding={encoding}"
            ) from exc

        return self.write_bytes(
            target,
            encoded,
            overwrite=overwrite,
        )

    # =========================================================================
    # JSON
    # =========================================================================
    def write_json(
        self,
        target: str | Path,
        payload: Mapping[str, Any],
        *,
        encoding: str = "utf-8",
        indent: Optional[int] = 2,
        sort_keys: bool = True,
        ensure_ascii: bool = False,
        overwrite: bool = True,
    ) -> AtomicWriteResult:
        if not isinstance(
            payload,
            Mapping,
        ):
            raise AtomicWriteError(
                "write_json payload Mapping olmalıdır."
            )

        try:
            serialized = json.dumps(
                dict(payload),
                ensure_ascii=ensure_ascii,
                indent=indent,
                sort_keys=sort_keys,
                default=str,
                allow_nan=False,
            )
        except (
            TypeError,
            ValueError,
        ) as exc:
            raise AtomicWriteError(
                "JSON payload serialize edilemedi."
            ) from exc

        return self.write_text(
            target,
            serialized,
            encoding=encoding,
            overwrite=overwrite,
        )

    # =========================================================================
    # REPRESENTATION
    # =========================================================================
    def __repr__(
        self,
    ) -> str:
        return (
            f"{self.__class__.__name__}("
            f"create_parent_directories="
            f"{self.create_parent_directories!r}"
            f")"
        )