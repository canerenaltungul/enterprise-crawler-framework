from __future__ import annotations

"""
Enterprise Crawler Framework - Downloader

HTTP kaynaklarını güvenli ve streaming biçimde yerel dosyaya indirir.

Sorumlulukları
--------------
* HttpClient üzerinden HTTP GET yapmak.
* Response body'yi RAM'e tamamen almadan stream etmek.
* Maksimum dosya boyutu sınırı uygulamak.
* SHA-256'yı streaming sırasında hesaplamak.
* Content-Length varsa erken boyut kontrolü yapmak.
* Geçici dosyaya yazmak.
* fsync sonrası hedef dosyaya atomik replace yapmak.
* Hata/cancellation durumunda temporary dosyayı temizlemek.
* İsteğe bağlı expected SHA-256 doğrulaması yapmak.
* Standart DownloadResult üretmek.

Bilerek içermez
---------------
* Genel storage abstraction.
* RawArchive / evidence chain.
* Cloud storage.
* ZIP/PDF parsing.
* MIME sniffing.
* Malware scanning.
* Dataset persistence.
* Retry policy.

HTTP retry ve transport davranışı HttpClient / SessionManager katmanındadır.
"""

import hashlib
import os
import re
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from enterprise_crawler.core.http_client import HttpClient
from enterprise_crawler.exceptions import (
    DownloadError,
    NetworkError,
    ShutdownRequested,
)


_SHA256_RE = re.compile(
    r"^[A-Fa-f0-9]{64}$"
)

DEFAULT_CHUNK_SIZE = 64 * 1024
DEFAULT_MAX_DOWNLOAD_BYTES = 256 * 1024 * 1024


# =============================================================================
# RESULT CONTRACT
# =============================================================================
@dataclass(frozen=True, slots=True)
class DownloadResult:
    url: str
    path: Path

    sha256: str
    size_bytes: int

    status_code: int
    content_type: Optional[str]
    content_length: Optional[int]

    chunk_count: int

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)

        payload["path"] = str(
            self.path
        )

        return payload


# =============================================================================
# HELPERS
# =============================================================================
def _normalize_sha256(
    value: str,
) -> str:
    normalized = str(
        value or ""
    ).strip().lower()

    if not _SHA256_RE.fullmatch(
        normalized
    ):
        raise DownloadError(
            "expected_sha256 geçerli SHA-256 değil."
        )

    return normalized


def _parse_content_length(
    value: Any,
) -> Optional[int]:
    if value in (
        None,
        "",
    ):
        return None

    if isinstance(
        value,
        bool,
    ):
        raise DownloadError(
            "Content-Length geçersiz."
        )

    try:
        parsed = int(
            str(value).strip()
        )
    except (
        TypeError,
        ValueError,
    ) as exc:
        raise DownloadError(
            "Content-Length geçersiz."
        ) from exc

    if parsed < 0:
        raise DownloadError(
            "Content-Length negatif olamaz."
        )

    return parsed


def _normalize_content_type(
    value: Any,
) -> Optional[str]:
    raw = str(
        value or ""
    ).strip()

    if not raw:
        return None

    return (
        raw.split(
            ";",
            1,
        )[0]
        .strip()
        .lower()
        or None
    )


# =============================================================================
# DOWNLOADER
# =============================================================================
class Downloader:
    """
    HttpClient üzerine kurulu streaming file downloader.

    Örnek::

        downloader = Downloader(
            http_client
        )

        result = downloader.download(
            "https://example.com/file.pdf",
            "data/file.pdf",
        )
    """

    def __init__(
        self,
        http_client: HttpClient,
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_download_bytes: Optional[int] = (
            DEFAULT_MAX_DOWNLOAD_BYTES
        ),
        stop_check: Optional[
            Callable[[], None]
        ] = None,
    ) -> None:
        if not isinstance(
            chunk_size,
            int,
        ) or isinstance(
            chunk_size,
            bool,
        ):
            raise ValueError(
                "chunk_size tam sayı olmalıdır."
            )

        if chunk_size < 1:
            raise ValueError(
                "chunk_size en az 1 olmalıdır."
            )

        if (
            max_download_bytes
            is not None
            and (
                isinstance(
                    max_download_bytes,
                    bool,
                )
                or not isinstance(
                    max_download_bytes,
                    int,
                )
                or max_download_bytes < 1
            )
        ):
            raise ValueError(
                "max_download_bytes None veya "
                "pozitif tam sayı olmalıdır."
            )

        self.http_client = (
            http_client
        )

        self.chunk_size = (
            chunk_size
        )

        self.max_download_bytes = (
            max_download_bytes
        )

        self.stop_check = (
            stop_check
        )

    # =========================================================================
    # STOP
    # =========================================================================
    def _run_stop_check(
        self,
    ) -> None:
        if self.stop_check is not None:
            self.stop_check()

    # =========================================================================
    # LIMIT
    # =========================================================================
    def _effective_limit(
        self,
        override: Optional[int],
    ) -> Optional[int]:
        if override is None:
            return (
                self.max_download_bytes
            )

        if (
            isinstance(
                override,
                bool,
            )
            or not isinstance(
                override,
                int,
            )
            or override < 1
        ):
            raise DownloadError(
                "max_bytes pozitif tam sayı olmalıdır."
            )

        return override

    # =========================================================================
    # DOWNLOAD
    # =========================================================================
    def download(
        self,
        url: str,
        target: str | Path,
        *,
        overwrite: bool = False,
        expected_sha256: Optional[
            str
        ] = None,
        max_bytes: Optional[
            int
        ] = None,
        headers: Optional[
            Mapping[str, str]
        ] = None,
        **request_kwargs: Any,
    ) -> DownloadResult:
        """
        URL'yi target dosyasına streaming biçimde indirir.

        Hedef dosya yalnız indirme tamamen başarılı olduktan sonra görünür.
        """

        target_path = Path(
            target
        )

        if not target_path.name:
            raise DownloadError(
                "Download target dosya adı içermelidir."
            )

        if (
            target_path.exists()
            and target_path.is_dir()
        ):
            raise DownloadError(
                "Download target bir klasör olamaz."
            )

        if (
            target_path.exists()
            and not overwrite
        ):
            raise DownloadError(
                f"Hedef dosya zaten mevcut: "
                f"{target_path}"
            )

        expected_digest = (
            _normalize_sha256(
                expected_sha256
            )
            if expected_sha256
            is not None
            else None
        )

        effective_limit = (
            self._effective_limit(
                max_bytes
            )
        )

        target_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        temporary_path: Optional[
            Path
        ] = None

        response: Any = None

        self._run_stop_check()

        try:
            response = (
                self.http_client.get(
                    url,
                    headers=headers,
                    stream=True,
                    **request_kwargs,
                )
            )

            status_code = int(
                response.status_code
            )

            response_headers = getattr(
                response,
                "headers",
                {},
            ) or {}

            content_length = (
                _parse_content_length(
                    response_headers.get(
                        "Content-Length"
                    )
                )
            )

            content_type = (
                _normalize_content_type(
                    response_headers.get(
                        "Content-Type"
                    )
                )
            )

            if (
                effective_limit
                is not None
                and content_length
                is not None
                and content_length
                > effective_limit
            ):
                raise DownloadError(
                    "Download Content-Length "
                    "maksimum boyutu aşıyor "
                    f"| content_length="
                    f"{content_length} "
                    f"| max_bytes="
                    f"{effective_limit}"
                )

            hasher = (
                hashlib.sha256()
            )

            size_bytes = 0
            chunk_count = 0

            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=target_path.parent,
                prefix=(
                    f".{target_path.name}."
                ),
                suffix=".part",
                delete=False,
            ) as handle:
                temporary_path = Path(
                    handle.name
                )

                iterator = (
                    response.iter_content(
                        chunk_size=(
                            self.chunk_size
                        )
                    )
                )

                for chunk in iterator:
                    self._run_stop_check()

                    if not chunk:
                        continue

                    if not isinstance(
                        chunk,
                        bytes,
                    ):
                        raise DownloadError(
                            "HTTP response iter_content "
                            "bytes döndürmelidir."
                        )

                    next_size = (
                        size_bytes
                        + len(chunk)
                    )

                    if (
                        effective_limit
                        is not None
                        and next_size
                        > effective_limit
                    ):
                        raise DownloadError(
                            "Download streaming sırasında "
                            "maksimum boyutu aştı "
                            f"| max_bytes="
                            f"{effective_limit}"
                        )

                    handle.write(
                        chunk
                    )

                    hasher.update(
                        chunk
                    )

                    size_bytes = (
                        next_size
                    )

                    chunk_count += 1

                handle.flush()
                os.fsync(
                    handle.fileno()
                )

            self._run_stop_check()

            calculated_digest = (
                hasher.hexdigest()
            )

            if (
                expected_digest
                is not None
                and calculated_digest
                != expected_digest
            ):
                raise DownloadError(
                    "Download SHA-256 doğrulaması "
                    "başarısız "
                    f"| expected="
                    f"{expected_digest} "
                    f"| actual="
                    f"{calculated_digest}"
                )

            if (
                target_path.exists()
                and not overwrite
            ):
                raise DownloadError(
                    "Hedef dosya indirme sırasında "
                    "oluşturuldu; overwrite=False."
                )

            os.replace(
                temporary_path,
                target_path,
            )

            temporary_path = None

            return DownloadResult(
                url=str(url),
                path=target_path,
                sha256=(
                    calculated_digest
                ),
                size_bytes=(
                    size_bytes
                ),
                status_code=(
                    status_code
                ),
                content_type=(
                    content_type
                ),
                content_length=(
                    content_length
                ),
                chunk_count=(
                    chunk_count
                ),
            )

        except ShutdownRequested:
            raise

        except DownloadError:
            raise

        except NetworkError as exc:
            raise DownloadError(
                f"HTTP download başarısız "
                f"| url={url} "
                f"| error={exc}"
            ) from exc

        except Exception as exc:
            raise DownloadError(
                "Beklenmeyen download hatası "
                f"| url={url} "
                f"| error_type="
                f"{exc.__class__.__name__}"
            ) from exc

        finally:
            if (
                temporary_path
                is not None
                and temporary_path.exists()
            ):
                try:
                    temporary_path.unlink()
                except OSError:
                    pass

            if response is not None:
                close_method = getattr(
                    response,
                    "close",
                    None,
                )

                if callable(
                    close_method
                ):
                    try:
                        close_method()
                    except Exception:
                        pass

    # =========================================================================
    # REPRESENTATION
    # =========================================================================
    def __repr__(
        self,
    ) -> str:
        return (
            f"{self.__class__.__name__}("
            f"chunk_size={self.chunk_size}, "
            f"max_download_bytes="
            f"{self.max_download_bytes!r}"
            f")"
        )