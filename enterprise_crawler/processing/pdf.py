from __future__ import annotations

"""
Enterprise Crawler Framework - PDF Processing

Bu modül PDF dosyaları için hafif, güvenli ve bağımlılıksız bir
binary validation katmanı sağlar.

Amaç
----
Bu ilk PDF processing sürümü:

- bytes / bytearray / memoryview kabul eder,
- Path üzerinden açıkça dosya okuyabilir,
- payload boyut sınırını uygular,
- PDF magic header doğrular,
- PDF version bilgisini çıkarır,
- EOF marker doğrular,
- SHA-256 fingerprint üretir,
- temel binary özellikleri gözlemler.

Bilerek yapmadıkları
--------------------
Bu modül:

- OCR yapmaz,
- PDF metni çıkarmaz,
- PDF render etmez,
- sayfa içeriğini yorumlamaz,
- JavaScript çalıştırmaz,
- embedded file açmaz,
- şifreli PDF çözmez.

Bu davranışlar daha üst seviyeli document-processing katmanlarının
sorumluluğudur.

Security
--------
PDF, oldukça karmaşık ve aktif içerik taşıyabilen bir container
formatıdır. Bu nedenle burada PDF'nin yalnızca güvenli biçimde
tanımlanması ve fingerprint edilmesi hedeflenir.

Binary içerisinde:

- /Encrypt
- /JavaScript
- /JS
- /EmbeddedFile
- /Launch

gibi belirteçler gözlemlenir fakat bu ilk sürümde çalıştırılmaz veya
yorumlanmaz.
"""

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


# =============================================================================
# CONSTANTS
# =============================================================================
DEFAULT_PDF_MAX_BYTES = (
    256 * 1024 * 1024
)

PDF_MAGIC = b"%PDF-"

PDF_EOF_MARKER = b"%%EOF"

PDF_VERSION_RE = re.compile(
    rb"^%PDF-(\d)\.(\d)"
)

SUPPORTED_PDF_VERSIONS = frozenset(
    {
        "1.0",
        "1.1",
        "1.2",
        "1.3",
        "1.4",
        "1.5",
        "1.6",
        "1.7",
        "2.0",
    }
)

PDF_EOF_SEARCH_BYTES = 1024


# =============================================================================
# EXCEPTIONS
# =============================================================================
class PdfProcessingError(
    RuntimeError
):
    """
    PDF processing sözleşmesi ihlal edildiğinde üretilir.
    """


# =============================================================================
# HELPERS
# =============================================================================
def _validate_optional_positive_int(
    value: Any,
    *,
    field_name: str,
) -> Optional[int]:
    if value is None:
        return None

    if isinstance(
        value,
        bool,
    ):
        raise PdfProcessingError(
            f"{field_name} boolean olamaz."
        )

    if not isinstance(
        value,
        int,
    ):
        raise PdfProcessingError(
            f"{field_name} tam sayı olmalıdır."
        )

    if value <= 0:
        raise PdfProcessingError(
            f"{field_name} sıfırdan büyük olmalıdır."
        )

    return value


def _validate_bool(
    value: Any,
    *,
    field_name: str,
) -> bool:
    if not isinstance(
        value,
        bool,
    ):
        raise PdfProcessingError(
            f"{field_name} boolean olmalıdır."
        )

    return value


def _sha256(
    payload: bytes,
) -> str:
    return hashlib.sha256(
        payload
    ).hexdigest()


def _contains_token(
    payload: bytes,
    token: bytes,
) -> bool:
    return (
        token.lower()
        in payload.lower()
    )


# =============================================================================
# CONFIGURATION
# =============================================================================
@dataclass(
    frozen=True,
    slots=True,
)
class PdfProcessorConfig:
    """
    PdfProcessor runtime configuration.

    Parameters
    ----------
    max_bytes:
        Kabul edilen maksimum PDF boyutu.

        ``None`` verilirse boyut limiti kaldırılır.

    require_eof_marker:
        PDF sonunda ``%%EOF`` marker aranıp aranmayacağını belirler.

    reject_unsupported_version:
        Bilinmeyen PDF version değerlerinin reddedilip reddedilmeyeceğini
        belirler.
    """

    max_bytes: Optional[
        int
    ] = DEFAULT_PDF_MAX_BYTES

    require_eof_marker: bool = True

    reject_unsupported_version: bool = True

    def __post_init__(
        self,
    ) -> None:
        validated_max_bytes = (
            _validate_optional_positive_int(
                self.max_bytes,
                field_name="max_bytes",
            )
        )

        validated_require_eof = (
            _validate_bool(
                self.require_eof_marker,
                field_name="require_eof_marker",
            )
        )

        validated_version_policy = (
            _validate_bool(
                self.reject_unsupported_version,
                field_name=(
                    "reject_unsupported_version"
                ),
            )
        )

        object.__setattr__(
            self,
            "max_bytes",
            validated_max_bytes,
        )

        object.__setattr__(
            self,
            "require_eof_marker",
            validated_require_eof,
        )

        object.__setattr__(
            self,
            "reject_unsupported_version",
            validated_version_policy,
        )

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "max_bytes": (
                self.max_bytes
            ),
            "require_eof_marker": (
                self.require_eof_marker
            ),
            "reject_unsupported_version": (
                self.reject_unsupported_version
            ),
        }


# =============================================================================
# DOCUMENT
# =============================================================================
@dataclass(
    frozen=True,
    slots=True,
)
class PdfDocument:
    """
    Doğrulanmış PDF binary document descriptor.

    Bu sınıf PDF'nin kendisini parse edilmiş object graph olarak temsil etmez.
    Yalnız doğrulanan binary hakkında deterministic gözlem taşır.
    """

    payload: bytes

    byte_size: int

    sha256: str

    version: str

    has_eof_marker: bool

    encrypted: bool

    contains_javascript: bool

    contains_embedded_files: bool

    contains_launch_action: bool

    source_path: Optional[
        Path
    ] = None

    @property
    def is_pdf(
        self,
    ) -> bool:
        return True

    def to_bytes(
        self,
    ) -> bytes:
        return bytes(
            self.payload
        )

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "byte_size": (
                self.byte_size
            ),
            "sha256": (
                self.sha256
            ),
            "version": (
                self.version
            ),
            "has_eof_marker": (
                self.has_eof_marker
            ),
            "encrypted": (
                self.encrypted
            ),
            "contains_javascript": (
                self.contains_javascript
            ),
            "contains_embedded_files": (
                self.contains_embedded_files
            ),
            "contains_launch_action": (
                self.contains_launch_action
            ),
            "source_path": (
                str(
                    self.source_path
                )
                if self.source_path
                is not None
                else None
            ),
        }


# =============================================================================
# PROCESSOR
# =============================================================================
class PdfProcessor:
    """
    Hafif ve fail-closed PDF binary processor.

    Example
    -------
    ::

        processor = PdfProcessor()

        document = processor.parse(
            pdf_bytes
        )

        print(document.sha256)
        print(document.version)

    File API
    --------
    ::

        document = processor.parse_file(
            Path("document.pdf")
        )

    ``parse()`` metoduna string path verilmez. Dosya okumak isteyen caller
    açık biçimde ``Path`` + ``parse_file()`` kullanmalıdır.
    """

    def __init__(
        self,
        config: Optional[
            PdfProcessorConfig
        ] = None,
    ) -> None:
        if (
            config is not None
            and not isinstance(
                config,
                PdfProcessorConfig,
            )
        ):
            raise PdfProcessingError(
                "config PdfProcessorConfig "
                "olmalıdır."
            )

        self.config = (
            config
            or PdfProcessorConfig()
        )

    # =========================================================================
    # SIZE VALIDATION
    # =========================================================================
    def _validate_size(
        self,
        byte_size: int,
    ) -> None:
        limit = (
            self.config.max_bytes
        )

        if (
            limit is not None
            and byte_size > limit
        ):
            raise PdfProcessingError(
                "PDF payload boyut limitini aşıyor "
                f"| size={byte_size} "
                f"| max_bytes={limit}"
            )

    # =========================================================================
    # PAYLOAD NORMALIZATION
    # =========================================================================
    def _normalize_payload(
        self,
        source: Any,
    ) -> bytes:
        if isinstance(
            source,
            bytes,
        ):
            payload = source

        elif isinstance(
            source,
            bytearray,
        ):
            payload = bytes(
                source
            )

        elif isinstance(
            source,
            memoryview,
        ):
            payload = (
                source.tobytes()
            )

        else:
            raise PdfProcessingError(
                "PDF source bytes, bytearray veya "
                "memoryview olmalıdır; "
                "dosya okumak için parse_file() kullan. "
                f"actual={type(source).__name__}"
            )

        if not payload:
            raise PdfProcessingError(
                "PDF payload boş olamaz."
            )

        self._validate_size(
            len(payload)
        )

        return payload

    # =========================================================================
    # FORMAT VALIDATION
    # =========================================================================
    @staticmethod
    def _extract_version(
        payload: bytes,
    ) -> str:
        match = (
            PDF_VERSION_RE.match(
                payload
            )
        )

        if match is None:
            raise PdfProcessingError(
                "Geçerli PDF header bulunamadı."
            )

        major = (
            match.group(1)
            .decode("ascii")
        )

        minor = (
            match.group(2)
            .decode("ascii")
        )

        return (
            f"{major}.{minor}"
        )

    @staticmethod
    def _has_eof_marker(
        payload: bytes,
    ) -> bool:
        search_window = payload[
            -PDF_EOF_SEARCH_BYTES:
        ]

        return (
            PDF_EOF_MARKER
            in search_window
        )

    def _validate_pdf(
        self,
        payload: bytes,
    ) -> tuple[
        str,
        bool,
    ]:
        if not payload.startswith(
            PDF_MAGIC
        ):
            raise PdfProcessingError(
                "PDF magic header bulunamadı."
            )

        version = (
            self._extract_version(
                payload
            )
        )

        if (
            self.config.reject_unsupported_version
            and version
            not in SUPPORTED_PDF_VERSIONS
        ):
            raise PdfProcessingError(
                "Desteklenmeyen PDF version "
                f"| version={version}"
            )

        has_eof_marker = (
            self._has_eof_marker(
                payload
            )
        )

        if (
            self.config.require_eof_marker
            and not has_eof_marker
        ):
            raise PdfProcessingError(
                "PDF EOF marker bulunamadı."
            )

        return (
            version,
            has_eof_marker,
        )

    # =========================================================================
    # SECURITY OBSERVATION
    # =========================================================================
    @staticmethod
    def _security_flags(
        payload: bytes,
    ) -> dict[str, bool]:
        encrypted = (
            _contains_token(
                payload,
                b"/Encrypt",
            )
        )

        contains_javascript = (
            _contains_token(
                payload,
                b"/JavaScript",
            )
            or _contains_token(
                payload,
                b"/JS",
            )
        )

        contains_embedded_files = (
            _contains_token(
                payload,
                b"/EmbeddedFile",
            )
        )

        contains_launch_action = (
            _contains_token(
                payload,
                b"/Launch",
            )
        )

        return {
            "encrypted": (
                encrypted
            ),
            "contains_javascript": (
                contains_javascript
            ),
            "contains_embedded_files": (
                contains_embedded_files
            ),
            "contains_launch_action": (
                contains_launch_action
            ),
        }

    # =========================================================================
    # PARSE
    # =========================================================================
    def parse(
        self,
        source: Any,
    ) -> PdfDocument:
        payload = (
            self._normalize_payload(
                source
            )
        )

        (
            version,
            has_eof_marker,
        ) = self._validate_pdf(
            payload
        )

        flags = (
            self._security_flags(
                payload
            )
        )

        return PdfDocument(
            payload=payload,
            byte_size=len(
                payload
            ),
            sha256=_sha256(
                payload
            ),
            version=version,
            has_eof_marker=(
                has_eof_marker
            ),
            encrypted=flags[
                "encrypted"
            ],
            contains_javascript=flags[
                "contains_javascript"
            ],
            contains_embedded_files=flags[
                "contains_embedded_files"
            ],
            contains_launch_action=flags[
                "contains_launch_action"
            ],
            source_path=None,
        )

    # =========================================================================
    # FILE API
    # =========================================================================
    def parse_file(
        self,
        path: Path,
    ) -> PdfDocument:
        if not isinstance(
            path,
            Path,
        ):
            raise PdfProcessingError(
                "parse_file() Path nesnesi "
                "gerektirir."
            )

        if not path.exists():
            raise PdfProcessingError(
                "PDF dosyası bulunamadı "
                f"| path={path}"
            )

        if not path.is_file():
            raise PdfProcessingError(
                "PDF path normal dosya olmalıdır "
                f"| path={path}"
            )

        try:
            file_size = (
                path.stat().st_size
            )

        except OSError as exc:
            raise PdfProcessingError(
                "PDF dosya boyutu okunamadı "
                f"| path={path}"
            ) from exc

        self._validate_size(
            file_size
        )

        try:
            payload = (
                path.read_bytes()
            )

        except OSError as exc:
            raise PdfProcessingError(
                "PDF dosyası okunamadı "
                f"| path={path}"
            ) from exc

        document = (
            self.parse(
                payload
            )
        )

        return PdfDocument(
            payload=(
                document.payload
            ),
            byte_size=(
                document.byte_size
            ),
            sha256=(
                document.sha256
            ),
            version=(
                document.version
            ),
            has_eof_marker=(
                document.has_eof_marker
            ),
            encrypted=(
                document.encrypted
            ),
            contains_javascript=(
                document.contains_javascript
            ),
            contains_embedded_files=(
                document.contains_embedded_files
            ),
            contains_launch_action=(
                document.contains_launch_action
            ),
            source_path=(
                path
            ),
        )

    # =========================================================================
    # SNAPSHOT
    # =========================================================================
    def snapshot(
        self,
    ) -> dict[str, Any]:
        return {
            "processor": (
                self.__class__.__name__
            ),
            "config": (
                self.config.to_dict()
            ),
            "supported_versions": (
                sorted(
                    SUPPORTED_PDF_VERSIONS
                )
            ),
        }

    # =========================================================================
    # REPRESENTATION
    # =========================================================================
    def __repr__(
        self,
    ) -> str:
        return (
            "PdfProcessor("
            f"max_bytes={self.config.max_bytes!r}, "
            "require_eof_marker="
            f"{self.config.require_eof_marker!r}, "
            "reject_unsupported_version="
            f"{self.config.reject_unsupported_version!r}"
            ")"
        )


# =============================================================================
# PUBLIC HELPERS
# =============================================================================
def parse_pdf(
    source: Any,
    *,
    config: Optional[
        PdfProcessorConfig
    ] = None,
) -> PdfDocument:
    """
    Tek seferlik PDF binary parse helper.
    """

    return PdfProcessor(
        config=config
    ).parse(
        source
    )


def parse_pdf_file(
    path: Path,
    *,
    config: Optional[
        PdfProcessorConfig
    ] = None,
) -> PdfDocument:
    """
    Tek seferlik PDF file parse helper.
    """

    return PdfProcessor(
        config=config
    ).parse_file(
        path
    )


__all__ = [
    "DEFAULT_PDF_MAX_BYTES",
    "PDF_EOF_MARKER",
    "PDF_EOF_SEARCH_BYTES",
    "PDF_MAGIC",
    "SUPPORTED_PDF_VERSIONS",
    "PdfDocument",
    "PdfProcessingError",
    "PdfProcessor",
    "PdfProcessorConfig",
    "parse_pdf",
    "parse_pdf_file",
]