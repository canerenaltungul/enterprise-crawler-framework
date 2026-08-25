from __future__ import annotations

"""
Enterprise Crawler Framework - CSV Processing

CSV/TSV benzeri delimiter-separated veriler için güvenli ve deterministik
processing katmanı.

Özellikler
----------
- str / bytes / bytearray / memoryview kaynakları
- Path üzerinden açık dosya okuma
- str kaynakların implicit file path olarak yorumlanmaması
- configurable encoding
- configurable delimiter
- header-aware parsing
- duplicate header koruması
- boş header koruması
- satır uzunluğu doğrulaması
- maksimum payload boyutu
- deterministic serialization
- dict/list yardımcıları

Önemli sözleşme
---------------
Path ile dosya okunur::

    processor.parse(Path("data.csv"))

String ise CSV içeriğidir::

    processor.parse("name,age\\nAda,36\\n")

Ancak yalnızca dosya yolu/dosya adı görünümündeki çıplak stringler,
örneğin ``"data.csv"``, sessiz biçimde tek hücreli CSV olarak kabul edilmez.
Bu, yanlış API kullanımını erken yakalamak için fail-fast davranıştır.
"""

import csv
import io
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from enterprise_crawler.exceptions import ProcessingError


DEFAULT_CSV_ENCODING = "utf-8"
DEFAULT_CSV_DELIMITER = ","
DEFAULT_CSV_MAX_BYTES = 32 * 1024 * 1024


class CsvProcessingError(ProcessingError):
    """CSV processing işlemleri sırasında oluşan framework hatası."""

    default_message = "CSV processing failed."


# =============================================================================
# INTERNAL HELPERS
# =============================================================================

_WINDOWS_DRIVE_PATH_RE = re.compile(
    r"^[A-Za-z]:[\\/]"
)


def _looks_like_explicit_file_path(
    value: str,
) -> bool:
    """
    String'in yüksek güvenle dosya yolu/dosya adı görünümünde olup olmadığını
    belirler.

    Amaç string'i dosyaya çevirmek DEĞİLDİR. Tam tersine, parse() sözleşmesinde
    dosyaların yalnız Path ile verilmesini zorlamaktır.

    Örnekler
    --------
    data.csv                -> True
    ./data.csv              -> True
    ../data.csv             -> True
    C:\\data\\data.csv      -> True
    folder/data.csv         -> True

    hello                   -> False
    name,age                -> False
    name,age\\nAda,36       -> False
    """

    if not isinstance(
        value,
        str,
    ):
        return False

    candidate = value.strip()

    if not candidate:
        return False

    # Gerçek multi-line CSV payload'ları path değildir.
    if (
        "\n" in candidate
        or "\r" in candidate
    ):
        return False

    # Delimiter-containing içerik normal CSV payload olabilir.
    # Burada default delimiter yanında yaygın TSV/semicolon biçimlerini
    # de path heuristiğinin dışında tutuyoruz.
    if (
        "," in candidate
        or ";" in candidate
        or "\t" in candidate
    ):
        return False

    normalized = candidate.replace(
        "\\",
        "/",
    )

    if normalized.startswith(
        (
            "./",
            "../",
            "/",
            "~/",
        )
    ):
        return True

    if _WINDOWS_DRIVE_PATH_RE.match(
        candidate
    ):
        return True

    if "/" in normalized:
        return True

    lowered = candidate.lower()

    return lowered.endswith(
        (
            ".csv",
            ".tsv",
        )
    )


# =============================================================================
# CONFIGURATION
# =============================================================================


@dataclass(
    frozen=True,
    slots=True,
)
class CsvProcessorConfig:
    """
    CSV processor yapılandırması.
    """

    encoding: str = DEFAULT_CSV_ENCODING
    delimiter: str = DEFAULT_CSV_DELIMITER
    max_bytes: int | None = DEFAULT_CSV_MAX_BYTES

    has_header: bool = True
    reject_duplicate_headers: bool = True
    reject_empty_headers: bool = True
    strict_row_length: bool = True

    skip_blank_rows: bool = True

    def __post_init__(
        self,
    ) -> None:
        encoding = self.encoding

        if not isinstance(
            encoding,
            str,
        ):
            raise CsvProcessingError(
                "encoding string olmalıdır."
            )

        normalized_encoding = (
            encoding.strip()
        )

        if not normalized_encoding:
            raise CsvProcessingError(
                "encoding boş olamaz."
            )

        try:
            "".encode(
                normalized_encoding
            )

        except LookupError as exc:
            raise CsvProcessingError(
                "Bilinmeyen CSV encoding "
                f"| encoding={normalized_encoding!r}"
            ) from exc

        object.__setattr__(
            self,
            "encoding",
            normalized_encoding,
        )

        delimiter = self.delimiter

        if not isinstance(
            delimiter,
            str,
        ):
            raise CsvProcessingError(
                "delimiter string olmalıdır."
            )

        if len(delimiter) != 1:
            raise CsvProcessingError(
                "delimiter tam olarak bir karakter olmalıdır."
            )

        object.__setattr__(
            self,
            "delimiter",
            delimiter,
        )

        if self.max_bytes is not None:
            if (
                isinstance(
                    self.max_bytes,
                    bool,
                )
                or not isinstance(
                    self.max_bytes,
                    int,
                )
                or self.max_bytes <= 0
            ):
                raise CsvProcessingError(
                    "max_bytes pozitif tam sayı "
                    "veya None olmalıdır."
                )

        for field_name in (
            "has_header",
            "reject_duplicate_headers",
            "reject_empty_headers",
            "strict_row_length",
            "skip_blank_rows",
        ):
            value = getattr(
                self,
                field_name,
            )

            if not isinstance(
                value,
                bool,
            ):
                raise CsvProcessingError(
                    f"{field_name} boolean olmalıdır."
                )

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "encoding": self.encoding,
            "delimiter": self.delimiter,
            "max_bytes": self.max_bytes,
            "has_header": self.has_header,
            "reject_duplicate_headers": (
                self.reject_duplicate_headers
            ),
            "reject_empty_headers": (
                self.reject_empty_headers
            ),
            "strict_row_length": (
                self.strict_row_length
            ),
            "skip_blank_rows": (
                self.skip_blank_rows
            ),
        }


# =============================================================================
# CSV DOCUMENT
# =============================================================================


@dataclass(
    slots=True,
)
class CsvDocument:
    """
    Normalize edilmiş CSV dokümanı.
    """

    headers: list[str]
    rows: list[list[str]]

    def __post_init__(
        self,
    ) -> None:
        if not isinstance(
            self.headers,
            list,
        ):
            raise CsvProcessingError(
                "headers list olmalıdır."
            )

        if not isinstance(
            self.rows,
            list,
        ):
            raise CsvProcessingError(
                "rows list olmalıdır."
            )

        for index, header in enumerate(
            self.headers,
            start=1,
        ):
            if not isinstance(
                header,
                str,
            ):
                raise CsvProcessingError(
                    "CSV header string olmalıdır "
                    f"| column={index} "
                    f"| actual={type(header).__name__}"
                )

        for row_index, row in enumerate(
            self.rows,
            start=1,
        ):
            if not isinstance(
                row,
                list,
            ):
                raise CsvProcessingError(
                    "CSV row list olmalıdır "
                    f"| row={row_index} "
                    f"| actual={type(row).__name__}"
                )

            for column_index, value in enumerate(
                row,
                start=1,
            ):
                if not isinstance(
                    value,
                    str,
                ):
                    raise CsvProcessingError(
                        "CSV cell string olmalıdır "
                        f"| row={row_index} "
                        f"| column={column_index} "
                        f"| actual={type(value).__name__}"
                    )

    @property
    def row_count(
        self,
    ) -> int:
        return len(
            self.rows
        )

    @property
    def column_count(
        self,
    ) -> int:
        if self.headers:
            return len(
                self.headers
            )

        if not self.rows:
            return 0

        return max(
            len(row)
            for row in self.rows
        )

    def to_rows(
        self,
    ) -> list[list[str]]:
        return [
            list(row)
            for row in self.rows
        ]

    def to_dicts(
        self,
    ) -> list[
        dict[str, str]
    ]:
        if not self.headers:
            raise CsvProcessingError(
                "Header bulunmayan CSV "
                "dict satırlarına dönüştürülemez."
            )

        result: list[
            dict[str, str]
        ] = []

        expected_length = len(
            self.headers
        )

        for index, row in enumerate(
            self.rows,
            start=1,
        ):
            if len(row) != expected_length:
                raise CsvProcessingError(
                    "CSV satır uzunluğu header ile eşleşmiyor "
                    f"| row={index} "
                    f"| expected={expected_length} "
                    f"| actual={len(row)}"
                )

            result.append(
                dict(
                    zip(
                        self.headers,
                        row,
                        strict=True,
                    )
                )
            )

        return result

    def column(
        self,
        name: str,
    ) -> list[str]:
        normalized_name = str(
            name or ""
        ).strip()

        if not normalized_name:
            raise CsvProcessingError(
                "column name boş olamaz."
            )

        if not self.headers:
            raise CsvProcessingError(
                "Header bulunmayan CSV'de "
                "isimle sütun seçilemez."
            )

        try:
            position = self.headers.index(
                normalized_name
            )

        except ValueError as exc:
            raise CsvProcessingError(
                "CSV sütunu bulunamadı "
                f"| column={normalized_name!r}"
            ) from exc

        values: list[str] = []

        for row_index, row in enumerate(
            self.rows,
            start=1,
        ):
            if position >= len(row):
                raise CsvProcessingError(
                    "CSV satırında sütun eksik "
                    f"| row={row_index} "
                    f"| column={normalized_name!r}"
                )

            values.append(
                row[position]
            )

        return values

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "headers": list(
                self.headers
            ),
            "rows": self.to_rows(),
            "row_count": (
                self.row_count
            ),
            "column_count": (
                self.column_count
            ),
        }


# =============================================================================
# PROCESSOR
# =============================================================================


class CsvProcessor:
    """
    CSV parser ve serializer.
    """

    def __init__(
        self,
        config: CsvProcessorConfig | None = None,
    ) -> None:
        if config is None:
            config = CsvProcessorConfig()

        if not isinstance(
            config,
            CsvProcessorConfig,
        ):
            raise CsvProcessingError(
                "config CsvProcessorConfig olmalıdır."
            )

        self.config = config

    # =========================================================================
    # PARSING
    # =========================================================================

    def parse(
        self,
        source: Any,
    ) -> CsvDocument:
        if isinstance(
            source,
            str,
        ):
            if _looks_like_explicit_file_path(
                source
            ):
                raise CsvProcessingError(
                    "String kaynak dosya yolu olarak "
                    "yorumlanmaz. CSV dosyaları için "
                    "pathlib.Path kullan "
                    f"| source={source!r}"
                )

            return self.parse_text(
                source
            )

        if isinstance(
            source,
            bytes,
        ):
            return self.parse_bytes(
                source
            )

        if isinstance(
            source,
            bytearray,
        ):
            return self.parse_bytes(
                bytes(source)
            )

        if isinstance(
            source,
            memoryview,
        ):
            return self.parse_bytes(
                source.tobytes()
            )

        if isinstance(
            source,
            Path,
        ):
            return self.parse_file(
                source
            )

        raise CsvProcessingError(
            "Desteklenmeyen CSV kaynak tipi "
            f"| actual={type(source).__name__}"
        )

    def parse_text(
        self,
        payload: str,
    ) -> CsvDocument:
        if not isinstance(
            payload,
            str,
        ):
            raise CsvProcessingError(
                "CSV text payload string olmalıdır."
            )

        if not payload.strip():
            raise CsvProcessingError(
                "CSV payload boş olamaz."
            )

        try:
            encoded = payload.encode(
                self.config.encoding
            )

        except UnicodeEncodeError as exc:
            raise CsvProcessingError(
                "CSV text configured encoding ile "
                "encode edilemedi "
                f"| encoding={self.config.encoding!r}"
            ) from exc

        self._check_size(
            len(encoded)
        )

        return self._parse_text(
            payload
        )

    def parse_bytes(
        self,
        payload: bytes,
    ) -> CsvDocument:
        if not isinstance(
            payload,
            bytes,
        ):
            raise CsvProcessingError(
                "CSV byte payload bytes olmalıdır."
            )

        if not payload:
            raise CsvProcessingError(
                "CSV payload boş olamaz."
            )

        self._check_size(
            len(payload)
        )

        try:
            text = payload.decode(
                self.config.encoding
            )

        except UnicodeDecodeError as exc:
            raise CsvProcessingError(
                "CSV byte payload decode edilemedi "
                f"| encoding={self.config.encoding!r}"
            ) from exc

        if not text.strip():
            raise CsvProcessingError(
                "CSV payload boş olamaz."
            )

        return self._parse_text(
            text
        )

    def parse_file(
        self,
        path: Path,
    ) -> CsvDocument:
        if not isinstance(
            path,
            Path,
        ):
            raise CsvProcessingError(
                "CSV file path pathlib.Path olmalıdır."
            )

        if not path.exists():
            raise CsvProcessingError(
                "CSV dosyası bulunamadı "
                f"| path={path}"
            )

        if not path.is_file():
            raise CsvProcessingError(
                "CSV path normal dosya olmalıdır "
                f"| path={path}"
            )

        try:
            size = path.stat().st_size

        except OSError as exc:
            raise CsvProcessingError(
                "CSV dosya boyutu okunamadı "
                f"| path={path}"
            ) from exc

        self._check_size(
            size
        )

        try:
            payload = path.read_bytes()

        except OSError as exc:
            raise CsvProcessingError(
                "CSV dosyası okunamadı "
                f"| path={path}"
            ) from exc

        return self.parse_bytes(
            payload
        )

    def _parse_text(
        self,
        payload: str,
    ) -> CsvDocument:
        stream = io.StringIO(
            payload,
            newline="",
        )

        try:
            reader = csv.reader(
                stream,
                delimiter=(
                    self.config.delimiter
                ),
            )

            raw_rows = [
                list(row)
                for row in reader
            ]

        except csv.Error as exc:
            raise CsvProcessingError(
                "CSV parse başarısız "
                f"| error={exc}"
            ) from exc

        rows = self._filter_rows(
            raw_rows
        )

        if not rows:
            raise CsvProcessingError(
                "CSV içerisinde kullanılabilir satır yok."
            )

        if self.config.has_header:
            headers = list(
                rows[0]
            )

            data_rows = [
                list(row)
                for row in rows[1:]
            ]

            self._validate_headers(
                headers
            )

            if (
                self.config.strict_row_length
                and data_rows
            ):
                self._validate_row_lengths(
                    data_rows,
                    expected=len(
                        headers
                    ),
                )

            return CsvDocument(
                headers=headers,
                rows=data_rows,
            )

        data_rows = [
            list(row)
            for row in rows
        ]

        if (
            self.config.strict_row_length
            and data_rows
        ):
            self._validate_row_lengths(
                data_rows,
                expected=len(
                    data_rows[0]
                ),
            )

        return CsvDocument(
            headers=[],
            rows=data_rows,
        )

    def _filter_rows(
        self,
        rows: Iterable[
            list[str]
        ],
    ) -> list[list[str]]:
        result: list[
            list[str]
        ] = []

        for row in rows:
            if (
                self.config.skip_blank_rows
                and (
                    not row
                    or all(
                        not value.strip()
                        for value in row
                    )
                )
            ):
                continue

            result.append(
                list(row)
            )

        return result

    def _validate_headers(
        self,
        headers: Sequence[str],
    ) -> None:
        if not headers:
            raise CsvProcessingError(
                "CSV header bulunamadı."
            )

        if self.config.reject_empty_headers:
            for index, header in enumerate(
                headers,
                start=1,
            ):
                if not str(
                    header
                ).strip():
                    raise CsvProcessingError(
                        "CSV header boş olamaz "
                        f"| column={index}"
                    )

        if (
            self.config.reject_duplicate_headers
        ):
            seen: set[str] = set()

            for header in headers:
                if header in seen:
                    raise CsvProcessingError(
                        "Duplicate CSV header "
                        f"| header={header!r}"
                    )

                seen.add(
                    header
                )

    @staticmethod
    def _validate_row_lengths(
        rows: Sequence[
            Sequence[str]
        ],
        *,
        expected: int,
    ) -> None:
        for index, row in enumerate(
            rows,
            start=1,
        ):
            actual = len(
                row
            )

            if actual != expected:
                raise CsvProcessingError(
                    "CSV satır uzunluğu beklenen "
                    "sütun sayısıyla eşleşmiyor "
                    f"| row={index} "
                    f"| expected={expected} "
                    f"| actual={actual}"
                )

    # =========================================================================
    # SERIALIZATION
    # =========================================================================

    def serialize(
        self,
        document: CsvDocument,
        *,
        line_terminator: str = "\n",
    ) -> str:
        if not isinstance(
            document,
            CsvDocument,
        ):
            raise CsvProcessingError(
                "document CsvDocument olmalıdır."
            )

        if not isinstance(
            line_terminator,
            str,
        ):
            raise CsvProcessingError(
                "line_terminator string olmalıdır."
            )

        if not line_terminator:
            raise CsvProcessingError(
                "line_terminator boş olamaz."
            )

        buffer = io.StringIO(
            newline=""
        )

        try:
            writer = csv.writer(
                buffer,
                delimiter=(
                    self.config.delimiter
                ),
                lineterminator=(
                    line_terminator
                ),
            )

            if document.headers:
                writer.writerow(
                    document.headers
                )

            writer.writerows(
                document.rows
            )

        except csv.Error as exc:
            raise CsvProcessingError(
                "CSV serialization başarısız "
                f"| error={exc}"
            ) from exc

        rendered = buffer.getvalue()

        try:
            encoded = rendered.encode(
                self.config.encoding
            )

        except UnicodeEncodeError as exc:
            raise CsvProcessingError(
                "Serialized CSV configured encoding "
                "ile encode edilemedi "
                f"| encoding={self.config.encoding!r}"
            ) from exc

        self._check_size(
            len(encoded)
        )

        return rendered

    def serialize_bytes(
        self,
        document: CsvDocument,
        *,
        line_terminator: str = "\n",
    ) -> bytes:
        text = self.serialize(
            document,
            line_terminator=(
                line_terminator
            ),
        )

        try:
            return text.encode(
                self.config.encoding
            )

        except UnicodeEncodeError as exc:
            raise CsvProcessingError(
                "Serialized CSV bytes üretilemedi "
                f"| encoding={self.config.encoding!r}"
            ) from exc

    # =========================================================================
    # ROW BUILDERS
    # =========================================================================

    def from_dicts(
        self,
        rows: Sequence[
            Mapping[str, Any]
        ],
        *,
        headers: Sequence[
            str
        ]
        | None = None,
    ) -> CsvDocument:
        if isinstance(
            rows,
            (
                str,
                bytes,
                bytearray,
                memoryview,
            ),
        ):
            raise CsvProcessingError(
                "rows mapping sequence olmalıdır."
            )

        try:
            normalized_rows = list(
                rows
            )

        except TypeError as exc:
            raise CsvProcessingError(
                "rows iterable olmalıdır."
            ) from exc

        for index, row in enumerate(
            normalized_rows,
            start=1,
        ):
            if not isinstance(
                row,
                Mapping,
            ):
                raise CsvProcessingError(
                    "Her CSV satırı mapping olmalıdır "
                    f"| row={index} "
                    f"| actual={type(row).__name__}"
                )

            for key in row.keys():
                if not isinstance(
                    key,
                    str,
                ):
                    raise CsvProcessingError(
                        "CSV mapping key string olmalıdır "
                        f"| row={index}"
                    )

        if headers is None:
            if not normalized_rows:
                return CsvDocument(
                    headers=[],
                    rows=[],
                )

            resolved_headers = list(
                normalized_rows[0].keys()
            )

        else:
            if isinstance(
                headers,
                (
                    str,
                    bytes,
                    bytearray,
                    memoryview,
                ),
            ):
                raise CsvProcessingError(
                    "headers string olmayan "
                    "sequence olmalıdır."
                )

            try:
                resolved_headers = list(
                    headers
                )

            except TypeError as exc:
                raise CsvProcessingError(
                    "headers sequence olmalıdır."
                ) from exc

            for index, header in enumerate(
                resolved_headers,
                start=1,
            ):
                if not isinstance(
                    header,
                    str,
                ):
                    raise CsvProcessingError(
                        "CSV header string olmalıdır "
                        f"| column={index} "
                        f"| actual={type(header).__name__}"
                    )

        self._validate_headers(
            resolved_headers
        )

        resolved_header_set = set(
            resolved_headers
        )

        document_rows: list[
            list[str]
        ] = []

        for row_index, row in enumerate(
            normalized_rows,
            start=1,
        ):
            unknown_keys = (
                set(row.keys())
                - resolved_header_set
            )

            if unknown_keys:
                raise CsvProcessingError(
                    "CSV mapping bilinmeyen alan içeriyor "
                    f"| row={row_index} "
                    f"| fields={sorted(unknown_keys)!r}"
                )

            document_rows.append(
                [
                    self._stringify_value(
                        row.get(
                            header,
                            "",
                        )
                    )
                    for header
                    in resolved_headers
                ]
            )

        return CsvDocument(
            headers=resolved_headers,
            rows=document_rows,
        )

    @staticmethod
    def _stringify_value(
        value: Any,
    ) -> str:
        if value is None:
            return ""

        if isinstance(
            value,
            str,
        ):
            return value

        if isinstance(
            value,
            (
                bool,
                int,
                float,
            ),
        ):
            return str(
                value
            )

        raise CsvProcessingError(
            "CSV hücre değeri scalar olmalıdır "
            f"| actual={type(value).__name__}"
        )

    # =========================================================================
    # INTERNAL
    # =========================================================================

    def _check_size(
        self,
        size: int,
    ) -> None:
        limit = (
            self.config.max_bytes
        )

        if (
            limit is not None
            and size > limit
        ):
            raise CsvProcessingError(
                "CSV payload izin verilen boyutu aşıyor "
                f"| size={size} "
                f"| max_bytes={limit}"
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
        }

    def __repr__(
        self,
    ) -> str:
        return (
            f"{self.__class__.__name__}("
            f"encoding={self.config.encoding!r}, "
            f"delimiter={self.config.delimiter!r}, "
            f"max_bytes={self.config.max_bytes!r}, "
            f"has_header={self.config.has_header!r}"
            f")"
        )


# =============================================================================
# PUBLIC HELPERS
# =============================================================================


def parse_csv(
    source: Any,
    *,
    config: CsvProcessorConfig | None = None,
) -> CsvDocument:
    return CsvProcessor(
        config
    ).parse(
        source
    )


def parse_csv_file(
    path: Path,
    *,
    config: CsvProcessorConfig | None = None,
) -> CsvDocument:
    return CsvProcessor(
        config
    ).parse_file(
        path
    )


def serialize_csv(
    document: CsvDocument,
    *,
    config: CsvProcessorConfig | None = None,
    line_terminator: str = "\n",
) -> str:
    return CsvProcessor(
        config
    ).serialize(
        document,
        line_terminator=(
            line_terminator
        ),
    )


__all__ = [
    "DEFAULT_CSV_ENCODING",
    "DEFAULT_CSV_DELIMITER",
    "DEFAULT_CSV_MAX_BYTES",
    "CsvProcessingError",
    "CsvProcessorConfig",
    "CsvDocument",
    "CsvProcessor",
    "parse_csv",
    "parse_csv_file",
    "serialize_csv",
]