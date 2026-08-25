from __future__ import annotations

"""
Enterprise Crawler Framework - JSON Processing

Framework seviyesinde güvenli ve deterministic JSON işleme yardımcıları.

Temel hedefler
--------------
- str, bytes ve Path girdilerini desteklemek
- UTF-8 JSON işlemek
- malformed JSON'u framework exception'ına dönüştürmek
- duplicate object key'lerini varsayılan olarak reddetmek
- NaN / Infinity gibi standard dışı JSON değerlerini reddetmek
- object / array root contract'larını doğrulamak
- deterministic serialization sağlamak
- isteğe bağlı payload boyut sınırı uygulamak

Bu modül storage katmanı değildir.

Dosya yazma ve atomic persistence için::

    enterprise_crawler.storage

kullanılmalıdır.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from enterprise_crawler.exceptions import (
    ContractValidationError,
    ProcessingError,
)


# =============================================================================
# TYPES
# =============================================================================
JsonScalar = (
    str
    | int
    | float
    | bool
    | None
)

JsonValue = (
    JsonScalar
    | list["JsonValue"]
    | dict[str, "JsonValue"]
)


# =============================================================================
# CONSTANTS
# =============================================================================
DEFAULT_JSON_ENCODING = "utf-8"
DEFAULT_MAX_JSON_BYTES = 16 * 1024 * 1024


# =============================================================================
# EXCEPTIONS
# =============================================================================
class JsonProcessingError(ProcessingError):
    """
    JSON decode / encode / dosya okuma hatası.
    """

    default_message = (
        "JSON processing failed."
    )


# =============================================================================
# CONFIGURATION
# =============================================================================
@dataclass(
    frozen=True,
    slots=True,
)
class JsonProcessorConfig:
    """
    JSON processor davranış ayarları.

    Parameters
    ----------
    encoding:
        Dosya ve byte decode encoding'i.

    max_bytes:
        Tek JSON payload için maksimum byte boyutu.
        None verilirse limit uygulanmaz.

    reject_duplicate_keys:
        JSON object içinde aynı key birden fazla kez geçerse hata üretir.

    allow_scalar_root:
        False ise JSON root yalnız object veya array olabilir.
    """

    encoding: str = DEFAULT_JSON_ENCODING

    max_bytes: int | None = (
        DEFAULT_MAX_JSON_BYTES
    )

    reject_duplicate_keys: bool = True

    allow_scalar_root: bool = True

    def __post_init__(
        self,
    ) -> None:
        encoding = str(
            self.encoding or ""
        ).strip()

        if not encoding:
            raise ContractValidationError(
                "JSON encoding boş olamaz."
            )

        object.__setattr__(
            self,
            "encoding",
            encoding,
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
                raise ContractValidationError(
                    "JSON max_bytes pozitif "
                    "tam sayı veya None olmalıdır."
                )

        if not isinstance(
            self.reject_duplicate_keys,
            bool,
        ):
            raise ContractValidationError(
                "reject_duplicate_keys "
                "boolean olmalıdır."
            )

        if not isinstance(
            self.allow_scalar_root,
            bool,
        ):
            raise ContractValidationError(
                "allow_scalar_root "
                "boolean olmalıdır."
            )


# =============================================================================
# INTERNAL HELPERS
# =============================================================================
def _safe_error_message(
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


def _reject_constant(
    value: str,
) -> None:
    """
    Python json modülü varsayılan olarak NaN / Infinity kabul edebilir.

    Bunlar RFC uyumlu JSON değildir.
    """

    raise JsonProcessingError(
        "Standard dışı JSON numeric value "
        f"reddedildi: {value}"
    )


def _object_pairs_no_duplicates(
    pairs: list[
        tuple[str, Any]
    ],
) -> dict[str, Any]:
    result: dict[
        str,
        Any,
    ] = {}

    for key, value in pairs:
        if key in result:
            raise JsonProcessingError(
                "Duplicate JSON object key "
                f"reddedildi: {key!r}"
            )

        result[key] = value

    return result


def _validate_json_value(
    value: Any,
    *,
    path: str = "$",
) -> None:
    """
    Python nesnesinin strict JSON olarak serialize edilebilir olduğunu doğrular.

    Özellikle:
    - dict key'leri string olmalı
    - float NaN / Infinity olamaz
    - custom object'ler kabul edilmez
    """

    if value is None:
        return

    if isinstance(
        value,
        bool,
    ):
        return

    if isinstance(
        value,
        str,
    ):
        return

    if isinstance(
        value,
        int,
    ):
        return

    if isinstance(
        value,
        float,
    ):
        # json.dumps(..., allow_nan=False)
        # zaten son güvenlik katmanıdır.
        #
        # Burada ayrıca açık validation yapmak,
        # daha anlaşılır framework hatası sağlar.
        if (
            value != value
            or value
            in (
                float("inf"),
                float("-inf"),
            )
        ):
            raise ContractValidationError(
                "Non-finite JSON number "
                f"reddedildi | path={path}"
            )

        return

    if isinstance(
        value,
        Mapping,
    ):
        for key, child in value.items():
            if not isinstance(
                key,
                str,
            ):
                raise ContractValidationError(
                    "JSON object key string "
                    "olmalıdır "
                    f"| path={path} "
                    f"| actual={type(key).__name__}"
                )

            _validate_json_value(
                child,
                path=(
                    f"{path}.{key}"
                ),
            )

        return

    if isinstance(
        value,
        (list, tuple),
    ):
        for index, child in enumerate(
            value
        ):
            _validate_json_value(
                child,
                path=(
                    f"{path}[{index}]"
                ),
            )

        return

    raise ContractValidationError(
        "JSON serialize edilemeyen "
        "değer "
        f"| path={path} "
        f"| actual={type(value).__name__}"
    )


# =============================================================================
# JSON PROCESSOR
# =============================================================================
class JsonProcessor:
    """
    Enterprise Crawler JSON processor.

    Örnek
    -----

    String::

        processor = JsonProcessor()

        data = processor.parse(
            '{"name": "crawler"}'
        )

    Bytes::

        data = processor.parse(
            b'{"ok": true}'
        )

    File::

        data = processor.parse(
            Path("data.json")
        )

    Object contract::

        payload = processor.parse_object(
            '{"id": 1}'
        )

    Deterministic serialization::

        text = processor.serialize(
            {
                "b": 2,
                "a": 1,
            }
        )
    """

    def __init__(
        self,
        *,
        config: (
            JsonProcessorConfig
            | None
        ) = None,
    ) -> None:
        if (
            config is not None
            and not isinstance(
                config,
                JsonProcessorConfig,
            )
        ):
            raise ContractValidationError(
                "config JsonProcessorConfig "
                "olmalıdır."
            )

        self.config = (
            config
            or JsonProcessorConfig()
        )

    # =========================================================================
    # PUBLIC PARSE
    # =========================================================================
    def parse(
        self,
        source: (
            str
            | bytes
            | bytearray
            | memoryview
            | Path
        ),
    ) -> Any:
        """
        JSON kaynağını parse eder.

        Source davranışı
        ----------------
        str:
            JSON text olarak yorumlanır.

        bytes / bytearray / memoryview:
            config.encoding ile decode edilir.

        Path:
            Dosya olarak okunur.

        String path otomatik algılanmaz.

        Bunun nedeni::

            '{"file": "data.json"}'

        gibi geçerli JSON stringleriyle filesystem path arasında
        belirsizlik oluşturmamaktır.
        """

        if isinstance(
            source,
            Path,
        ):
            value = self._parse_path(
                source
            )

        elif isinstance(
            source,
            str,
        ):
            self._check_text_size(
                source
            )

            value = self._loads(
                source
            )

        elif isinstance(
            source,
            (
                bytes,
                bytearray,
                memoryview,
            ),
        ):
            raw = bytes(
                source
            )

            self._check_byte_size(
                raw
            )

            value = self._parse_bytes(
                raw
            )

        else:
            raise ContractValidationError(
                "JSON source str, bytes, "
                "bytearray, memoryview veya "
                "Path olmalıdır; "
                f"actual={type(source).__name__}."
            )

        if (
            not self.config.allow_scalar_root
            and not isinstance(
                value,
                (dict, list),
            )
        ):
            raise JsonProcessingError(
                "JSON root object veya array "
                "olmalıdır; "
                f"actual={type(value).__name__}."
            )

        return value

    def parse_object(
        self,
        source: (
            str
            | bytes
            | bytearray
            | memoryview
            | Path
        ),
    ) -> dict[str, Any]:
        """
        Root object olmak zorundadır.
        """

        value = self.parse(
            source
        )

        if not isinstance(
            value,
            dict,
        ):
            raise JsonProcessingError(
                "JSON root object olmalıdır; "
                f"actual={type(value).__name__}."
            )

        return value

    def parse_array(
        self,
        source: (
            str
            | bytes
            | bytearray
            | memoryview
            | Path
        ),
    ) -> list[Any]:
        """
        Root array olmak zorundadır.
        """

        value = self.parse(
            source
        )

        if not isinstance(
            value,
            list,
        ):
            raise JsonProcessingError(
                "JSON root array olmalıdır; "
                f"actual={type(value).__name__}."
            )

        return value

    # =========================================================================
    # SERIALIZATION
    # =========================================================================
    def serialize(
        self,
        value: Any,
        *,
        pretty: bool = False,
        sort_keys: bool = True,
    ) -> str:
        """
        Python değerini strict JSON string'e dönüştürür.

        Varsayılan çıktı deterministic'tir.
        """

        if not isinstance(
            pretty,
            bool,
        ):
            raise ContractValidationError(
                "pretty boolean olmalıdır."
            )

        if not isinstance(
            sort_keys,
            bool,
        ):
            raise ContractValidationError(
                "sort_keys boolean olmalıdır."
            )

        _validate_json_value(
            value
        )

        try:
            if pretty:
                rendered = json.dumps(
                    value,
                    ensure_ascii=False,
                    allow_nan=False,
                    sort_keys=(
                        sort_keys
                    ),
                    indent=2,
                )

            else:
                rendered = json.dumps(
                    value,
                    ensure_ascii=False,
                    allow_nan=False,
                    sort_keys=(
                        sort_keys
                    ),
                    separators=(
                        ",",
                        ":",
                    ),
                )

        except (
            TypeError,
            ValueError,
        ) as exc:
            raise JsonProcessingError(
                "JSON serialization "
                "başarısız "
                f"| error={_safe_error_message(exc)}"
            ) from exc

        self._check_text_size(
            rendered
        )

        return rendered

    def serialize_bytes(
        self,
        value: Any,
        *,
        pretty: bool = False,
        sort_keys: bool = True,
    ) -> bytes:
        """
        Deterministic JSON bytes üretir.
        """

        rendered = self.serialize(
            value,
            pretty=pretty,
            sort_keys=sort_keys,
        )

        try:
            raw = rendered.encode(
                self.config.encoding
            )

        except UnicodeError as exc:
            raise JsonProcessingError(
                "JSON encode başarısız "
                f"| encoding={self.config.encoding!r} "
                f"| error={_safe_error_message(exc)}"
            ) from exc

        self._check_byte_size(
            raw
        )

        return raw

    # =========================================================================
    # INTERNAL PARSING
    # =========================================================================
    def _parse_path(
        self,
        path: Path,
    ) -> Any:
        resolved = path.expanduser()

        if not resolved.exists():
            raise JsonProcessingError(
                "JSON dosyası bulunamadı "
                f"| path={resolved}"
            )

        if not resolved.is_file():
            raise JsonProcessingError(
                "JSON source normal dosya "
                "olmalıdır "
                f"| path={resolved}"
            )

        try:
            file_size = (
                resolved.stat().st_size
            )

        except OSError as exc:
            raise JsonProcessingError(
                "JSON dosya metadata'sı "
                "okunamadı "
                f"| path={resolved} "
                f"| error={_safe_error_message(exc)}"
            ) from exc

        self._check_size_value(
            file_size
        )

        try:
            raw = resolved.read_bytes()

        except OSError as exc:
            raise JsonProcessingError(
                "JSON dosyası okunamadı "
                f"| path={resolved} "
                f"| error={_safe_error_message(exc)}"
            ) from exc

        self._check_byte_size(
            raw
        )

        return self._parse_bytes(
            raw
        )

    def _parse_bytes(
        self,
        raw: bytes,
    ) -> Any:
        try:
            text = raw.decode(
                self.config.encoding
            )

        except UnicodeError as exc:
            raise JsonProcessingError(
                "JSON byte payload decode "
                "edilemedi "
                f"| encoding={self.config.encoding!r} "
                f"| error={_safe_error_message(exc)}"
            ) from exc

        return self._loads(
            text
        )

    def _loads(
        self,
        text: str,
    ) -> Any:
        if not text.strip():
            raise JsonProcessingError(
                "JSON payload boş olamaz."
            )

        kwargs: dict[
            str,
            Any,
        ] = {
            "parse_constant": (
                _reject_constant
            ),
        }

        if (
            self.config.reject_duplicate_keys
        ):
            kwargs[
                "object_pairs_hook"
            ] = (
                _object_pairs_no_duplicates
            )

        try:
            return json.loads(
                text,
                **kwargs,
            )

        except JsonProcessingError:
            raise

        except json.JSONDecodeError as exc:
            raise JsonProcessingError(
                "Geçersiz JSON "
                f"| line={exc.lineno} "
                f"| column={exc.colno} "
                f"| position={exc.pos} "
                f"| error={exc.msg}"
            ) from exc

        except (
            TypeError,
            ValueError,
        ) as exc:
            raise JsonProcessingError(
                "JSON decode başarısız "
                f"| error={_safe_error_message(exc)}"
            ) from exc

    # =========================================================================
    # SIZE POLICY
    # =========================================================================
    def _check_text_size(
        self,
        text: str,
    ) -> None:
        try:
            size = len(
                text.encode(
                    self.config.encoding
                )
            )

        except UnicodeError as exc:
            raise JsonProcessingError(
                "JSON text byte boyutu "
                "hesaplanamadı "
                f"| encoding={self.config.encoding!r}"
            ) from exc

        self._check_size_value(
            size
        )

    def _check_byte_size(
        self,
        raw: bytes,
    ) -> None:
        self._check_size_value(
            len(raw)
        )

    def _check_size_value(
        self,
        size: int,
    ) -> None:
        max_bytes = (
            self.config.max_bytes
        )

        if max_bytes is None:
            return

        if size > max_bytes:
            raise JsonProcessingError(
                "JSON payload izin verilen "
                "boyutu aşıyor "
                f"| size={size} "
                f"| max_bytes={max_bytes}"
            )

    # =========================================================================
    # SNAPSHOT
    # =========================================================================
    def snapshot(
        self,
    ) -> dict[str, Any]:
        return {
            "encoding": (
                self.config.encoding
            ),
            "max_bytes": (
                self.config.max_bytes
            ),
            "reject_duplicate_keys": (
                self.config.reject_duplicate_keys
            ),
            "allow_scalar_root": (
                self.config.allow_scalar_root
            ),
        }

    # =========================================================================
    # REPRESENTATION
    # =========================================================================
    def __repr__(
        self,
    ) -> str:
        return (
            "JsonProcessor("
            f"encoding={self.config.encoding!r}, "
            f"max_bytes={self.config.max_bytes!r}, "
            "reject_duplicate_keys="
            f"{self.config.reject_duplicate_keys!r}, "
            "allow_scalar_root="
            f"{self.config.allow_scalar_root!r}"
            ")"
        )


# =============================================================================
# CONVENIENCE API
# =============================================================================
def parse_json(
    source: (
        str
        | bytes
        | bytearray
        | memoryview
        | Path
    ),
) -> Any:
    """
    Default JsonProcessor ile parse.
    """

    return JsonProcessor().parse(
        source
    )


def parse_json_object(
    source: (
        str
        | bytes
        | bytearray
        | memoryview
        | Path
    ),
) -> dict[str, Any]:
    """
    Default JsonProcessor ile object parse.
    """

    return (
        JsonProcessor().parse_object(
            source
        )
    )


def parse_json_array(
    source: (
        str
        | bytes
        | bytearray
        | memoryview
        | Path
    ),
) -> list[Any]:
    """
    Default JsonProcessor ile array parse.
    """

    return (
        JsonProcessor().parse_array(
            source
        )
    )


def serialize_json(
    value: Any,
    *,
    pretty: bool = False,
    sort_keys: bool = True,
) -> str:
    """
    Default JsonProcessor ile serialize.
    """

    return JsonProcessor().serialize(
        value,
        pretty=pretty,
        sort_keys=sort_keys,
    )


__all__ = [
    "DEFAULT_JSON_ENCODING",
    "DEFAULT_MAX_JSON_BYTES",
    "JsonProcessingError",
    "JsonProcessorConfig",
    "JsonProcessor",
    "parse_json",
    "parse_json_object",
    "parse_json_array",
    "serialize_json",
]