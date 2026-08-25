from __future__ import annotations

"""
Enterprise Crawler Framework - XML Processing

Strict ve güvenli XML parsing yardımcıları.

Amaçlar
-------
- str / bytes / bytearray / memoryview / Path desteği
- payload boyut sınırı
- malformed XML rejection
- DOCTYPE rejection
- ENTITY declaration rejection
- namespace yardımcıları
- root validation
- element lookup
- güvenli serialization

Security
--------
Bu modül, crawler tarafından dış kaynaklardan alınan XML payload'larını
işlediği için XML entity özelliklerini fail-closed biçimde reddeder.

DOCTYPE ve ENTITY declaration'ları parse işleminden önce engellenir.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional
import re
import xml.etree.ElementTree as ET

from enterprise_crawler.exceptions import ProcessingError


# =============================================================================
# CONSTANTS
# =============================================================================
DEFAULT_MAX_XML_BYTES = 16 * 1024 * 1024

_DOCTYPE_PATTERN = re.compile(
    rb"<!\s*DOCTYPE\b",
    re.IGNORECASE,
)

_ENTITY_PATTERN = re.compile(
    rb"<!\s*ENTITY\b",
    re.IGNORECASE,
)


# =============================================================================
# EXCEPTIONS
# =============================================================================
class XmlProcessingError(ProcessingError):
    """XML processing sırasında oluşan hata."""

    default_message = "XML processing failed."


# =============================================================================
# CONFIG
# =============================================================================
@dataclass(
    frozen=True,
    slots=True,
)
class XmlProcessorConfig:
    """
    XmlProcessor yapılandırması.

    Parameters
    ----------
    max_bytes:
        Kabul edilecek maksimum XML payload boyutu.

        ``None`` verilirse boyut sınırı kaldırılır.

    encoding:
        ``str`` payload'ların byte-size kontrolünde kullanılacak encoding.

        XML byte payload'larının gerçek decoding işlemi ElementTree tarafından
        XML declaration'a göre yapılır.

    reject_doctype:
        DOCTYPE declaration'larını reddeder.

    reject_entities:
        ENTITY declaration'larını reddeder.
    """

    max_bytes: Optional[int] = (
        DEFAULT_MAX_XML_BYTES
    )

    encoding: str = "utf-8"

    reject_doctype: bool = True
    reject_entities: bool = True

    def __post_init__(
        self,
    ) -> None:
        max_bytes = self.max_bytes

        if max_bytes is not None:
            if (
                isinstance(
                    max_bytes,
                    bool,
                )
                or not isinstance(
                    max_bytes,
                    int,
                )
            ):
                raise XmlProcessingError(
                    "max_bytes pozitif tam sayı "
                    "veya None olmalıdır."
                )

            if max_bytes <= 0:
                raise XmlProcessingError(
                    "max_bytes sıfırdan büyük olmalıdır."
                )

        encoding = str(
            self.encoding or ""
        ).strip()

        if not encoding:
            raise XmlProcessingError(
                "encoding boş olamaz."
            )

        if not isinstance(
            self.reject_doctype,
            bool,
        ):
            raise XmlProcessingError(
                "reject_doctype boolean olmalıdır."
            )

        if not isinstance(
            self.reject_entities,
            bool,
        ):
            raise XmlProcessingError(
                "reject_entities boolean olmalıdır."
            )

        object.__setattr__(
            self,
            "encoding",
            encoding,
        )

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "max_bytes": self.max_bytes,
            "encoding": self.encoding,
            "reject_doctype": (
                self.reject_doctype
            ),
            "reject_entities": (
                self.reject_entities
            ),
        }


# =============================================================================
# XML PROCESSOR
# =============================================================================
class XmlProcessor:
    """
    Strict XML parser.

    Example
    -------
    ::

        processor = XmlProcessor()

        root = processor.parse(
            "<root><item>hello</item></root>"
        )

        item = processor.find(
            root,
            "item",
        )
    """

    def __init__(
        self,
        config: Optional[
            XmlProcessorConfig
        ] = None,
    ) -> None:
        if (
            config is not None
            and not isinstance(
                config,
                XmlProcessorConfig,
            )
        ):
            raise XmlProcessingError(
                "config XmlProcessorConfig "
                "olmalıdır."
            )

        self.config = (
            config
            or XmlProcessorConfig()
        )

    # =========================================================================
    # PARSING
    # =========================================================================
    def parse(
        self,
        source: Any,
    ) -> ET.Element:
        """
        XML payload'ını parse eder.

        Desteklenen tipler:
        - str
        - bytes
        - bytearray
        - memoryview
        - pathlib.Path

        ``str`` otomatik olarak dosya yolu sayılmaz.
        Dosya okumak için açıkça ``Path`` kullanılmalıdır.
        """

        if isinstance(
            source,
            Path,
        ):
            return self.parse_file(
                source
            )

        if isinstance(
            source,
            str,
        ):
            return self._parse_text(
                source
            )

        if isinstance(
            source,
            bytes,
        ):
            return self._parse_bytes(
                source
            )

        if isinstance(
            source,
            bytearray,
        ):
            return self._parse_bytes(
                bytes(source)
            )

        if isinstance(
            source,
            memoryview,
        ):
            return self._parse_bytes(
                source.tobytes()
            )

        raise XmlProcessingError(
            "Desteklenmeyen XML source tipi "
            f"| actual={type(source).__name__}"
        )

    def parse_file(
        self,
        path: Path,
    ) -> ET.Element:
        if not isinstance(
            path,
            Path,
        ):
            raise XmlProcessingError(
                "path pathlib.Path olmalıdır."
            )

        if not path.exists():
            raise XmlProcessingError(
                "XML dosyası bulunamadı "
                f"| path={path}"
            )

        if not path.is_file():
            raise XmlProcessingError(
                "XML kaynağı dosya olmalıdır "
                f"| path={path}"
            )

        try:
            file_size = (
                path.stat().st_size
            )

        except OSError as exc:
            raise XmlProcessingError(
                "XML dosya boyutu okunamadı "
                f"| path={path}"
            ) from exc

        self._validate_size(
            file_size
        )

        try:
            payload = path.read_bytes()

        except OSError as exc:
            raise XmlProcessingError(
                "XML dosyası okunamadı "
                f"| path={path}"
            ) from exc

        return self._parse_bytes(
            payload
        )

    def _parse_text(
        self,
        payload: str,
    ) -> ET.Element:
        if not payload.strip():
            raise XmlProcessingError(
                "XML payload boş olamaz."
            )

        try:
            encoded = payload.encode(
                self.config.encoding
            )

        except (
            UnicodeError,
            LookupError,
        ) as exc:
            raise XmlProcessingError(
                "XML payload encoding başarısız "
                f"| encoding={self.config.encoding}"
            ) from exc

        self._validate_size(
            len(encoded)
        )

        self._validate_security(
            encoded
        )

        try:
            return ET.fromstring(
                payload
            )

        except ET.ParseError as exc:
            raise XmlProcessingError(
                "Malformed XML payload."
            ) from exc

    def _parse_bytes(
        self,
        payload: bytes,
    ) -> ET.Element:
        if not payload:
            raise XmlProcessingError(
                "XML payload boş olamaz."
            )

        self._validate_size(
            len(payload)
        )

        self._validate_security(
            payload
        )

        try:
            return ET.fromstring(
                payload
            )

        except ET.ParseError as exc:
            raise XmlProcessingError(
                "Malformed XML payload."
            ) from exc

        except UnicodeError as exc:
            raise XmlProcessingError(
                "XML payload encoding geçersiz."
            ) from exc

    # =========================================================================
    # SECURITY
    # =========================================================================
    def _validate_security(
        self,
        payload: bytes,
    ) -> None:
        if (
            self.config.reject_doctype
            and _DOCTYPE_PATTERN.search(
                payload
            )
        ):
            raise XmlProcessingError(
                "XML DOCTYPE declaration "
                "güvenlik nedeniyle reddedildi."
            )

        if (
            self.config.reject_entities
            and _ENTITY_PATTERN.search(
                payload
            )
        ):
            raise XmlProcessingError(
                "XML ENTITY declaration "
                "güvenlik nedeniyle reddedildi."
            )

    # =========================================================================
    # SIZE
    # =========================================================================
    def _validate_size(
        self,
        size: int,
    ) -> None:
        max_bytes = (
            self.config.max_bytes
        )

        if max_bytes is None:
            return

        if size > max_bytes:
            raise XmlProcessingError(
                "XML payload maksimum boyutu aşıyor "
                f"| size={size} "
                f"| max_bytes={max_bytes}"
            )

    # =========================================================================
    # ROOT CONTRACT
    # =========================================================================
    @staticmethod
    def require_root(
        root: ET.Element,
        expected_tag: str,
    ) -> ET.Element:
        if not isinstance(
            root,
            ET.Element,
        ):
            raise XmlProcessingError(
                "root Element olmalıdır."
            )

        normalized = str(
            expected_tag or ""
        ).strip()

        if not normalized:
            raise XmlProcessingError(
                "expected_tag boş olamaz."
            )

        if root.tag != normalized:
            raise XmlProcessingError(
                "Beklenmeyen XML root "
                f"| expected={normalized!r} "
                f"| actual={root.tag!r}"
            )

        return root

    @staticmethod
    def local_name(
        tag: str,
    ) -> str:
        """
        Namespace-qualified XML tag'ından local name döndürür.

        Örnek::

            {http://example.com/ns}item

        ->

            item
        """

        normalized = str(
            tag or ""
        )

        if normalized.startswith(
            "{"
        ):
            closing = normalized.find(
                "}"
            )

            if closing >= 0:
                return normalized[
                    closing + 1:
                ]

        return normalized

    @staticmethod
    def namespace_uri(
        tag: str,
    ) -> Optional[str]:
        normalized = str(
            tag or ""
        )

        if not normalized.startswith(
            "{"
        ):
            return None

        closing = normalized.find(
            "}"
        )

        if closing <= 1:
            return None

        return normalized[
            1:closing
        ]

    @staticmethod
    def qualified_name(
        namespace: str,
        local_name: str,
    ) -> str:
        normalized_namespace = str(
            namespace or ""
        ).strip()

        normalized_local = str(
            local_name or ""
        ).strip()

        if not normalized_namespace:
            raise XmlProcessingError(
                "namespace boş olamaz."
            )

        if not normalized_local:
            raise XmlProcessingError(
                "local_name boş olamaz."
            )

        return (
            "{"
            + normalized_namespace
            + "}"
            + normalized_local
        )

    # =========================================================================
    # ELEMENT LOOKUP
    # =========================================================================
    @staticmethod
    def find(
        root: ET.Element,
        path: str,
        namespaces: Optional[
            Mapping[str, str]
        ] = None,
    ) -> Optional[ET.Element]:
        XmlProcessor._validate_element(
            root
        )

        normalized_path = str(
            path or ""
        ).strip()

        if not normalized_path:
            raise XmlProcessingError(
                "XML find path boş olamaz."
            )

        try:
            return root.find(
                normalized_path,
                namespaces=(
                    dict(namespaces)
                    if namespaces
                    else None
                ),
            )

        except (
            SyntaxError,
            TypeError,
        ) as exc:
            raise XmlProcessingError(
                "Geçersiz XML find expression "
                f"| path={normalized_path!r}"
            ) from exc

    @staticmethod
    def findall(
        root: ET.Element,
        path: str,
        namespaces: Optional[
            Mapping[str, str]
        ] = None,
    ) -> list[ET.Element]:
        XmlProcessor._validate_element(
            root
        )

        normalized_path = str(
            path or ""
        ).strip()

        if not normalized_path:
            raise XmlProcessingError(
                "XML findall path boş olamaz."
            )

        try:
            return list(
                root.findall(
                    normalized_path,
                    namespaces=(
                        dict(namespaces)
                        if namespaces
                        else None
                    ),
                )
            )

        except (
            SyntaxError,
            TypeError,
        ) as exc:
            raise XmlProcessingError(
                "Geçersiz XML findall expression "
                f"| path={normalized_path!r}"
            ) from exc

    @staticmethod
    def find_text(
        root: ET.Element,
        path: str,
        *,
        default: Optional[str] = None,
        namespaces: Optional[
            Mapping[str, str]
        ] = None,
        strip: bool = True,
    ) -> Optional[str]:
        if not isinstance(
            strip,
            bool,
        ):
            raise XmlProcessingError(
                "strip boolean olmalıdır."
            )

        element = XmlProcessor.find(
            root,
            path,
            namespaces,
        )

        if element is None:
            return default

        text = element.text

        if text is None:
            return default

        if strip:
            text = text.strip()

        return text

    # =========================================================================
    # SERIALIZATION
    # =========================================================================
    def serialize(
        self,
        element: ET.Element,
        *,
        encoding: str = "unicode",
        xml_declaration: Optional[
            bool
        ] = None,
        short_empty_elements: bool = True,
    ) -> str | bytes:
        self._validate_element(
            element
        )

        normalized_encoding = str(
            encoding or ""
        ).strip()

        if not normalized_encoding:
            raise XmlProcessingError(
                "encoding boş olamaz."
            )

        if (
            xml_declaration is not None
            and not isinstance(
                xml_declaration,
                bool,
            )
        ):
            raise XmlProcessingError(
                "xml_declaration boolean "
                "veya None olmalıdır."
            )

        if not isinstance(
            short_empty_elements,
            bool,
        ):
            raise XmlProcessingError(
                "short_empty_elements "
                "boolean olmalıdır."
            )

        try:
            serialized = ET.tostring(
                element,
                encoding=(
                    normalized_encoding
                ),
                xml_declaration=(
                    xml_declaration
                ),
                short_empty_elements=(
                    short_empty_elements
                ),
            )

        except (
            TypeError,
            LookupError,
            ValueError,
        ) as exc:
            raise XmlProcessingError(
                "XML serialization başarısız."
            ) from exc

        if isinstance(
            serialized,
            str,
        ):
            size = len(
                serialized.encode(
                    self.config.encoding
                )
            )
        else:
            size = len(
                serialized
            )

        self._validate_size(
            size
        )

        return serialized

    def serialize_bytes(
        self,
        element: ET.Element,
        *,
        encoding: str = "utf-8",
        xml_declaration: bool = True,
        short_empty_elements: bool = True,
    ) -> bytes:
        result = self.serialize(
            element,
            encoding=encoding,
            xml_declaration=(
                xml_declaration
            ),
            short_empty_elements=(
                short_empty_elements
            ),
        )

        if isinstance(
            result,
            bytes,
        ):
            return result

        try:
            return result.encode(
                encoding
            )

        except (
            UnicodeError,
            LookupError,
        ) as exc:
            raise XmlProcessingError(
                "XML bytes serialization başarısız "
                f"| encoding={encoding}"
            ) from exc

    # =========================================================================
    # INTERNAL VALIDATION
    # =========================================================================
    @staticmethod
    def _validate_element(
        element: Any,
    ) -> None:
        if not isinstance(
            element,
            ET.Element,
        ):
            raise XmlProcessingError(
                "XML element Element olmalıdır."
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

    # =========================================================================
    # REPRESENTATION
    # =========================================================================
    def __repr__(
        self,
    ) -> str:
        return (
            "XmlProcessor("
            f"max_bytes={self.config.max_bytes!r}, "
            f"encoding={self.config.encoding!r}, "
            f"reject_doctype="
            f"{self.config.reject_doctype!r}, "
            f"reject_entities="
            f"{self.config.reject_entities!r}"
            ")"
        )


# =============================================================================
# CONVENIENCE HELPERS
# =============================================================================
def parse_xml(
    source: Any,
    *,
    config: Optional[
        XmlProcessorConfig
    ] = None,
) -> ET.Element:
    return XmlProcessor(
        config
    ).parse(
        source
    )


def parse_xml_file(
    path: Path,
    *,
    config: Optional[
        XmlProcessorConfig
    ] = None,
) -> ET.Element:
    return XmlProcessor(
        config
    ).parse_file(
        path
    )


def serialize_xml(
    element: ET.Element,
    *,
    config: Optional[
        XmlProcessorConfig
    ] = None,
    encoding: str = "unicode",
    xml_declaration: Optional[
        bool
    ] = None,
) -> str | bytes:
    return XmlProcessor(
        config
    ).serialize(
        element,
        encoding=encoding,
        xml_declaration=(
            xml_declaration
        ),
    )


__all__ = [
    "DEFAULT_MAX_XML_BYTES",
    "XmlProcessingError",
    "XmlProcessorConfig",
    "XmlProcessor",
    "parse_xml",
    "parse_xml_file",
    "serialize_xml",
]