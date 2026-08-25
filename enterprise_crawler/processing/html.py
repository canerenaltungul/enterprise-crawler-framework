from __future__ import annotations

"""
Enterprise Crawler Framework - HTML Processing

Dependency-free HTML parsing and lightweight extraction helpers.

Bu modül bilinçli olarak BeautifulSoup/lxml gibi harici dependency kullanmaz.
İlk framework sürümünde amaç:

- kontrollü HTML parsing,
- payload size sınırı,
- encoding doğrulaması,
- lightweight DOM-benzeri ağaç,
- tag/attribute/class araması,
- metin, title, link ve meta extraction,
- deterministic ve test edilebilir davranış.

Bu bir browser engine veya tam CSS selector engine değildir.
JavaScript çalıştırmaz.
Harici resource yüklemez.
"""

from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

from enterprise_crawler.exceptions import ProcessingError


# =============================================================================
# CONSTANTS
# =============================================================================

DEFAULT_MAX_HTML_BYTES = 8 * 1024 * 1024
DEFAULT_HTML_ENCODING = "utf-8"

_VOID_TAGS = frozenset(
    {
        "area",
        "base",
        "br",
        "col",
        "embed",
        "hr",
        "img",
        "input",
        "link",
        "meta",
        "param",
        "source",
        "track",
        "wbr",
    }
)

_HIDDEN_TEXT_TAGS = frozenset(
    {
        "script",
        "style",
        "template",
        "noscript",
    }
)


# =============================================================================
# EXCEPTIONS
# =============================================================================

class HtmlProcessingError(ProcessingError):
    """
    HTML parsing/extraction hatası.
    """

    default_message = "HTML processing failed."


# =============================================================================
# HELPERS
# =============================================================================

def _normalize_positive_optional_int(
    value: Any,
    *,
    field_name: str,
) -> Optional[int]:
    if value is None:
        return None

    if isinstance(value, bool):
        raise HtmlProcessingError(
            f"{field_name} boolean olamaz."
        )

    if not isinstance(value, int):
        raise HtmlProcessingError(
            f"{field_name} tam sayı olmalıdır."
        )

    if value <= 0:
        raise HtmlProcessingError(
            f"{field_name} sıfırdan büyük olmalıdır."
        )

    return value


def _normalize_non_empty_string(
    value: Any,
    *,
    field_name: str,
) -> str:
    if not isinstance(value, str):
        raise HtmlProcessingError(
            f"{field_name} string olmalıdır."
        )

    normalized = value.strip()

    if not normalized:
        raise HtmlProcessingError(
            f"{field_name} boş olamaz."
        )

    return normalized


def _normalize_bool(
    value: Any,
    *,
    field_name: str,
) -> bool:
    if not isinstance(value, bool):
        raise HtmlProcessingError(
            f"{field_name} boolean olmalıdır."
        )

    return value


def _normalize_tag_name(
    value: Any,
    *,
    field_name: str = "tag",
) -> str:
    return _normalize_non_empty_string(
        value,
        field_name=field_name,
    ).lower()


def _normalize_attribute_name(
    value: Any,
) -> str:
    return _normalize_non_empty_string(
        value,
        field_name="attribute name",
    ).lower()


def _normalized_class_tokens(
    value: Optional[str],
) -> set[str]:
    if not value:
        return set()

    return {
        token
        for token in value.split()
        if token
    }


def _collapse_whitespace(
    value: str,
) -> str:
    return " ".join(
        value.split()
    )


# =============================================================================
# CONFIG
# =============================================================================

@dataclass(
    frozen=True,
    slots=True,
)
class HtmlProcessorConfig:
    """
    HTML processor runtime configuration.
    """

    max_bytes: Optional[int] = (
        DEFAULT_MAX_HTML_BYTES
    )

    encoding: str = (
        DEFAULT_HTML_ENCODING
    )

    include_comments: bool = False

    include_hidden_text: bool = False

    def __post_init__(
        self,
    ) -> None:
        normalized_max_bytes = (
            _normalize_positive_optional_int(
                self.max_bytes,
                field_name="max_bytes",
            )
        )

        normalized_encoding = (
            _normalize_non_empty_string(
                self.encoding,
                field_name="encoding",
            )
        )

        include_comments = _normalize_bool(
            self.include_comments,
            field_name="include_comments",
        )

        include_hidden_text = _normalize_bool(
            self.include_hidden_text,
            field_name="include_hidden_text",
        )

        try:
            "".encode(
                normalized_encoding
            )

        except LookupError as exc:
            raise HtmlProcessingError(
                "Geçersiz HTML encoding "
                f"| encoding={normalized_encoding!r}"
            ) from exc

        object.__setattr__(
            self,
            "max_bytes",
            normalized_max_bytes,
        )

        object.__setattr__(
            self,
            "encoding",
            normalized_encoding,
        )

        object.__setattr__(
            self,
            "include_comments",
            include_comments,
        )

        object.__setattr__(
            self,
            "include_hidden_text",
            include_hidden_text,
        )

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "max_bytes": self.max_bytes,
            "encoding": self.encoding,
            "include_comments": (
                self.include_comments
            ),
            "include_hidden_text": (
                self.include_hidden_text
            ),
        }


# =============================================================================
# HTML NODE
# =============================================================================

@dataclass(
    slots=True,
)
class HtmlNode:
    """
    Lightweight HTML element node.

    ``text_parts`` yalnız bu elementin doğrudan data parçalarını tutar.
    Descendant text ``text()`` ile recursive çıkarılır.
    """

    tag: str

    attributes: dict[
        str,
        Optional[str],
    ] = field(
        default_factory=dict
    )

    children: list[
        "HtmlNode"
    ] = field(
        default_factory=list
    )

    text_parts: list[
        str
    ] = field(
        default_factory=list
    )

    parent: Optional[
        "HtmlNode"
    ] = field(
        default=None,
        repr=False,
    )

    def __post_init__(
        self,
    ) -> None:
        self.tag = _normalize_tag_name(
            self.tag
        )

        normalized_attributes: dict[
            str,
            Optional[str],
        ] = {}

        for key, value in (
            self.attributes.items()
        ):
            normalized_key = (
                _normalize_attribute_name(
                    key
                )
            )

            if (
                value is not None
                and not isinstance(
                    value,
                    str,
                )
            ):
                raise HtmlProcessingError(
                    "HTML attribute value "
                    "string veya None olmalıdır "
                    f"| attribute={normalized_key!r}"
                )

            normalized_attributes[
                normalized_key
            ] = value

        self.attributes = (
            normalized_attributes
        )

    @property
    def attrs(
        self,
    ) -> dict[str, Optional[str]]:
        return dict(
            self.attributes
        )

    def get(
        self,
        name: str,
        default: Optional[str] = None,
    ) -> Optional[str]:
        normalized_name = (
            _normalize_attribute_name(
                name
            )
        )

        return self.attributes.get(
            normalized_name,
            default,
        )

    @property
    def id(
        self,
    ) -> Optional[str]:
        return self.attributes.get(
            "id"
        )

    @property
    def classes(
        self,
    ) -> set[str]:
        return _normalized_class_tokens(
            self.attributes.get(
                "class"
            )
        )

    def has_class(
        self,
        class_name: str,
    ) -> bool:
        normalized = (
            _normalize_non_empty_string(
                class_name,
                field_name="class_name",
            )
        )

        return (
            normalized
            in self.classes
        )

    def iter_descendants(
        self,
        *,
        include_self: bool = False,
    ) -> Iterable["HtmlNode"]:
        if include_self:
            yield self

        for child in self.children:
            yield child

            yield from (
                child.iter_descendants()
            )

    def matches(
        self,
        *,
        tag: Optional[str] = None,
        attrs: Optional[
            Mapping[
                str,
                Optional[str],
            ]
        ] = None,
        class_name: Optional[str] = None,
    ) -> bool:
        if tag is not None:
            normalized_tag = (
                _normalize_tag_name(
                    tag
                )
            )

            if self.tag != normalized_tag:
                return False

        if attrs is not None:
            if not isinstance(
                attrs,
                Mapping,
            ):
                raise HtmlProcessingError(
                    "attrs mapping olmalıdır."
                )

            for key, expected in (
                attrs.items()
            ):
                normalized_key = (
                    _normalize_attribute_name(
                        key
                    )
                )

                if (
                    normalized_key
                    not in self.attributes
                ):
                    return False

                if (
                    expected is not None
                    and self.attributes[
                        normalized_key
                    ]
                    != str(expected)
                ):
                    return False

        if class_name is not None:
            if not self.has_class(
                class_name
            ):
                return False

        return True

    def find(
        self,
        tag: Optional[str] = None,
        *,
        attrs: Optional[
            Mapping[
                str,
                Optional[str],
            ]
        ] = None,
        class_name: Optional[str] = None,
        include_self: bool = False,
    ) -> Optional["HtmlNode"]:
        for node in self.iter_descendants(
            include_self=include_self
        ):
            if node.matches(
                tag=tag,
                attrs=attrs,
                class_name=class_name,
            ):
                return node

        return None

    def find_all(
        self,
        tag: Optional[str] = None,
        *,
        attrs: Optional[
            Mapping[
                str,
                Optional[str],
            ]
        ] = None,
        class_name: Optional[str] = None,
        include_self: bool = False,
    ) -> list["HtmlNode"]:
        matches: list[
            HtmlNode
        ] = []

        for node in self.iter_descendants(
            include_self=include_self
        ):
            if node.matches(
                tag=tag,
                attrs=attrs,
                class_name=class_name,
            ):
                matches.append(
                    node
                )

        return matches

    def text(
        self,
        *,
        separator: str = " ",
        strip: bool = True,
        include_hidden: bool = False,
    ) -> str:
        if not isinstance(
            separator,
            str,
        ):
            raise HtmlProcessingError(
                "separator string olmalıdır."
            )

        strip = _normalize_bool(
            strip,
            field_name="strip",
        )

        include_hidden = (
            _normalize_bool(
                include_hidden,
                field_name="include_hidden",
            )
        )

        parts: list[str] = []

        def collect(
            node: HtmlNode,
        ) -> None:
            if (
                not include_hidden
                and node.tag
                in _HIDDEN_TEXT_TAGS
            ):
                return

            parts.extend(
                node.text_parts
            )

            for child in (
                node.children
            ):
                collect(
                    child
                )

        collect(
            self
        )

        if strip:
            normalized_parts = [
                _collapse_whitespace(
                    part
                )
                for part in parts
                if part.strip()
            ]

            return separator.join(
                part
                for part in normalized_parts
                if part
            ).strip()

        return separator.join(
            parts
        )

    def to_dict(
        self,
        *,
        recursive: bool = True,
    ) -> dict[str, Any]:
        recursive = _normalize_bool(
            recursive,
            field_name="recursive",
        )

        payload: dict[str, Any] = {
            "tag": self.tag,
            "attributes": dict(
                self.attributes
            ),
            "text_parts": list(
                self.text_parts
            ),
        }

        if recursive:
            payload["children"] = [
                child.to_dict(
                    recursive=True
                )
                for child
                in self.children
            ]

        return payload


# =============================================================================
# DOCUMENT TYPES
# =============================================================================

@dataclass(
    frozen=True,
    slots=True,
)
class HtmlLink:
    href: str

    text: str = ""

    title: Optional[str] = None

    rel: tuple[str, ...] = ()

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "href": self.href,
            "text": self.text,
            "title": self.title,
            "rel": list(
                self.rel
            ),
        }


@dataclass(
    frozen=True,
    slots=True,
)
class HtmlDocument:
    """
    Parsed HTML document.
    """

    roots: tuple[
        HtmlNode,
        ...,
    ]

    comments: tuple[
        str,
        ...,
    ] = ()

    declarations: tuple[
        str,
        ...,
    ] = ()

    source_bytes: int = 0

    encoding: str = (
        DEFAULT_HTML_ENCODING
    )

    include_hidden_text: bool = False

    def iter_nodes(
        self,
    ) -> Iterable[HtmlNode]:
        for root in self.roots:
            yield root

            yield from (
                root.iter_descendants()
            )

    def find(
        self,
        tag: Optional[str] = None,
        *,
        attrs: Optional[
            Mapping[
                str,
                Optional[str],
            ]
        ] = None,
        class_name: Optional[str] = None,
    ) -> Optional[HtmlNode]:
        for node in self.iter_nodes():
            if node.matches(
                tag=tag,
                attrs=attrs,
                class_name=class_name,
            ):
                return node

        return None

    def find_all(
        self,
        tag: Optional[str] = None,
        *,
        attrs: Optional[
            Mapping[
                str,
                Optional[str],
            ]
        ] = None,
        class_name: Optional[str] = None,
    ) -> list[HtmlNode]:
        return [
            node
            for node
            in self.iter_nodes()
            if node.matches(
                tag=tag,
                attrs=attrs,
                class_name=class_name,
            )
        ]

    @property
    def title(
        self,
    ) -> Optional[str]:
        title_node = self.find(
            "title"
        )

        if title_node is None:
            return None

        value = title_node.text(
            include_hidden=True
        ).strip()

        return (
            value
            if value
            else None
        )

    def text(
        self,
        *,
        separator: str = " ",
        strip: bool = True,
        include_hidden: Optional[
            bool
        ] = None,
    ) -> str:
        if not isinstance(
            separator,
            str,
        ):
            raise HtmlProcessingError(
                "separator string olmalıdır."
            )

        strip = _normalize_bool(
            strip,
            field_name="strip",
        )

        if include_hidden is None:
            resolved_hidden = (
                self.include_hidden_text
            )

        else:
            resolved_hidden = (
                _normalize_bool(
                    include_hidden,
                    field_name=(
                        "include_hidden"
                    ),
                )
            )

        parts = [
            root.text(
                separator=separator,
                strip=strip,
                include_hidden=(
                    resolved_hidden
                ),
            )
            for root
            in self.roots
        ]

        if strip:
            return separator.join(
                part
                for part in parts
                if part
            ).strip()

        return separator.join(
            parts
        )

    def links(
        self,
        *,
        include_empty_href: bool = False,
    ) -> list[HtmlLink]:
        include_empty_href = (
            _normalize_bool(
                include_empty_href,
                field_name=(
                    "include_empty_href"
                ),
            )
        )

        links: list[
            HtmlLink
        ] = []

        for node in self.find_all(
            "a"
        ):
            href = (
                node.get(
                    "href"
                )
                or ""
            ).strip()

            if (
                not href
                and not include_empty_href
            ):
                continue

            rel_raw = (
                node.get(
                    "rel"
                )
                or ""
            )

            rel = tuple(
                token
                for token
                in rel_raw.split()
                if token
            )

            links.append(
                HtmlLink(
                    href=href,
                    text=node.text(),
                    title=node.get(
                        "title"
                    ),
                    rel=rel,
                )
            )

        return links

    def meta(
        self,
        name: str,
    ) -> Optional[str]:
        normalized_name = (
            _normalize_non_empty_string(
                name,
                field_name="name",
            ).lower()
        )

        for node in self.find_all(
            "meta"
        ):
            candidate_name = (
                node.get(
                    "name"
                )
                or node.get(
                    "property"
                )
                or ""
            ).strip().lower()

            if (
                candidate_name
                == normalized_name
            ):
                content = node.get(
                    "content"
                )

                return (
                    content
                    if content is not None
                    else ""
                )

        return None

    @property
    def node_count(
        self,
    ) -> int:
        return sum(
            1
            for _
            in self.iter_nodes()
        )

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "roots": [
                root.to_dict()
                for root
                in self.roots
            ],
            "comments": list(
                self.comments
            ),
            "declarations": list(
                self.declarations
            ),
            "source_bytes": (
                self.source_bytes
            ),
            "encoding": (
                self.encoding
            ),
            "include_hidden_text": (
                self.include_hidden_text
            ),
            "node_count": (
                self.node_count
            ),
        }


# =============================================================================
# INTERNAL PARSER
# =============================================================================

class _TreeBuilder(
    HTMLParser
):
    def __init__(
        self,
        *,
        include_comments: bool,
    ) -> None:
        super().__init__(
            convert_charrefs=True
        )

        self.include_comments = (
            include_comments
        )

        self.roots: list[
            HtmlNode
        ] = []

        self.stack: list[
            HtmlNode
        ] = []

        self.comments: list[
            str
        ] = []

        self.declarations: list[
            str
        ] = []

    def _append_node(
        self,
        node: HtmlNode,
    ) -> None:
        if self.stack:
            parent = self.stack[-1]

            node.parent = parent

            parent.children.append(
                node
            )

        else:
            self.roots.append(
                node
            )

    def handle_starttag(
        self,
        tag: str,
        attrs: list[
            tuple[
                str,
                Optional[str],
            ]
        ],
    ) -> None:
        normalized_tag = (
            tag.lower()
        )

        node = HtmlNode(
            tag=normalized_tag,
            attributes={
                key.lower(): value
                for key, value
                in attrs
            },
        )

        self._append_node(
            node
        )

        if (
            normalized_tag
            not in _VOID_TAGS
        ):
            self.stack.append(
                node
            )

    def handle_startendtag(
        self,
        tag: str,
        attrs: list[
            tuple[
                str,
                Optional[str],
            ]
        ],
    ) -> None:
        node = HtmlNode(
            tag=tag.lower(),
            attributes={
                key.lower(): value
                for key, value
                in attrs
            },
        )

        self._append_node(
            node
        )

    def handle_endtag(
        self,
        tag: str,
    ) -> None:
        normalized_tag = (
            tag.lower()
        )

        if (
            normalized_tag
            in _VOID_TAGS
        ):
            return

        # HTML gerçek dünyada çoğu zaman strict XML gibi kapatılmaz.
        # Bu nedenle parser fail etmez; stack içinde uygun ancestor bulunursa
        # oraya kadar kontrollü şekilde kapanır.
        matching_index: Optional[
            int
        ] = None

        for index in range(
            len(self.stack) - 1,
            -1,
            -1,
        ):
            if (
                self.stack[index].tag
                == normalized_tag
            ):
                matching_index = index
                break

        if matching_index is None:
            return

        del self.stack[
            matching_index:
        ]

    def handle_data(
        self,
        data: str,
    ) -> None:
        if not data:
            return

        if not self.stack:
            return

        self.stack[
            -1
        ].text_parts.append(
            data
        )

    def handle_comment(
        self,
        data: str,
    ) -> None:
        if self.include_comments:
            self.comments.append(
                data
            )

    def handle_decl(
        self,
        decl: str,
    ) -> None:
        self.declarations.append(
            decl
        )


# =============================================================================
# PROCESSOR
# =============================================================================

class HtmlProcessor:
    """
    Dependency-free HTML processor.
    """

    def __init__(
        self,
        config: Optional[
            HtmlProcessorConfig
        ] = None,
    ) -> None:
        if config is None:
            config = (
                HtmlProcessorConfig()
            )

        if not isinstance(
            config,
            HtmlProcessorConfig,
        ):
            raise HtmlProcessingError(
                "config HtmlProcessorConfig "
                "olmalıdır."
            )

        self.config = config

    def _check_size(
        self,
        size: int,
        *,
        source: str,
    ) -> None:
        maximum = (
            self.config.max_bytes
        )

        if (
            maximum is not None
            and size > maximum
        ):
            raise HtmlProcessingError(
                "HTML payload size limiti aşıldı "
                f"| source={source} "
                f"| bytes={size} "
                f"| max_bytes={maximum}"
            )

    def _decode_bytes(
        self,
        payload: bytes,
    ) -> str:
        self._check_size(
            len(payload),
            source="bytes",
        )

        try:
            return payload.decode(
                self.config.encoding
            )

        except UnicodeDecodeError as exc:
            raise HtmlProcessingError(
                "HTML payload decode edilemedi "
                f"| encoding="
                f"{self.config.encoding!r}"
            ) from exc

    def _encode_text_for_size(
        self,
        payload: str,
    ) -> bytes:
        try:
            return payload.encode(
                self.config.encoding
            )

        except UnicodeEncodeError as exc:
            raise HtmlProcessingError(
                "HTML text configured encoding "
                "ile encode edilemedi "
                f"| encoding="
                f"{self.config.encoding!r}"
            ) from exc

    def _normalize_source(
        self,
        source: Any,
    ) -> tuple[
        str,
        int,
    ]:
        if isinstance(
            source,
            str,
        ):
            encoded = (
                self._encode_text_for_size(
                    source
                )
            )

            self._check_size(
                len(encoded),
                source="text",
            )

            return (
                source,
                len(encoded),
            )

        if isinstance(
            source,
            bytes,
        ):
            return (
                self._decode_bytes(
                    source
                ),
                len(source),
            )

        if isinstance(
            source,
            bytearray,
        ):
            payload = bytes(
                source
            )

            return (
                self._decode_bytes(
                    payload
                ),
                len(payload),
            )

        if isinstance(
            source,
            memoryview,
        ):
            payload = source.tobytes()

            return (
                self._decode_bytes(
                    payload
                ),
                len(payload),
            )

        if isinstance(
            source,
            Path,
        ):
            return self._read_file(
                source
            )

        raise HtmlProcessingError(
            "Desteklenmeyen HTML source type "
            f"| actual={type(source).__name__}"
        )

    def _read_file(
        self,
        path: Path,
    ) -> tuple[
        str,
        int,
    ]:
        if not isinstance(
            path,
            Path,
        ):
            raise HtmlProcessingError(
                "HTML file path pathlib.Path "
                "olmalıdır."
            )

        if not path.exists():
            raise HtmlProcessingError(
                "HTML file bulunamadı "
                f"| path={path}"
            )

        if not path.is_file():
            raise HtmlProcessingError(
                "HTML source regular file "
                "olmalıdır "
                f"| path={path}"
            )

        try:
            file_size = (
                path.stat().st_size
            )

        except OSError as exc:
            raise HtmlProcessingError(
                "HTML file metadata okunamadı "
                f"| path={path}"
            ) from exc

        self._check_size(
            file_size,
            source=str(path),
        )

        try:
            payload = (
                path.read_bytes()
            )

        except OSError as exc:
            raise HtmlProcessingError(
                "HTML file okunamadı "
                f"| path={path}"
            ) from exc

        self._check_size(
            len(payload),
            source=str(path),
        )

        try:
            text = payload.decode(
                self.config.encoding
            )

        except UnicodeDecodeError as exc:
            raise HtmlProcessingError(
                "HTML file decode edilemedi "
                f"| path={path} "
                f"| encoding="
                f"{self.config.encoding!r}"
            ) from exc

        return (
            text,
            len(payload),
        )

    def parse(
        self,
        source: Any,
    ) -> HtmlDocument:
        text, source_bytes = (
            self._normalize_source(
                source
            )
        )

        if not text.strip():
            raise HtmlProcessingError(
                "HTML payload boş olamaz."
            )

        parser = _TreeBuilder(
            include_comments=(
                self.config.include_comments
            ),
        )

        try:
            parser.feed(
                text
            )

            parser.close()

        except (
            ValueError,
            AssertionError,
        ) as exc:
            raise HtmlProcessingError(
                "HTML parse başarısız."
            ) from exc

        if not parser.roots:
            raise HtmlProcessingError(
                "HTML payload element içermiyor."
            )

        return HtmlDocument(
            roots=tuple(
                parser.roots
            ),
            comments=tuple(
                parser.comments
            ),
            declarations=tuple(
                parser.declarations
            ),
            source_bytes=(
                source_bytes
            ),
            encoding=(
                self.config.encoding
            ),
            include_hidden_text=(
                self.config.include_hidden_text
            ),
        )

    def parse_file(
        self,
        path: Path,
    ) -> HtmlDocument:
        if not isinstance(
            path,
            Path,
        ):
            raise HtmlProcessingError(
                "path pathlib.Path olmalıdır."
            )

        return self.parse(
            path
        )

    def extract_text(
        self,
        source: Any,
        *,
        separator: str = " ",
        strip: bool = True,
        include_hidden: Optional[
            bool
        ] = None,
    ) -> str:
        document = self.parse(
            source
        )

        return document.text(
            separator=separator,
            strip=strip,
            include_hidden=(
                include_hidden
            ),
        )

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
            f"max_bytes={self.config.max_bytes!r}, "
            f"encoding={self.config.encoding!r}, "
            f"include_comments="
            f"{self.config.include_comments!r}, "
            f"include_hidden_text="
            f"{self.config.include_hidden_text!r}"
            f")"
        )


# =============================================================================
# PUBLIC HELPERS
# =============================================================================

def parse_html(
    source: Any,
    *,
    config: Optional[
        HtmlProcessorConfig
    ] = None,
) -> HtmlDocument:
    return HtmlProcessor(
        config=config
    ).parse(
        source
    )


def parse_html_file(
    path: Path,
    *,
    config: Optional[
        HtmlProcessorConfig
    ] = None,
) -> HtmlDocument:
    return HtmlProcessor(
        config=config
    ).parse_file(
        path
    )


def extract_html_text(
    source: Any,
    *,
    config: Optional[
        HtmlProcessorConfig
    ] = None,
    separator: str = " ",
    strip: bool = True,
    include_hidden: Optional[
        bool
    ] = None,
) -> str:
    return HtmlProcessor(
        config=config
    ).extract_text(
        source,
        separator=separator,
        strip=strip,
        include_hidden=include_hidden,
    )


__all__ = [
    "DEFAULT_MAX_HTML_BYTES",
    "DEFAULT_HTML_ENCODING",
    "HtmlProcessingError",
    "HtmlProcessorConfig",
    "HtmlNode",
    "HtmlLink",
    "HtmlDocument",
    "HtmlProcessor",
    "parse_html",
    "parse_html_file",
    "extract_html_text",
]