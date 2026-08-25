from __future__ import annotations

"""
Enterprise Crawler Framework - Feed Processing

RSS 2.0 ve Atom feed'lerini ortak bir veri modeline normalize eder.

Desteklenen formatlar
---------------------
- RSS 2.0
- Atom 1.0

Mimari
------
FeedProcessor
    ↓
XmlProcessor
    ↓
ElementTree
    ↓
FeedDocument
    ├── FeedEntry
    └── FeedLink

Amaç
----
Format-spesifik XML ayrıntılarını framework kullanıcılarından gizleyerek
RSS ve Atom kaynaklarını ortak, deterministik bir sözleşmeye dönüştürmek.

Güvenlik
--------
XML parsing doğrudan ElementTree ile tekrar yapılmaz. FeedProcessor mevcut
XmlProcessor katmanını kullanır. Böylece XML security policy, DOCTYPE/entity
korumaları ve payload size limitleri tek yerde uygulanır.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional
from xml.etree.ElementTree import Element

from enterprise_crawler.processing.xml import (
    XmlProcessingError,
    XmlProcessor,
    XmlProcessorConfig,
)


# =============================================================================
# CONSTANTS
# =============================================================================
ATOM_NAMESPACE = "http://www.w3.org/2005/Atom"

RSS_ROOT_TAG = "rss"
RSS_CHANNEL_TAG = "channel"
RSS_ITEM_TAG = "item"

ATOM_FEED_TAG = "feed"
ATOM_ENTRY_TAG = "entry"

SUPPORTED_FEED_TYPES = frozenset(
    {
        "rss",
        "atom",
    }
)


# =============================================================================
# EXCEPTIONS
# =============================================================================
class FeedProcessingError(Exception):
    """
    Feed processing sırasında oluşan framework-level hata.
    """


# =============================================================================
# HELPERS
# =============================================================================
def _local_name(
    tag: Any,
) -> str:
    if not isinstance(
        tag,
        str,
    ):
        return ""

    if tag.startswith("{"):
        _, separator, local = (
            tag[1:].partition("}")
        )

        if separator:
            return local

    if ":" in tag:
        return tag.rsplit(
            ":",
            1,
        )[-1]

    return tag


def _namespace_uri(
    tag: Any,
) -> Optional[str]:
    if not isinstance(
        tag,
        str,
    ):
        return None

    if not tag.startswith("{"):
        return None

    namespace, separator, _ = (
        tag[1:].partition("}")
    )

    if not separator:
        return None

    return namespace or None


def _normalize_text(
    value: Optional[str],
) -> Optional[str]:
    if value is None:
        return None

    normalized = str(
        value
    ).strip()

    if not normalized:
        return None

    return normalized


def _element_text(
    element: Optional[Element],
) -> Optional[str]:
    if element is None:
        return None

    pieces: list[str] = []

    for value in element.itertext():
        normalized = str(
            value
        ).strip()

        if normalized:
            pieces.append(
                normalized
            )

    if not pieces:
        return None

    return " ".join(
        pieces
    )


def _first_child(
    parent: Element,
    local_name: str,
) -> Optional[Element]:
    for child in list(
        parent
    ):
        if _local_name(
            child.tag
        ) == local_name:
            return child

    return None


def _children(
    parent: Element,
    local_name: str,
) -> list[Element]:
    return [
        child
        for child in list(parent)
        if _local_name(
            child.tag
        )
        == local_name
    ]


def _child_text(
    parent: Element,
    local_name: str,
) -> Optional[str]:
    return _element_text(
        _first_child(
            parent,
            local_name,
        )
    )


def _normalize_attribute(
    element: Element,
    key: str,
) -> Optional[str]:
    return _normalize_text(
        element.attrib.get(
            key
        )
    )


def _validate_bool(
    value: Any,
    *,
    field_name: str,
) -> bool:
    if not isinstance(
        value,
        bool,
    ):
        raise FeedProcessingError(
            f"{field_name} bool olmalıdır."
        )

    return value


# =============================================================================
# DATA MODELS
# =============================================================================
@dataclass(frozen=True)
class FeedLink:
    href: str
    rel: Optional[str] = None
    type: Optional[str] = None
    title: Optional[str] = None

    def __post_init__(
        self,
    ) -> None:
        normalized_href = (
            str(
                self.href
                or ""
            ).strip()
        )

        if not normalized_href:
            raise FeedProcessingError(
                "FeedLink.href boş olamaz."
            )

        object.__setattr__(
            self,
            "href",
            normalized_href,
        )

        object.__setattr__(
            self,
            "rel",
            _normalize_text(
                self.rel
            ),
        )

        object.__setattr__(
            self,
            "type",
            _normalize_text(
                self.type
            ),
        )

        object.__setattr__(
            self,
            "title",
            _normalize_text(
                self.title
            ),
        )

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "href": self.href,
            "rel": self.rel,
            "type": self.type,
            "title": self.title,
        }


@dataclass(frozen=True)
class FeedEntry:
    id: Optional[str] = None
    title: Optional[str] = None
    link: Optional[str] = None

    links: tuple[
        FeedLink,
        ...,
    ] = ()

    published: Optional[str] = None
    updated: Optional[str] = None

    summary: Optional[str] = None
    content: Optional[str] = None

    author: Optional[str] = None

    categories: tuple[
        str,
        ...,
    ] = ()

    metadata: dict[
        str,
        Any,
    ] = field(
        default_factory=dict
    )

    def __post_init__(
        self,
    ) -> None:
        object.__setattr__(
            self,
            "id",
            _normalize_text(
                self.id
            ),
        )

        object.__setattr__(
            self,
            "title",
            _normalize_text(
                self.title
            ),
        )

        object.__setattr__(
            self,
            "link",
            _normalize_text(
                self.link
            ),
        )

        object.__setattr__(
            self,
            "published",
            _normalize_text(
                self.published
            ),
        )

        object.__setattr__(
            self,
            "updated",
            _normalize_text(
                self.updated
            ),
        )

        object.__setattr__(
            self,
            "summary",
            _normalize_text(
                self.summary
            ),
        )

        object.__setattr__(
            self,
            "content",
            _normalize_text(
                self.content
            ),
        )

        object.__setattr__(
            self,
            "author",
            _normalize_text(
                self.author
            ),
        )

        normalized_links: list[
            FeedLink
        ] = []

        for link in self.links:
            if not isinstance(
                link,
                FeedLink,
            ):
                raise FeedProcessingError(
                    "FeedEntry.links yalnızca "
                    "FeedLink içermelidir."
                )

            normalized_links.append(
                link
            )

        object.__setattr__(
            self,
            "links",
            tuple(
                normalized_links
            ),
        )

        normalized_categories: list[
            str
        ] = []

        for category in self.categories:
            normalized = (
                _normalize_text(
                    category
                )
            )

            if normalized is not None:
                normalized_categories.append(
                    normalized
                )

        object.__setattr__(
            self,
            "categories",
            tuple(
                normalized_categories
            ),
        )

        if not isinstance(
            self.metadata,
            dict,
        ):
            raise FeedProcessingError(
                "FeedEntry.metadata dict olmalıdır."
            )

        object.__setattr__(
            self,
            "metadata",
            dict(
                self.metadata
            ),
        )

    @property
    def identity(
        self,
    ) -> Optional[str]:
        """
        Entry için deduplication kimliği.

        Öncelik:
        1. id / guid
        2. canonical link
        """

        if self.id:
            return self.id

        if self.link:
            return self.link

        return None

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "link": self.link,
            "links": [
                link.to_dict()
                for link in self.links
            ],
            "published": self.published,
            "updated": self.updated,
            "summary": self.summary,
            "content": self.content,
            "author": self.author,
            "categories": list(
                self.categories
            ),
            "metadata": dict(
                self.metadata
            ),
        }


@dataclass(frozen=True)
class FeedDocument:
    feed_type: str

    title: Optional[str] = None
    id: Optional[str] = None
    link: Optional[str] = None

    links: tuple[
        FeedLink,
        ...,
    ] = ()

    description: Optional[str] = None

    updated: Optional[str] = None
    language: Optional[str] = None

    entries: tuple[
        FeedEntry,
        ...,
    ] = ()

    metadata: dict[
        str,
        Any,
    ] = field(
        default_factory=dict
    )

    def __post_init__(
        self,
    ) -> None:
        normalized_type = str(
            self.feed_type
            or ""
        ).strip().lower()

        if (
            normalized_type
            not in SUPPORTED_FEED_TYPES
        ):
            raise FeedProcessingError(
                "Geçersiz feed_type: "
                f"{self.feed_type!r}"
            )

        object.__setattr__(
            self,
            "feed_type",
            normalized_type,
        )

        for field_name in (
            "title",
            "id",
            "link",
            "description",
            "updated",
            "language",
        ):
            object.__setattr__(
                self,
                field_name,
                _normalize_text(
                    getattr(
                        self,
                        field_name,
                    )
                ),
            )

        normalized_links: list[
            FeedLink
        ] = []

        for link in self.links:
            if not isinstance(
                link,
                FeedLink,
            ):
                raise FeedProcessingError(
                    "FeedDocument.links yalnızca "
                    "FeedLink içermelidir."
                )

            normalized_links.append(
                link
            )

        object.__setattr__(
            self,
            "links",
            tuple(
                normalized_links
            ),
        )

        normalized_entries: list[
            FeedEntry
        ] = []

        for entry in self.entries:
            if not isinstance(
                entry,
                FeedEntry,
            ):
                raise FeedProcessingError(
                    "FeedDocument.entries yalnızca "
                    "FeedEntry içermelidir."
                )

            normalized_entries.append(
                entry
            )

        object.__setattr__(
            self,
            "entries",
            tuple(
                normalized_entries
            ),
        )

        if not isinstance(
            self.metadata,
            dict,
        ):
            raise FeedProcessingError(
                "FeedDocument.metadata dict olmalıdır."
            )

        object.__setattr__(
            self,
            "metadata",
            dict(
                self.metadata
            ),
        )

    @property
    def entry_count(
        self,
    ) -> int:
        return len(
            self.entries
        )

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "feed_type": self.feed_type,
            "title": self.title,
            "id": self.id,
            "link": self.link,
            "links": [
                link.to_dict()
                for link in self.links
            ],
            "description": (
                self.description
            ),
            "updated": self.updated,
            "language": self.language,
            "entry_count": (
                self.entry_count
            ),
            "entries": [
                entry.to_dict()
                for entry in self.entries
            ],
            "metadata": dict(
                self.metadata
            ),
        }


# =============================================================================
# CONFIG
# =============================================================================
@dataclass(frozen=True)
class FeedProcessorConfig:
    """
    Feed processor configuration.

    max_bytes XmlProcessor'a aktarılır.

    reject_duplicate_entries:
        Aynı id/guid/link kimliğine sahip birden fazla entry bulunursa
        fail-fast davranışını kontrol eder.

    require_entry_identity:
        Her entry'nin id/guid veya link ile kimliklendirilmesini zorunlu kılar.
    """

    max_bytes: Optional[
        int
    ] = (
        8 * 1024 * 1024
    )

    encoding: str = "utf-8"

    reject_duplicate_entries: bool = True
    require_entry_identity: bool = False

    reject_doctype: bool = True
    reject_entities: bool = True

    def __post_init__(
        self,
    ) -> None:
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
                raise FeedProcessingError(
                    "max_bytes pozitif int "
                    "veya None olmalıdır."
                )

        normalized_encoding = str(
            self.encoding
            or ""
        ).strip()

        if not normalized_encoding:
            raise FeedProcessingError(
                "encoding boş olamaz."
            )

        object.__setattr__(
            self,
            "encoding",
            normalized_encoding,
        )

        _validate_bool(
            self.reject_duplicate_entries,
            field_name=(
                "reject_duplicate_entries"
            ),
        )

        _validate_bool(
            self.require_entry_identity,
            field_name=(
                "require_entry_identity"
            ),
        )

        _validate_bool(
            self.reject_doctype,
            field_name="reject_doctype",
        )

        _validate_bool(
            self.reject_entities,
            field_name="reject_entities",
        )

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "max_bytes": (
                self.max_bytes
            ),
            "encoding": self.encoding,
            "reject_duplicate_entries": (
                self.reject_duplicate_entries
            ),
            "require_entry_identity": (
                self.require_entry_identity
            ),
            "reject_doctype": (
                self.reject_doctype
            ),
            "reject_entities": (
                self.reject_entities
            ),
        }


# =============================================================================
# PROCESSOR
# =============================================================================
class FeedProcessor:
    """
    RSS 2.0 ve Atom parser/normalizer.
    """

    def __init__(
        self,
        config: Optional[
            FeedProcessorConfig
        ] = None,
    ) -> None:
        if config is None:
            config = (
                FeedProcessorConfig()
            )

        if not isinstance(
            config,
            FeedProcessorConfig,
        ):
            raise FeedProcessingError(
                "config FeedProcessorConfig "
                "olmalıdır."
            )

        self.config = config

        try:
            xml_config = (
                XmlProcessorConfig(
                    max_bytes=(
                        config.max_bytes
                    ),
                    encoding=(
                        config.encoding
                    ),
                    reject_doctype=(
                        config.reject_doctype
                    ),
                    reject_entities=(
                        config.reject_entities
                    ),
                )
            )

            self.xml = XmlProcessor(
                xml_config
            )

        except XmlProcessingError as exc:
            raise FeedProcessingError(
                "XML processor yapılandırılamadı."
            ) from exc

    # =========================================================================
    # PUBLIC PARSING
    # =========================================================================
    def parse(
        self,
        source: Any,
    ) -> FeedDocument:
        """
        str / bytes / bytearray / memoryview XML feed parse eder.

        Path semantiği burada bilinçli olarak implicit değildir.
        Dosya okumak için parse_file() kullanılır.
        """

        if isinstance(
            source,
            Path,
        ):
            raise FeedProcessingError(
                "Path parse() ile kullanılamaz; "
                "parse_file() kullan."
            )

        try:
            root = self.xml.parse(
                source
            )

        except XmlProcessingError as exc:
            raise FeedProcessingError(
                f"Feed XML parse edilemedi: {exc}"
            ) from exc

        return self._from_root(
            root
        )

    def parse_file(
        self,
        path: Path,
    ) -> FeedDocument:
        if not isinstance(
            path,
            Path,
        ):
            raise FeedProcessingError(
                "parse_file() Path ister."
            )

        try:
            root = self.xml.parse_file(
                path
            )

        except XmlProcessingError as exc:
            raise FeedProcessingError(
                f"Feed dosyası parse edilemedi: {exc}"
            ) from exc

        return self._from_root(
            root
        )

    # =========================================================================
    # ROOT DISPATCH
    # =========================================================================
    def _from_root(
        self,
        root: Element,
    ) -> FeedDocument:
        if not isinstance(
            root,
            Element,
        ):
            raise FeedProcessingError(
                "XML parser Element root "
                "döndürmelidir."
            )

        root_name = _local_name(
            root.tag
        ).lower()

        if root_name == RSS_ROOT_TAG:
            document = (
                self._parse_rss(
                    root
                )
            )

        elif (
            root_name
            == ATOM_FEED_TAG
        ):
            document = (
                self._parse_atom(
                    root
                )
            )

        else:
            raise FeedProcessingError(
                "Desteklenmeyen feed root: "
                f"{root.tag!r}. "
                "Yalnız RSS 2.0 ve Atom desteklenir."
            )

        self._validate_entries(
            document.entries
        )

        return document

    # =========================================================================
    # RSS
    # =========================================================================
    def _parse_rss(
        self,
        root: Element,
    ) -> FeedDocument:
        channel = _first_child(
            root,
            RSS_CHANNEL_TAG,
        )

        if channel is None:
            raise FeedProcessingError(
                "RSS feed içinde channel "
                "elementi bulunamadı."
            )

        version = _normalize_attribute(
            root,
            "version",
        )

        if (
            version is not None
            and version != "2.0"
        ):
            raise FeedProcessingError(
                "Desteklenmeyen RSS version: "
                f"{version!r}"
            )

        links = self._rss_feed_links(
            channel
        )

        primary_link = (
            links[0].href
            if links
            else _child_text(
                channel,
                "link",
            )
        )

        entries = tuple(
            self._parse_rss_item(
                item
            )
            for item in _children(
                channel,
                RSS_ITEM_TAG,
            )
        )

        language = _child_text(
            channel,
            "language",
        )

        return FeedDocument(
            feed_type="rss",
            title=_child_text(
                channel,
                "title",
            ),
            id=primary_link,
            link=primary_link,
            links=links,
            description=_child_text(
                channel,
                "description",
            ),
            updated=(
                _child_text(
                    channel,
                    "lastBuildDate",
                )
                or _child_text(
                    channel,
                    "pubDate",
                )
            ),
            language=language,
            entries=entries,
            metadata={
                "rss_version": (
                    version or "2.0"
                )
            },
        )

    def _rss_feed_links(
        self,
        channel: Element,
    ) -> tuple[
        FeedLink,
        ...,
    ]:
        value = _child_text(
            channel,
            "link",
        )

        if not value:
            return ()

        return (
            FeedLink(
                href=value,
            ),
        )

    def _parse_rss_item(
        self,
        item: Element,
    ) -> FeedEntry:
        guid = _child_text(
            item,
            "guid",
        )

        link = _child_text(
            item,
            "link",
        )

        description = _child_text(
            item,
            "description",
        )

        content = None

        for child in list(
            item
        ):
            if (
                _local_name(
                    child.tag
                ).lower()
                == "encoded"
            ):
                content = (
                    _element_text(
                        child
                    )
                )
                break

        categories = tuple(
            value
            for category in _children(
                item,
                "category",
            )
            if (
                value := _element_text(
                    category
                )
            )
        )

        author = (
            _child_text(
                item,
                "author",
            )
            or _child_text(
                item,
                "creator",
            )
        )

        links: tuple[
            FeedLink,
            ...,
        ]

        if link:
            links = (
                FeedLink(
                    href=link
                ),
            )

        else:
            links = ()

        return FeedEntry(
            id=guid or link,
            title=_child_text(
                item,
                "title",
            ),
            link=link,
            links=links,
            published=_child_text(
                item,
                "pubDate",
            ),
            updated=None,
            summary=description,
            content=content,
            author=author,
            categories=categories,
            metadata={
                "guid": guid,
            },
        )

    # =========================================================================
    # ATOM
    # =========================================================================
    def _parse_atom(
        self,
        root: Element,
    ) -> FeedDocument:
        namespace = (
            _namespace_uri(
                root.tag
            )
        )

        if (
            namespace is not None
            and namespace
            != ATOM_NAMESPACE
        ):
            raise FeedProcessingError(
                "Desteklenmeyen Atom namespace: "
                f"{namespace!r}"
            )

        links = self._atom_links(
            root
        )

        primary_link = (
            self._select_primary_link(
                links
            )
        )

        entries = tuple(
            self._parse_atom_entry(
                entry
            )
            for entry in _children(
                root,
                ATOM_ENTRY_TAG,
            )
        )

        language = (
            root.attrib.get(
                "{http://www.w3.org/XML/1998/namespace}lang"
            )
            or root.attrib.get(
                "lang"
            )
        )

        return FeedDocument(
            feed_type="atom",
            title=_child_text(
                root,
                "title",
            ),
            id=_child_text(
                root,
                "id",
            ),
            link=primary_link,
            links=links,
            description=(
                _child_text(
                    root,
                    "subtitle",
                )
            ),
            updated=_child_text(
                root,
                "updated",
            ),
            language=language,
            entries=entries,
            metadata={
                "atom_namespace": (
                    namespace
                    or ATOM_NAMESPACE
                )
            },
        )

    def _parse_atom_entry(
        self,
        entry: Element,
    ) -> FeedEntry:
        links = self._atom_links(
            entry
        )

        primary_link = (
            self._select_primary_link(
                links
            )
        )

        categories: list[
            str
        ] = []

        for category in _children(
            entry,
            "category",
        ):
            term = (
                _normalize_attribute(
                    category,
                    "term",
                )
                or _element_text(
                    category
                )
            )

            if term:
                categories.append(
                    term
                )

        author = self._atom_author(
            entry
        )

        return FeedEntry(
            id=_child_text(
                entry,
                "id",
            ),
            title=_child_text(
                entry,
                "title",
            ),
            link=primary_link,
            links=links,
            published=_child_text(
                entry,
                "published",
            ),
            updated=_child_text(
                entry,
                "updated",
            ),
            summary=_child_text(
                entry,
                "summary",
            ),
            content=_child_text(
                entry,
                "content",
            ),
            author=author,
            categories=tuple(
                categories
            ),
            metadata={},
        )

    def _atom_links(
        self,
        parent: Element,
    ) -> tuple[
        FeedLink,
        ...,
    ]:
        links: list[
            FeedLink
        ] = []

        for element in _children(
            parent,
            "link",
        ):
            href = (
                _normalize_attribute(
                    element,
                    "href",
                )
            )

            if not href:
                continue

            links.append(
                FeedLink(
                    href=href,
                    rel=(
                        _normalize_attribute(
                            element,
                            "rel",
                        )
                    ),
                    type=(
                        _normalize_attribute(
                            element,
                            "type",
                        )
                    ),
                    title=(
                        _normalize_attribute(
                            element,
                            "title",
                        )
                    ),
                )
            )

        return tuple(
            links
        )

    @staticmethod
    def _select_primary_link(
        links: Iterable[
            FeedLink
        ],
    ) -> Optional[str]:
        materialized = tuple(
            links
        )

        for link in materialized:
            rel = (
                link.rel
                or "alternate"
            ).lower()

            if rel == "alternate":
                return link.href

        if materialized:
            return (
                materialized[0].href
            )

        return None

    @staticmethod
    def _atom_author(
        parent: Element,
    ) -> Optional[str]:
        author = _first_child(
            parent,
            "author",
        )

        if author is None:
            return None

        return (
            _child_text(
                author,
                "name",
            )
            or _element_text(
                author
            )
        )

    # =========================================================================
    # VALIDATION
    # =========================================================================
    def _validate_entries(
        self,
        entries: tuple[
            FeedEntry,
            ...,
        ],
    ) -> None:
        identities: set[
            str
        ] = set()

        for index, entry in enumerate(
            entries
        ):
            identity = (
                entry.identity
            )

            if (
                identity is None
                and self.config.require_entry_identity
            ):
                raise FeedProcessingError(
                    "Feed entry kimliği eksik "
                    f"| index={index}"
                )

            if identity is None:
                continue

            if (
                identity in identities
                and self.config.reject_duplicate_entries
            ):
                raise FeedProcessingError(
                    "Duplicate feed entry "
                    f"identity: {identity!r}"
                )

            identities.add(
                identity
            )

    # =========================================================================
    # SNAPSHOT
    # =========================================================================
    def snapshot(
        self,
    ) -> dict[str, Any]:
        return {
            "processor": (
                "feed"
            ),
            "supported_feed_types": sorted(
                SUPPORTED_FEED_TYPES
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
            "FeedProcessor("
            f"max_bytes={self.config.max_bytes!r}, "
            "reject_duplicate_entries="
            f"{self.config.reject_duplicate_entries!r}, "
            "require_entry_identity="
            f"{self.config.require_entry_identity!r}"
            ")"
        )


# =============================================================================
# PUBLIC HELPERS
# =============================================================================
def parse_feed(
    source: Any,
    *,
    config: Optional[
        FeedProcessorConfig
    ] = None,
) -> FeedDocument:
    return FeedProcessor(
        config
    ).parse(
        source
    )


def parse_feed_file(
    path: Path,
    *,
    config: Optional[
        FeedProcessorConfig
    ] = None,
) -> FeedDocument:
    return FeedProcessor(
        config
    ).parse_file(
        path
    )


__all__ = [
    "ATOM_NAMESPACE",
    "SUPPORTED_FEED_TYPES",
    "FeedDocument",
    "FeedEntry",
    "FeedLink",
    "FeedProcessingError",
    "FeedProcessor",
    "FeedProcessorConfig",
    "parse_feed",
    "parse_feed_file",
]