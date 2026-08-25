from __future__ import annotations

from pathlib import Path

import pytest

from enterprise_crawler.processing.feed import (
    ATOM_NAMESPACE,
    SUPPORTED_FEED_TYPES,
    FeedDocument,
    FeedEntry,
    FeedLink,
    FeedProcessingError,
    FeedProcessor,
    FeedProcessorConfig,
    parse_feed,
    parse_feed_file,
)


# =============================================================================
# FIXTURES
# =============================================================================
RSS_SAMPLE = """\
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Example RSS</title>
    <link>https://example.com</link>
    <description>Example RSS feed</description>
    <language>en</language>
    <lastBuildDate>Wed, 13 Aug 2026 10:00:00 GMT</lastBuildDate>

    <item>
      <guid>item-1</guid>
      <title>First Item</title>
      <link>https://example.com/items/1</link>
      <description>First summary</description>
      <pubDate>Wed, 13 Aug 2026 09:00:00 GMT</pubDate>
      <author>author@example.com</author>
      <category>news</category>
      <category>technology</category>
    </item>

    <item>
      <guid>item-2</guid>
      <title>Second Item</title>
      <link>https://example.com/items/2</link>
      <description>Second summary</description>
      <pubDate>Wed, 13 Aug 2026 08:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""


ATOM_SAMPLE = f"""\
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="{ATOM_NAMESPACE}">
  <id>urn:example:feed</id>
  <title>Example Atom</title>
  <subtitle>Example Atom feed</subtitle>
  <updated>2026-08-13T10:00:00Z</updated>
  <link href="https://example.com/feed" rel="self" type="application/atom+xml"/>
  <link href="https://example.com" rel="alternate" type="text/html"/>

  <entry>
    <id>urn:example:item:1</id>
    <title>First Atom Item</title>
    <updated>2026-08-13T09:00:00Z</updated>
    <published>2026-08-13T08:30:00Z</published>
    <summary>Atom summary</summary>
    <content>Atom content</content>
    <link href="https://example.com/items/1" rel="alternate"/>
    <author>
      <name>Example Author</name>
    </author>
    <category term="news"/>
    <category term="atom"/>
  </entry>
</feed>
"""


# =============================================================================
# CONFIG
# =============================================================================
def test_default_config_is_valid() -> None:
    config = FeedProcessorConfig()

    assert (
        config.max_bytes
        == 8 * 1024 * 1024
    )

    assert (
        config.encoding
        == "utf-8"
    )

    assert (
        config.reject_duplicate_entries
        is True
    )

    assert (
        config.require_entry_identity
        is False
    )

    assert (
        config.reject_doctype
        is True
    )

    assert (
        config.reject_entities
        is True
    )


@pytest.mark.parametrize(
    "value",
    [
        0,
        -1,
        True,
        1.5,
        "100",
    ],
)
def test_invalid_max_bytes_is_rejected(
    value: object,
) -> None:
    with pytest.raises(
        FeedProcessingError
    ):
        FeedProcessorConfig(
            max_bytes=value,  # type: ignore[arg-type]
        )


def test_unlimited_max_bytes_is_allowed() -> None:
    config = FeedProcessorConfig(
        max_bytes=None
    )

    assert (
        config.max_bytes
        is None
    )


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        "\n\t",
    ],
)
def test_empty_encoding_is_rejected(
    value: str,
) -> None:
    with pytest.raises(
        FeedProcessingError
    ):
        FeedProcessorConfig(
            encoding=value
        )


@pytest.mark.parametrize(
    (
        "field_name",
        "value",
    ),
    [
        (
            "reject_duplicate_entries",
            1,
        ),
        (
            "require_entry_identity",
            "true",
        ),
        (
            "reject_doctype",
            1,
        ),
        (
            "reject_entities",
            None,
        ),
    ],
)
def test_invalid_boolean_config_is_rejected(
    field_name: str,
    value: object,
) -> None:
    kwargs = {
        field_name: value,
    }

    with pytest.raises(
        FeedProcessingError
    ):
        FeedProcessorConfig(
            **kwargs  # type: ignore[arg-type]
        )


def test_config_to_dict() -> None:
    config = FeedProcessorConfig(
        max_bytes=1024,
        encoding="utf-8",
        reject_duplicate_entries=False,
        require_entry_identity=True,
        reject_doctype=True,
        reject_entities=True,
    )

    assert config.to_dict() == {
        "max_bytes": 1024,
        "encoding": "utf-8",
        "reject_duplicate_entries": False,
        "require_entry_identity": True,
        "reject_doctype": True,
        "reject_entities": True,
    }


def test_invalid_processor_config_type_is_rejected() -> None:
    with pytest.raises(
        FeedProcessingError
    ):
        FeedProcessor(
            config={}  # type: ignore[arg-type]
        )


# =============================================================================
# FEED LINK
# =============================================================================
def test_feed_link_is_normalized() -> None:
    link = FeedLink(
        href="  https://example.com/item  ",
        rel=" alternate ",
        type=" text/html ",
        title=" Example ",
    )

    assert (
        link.href
        == "https://example.com/item"
    )

    assert (
        link.rel
        == "alternate"
    )

    assert (
        link.type
        == "text/html"
    )

    assert (
        link.title
        == "Example"
    )


@pytest.mark.parametrize(
    "href",
    [
        "",
        " ",
        "\n",
    ],
)
def test_empty_feed_link_href_is_rejected(
    href: str,
) -> None:
    with pytest.raises(
        FeedProcessingError
    ):
        FeedLink(
            href=href
        )


def test_feed_link_to_dict() -> None:
    link = FeedLink(
        href="https://example.com",
        rel="alternate",
        type="text/html",
        title="Example",
    )

    assert link.to_dict() == {
        "href": "https://example.com",
        "rel": "alternate",
        "type": "text/html",
        "title": "Example",
    }


# =============================================================================
# FEED ENTRY
# =============================================================================
def test_feed_entry_identity_prefers_id() -> None:
    entry = FeedEntry(
        id="entry-1",
        link="https://example.com/1",
    )

    assert (
        entry.identity
        == "entry-1"
    )


def test_feed_entry_identity_falls_back_to_link() -> None:
    entry = FeedEntry(
        link="https://example.com/1"
    )

    assert (
        entry.identity
        == "https://example.com/1"
    )


def test_feed_entry_identity_can_be_none() -> None:
    entry = FeedEntry()

    assert (
        entry.identity
        is None
    )


def test_feed_entry_categories_are_normalized() -> None:
    entry = FeedEntry(
        categories=(
            " news ",
            "",
            "technology",
        )
    )

    assert entry.categories == (
        "news",
        "technology",
    )


def test_feed_entry_rejects_invalid_link_type() -> None:
    with pytest.raises(
        FeedProcessingError
    ):
        FeedEntry(
            links=(
                "https://example.com",  # type: ignore[arg-type]
            )
        )


def test_feed_entry_metadata_must_be_dict() -> None:
    with pytest.raises(
        FeedProcessingError
    ):
        FeedEntry(
            metadata=[]  # type: ignore[arg-type]
        )


def test_feed_entry_to_dict() -> None:
    entry = FeedEntry(
        id="entry-1",
        title="Title",
        link="https://example.com/1",
        links=(
            FeedLink(
                href="https://example.com/1"
            ),
        ),
        published="2026-08-13",
        updated="2026-08-14",
        summary="Summary",
        content="Content",
        author="Author",
        categories=(
            "news",
        ),
        metadata={
            "source": "test",
        },
    )

    rendered = entry.to_dict()

    assert (
        rendered["id"]
        == "entry-1"
    )

    assert (
        rendered["title"]
        == "Title"
    )

    assert (
        rendered["link"]
        == "https://example.com/1"
    )

    assert (
        rendered["categories"]
        == ["news"]
    )

    assert (
        rendered["metadata"]
        == {
            "source": "test",
        }
    )


# =============================================================================
# FEED DOCUMENT
# =============================================================================
def test_feed_document_supported_types() -> None:
    assert SUPPORTED_FEED_TYPES == {
        "rss",
        "atom",
    }


@pytest.mark.parametrize(
    "value",
    [
        "",
        "xml",
        "json",
        "rss2",
    ],
)
def test_invalid_feed_type_is_rejected(
    value: str,
) -> None:
    with pytest.raises(
        FeedProcessingError
    ):
        FeedDocument(
            feed_type=value
        )


def test_feed_document_type_is_normalized() -> None:
    document = FeedDocument(
        feed_type=" RSS "
    )

    assert (
        document.feed_type
        == "rss"
    )


def test_feed_document_entry_count() -> None:
    document = FeedDocument(
        feed_type="rss",
        entries=(
            FeedEntry(
                id="1"
            ),
            FeedEntry(
                id="2"
            ),
        ),
    )

    assert (
        document.entry_count
        == 2
    )


def test_feed_document_rejects_invalid_entry_type() -> None:
    with pytest.raises(
        FeedProcessingError
    ):
        FeedDocument(
            feed_type="rss",
            entries=(
                "entry",  # type: ignore[arg-type]
            ),
        )


def test_feed_document_metadata_must_be_dict() -> None:
    with pytest.raises(
        FeedProcessingError
    ):
        FeedDocument(
            feed_type="rss",
            metadata=[]  # type: ignore[arg-type]
        )


def test_feed_document_to_dict() -> None:
    document = FeedDocument(
        feed_type="rss",
        title="Example",
        entries=(
            FeedEntry(
                id="1",
                title="Item",
            ),
        ),
    )

    rendered = (
        document.to_dict()
    )

    assert (
        rendered["feed_type"]
        == "rss"
    )

    assert (
        rendered["entry_count"]
        == 1
    )

    assert (
        rendered["entries"][0]["id"]
        == "1"
    )


# =============================================================================
# RSS PARSING
# =============================================================================
def test_parse_rss_feed() -> None:
    document = FeedProcessor().parse(
        RSS_SAMPLE
    )

    assert (
        document.feed_type
        == "rss"
    )

    assert (
        document.title
        == "Example RSS"
    )

    assert (
        document.link
        == "https://example.com"
    )

    assert (
        document.description
        == "Example RSS feed"
    )

    assert (
        document.language
        == "en"
    )

    assert (
        document.entry_count
        == 2
    )


def test_rss_metadata_contains_version() -> None:
    document = FeedProcessor().parse(
        RSS_SAMPLE
    )

    assert (
        document.metadata[
            "rss_version"
        ]
        == "2.0"
    )


def test_rss_entry_fields_are_normalized() -> None:
    document = FeedProcessor().parse(
        RSS_SAMPLE
    )

    entry = (
        document.entries[0]
    )

    assert (
        entry.id
        == "item-1"
    )

    assert (
        entry.title
        == "First Item"
    )

    assert (
        entry.link
        == "https://example.com/items/1"
    )

    assert (
        entry.summary
        == "First summary"
    )

    assert (
        entry.author
        == "author@example.com"
    )

    assert entry.categories == (
        "news",
        "technology",
    )


def test_rss_guid_falls_back_to_link() -> None:
    payload = """\
<rss version="2.0">
  <channel>
    <title>Feed</title>
    <link>https://example.com</link>
    <description>Feed</description>
    <item>
      <title>Item</title>
      <link>https://example.com/item</link>
    </item>
  </channel>
</rss>
"""

    document = FeedProcessor().parse(
        payload
    )

    assert (
        document.entries[0].id
        == "https://example.com/item"
    )


def test_rss_without_channel_is_rejected() -> None:
    with pytest.raises(
        FeedProcessingError
    ):
        FeedProcessor().parse(
            '<rss version="2.0"></rss>'
        )


def test_unsupported_rss_version_is_rejected() -> None:
    payload = """\
<rss version="0.91">
  <channel>
    <title>Feed</title>
  </channel>
</rss>
"""

    with pytest.raises(
        FeedProcessingError
    ):
        FeedProcessor().parse(
            payload
        )


# =============================================================================
# RSS NAMESPACED CONTENT
# =============================================================================
def test_rss_content_encoded_is_supported() -> None:
    payload = """\
<rss version="2.0"
     xmlns:content="http://purl.org/rss/1.0/modules/content/">
  <channel>
    <title>Feed</title>
    <link>https://example.com</link>
    <description>Feed</description>
    <item>
      <guid>1</guid>
      <content:encoded>Full content</content:encoded>
    </item>
  </channel>
</rss>
"""

    document = FeedProcessor().parse(
        payload
    )

    assert (
        document.entries[0].content
        == "Full content"
    )


def test_rss_dc_creator_is_supported() -> None:
    payload = """\
<rss version="2.0"
     xmlns:dc="http://purl.org/dc/elements/1.1/">
  <channel>
    <title>Feed</title>
    <link>https://example.com</link>
    <description>Feed</description>
    <item>
      <guid>1</guid>
      <dc:creator>Jane Doe</dc:creator>
    </item>
  </channel>
</rss>
"""

    document = FeedProcessor().parse(
        payload
    )

    assert (
        document.entries[0].author
        == "Jane Doe"
    )


# =============================================================================
# ATOM PARSING
# =============================================================================
def test_parse_atom_feed() -> None:
    document = FeedProcessor().parse(
        ATOM_SAMPLE
    )

    assert (
        document.feed_type
        == "atom"
    )

    assert (
        document.id
        == "urn:example:feed"
    )

    assert (
        document.title
        == "Example Atom"
    )

    assert (
        document.description
        == "Example Atom feed"
    )

    assert (
        document.link
        == "https://example.com"
    )

    assert (
        document.entry_count
        == 1
    )


def test_atom_metadata_contains_namespace() -> None:
    document = FeedProcessor().parse(
        ATOM_SAMPLE
    )

    assert (
        document.metadata[
            "atom_namespace"
        ]
        == ATOM_NAMESPACE
    )


def test_atom_entry_fields_are_normalized() -> None:
    document = FeedProcessor().parse(
        ATOM_SAMPLE
    )

    entry = (
        document.entries[0]
    )

    assert (
        entry.id
        == "urn:example:item:1"
    )

    assert (
        entry.title
        == "First Atom Item"
    )

    assert (
        entry.link
        == "https://example.com/items/1"
    )

    assert (
        entry.published
        == "2026-08-13T08:30:00Z"
    )

    assert (
        entry.updated
        == "2026-08-13T09:00:00Z"
    )

    assert (
        entry.summary
        == "Atom summary"
    )

    assert (
        entry.content
        == "Atom content"
    )

    assert (
        entry.author
        == "Example Author"
    )

    assert entry.categories == (
        "news",
        "atom",
    )


def test_atom_alternate_link_is_preferred() -> None:
    document = FeedProcessor().parse(
        ATOM_SAMPLE
    )

    assert (
        document.link
        == "https://example.com"
    )


def test_atom_first_link_is_fallback_when_no_alternate() -> None:
    payload = f"""\
<feed xmlns="{ATOM_NAMESPACE}">
  <id>feed</id>
  <title>Feed</title>
  <link href="https://example.com/self" rel="self"/>
</feed>
"""

    document = FeedProcessor().parse(
        payload
    )

    assert (
        document.link
        == "https://example.com/self"
    )


def test_atom_language_is_extracted() -> None:
    payload = f"""\
<feed xmlns="{ATOM_NAMESPACE}"
      xml:lang="tr">
  <id>feed</id>
  <title>Feed</title>
</feed>
"""

    document = FeedProcessor().parse(
        payload
    )

    assert (
        document.language
        == "tr"
    )


def test_atom_unknown_namespace_is_rejected() -> None:
    payload = """\
<feed xmlns="https://example.com/not-atom">
  <title>Feed</title>
</feed>
"""

    with pytest.raises(
        FeedProcessingError
    ):
        FeedProcessor().parse(
            payload
        )


# =============================================================================
# ROOT DETECTION
# =============================================================================
@pytest.mark.parametrize(
    "payload",
    [
        "<root></root>",
        "<channel></channel>",
        "<items></items>",
    ],
)
def test_unsupported_feed_root_is_rejected(
    payload: str,
) -> None:
    with pytest.raises(
        FeedProcessingError
    ):
        FeedProcessor().parse(
            payload
        )


# =============================================================================
# XML SECURITY INHERITANCE
# =============================================================================
def test_doctype_is_rejected_by_default() -> None:
    payload = """\
<!DOCTYPE rss>
<rss version="2.0">
  <channel>
    <title>Feed</title>
  </channel>
</rss>
"""

    with pytest.raises(
        FeedProcessingError
    ):
        FeedProcessor().parse(
            payload
        )


def test_entity_declaration_is_rejected_by_default() -> None:
    payload = """\
<!DOCTYPE rss [
  <!ENTITY example "test">
]>
<rss version="2.0">
  <channel>
    <title>&example;</title>
  </channel>
</rss>
"""

    with pytest.raises(
        FeedProcessingError
    ):
        FeedProcessor().parse(
            payload
        )


# =============================================================================
# SIZE LIMITS
# =============================================================================
def test_text_over_size_limit_is_rejected() -> None:
    processor = FeedProcessor(
        FeedProcessorConfig(
            max_bytes=16
        )
    )

    with pytest.raises(
        FeedProcessingError
    ):
        processor.parse(
            RSS_SAMPLE
        )


def test_bytes_over_size_limit_is_rejected() -> None:
    processor = FeedProcessor(
        FeedProcessorConfig(
            max_bytes=16
        )
    )

    with pytest.raises(
        FeedProcessingError
    ):
        processor.parse(
            RSS_SAMPLE.encode(
                "utf-8"
            )
        )


def test_size_limit_can_be_disabled() -> None:
    processor = FeedProcessor(
        FeedProcessorConfig(
            max_bytes=None
        )
    )

    document = processor.parse(
        RSS_SAMPLE
    )

    assert (
        document.entry_count
        == 2
    )


# =============================================================================
# DUPLICATE ENTRY POLICY
# =============================================================================
def test_duplicate_rss_entries_are_rejected_by_default() -> None:
    payload = """\
<rss version="2.0">
  <channel>
    <title>Feed</title>
    <item>
      <guid>same</guid>
    </item>
    <item>
      <guid>same</guid>
    </item>
  </channel>
</rss>
"""

    with pytest.raises(
        FeedProcessingError
    ):
        FeedProcessor().parse(
            payload
        )


def test_duplicate_entries_can_be_allowed() -> None:
    payload = """\
<rss version="2.0">
  <channel>
    <title>Feed</title>
    <item>
      <guid>same</guid>
    </item>
    <item>
      <guid>same</guid>
    </item>
  </channel>
</rss>
"""

    processor = FeedProcessor(
        FeedProcessorConfig(
            reject_duplicate_entries=False
        )
    )

    document = processor.parse(
        payload
    )

    assert (
        document.entry_count
        == 2
    )


def test_duplicate_identity_uses_link_fallback() -> None:
    payload = """\
<rss version="2.0">
  <channel>
    <title>Feed</title>
    <item>
      <link>https://example.com/item</link>
    </item>
    <item>
      <link>https://example.com/item</link>
    </item>
  </channel>
</rss>
"""

    with pytest.raises(
        FeedProcessingError
    ):
        FeedProcessor().parse(
            payload
        )


# =============================================================================
# IDENTITY REQUIREMENT
# =============================================================================
def test_missing_entry_identity_is_allowed_by_default() -> None:
    payload = """\
<rss version="2.0">
  <channel>
    <title>Feed</title>
    <item>
      <title>Anonymous</title>
    </item>
  </channel>
</rss>
"""

    document = FeedProcessor().parse(
        payload
    )

    assert (
        document.entries[0].identity
        is None
    )


def test_missing_entry_identity_can_be_rejected() -> None:
    payload = """\
<rss version="2.0">
  <channel>
    <title>Feed</title>
    <item>
      <title>Anonymous</title>
    </item>
  </channel>
</rss>
"""

    processor = FeedProcessor(
        FeedProcessorConfig(
            require_entry_identity=True
        )
    )

    with pytest.raises(
        FeedProcessingError
    ):
        processor.parse(
            payload
        )


# =============================================================================
# INPUT TYPES
# =============================================================================
def test_parse_bytes() -> None:
    document = FeedProcessor().parse(
        RSS_SAMPLE.encode(
            "utf-8"
        )
    )

    assert (
        document.feed_type
        == "rss"
    )


def test_parse_bytearray() -> None:
    document = FeedProcessor().parse(
        bytearray(
            RSS_SAMPLE.encode(
                "utf-8"
            )
        )
    )

    assert (
        document.entry_count
        == 2
    )


def test_parse_memoryview() -> None:
    document = FeedProcessor().parse(
        memoryview(
            RSS_SAMPLE.encode(
                "utf-8"
            )
        )
    )

    assert (
        document.entry_count
        == 2
    )


def test_path_is_not_accepted_by_parse() -> None:
    with pytest.raises(
        FeedProcessingError
    ):
        FeedProcessor().parse(
            Path("feed.xml")
        )


@pytest.mark.parametrize(
    "source",
    [
        None,
        123,
        1.5,
        True,
        [],
        {},
    ],
)
def test_unsupported_source_type_is_rejected(
    source: object,
) -> None:
    with pytest.raises(
        FeedProcessingError
    ):
        FeedProcessor().parse(
            source
        )


# =============================================================================
# FILE PARSING
# =============================================================================
def test_parse_feed_file(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "feed.xml"
    )

    path.write_text(
        RSS_SAMPLE,
        encoding="utf-8",
    )

    document = (
        FeedProcessor().parse_file(
            path
        )
    )

    assert (
        document.title
        == "Example RSS"
    )


def test_missing_feed_file_is_rejected(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        FeedProcessingError
    ):
        FeedProcessor().parse_file(
            tmp_path
            / "missing.xml"
        )


def test_directory_is_rejected_as_feed_file(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        FeedProcessingError
    ):
        FeedProcessor().parse_file(
            tmp_path
        )


def test_parse_file_requires_path_object() -> None:
    with pytest.raises(
        FeedProcessingError
    ):
        FeedProcessor().parse_file(
            "feed.xml"  # type: ignore[arg-type]
        )


# =============================================================================
# HELPERS
# =============================================================================
def test_parse_feed_helper() -> None:
    document = parse_feed(
        RSS_SAMPLE
    )

    assert (
        document.feed_type
        == "rss"
    )


def test_parse_feed_file_helper(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "feed.xml"
    )

    path.write_text(
        ATOM_SAMPLE,
        encoding="utf-8",
    )

    document = parse_feed_file(
        path
    )

    assert (
        document.feed_type
        == "atom"
    )


# =============================================================================
# SNAPSHOT / REPR
# =============================================================================
def test_snapshot_contains_configuration() -> None:
    processor = FeedProcessor(
        FeedProcessorConfig(
            max_bytes=4096,
            reject_duplicate_entries=False,
            require_entry_identity=True,
        )
    )

    snapshot = (
        processor.snapshot()
    )

    assert (
        snapshot["processor"]
        == "feed"
    )

    assert (
        snapshot[
            "supported_feed_types"
        ]
        == [
            "atom",
            "rss",
        ]
    )

    assert (
        snapshot["config"][
            "max_bytes"
        ]
        == 4096
    )

    assert (
        snapshot["config"][
            "reject_duplicate_entries"
        ]
        is False
    )

    assert (
        snapshot["config"][
            "require_entry_identity"
        ]
        is True
    )


def test_repr_contains_configuration() -> None:
    processor = FeedProcessor(
        FeedProcessorConfig(
            max_bytes=2048
        )
    )

    rendered = repr(
        processor
    )

    assert (
        "FeedProcessor"
        in rendered
    )

    assert (
        "2048"
        in rendered
    )

    assert (
        "reject_duplicate_entries"
        in rendered
    )