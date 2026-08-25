from __future__ import annotations

from pathlib import Path

import pytest

from enterprise_crawler.processing.html import (
    DEFAULT_HTML_ENCODING,
    DEFAULT_MAX_HTML_BYTES,
    HtmlDocument,
    HtmlLink,
    HtmlNode,
    HtmlProcessingError,
    HtmlProcessor,
    HtmlProcessorConfig,
    extract_html_text,
    parse_html,
    parse_html_file,
)


SIMPLE_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Enterprise Crawler</title>
    <meta name="description" content="Crawler framework">
</head>
<body>
    <main id="content" class="page primary">
        <h1>Hello World</h1>
        <p class="lead">Crawler is ready.</p>
        <a href="/docs" title="Docs" rel="help external">
            Documentation
        </a>
    </main>
</body>
</html>
""".strip()


# =============================================================================
# CONFIG
# =============================================================================

def test_default_config_is_valid() -> None:
    config = HtmlProcessorConfig()

    assert (
        config.max_bytes
        == DEFAULT_MAX_HTML_BYTES
    )

    assert (
        config.encoding
        == DEFAULT_HTML_ENCODING
    )

    assert (
        config.include_comments
        is False
    )

    assert (
        config.include_hidden_text
        is False
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
    value,
) -> None:
    with pytest.raises(
        HtmlProcessingError
    ):
        HtmlProcessorConfig(
            max_bytes=value
        )


def test_unlimited_max_bytes_is_allowed() -> None:
    config = HtmlProcessorConfig(
        max_bytes=None
    )

    assert config.max_bytes is None


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
    ],
)
def test_empty_encoding_is_rejected(
    value: str,
) -> None:
    with pytest.raises(
        HtmlProcessingError
    ):
        HtmlProcessorConfig(
            encoding=value
        )


def test_unknown_encoding_is_rejected() -> None:
    with pytest.raises(
        HtmlProcessingError
    ):
        HtmlProcessorConfig(
            encoding=(
                "definitely-not-an-encoding"
            )
        )


@pytest.mark.parametrize(
    (
        "field_name",
        "value",
    ),
    [
        (
            "include_comments",
            1,
        ),
        (
            "include_hidden_text",
            "true",
        ),
    ],
)
def test_invalid_boolean_config_is_rejected(
    field_name: str,
    value,
) -> None:
    kwargs = {
        field_name: value
    }

    with pytest.raises(
        HtmlProcessingError
    ):
        HtmlProcessorConfig(
            **kwargs
        )


def test_config_to_dict() -> None:
    config = HtmlProcessorConfig(
        max_bytes=1024,
        encoding="utf-8",
        include_comments=True,
        include_hidden_text=True,
    )

    assert config.to_dict() == {
        "max_bytes": 1024,
        "encoding": "utf-8",
        "include_comments": True,
        "include_hidden_text": True,
    }


def test_invalid_processor_config_type_is_rejected() -> None:
    with pytest.raises(
        HtmlProcessingError
    ):
        HtmlProcessor(
            config={}  # type: ignore[arg-type]
        )


# =============================================================================
# BASIC PARSING
# =============================================================================

def test_parse_html_from_string() -> None:
    document = (
        HtmlProcessor().parse(
            SIMPLE_HTML
        )
    )

    assert isinstance(
        document,
        HtmlDocument,
    )

    html = document.find(
        "html"
    )

    assert html is not None


def test_parse_html_from_bytes() -> None:
    document = (
        HtmlProcessor().parse(
            b"<html><body>Hello</body></html>"
        )
    )

    body = document.find(
        "body"
    )

    assert body is not None

    assert (
        body.text()
        == "Hello"
    )


def test_parse_html_from_bytearray() -> None:
    document = (
        HtmlProcessor().parse(
            bytearray(
                b"<html><p>Hello</p></html>"
            )
        )
    )

    assert (
        document.find(
            "p"
        ).text()
        == "Hello"
    )


def test_parse_html_from_memoryview() -> None:
    document = (
        HtmlProcessor().parse(
            memoryview(
                b"<html><p>Hello</p></html>"
            )
        )
    )

    assert (
        document.find(
            "p"
        ).text()
        == "Hello"
    )


def test_unicode_html_is_preserved() -> None:
    document = (
        HtmlProcessor().parse(
            "<html><body>"
            "<p>İstanbul Şişli ğüşöç</p>"
            "</body></html>"
        )
    )

    assert (
        document.find(
            "p"
        ).text()
        == "İstanbul Şişli ğüşöç"
    )


@pytest.mark.parametrize(
    "payload",
    [
        "",
        " ",
        "\n\t",
    ],
)
def test_empty_html_is_rejected(
    payload: str,
) -> None:
    with pytest.raises(
        HtmlProcessingError
    ):
        HtmlProcessor().parse(
            payload
        )


def test_text_without_element_is_rejected() -> None:
    with pytest.raises(
        HtmlProcessingError
    ):
        HtmlProcessor().parse(
            "plain text only"
        )


def test_string_is_not_treated_as_path() -> None:
    with pytest.raises(
        HtmlProcessingError
    ):
        HtmlProcessor().parse(
            "missing.html"
        )


def test_invalid_utf8_is_rejected() -> None:
    with pytest.raises(
        HtmlProcessingError
    ):
        HtmlProcessor().parse(
            b"\xff\xfe<html></html>"
        )


# =============================================================================
# FILE PARSING
# =============================================================================

def test_parse_html_file(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "page.html"
    )

    path.write_text(
        SIMPLE_HTML,
        encoding="utf-8",
    )

    document = (
        HtmlProcessor().parse(
            path
        )
    )

    assert (
        document.title
        == "Enterprise Crawler"
    )


def test_parse_file_method(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "page.html"
    )

    path.write_text(
        "<html><p>Hello</p></html>",
        encoding="utf-8",
    )

    document = (
        HtmlProcessor().parse_file(
            path
        )
    )

    assert (
        document.find(
            "p"
        ).text()
        == "Hello"
    )


def test_missing_html_file_is_rejected(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        HtmlProcessingError
    ):
        HtmlProcessor().parse(
            tmp_path
            / "missing.html"
        )


def test_directory_is_rejected_as_html_file(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        HtmlProcessingError
    ):
        HtmlProcessor().parse(
            tmp_path
        )


def test_parse_file_requires_path_object() -> None:
    with pytest.raises(
        HtmlProcessingError
    ):
        HtmlProcessor().parse_file(
            "page.html"  # type: ignore[arg-type]
        )


# =============================================================================
# SIZE LIMIT
# =============================================================================

def test_text_over_size_limit_is_rejected() -> None:
    processor = HtmlProcessor(
        HtmlProcessorConfig(
            max_bytes=8
        )
    )

    with pytest.raises(
        HtmlProcessingError
    ):
        processor.parse(
            "<p>Hello</p>"
        )


def test_text_exactly_at_size_limit_is_allowed() -> None:
    payload = "<p>x</p>"

    size = len(
        payload.encode(
            "utf-8"
        )
    )

    processor = HtmlProcessor(
        HtmlProcessorConfig(
            max_bytes=size
        )
    )

    document = processor.parse(
        payload
    )

    assert (
        document.find(
            "p"
        ).text()
        == "x"
    )


def test_bytes_over_size_limit_is_rejected() -> None:
    processor = HtmlProcessor(
        HtmlProcessorConfig(
            max_bytes=5
        )
    )

    with pytest.raises(
        HtmlProcessingError
    ):
        processor.parse(
            b"<p>x</p>"
        )


def test_file_over_size_limit_is_rejected(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "large.html"
    )

    path.write_text(
        "<html><body>Hello</body></html>",
        encoding="utf-8",
    )

    processor = HtmlProcessor(
        HtmlProcessorConfig(
            max_bytes=4
        )
    )

    with pytest.raises(
        HtmlProcessingError
    ):
        processor.parse(
            path
        )


def test_size_limit_can_be_disabled() -> None:
    processor = HtmlProcessor(
        HtmlProcessorConfig(
            max_bytes=None
        )
    )

    payload = (
        "<html><body>"
        + ("x" * 10_000)
        + "</body></html>"
    )

    document = processor.parse(
        payload
    )

    assert (
        "x" * 100
        in document.text()
    )


# =============================================================================
# TREE
# =============================================================================

def test_node_attributes_are_available() -> None:
    document = parse_html(
        SIMPLE_HTML
    )

    main = document.find(
        "main"
    )

    assert main is not None

    assert (
        main.get(
            "id"
        )
        == "content"
    )

    assert (
        main.id
        == "content"
    )


def test_node_classes_are_tokenized() -> None:
    document = parse_html(
        SIMPLE_HTML
    )

    main = document.find(
        "main"
    )

    assert main is not None

    assert main.classes == {
        "page",
        "primary",
    }


def test_has_class() -> None:
    document = parse_html(
        SIMPLE_HTML
    )

    main = document.find(
        "main"
    )

    assert main is not None

    assert (
        main.has_class(
            "primary"
        )
        is True
    )

    assert (
        main.has_class(
            "missing"
        )
        is False
    )


def test_find_by_tag() -> None:
    document = parse_html(
        SIMPLE_HTML
    )

    node = document.find(
        "h1"
    )

    assert node is not None

    assert (
        node.text()
        == "Hello World"
    )


def test_find_by_attribute() -> None:
    document = parse_html(
        SIMPLE_HTML
    )

    node = document.find(
        attrs={
            "id": "content"
        }
    )

    assert node is not None

    assert node.tag == "main"


def test_find_by_class_name() -> None:
    document = parse_html(
        SIMPLE_HTML
    )

    node = document.find(
        class_name="lead"
    )

    assert node is not None

    assert node.tag == "p"


def test_find_by_tag_and_class() -> None:
    document = parse_html(
        SIMPLE_HTML
    )

    node = document.find(
        "p",
        class_name="lead",
    )

    assert node is not None


def test_find_returns_none_when_missing() -> None:
    document = parse_html(
        SIMPLE_HTML
    )

    assert (
        document.find(
            "article"
        )
        is None
    )


def test_find_all_returns_matches() -> None:
    document = parse_html(
        """
        <html>
            <p>A</p>
            <p>B</p>
            <p>C</p>
        </html>
        """
    )

    nodes = document.find_all(
        "p"
    )

    assert len(nodes) == 3

    assert [
        node.text()
        for node
        in nodes
    ] == [
        "A",
        "B",
        "C",
    ]


def test_find_all_returns_empty_list() -> None:
    document = parse_html(
        "<html></html>"
    )

    assert (
        document.find_all(
            "article"
        )
        == []
    )


def test_node_find_searches_descendants() -> None:
    document = parse_html(
        SIMPLE_HTML
    )

    main = document.find(
        "main"
    )

    assert main is not None

    heading = main.find(
        "h1"
    )

    assert heading is not None


def test_parent_relationship_is_preserved() -> None:
    document = parse_html(
        "<html><body><p>Hello</p></body></html>"
    )

    paragraph = document.find(
        "p"
    )

    assert paragraph is not None

    assert paragraph.parent is not None

    assert (
        paragraph.parent.tag
        == "body"
    )


def test_void_elements_do_not_capture_following_content() -> None:
    document = parse_html(
        "<html><body>"
        "<img src='x.png'>"
        "<p>Hello</p>"
        "</body></html>"
    )

    image = document.find(
        "img"
    )

    paragraph = document.find(
        "p"
    )

    assert image is not None
    assert paragraph is not None

    assert image.children == []

    assert (
        paragraph.text()
        == "Hello"
    )


def test_self_closing_tag_is_supported() -> None:
    document = parse_html(
        "<html><custom value='1'/></html>"
    )

    node = document.find(
        "custom"
    )

    assert node is not None

    assert (
        node.get(
            "value"
        )
        == "1"
    )


def test_html_is_case_normalized() -> None:
    document = parse_html(
        "<HTML><BODY><P>Hello</P></BODY></HTML>"
    )

    assert (
        document.find(
            "p"
        )
        is not None
    )


# =============================================================================
# TEXT EXTRACTION
# =============================================================================

def test_document_text_extracts_visible_text() -> None:
    document = parse_html(
        """
        <html>
            <body>
                <h1>Hello</h1>
                <p>World</p>
            </body>
        </html>
        """
    )

    assert (
        document.text()
        == "Hello World"
    )


def test_whitespace_is_collapsed_by_default() -> None:
    document = parse_html(
        """
        <html>
            <p>
                Hello

                World
            </p>
        </html>
        """
    )

    assert (
        document.find(
            "p"
        ).text()
        == "Hello World"
    )


def test_custom_text_separator() -> None:
    document = parse_html(
        "<html><p>A</p><p>B</p></html>"
    )

    assert (
        document.text(
            separator="|"
        )
        == "A|B"
    )


def test_script_and_style_are_hidden_by_default() -> None:
    document = parse_html(
        """
        <html>
            <head>
                <style>body { color: red; }</style>
            </head>
            <body>
                <script>alert('x')</script>
                <p>Hello</p>
            </body>
        </html>
        """
    )

    text = document.text()

    assert (
        "Hello"
        in text
    )

    assert (
        "alert"
        not in text
    )

    assert (
        "color"
        not in text
    )


def test_hidden_text_can_be_included() -> None:
    document = parse_html(
        """
        <html>
            <body>
                <script>secret-script</script>
                <p>Hello</p>
            </body>
        </html>
        """
    )

    text = document.text(
        include_hidden=True
    )

    assert (
        "secret-script"
        in text
    )

    assert (
        "Hello"
        in text
    )


def test_config_can_enable_hidden_text_by_default() -> None:
    processor = HtmlProcessor(
        HtmlProcessorConfig(
            include_hidden_text=True
        )
    )

    document = processor.parse(
        """
        <html>
            <script>hidden</script>
            <p>visible</p>
        </html>
        """
    )

    assert (
        "hidden"
        in document.text()
    )


def test_extract_text_method() -> None:
    processor = HtmlProcessor()

    text = processor.extract_text(
        "<html><p>Hello</p><p>World</p></html>"
    )

    assert text == "Hello World"


# =============================================================================
# TITLE
# =============================================================================

def test_title_is_extracted() -> None:
    document = parse_html(
        SIMPLE_HTML
    )

    assert (
        document.title
        == "Enterprise Crawler"
    )


def test_title_returns_none_when_missing() -> None:
    document = parse_html(
        "<html><body>Hello</body></html>"
    )

    assert (
        document.title
        is None
    )


# =============================================================================
# LINKS
# =============================================================================

def test_links_are_extracted() -> None:
    document = parse_html(
        SIMPLE_HTML
    )

    links = document.links()

    assert len(links) == 1

    link = links[0]

    assert isinstance(
        link,
        HtmlLink,
    )

    assert (
        link.href
        == "/docs"
    )

    assert (
        link.text
        == "Documentation"
    )

    assert (
        link.title
        == "Docs"
    )

    assert link.rel == (
        "help",
        "external",
    )


def test_links_without_href_are_skipped_by_default() -> None:
    document = parse_html(
        "<html><a>No href</a></html>"
    )

    assert (
        document.links()
        == []
    )


def test_links_without_href_can_be_included() -> None:
    document = parse_html(
        "<html><a>No href</a></html>"
    )

    links = document.links(
        include_empty_href=True
    )

    assert len(links) == 1

    assert (
        links[0].href
        == ""
    )


def test_html_link_to_dict() -> None:
    link = HtmlLink(
        href="/docs",
        text="Docs",
        title="Documentation",
        rel=(
            "help",
        ),
    )

    assert link.to_dict() == {
        "href": "/docs",
        "text": "Docs",
        "title": "Documentation",
        "rel": [
            "help",
        ],
    }


# =============================================================================
# META
# =============================================================================

def test_meta_name_is_extracted() -> None:
    document = parse_html(
        SIMPLE_HTML
    )

    assert (
        document.meta(
            "description"
        )
        == "Crawler framework"
    )


def test_meta_property_is_supported() -> None:
    document = parse_html(
        """
        <html>
            <head>
                <meta
                    property="og:title"
                    content="Crawler"
                >
            </head>
        </html>
        """
    )

    assert (
        document.meta(
            "og:title"
        )
        == "Crawler"
    )


def test_meta_lookup_is_case_insensitive() -> None:
    document = parse_html(
        """
        <html>
            <head>
                <meta
                    name="Description"
                    content="Crawler"
                >
            </head>
        </html>
        """
    )

    assert (
        document.meta(
            "DESCRIPTION"
        )
        == "Crawler"
    )


def test_meta_returns_none_when_missing() -> None:
    document = parse_html(
        "<html></html>"
    )

    assert (
        document.meta(
            "description"
        )
        is None
    )


# =============================================================================
# COMMENTS / DECLARATIONS
# =============================================================================

def test_comments_are_ignored_by_default() -> None:
    document = parse_html(
        "<html><!-- secret --><p>Hello</p></html>"
    )

    assert (
        document.comments
        == ()
    )


def test_comments_can_be_collected() -> None:
    processor = HtmlProcessor(
        HtmlProcessorConfig(
            include_comments=True
        )
    )

    document = processor.parse(
        "<html><!-- hello --></html>"
    )

    assert (
        document.comments
        == (
            " hello ",
        )
    )


def test_doctype_is_observed() -> None:
    document = parse_html(
        "<!DOCTYPE html><html></html>"
    )

    assert (
        document.declarations
        == (
            "DOCTYPE html",
        )
    )


# =============================================================================
# MALFORMED REAL-WORLD HTML
# =============================================================================

def test_unclosed_tag_is_tolerated() -> None:
    document = parse_html(
        "<html><body><p>Hello"
    )

    paragraph = document.find(
        "p"
    )

    assert paragraph is not None

    assert (
        paragraph.text()
        == "Hello"
    )


def test_mismatched_end_tag_does_not_crash_parser() -> None:
    document = parse_html(
        "<html><body><div><p>Hello</div></body></html>"
    )

    paragraph = document.find(
        "p"
    )

    assert paragraph is not None

    assert (
        paragraph.text()
        == "Hello"
    )


# =============================================================================
# SERIALIZATION / DICT
# =============================================================================

def test_node_to_dict() -> None:
    document = parse_html(
        "<html><p id='x'>Hello</p></html>"
    )

    paragraph = document.find(
        "p"
    )

    assert paragraph is not None

    payload = paragraph.to_dict()

    assert (
        payload["tag"]
        == "p"
    )

    assert (
        payload["attributes"]["id"]
        == "x"
    )

    assert (
        payload["text_parts"]
        == [
            "Hello",
        ]
    )


def test_document_to_dict() -> None:
    document = parse_html(
        "<html><body><p>Hello</p></body></html>"
    )

    payload = (
        document.to_dict()
    )

    assert (
        payload["encoding"]
        == "utf-8"
    )

    assert (
        payload["node_count"]
        == 3
    )

    assert (
        payload["source_bytes"]
        > 0
    )


def test_node_count() -> None:
    document = parse_html(
        "<html><body><p>Hello</p></body></html>"
    )

    assert (
        document.node_count
        == 3
    )


# =============================================================================
# HELPERS
# =============================================================================

def test_parse_html_helper() -> None:
    document = parse_html(
        "<html><p>Hello</p></html>"
    )

    assert isinstance(
        document,
        HtmlDocument,
    )


def test_parse_html_file_helper(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "page.html"
    )

    path.write_text(
        "<html><p>Hello</p></html>",
        encoding="utf-8",
    )

    document = parse_html_file(
        path
    )

    assert (
        document.find(
            "p"
        ).text()
        == "Hello"
    )


def test_extract_html_text_helper() -> None:
    assert (
        extract_html_text(
            "<html><p>Hello</p><p>World</p></html>"
        )
        == "Hello World"
    )


# =============================================================================
# INVALID LOOKUPS
# =============================================================================

@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
    ],
)
def test_empty_class_name_is_rejected(
    value: str,
) -> None:
    node = HtmlNode(
        "div"
    )

    with pytest.raises(
        HtmlProcessingError
    ):
        node.has_class(
            value
        )


def test_invalid_attrs_filter_is_rejected() -> None:
    node = HtmlNode(
        "div"
    )

    with pytest.raises(
        HtmlProcessingError
    ):
        node.matches(
            attrs=[]  # type: ignore[arg-type]
        )


def test_invalid_text_separator_is_rejected() -> None:
    document = parse_html(
        "<html></html>"
    )

    with pytest.raises(
        HtmlProcessingError
    ):
        document.text(
            separator=1  # type: ignore[arg-type]
        )


def test_invalid_include_hidden_is_rejected() -> None:
    document = parse_html(
        "<html></html>"
    )

    with pytest.raises(
        HtmlProcessingError
    ):
        document.text(
            include_hidden="true"  # type: ignore[arg-type]
        )


def test_invalid_include_empty_href_is_rejected() -> None:
    document = parse_html(
        "<html></html>"
    )

    with pytest.raises(
        HtmlProcessingError
    ):
        document.links(
            include_empty_href=1  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "source",
    [
        None,
        123,
        1.5,
        True,
        {},
        [],
        object(),
    ],
)
def test_unsupported_source_type_is_rejected(
    source,
) -> None:
    with pytest.raises(
        HtmlProcessingError
    ):
        HtmlProcessor().parse(
            source
        )


# =============================================================================
# SNAPSHOT / REPR
# =============================================================================

def test_snapshot_contains_configuration() -> None:
    processor = HtmlProcessor(
        HtmlProcessorConfig(
            max_bytes=1024,
            include_comments=True,
        )
    )

    snapshot = (
        processor.snapshot()
    )

    assert (
        snapshot["processor"]
        == "HtmlProcessor"
    )

    assert (
        snapshot[
            "config"
        ][
            "max_bytes"
        ]
        == 1024
    )

    assert (
        snapshot[
            "config"
        ][
            "include_comments"
        ]
        is True
    )


def test_repr_contains_configuration() -> None:
    processor = HtmlProcessor(
        HtmlProcessorConfig(
            max_bytes=1024
        )
    )

    rendered = repr(
        processor
    )

    assert (
        "HtmlProcessor"
        in rendered
    )

    assert (
        "1024"
        in rendered
    )

    assert (
        "utf-8"
        in rendered
    )