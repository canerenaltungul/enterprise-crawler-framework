from __future__ import annotations

from pathlib import Path
import xml.etree.ElementTree as ET

import pytest

from enterprise_crawler.processing.xml import (
    DEFAULT_MAX_XML_BYTES,
    XmlProcessingError,
    XmlProcessor,
    XmlProcessorConfig,
    parse_xml,
    parse_xml_file,
    serialize_xml,
)


# =============================================================================
# CONFIG
# =============================================================================
def test_default_config_is_valid() -> None:
    config = XmlProcessorConfig()

    assert (
        config.max_bytes
        == DEFAULT_MAX_XML_BYTES
    )

    assert (
        config.encoding
        == "utf-8"
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
    value,
) -> None:
    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessorConfig(
            max_bytes=value
        )


def test_unlimited_max_bytes_is_allowed() -> None:
    config = XmlProcessorConfig(
        max_bytes=None
    )

    assert (
        config.max_bytes
        is None
    )


@pytest.mark.parametrize(
    "encoding",
    [
        "",
        " ",
    ],
)
def test_empty_encoding_is_rejected(
    encoding: str,
) -> None:
    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessorConfig(
            encoding=encoding
        )


@pytest.mark.parametrize(
    (
        "field_name",
        "value",
    ),
    [
        (
            "reject_doctype",
            1,
        ),
        (
            "reject_entities",
            "true",
        ),
    ],
)
def test_invalid_security_flags_are_rejected(
    field_name: str,
    value,
) -> None:
    kwargs = {
        field_name: value
    }

    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessorConfig(
            **kwargs
        )


def test_config_to_dict() -> None:
    config = XmlProcessorConfig(
        max_bytes=1024,
        reject_doctype=True,
        reject_entities=True,
    )

    rendered = config.to_dict()

    assert (
        rendered["max_bytes"]
        == 1024
    )

    assert (
        rendered["encoding"]
        == "utf-8"
    )


def test_invalid_processor_config_type_is_rejected() -> None:
    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor(
            config="invalid"
        )


# =============================================================================
# BASIC PARSING
# =============================================================================
def test_parse_xml_from_string() -> None:
    processor = XmlProcessor()

    root = processor.parse(
        "<root><item>hello</item></root>"
    )

    assert root.tag == "root"

    assert (
        root.find("item").text
        == "hello"
    )


def test_parse_xml_from_bytes() -> None:
    processor = XmlProcessor()

    root = processor.parse(
        b"<root><item>hello</item></root>"
    )

    assert root.tag == "root"


def test_parse_xml_from_bytearray() -> None:
    processor = XmlProcessor()

    root = processor.parse(
        bytearray(
            b"<root><item>hello</item></root>"
        )
    )

    assert root.tag == "root"


def test_parse_xml_from_memoryview() -> None:
    processor = XmlProcessor()

    root = processor.parse(
        memoryview(
            b"<root><item>hello</item></root>"
        )
    )

    assert root.tag == "root"


def test_unicode_xml_is_preserved() -> None:
    processor = XmlProcessor()

    root = processor.parse(
        "<root><city>İstanbul</city></root>"
    )

    assert (
        root.find("city").text
        == "İstanbul"
    )


@pytest.mark.parametrize(
    "payload",
    [
        "",
        " ",
        "\n\t",
    ],
)
def test_empty_text_payload_is_rejected(
    payload: str,
) -> None:
    processor = XmlProcessor()

    with pytest.raises(
        XmlProcessingError
    ):
        processor.parse(
            payload
        )


def test_empty_bytes_payload_is_rejected() -> None:
    processor = XmlProcessor()

    with pytest.raises(
        XmlProcessingError
    ):
        processor.parse(
            b""
        )


@pytest.mark.parametrize(
    "payload",
    [
        "<root>",
        "<root></wrong>",
        "<root><item></root>",
        "not xml",
    ],
)
def test_malformed_xml_is_rejected(
    payload: str,
) -> None:
    processor = XmlProcessor()

    with pytest.raises(
        XmlProcessingError
    ):
        processor.parse(
            payload
        )


# =============================================================================
# FILE PARSING
# =============================================================================
def test_parse_xml_file(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "sample.xml"
    )

    path.write_text(
        "<root><item>hello</item></root>",
        encoding="utf-8",
    )

    processor = XmlProcessor()

    root = processor.parse(
        path
    )

    assert root.tag == "root"


def test_parse_file_method(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "sample.xml"
    )

    path.write_text(
        "<root />",
        encoding="utf-8",
    )

    root = (
        XmlProcessor()
        .parse_file(
            path
        )
    )

    assert root.tag == "root"


def test_missing_xml_file_is_rejected(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "missing.xml"
    )

    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor().parse(
            path
        )


def test_directory_is_rejected_as_xml_file(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor().parse(
            tmp_path
        )


def test_parse_file_requires_path_object() -> None:
    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor().parse_file(
            "sample.xml"
        )


def test_string_is_not_implicitly_treated_as_path(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "sample.xml"
    )

    path.write_text(
        "<root />",
        encoding="utf-8",
    )

    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor().parse(
            str(path)
        )


# =============================================================================
# SIZE LIMIT
# =============================================================================
def test_text_over_size_limit_is_rejected() -> None:
    processor = XmlProcessor(
        XmlProcessorConfig(
            max_bytes=7
        )
    )

    with pytest.raises(
        XmlProcessingError
    ):
        processor.parse(
            "<root />"
        )


def test_text_exactly_at_size_limit_is_allowed() -> None:
    processor = XmlProcessor(
        XmlProcessorConfig(
            max_bytes=8
        )
    )

    root = processor.parse(
        "<root />"
    )

    assert root.tag == "root"


def test_bytes_over_size_limit_is_rejected() -> None:
    processor = XmlProcessor(
        XmlProcessorConfig(
            max_bytes=5
        )
    )

    with pytest.raises(
        XmlProcessingError
    ):
        processor.parse(
            b"<root />"
        )


def test_file_over_size_limit_is_rejected(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "sample.xml"
    )

    path.write_text(
        "<root><item>hello</item></root>",
        encoding="utf-8",
    )

    processor = XmlProcessor(
        XmlProcessorConfig(
            max_bytes=8
        )
    )

    with pytest.raises(
        XmlProcessingError
    ):
        processor.parse(
            path
        )


def test_size_limit_can_be_disabled() -> None:
    processor = XmlProcessor(
        XmlProcessorConfig(
            max_bytes=None
        )
    )

    payload = (
        "<root>"
        + ("x" * 100_000)
        + "</root>"
    )

    root = processor.parse(
        payload
    )

    assert root.tag == "root"


# =============================================================================
# SECURITY
# =============================================================================
def test_doctype_is_rejected() -> None:
    payload = """
    <!DOCTYPE root>
    <root />
    """

    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor().parse(
            payload
        )


def test_doctype_detection_is_case_insensitive() -> None:
    payload = """
    <!doctype root>
    <root />
    """

    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor().parse(
            payload
        )


def test_entity_declaration_is_rejected() -> None:
    payload = """
    <!DOCTYPE root [
        <!ENTITY test "hello">
    ]>
    <root>&test;</root>
    """

    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor().parse(
            payload
        )


def test_external_entity_payload_is_rejected() -> None:
    payload = """
    <!DOCTYPE root [
        <!ENTITY xxe SYSTEM "file:///etc/passwd">
    ]>
    <root>&xxe;</root>
    """

    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor().parse(
            payload
        )


def test_entity_expansion_payload_is_rejected() -> None:
    payload = """
    <!DOCTYPE lolz [
        <!ENTITY lol "lol">
        <!ENTITY lol2 "&lol;&lol;&lol;&lol;">
    ]>
    <lolz>&lol2;</lolz>
    """

    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor().parse(
            payload
        )


def test_doctype_can_be_explicitly_allowed() -> None:
    processor = XmlProcessor(
        XmlProcessorConfig(
            reject_doctype=False,
            reject_entities=True,
        )
    )

    root = processor.parse(
        """
        <!DOCTYPE root>
        <root />
        """
    )

    assert root.tag == "root"


# =============================================================================
# ROOT CONTRACT
# =============================================================================
def test_require_root_accepts_expected_root() -> None:
    root = ET.fromstring(
        "<root />"
    )

    result = (
        XmlProcessor.require_root(
            root,
            "root",
        )
    )

    assert result is root


def test_require_root_rejects_unexpected_root() -> None:
    root = ET.fromstring(
        "<root />"
    )

    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor.require_root(
            root,
            "other",
        )


def test_require_root_rejects_empty_expected_tag() -> None:
    root = ET.fromstring(
        "<root />"
    )

    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor.require_root(
            root,
            "",
        )


def test_require_root_rejects_non_element() -> None:
    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor.require_root(
            "root",
            "root",
        )


# =============================================================================
# NAMESPACES
# =============================================================================
def test_local_name_without_namespace() -> None:
    assert (
        XmlProcessor.local_name(
            "item"
        )
        == "item"
    )


def test_local_name_with_namespace() -> None:
    assert (
        XmlProcessor.local_name(
            "{https://example.com/ns}item"
        )
        == "item"
    )


def test_namespace_uri_without_namespace() -> None:
    assert (
        XmlProcessor.namespace_uri(
            "item"
        )
        is None
    )


def test_namespace_uri_with_namespace() -> None:
    assert (
        XmlProcessor.namespace_uri(
            "{https://example.com/ns}item"
        )
        == "https://example.com/ns"
    )


def test_qualified_name() -> None:
    result = (
        XmlProcessor.qualified_name(
            "https://example.com/ns",
            "item",
        )
    )

    assert result == (
        "{https://example.com/ns}item"
    )


def test_qualified_name_rejects_empty_namespace() -> None:
    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor.qualified_name(
            "",
            "item",
        )


def test_qualified_name_rejects_empty_local_name() -> None:
    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor.qualified_name(
            "https://example.com/ns",
            "",
        )


def test_namespace_find() -> None:
    root = XmlProcessor().parse(
        """
        <root xmlns:x="https://example.com/ns">
            <x:item>hello</x:item>
        </root>
        """
    )

    element = XmlProcessor.find(
        root,
        "x:item",
        namespaces={
            "x": (
                "https://example.com/ns"
            )
        },
    )

    assert element is not None

    assert (
        element.text.strip()
        == "hello"
    )


# =============================================================================
# LOOKUP
# =============================================================================
def test_find_returns_element() -> None:
    root = XmlProcessor().parse(
        "<root><item>hello</item></root>"
    )

    item = XmlProcessor.find(
        root,
        "item",
    )

    assert item is not None

    assert item.text == "hello"


def test_find_returns_none_when_missing() -> None:
    root = XmlProcessor().parse(
        "<root />"
    )

    assert (
        XmlProcessor.find(
            root,
            "missing",
        )
        is None
    )


def test_find_rejects_empty_path() -> None:
    root = XmlProcessor().parse(
        "<root />"
    )

    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor.find(
            root,
            "",
        )


def test_findall_returns_elements() -> None:
    root = XmlProcessor().parse(
        """
        <root>
            <item>1</item>
            <item>2</item>
            <item>3</item>
        </root>
        """
    )

    elements = (
        XmlProcessor.findall(
            root,
            "item",
        )
    )

    assert len(elements) == 3


def test_findall_returns_empty_list_when_missing() -> None:
    root = XmlProcessor().parse(
        "<root />"
    )

    assert (
        XmlProcessor.findall(
            root,
            "item",
        )
        == []
    )


def test_findall_rejects_empty_path() -> None:
    root = XmlProcessor().parse(
        "<root />"
    )

    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor.findall(
            root,
            "",
        )


def test_find_text_returns_text() -> None:
    root = XmlProcessor().parse(
        "<root><item> hello </item></root>"
    )

    assert (
        XmlProcessor.find_text(
            root,
            "item",
        )
        == "hello"
    )


def test_find_text_can_preserve_whitespace() -> None:
    root = XmlProcessor().parse(
        "<root><item> hello </item></root>"
    )

    assert (
        XmlProcessor.find_text(
            root,
            "item",
            strip=False,
        )
        == " hello "
    )


def test_find_text_returns_default_when_missing() -> None:
    root = XmlProcessor().parse(
        "<root />"
    )

    assert (
        XmlProcessor.find_text(
            root,
            "missing",
            default="fallback",
        )
        == "fallback"
    )


def test_invalid_strip_flag_is_rejected() -> None:
    root = XmlProcessor().parse(
        "<root />"
    )

    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor.find_text(
            root,
            "item",
            strip="yes",
        )


def test_lookup_rejects_non_element() -> None:
    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor.find(
            "not-element",
            "item",
        )


# =============================================================================
# SERIALIZATION
# =============================================================================
def test_serialize_xml() -> None:
    root = ET.Element(
        "root"
    )

    child = ET.SubElement(
        root,
        "item",
    )

    child.text = "hello"

    result = (
        XmlProcessor()
        .serialize(
            root
        )
    )

    assert isinstance(
        result,
        str,
    )

    assert (
        "<item>hello</item>"
        in result
    )


def test_serialize_bytes() -> None:
    root = ET.Element(
        "root"
    )

    result = (
        XmlProcessor()
        .serialize_bytes(
            root
        )
    )

    assert isinstance(
        result,
        bytes,
    )

    assert (
        b"<root"
        in result
    )


def test_serialization_can_include_xml_declaration() -> None:
    root = ET.Element(
        "root"
    )

    result = (
        XmlProcessor()
        .serialize_bytes(
            root,
            xml_declaration=True,
        )
    )

    assert result.startswith(
        b"<?xml"
    )


def test_unicode_serialization_is_preserved() -> None:
    root = ET.Element(
        "root"
    )

    root.text = "İstanbul"

    result = (
        XmlProcessor()
        .serialize(
            root
        )
    )

    assert isinstance(
        result,
        str,
    )

    assert (
        "İstanbul"
        in result
    )


def test_serialized_output_respects_size_limit() -> None:
    processor = XmlProcessor(
        XmlProcessorConfig(
            max_bytes=10
        )
    )

    root = ET.Element(
        "root"
    )

    root.text = (
        "x" * 100
    )

    with pytest.raises(
        XmlProcessingError
    ):
        processor.serialize(
            root
        )


def test_serialize_rejects_non_element() -> None:
    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor().serialize(
            "root"
        )


def test_invalid_serialization_encoding_is_rejected() -> None:
    root = ET.Element(
        "root"
    )

    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor().serialize(
            root,
            encoding="",
        )


def test_invalid_xml_declaration_flag_is_rejected() -> None:
    root = ET.Element(
        "root"
    )

    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor().serialize(
            root,
            xml_declaration="yes",
        )


def test_invalid_short_empty_elements_flag_is_rejected() -> None:
    root = ET.Element(
        "root"
    )

    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor().serialize(
            root,
            short_empty_elements="yes",
        )


# =============================================================================
# HELPERS
# =============================================================================
def test_parse_xml_helper() -> None:
    root = parse_xml(
        "<root />"
    )

    assert root.tag == "root"


def test_parse_xml_file_helper(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "sample.xml"
    )

    path.write_text(
        "<root />",
        encoding="utf-8",
    )

    root = parse_xml_file(
        path
    )

    assert root.tag == "root"


def test_serialize_xml_helper() -> None:
    root = ET.Element(
        "root"
    )

    result = serialize_xml(
        root
    )

    assert isinstance(
        result,
        str,
    )

    assert (
        "<root"
        in result
    )


# =============================================================================
# UNSUPPORTED SOURCE TYPES
# =============================================================================
@pytest.mark.parametrize(
    "source",
    [
        None,
        123,
        1.5,
        True,
        [],
        {},
        object(),
    ],
)
def test_unsupported_source_type_is_rejected(
    source,
) -> None:
    with pytest.raises(
        XmlProcessingError
    ):
        XmlProcessor().parse(
            source
        )


# =============================================================================
# SNAPSHOT / REPR
# =============================================================================
def test_snapshot_contains_configuration() -> None:
    processor = XmlProcessor(
        XmlProcessorConfig(
            max_bytes=2048
        )
    )

    snapshot = (
        processor.snapshot()
    )

    assert (
        snapshot["processor"]
        == "XmlProcessor"
    )

    assert (
        snapshot["config"][
            "max_bytes"
        ]
        == 2048
    )

    assert (
        snapshot["config"][
            "reject_doctype"
        ]
        is True
    )


def test_repr_contains_configuration() -> None:
    processor = XmlProcessor(
        XmlProcessorConfig(
            max_bytes=4096
        )
    )

    rendered = repr(
        processor
    )

    assert (
        "XmlProcessor"
        in rendered
    )

    assert (
        "4096"
        in rendered
    )

    assert (
        "reject_doctype"
        in rendered
    )