from __future__ import annotations

from pathlib import Path

import pytest

from enterprise_crawler.processing.csv import (
    DEFAULT_CSV_DELIMITER,
    DEFAULT_CSV_ENCODING,
    CsvDocument,
    CsvProcessingError,
    CsvProcessor,
    CsvProcessorConfig,
    parse_csv,
    parse_csv_file,
    serialize_csv,
)


# =============================================================================
# CONFIG
# =============================================================================
def test_default_config_is_valid() -> None:
    config = CsvProcessorConfig()

    assert (
        config.encoding
        == DEFAULT_CSV_ENCODING
    )

    assert (
        config.delimiter
        == DEFAULT_CSV_DELIMITER
    )

    assert config.has_header is True


@pytest.mark.parametrize(
    "value",
    [
        0,
        -1,
        True,
        1.5,
    ],
)
def test_invalid_max_bytes_is_rejected(
    value,
) -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessorConfig(
            max_bytes=value
        )


def test_unlimited_max_bytes_is_allowed() -> None:
    config = CsvProcessorConfig(
        max_bytes=None
    )

    assert config.max_bytes is None


@pytest.mark.parametrize(
    "value",
    [
        "",
        "  ",
    ],
)
def test_empty_encoding_is_rejected(
    value: str,
) -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessorConfig(
            encoding=value
        )


def test_unknown_encoding_is_rejected() -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessorConfig(
            encoding=(
                "enterprise-crawler-unknown"
            )
        )


@pytest.mark.parametrize(
    "value",
    [
        "",
        "::",
    ],
)
def test_invalid_delimiter_length_is_rejected(
    value: str,
) -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessorConfig(
            delimiter=value
        )


def test_non_string_delimiter_is_rejected() -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessorConfig(
            delimiter=1
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("has_header", 1),
        (
            "reject_duplicate_headers",
            "true",
        ),
        (
            "reject_empty_headers",
            None,
        ),
        (
            "strict_row_length",
            1,
        ),
        (
            "skip_blank_rows",
            "yes",
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
        CsvProcessingError
    ):
        CsvProcessorConfig(
            **kwargs
        )


def test_config_to_dict() -> None:
    config = CsvProcessorConfig(
        delimiter=";",
        max_bytes=1000,
    )

    payload = config.to_dict()

    assert payload["delimiter"] == ";"
    assert payload["max_bytes"] == 1000
    assert payload["has_header"] is True


def test_invalid_processor_config_type_is_rejected() -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessor(
            config={}
        )


# =============================================================================
# BASIC PARSING
# =============================================================================
def test_parse_csv_from_string() -> None:
    processor = CsvProcessor()

    document = processor.parse(
        "name,age\nAda,36\nAlan,41\n"
    )

    assert document.headers == [
        "name",
        "age",
    ]

    assert document.rows == [
        ["Ada", "36"],
        ["Alan", "41"],
    ]


def test_parse_csv_from_bytes() -> None:
    processor = CsvProcessor()

    document = processor.parse(
        b"name,age\nAda,36\n"
    )

    assert document.row_count == 1


def test_parse_csv_from_bytearray() -> None:
    processor = CsvProcessor()

    document = processor.parse(
        bytearray(
            b"name,age\nAda,36\n"
        )
    )

    assert document.row_count == 1


def test_parse_csv_from_memoryview() -> None:
    processor = CsvProcessor()

    document = processor.parse(
        memoryview(
            b"name,age\nAda,36\n"
        )
    )

    assert document.row_count == 1


def test_semicolon_delimiter() -> None:
    processor = CsvProcessor(
        CsvProcessorConfig(
            delimiter=";"
        )
    )

    document = processor.parse(
        "name;age\nAda;36\n"
    )

    assert document.headers == [
        "name",
        "age",
    ]


def test_tab_delimiter() -> None:
    processor = CsvProcessor(
        CsvProcessorConfig(
            delimiter="\t"
        )
    )

    document = processor.parse(
        "name\tage\nAda\t36\n"
    )

    assert document.rows == [
        ["Ada", "36"]
    ]


def test_unicode_csv_is_preserved() -> None:
    document = CsvProcessor().parse(
        "şehir,ilçe\nİstanbul,Beyoğlu\n"
    )

    assert document.rows == [
        ["İstanbul", "Beyoğlu"]
    ]


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
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessor().parse(
            payload
        )


def test_empty_bytes_payload_is_rejected() -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessor().parse(
            b""
        )


def test_invalid_utf8_is_rejected() -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessor().parse(
            b"\xff\xfe"
        )


# =============================================================================
# FILE
# =============================================================================
def test_parse_csv_file(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "data.csv"
    )

    path.write_text(
        "name,age\nAda,36\n",
        encoding="utf-8",
    )

    document = CsvProcessor().parse(
        path
    )

    assert document.row_count == 1


def test_parse_file_method(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "data.csv"
    )

    path.write_text(
        "a,b\n1,2\n",
        encoding="utf-8",
    )

    document = (
        CsvProcessor().parse_file(
            path
        )
    )

    assert document.headers == [
        "a",
        "b",
    ]


def test_missing_csv_file_is_rejected(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessor().parse_file(
            tmp_path
            / "missing.csv"
        )


def test_directory_is_rejected_as_csv_file(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessor().parse_file(
            tmp_path
        )


def test_parse_file_requires_path_object() -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessor().parse_file(
            "data.csv"
        )


def test_string_is_not_implicitly_treated_as_path() -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessor().parse(
            "data.csv"
        )


# =============================================================================
# SIZE LIMIT
# =============================================================================
def test_text_over_size_limit_is_rejected() -> None:
    processor = CsvProcessor(
        CsvProcessorConfig(
            max_bytes=8
        )
    )

    with pytest.raises(
        CsvProcessingError
    ):
        processor.parse(
            "name,age\nAda,36\n"
        )


def test_text_exactly_at_size_limit_is_allowed() -> None:
    payload = "a,b\n1,2\n"

    processor = CsvProcessor(
        CsvProcessorConfig(
            max_bytes=len(
                payload.encode(
                    "utf-8"
                )
            )
        )
    )

    document = processor.parse(
        payload
    )

    assert document.row_count == 1


def test_bytes_over_size_limit_is_rejected() -> None:
    processor = CsvProcessor(
        CsvProcessorConfig(
            max_bytes=4
        )
    )

    with pytest.raises(
        CsvProcessingError
    ):
        processor.parse(
            b"a,b\n1,2\n"
        )


def test_file_over_size_limit_is_rejected(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "large.csv"
    )

    path.write_text(
        "a,b\n1,2\n",
        encoding="utf-8",
    )

    processor = CsvProcessor(
        CsvProcessorConfig(
            max_bytes=4
        )
    )

    with pytest.raises(
        CsvProcessingError
    ):
        processor.parse_file(
            path
        )


def test_size_limit_can_be_disabled() -> None:
    processor = CsvProcessor(
        CsvProcessorConfig(
            max_bytes=None
        )
    )

    document = processor.parse(
        "a,b\n"
        + ("x,y\n" * 1000)
    )

    assert document.row_count == 1000


# =============================================================================
# HEADER VALIDATION
# =============================================================================
def test_duplicate_headers_are_rejected_by_default() -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessor().parse(
            "name,name\nAda,Lovelace\n"
        )


def test_duplicate_headers_can_be_allowed() -> None:
    processor = CsvProcessor(
        CsvProcessorConfig(
            reject_duplicate_headers=False
        )
    )

    document = processor.parse(
        "name,name\nAda,Lovelace\n"
    )

    assert document.headers == [
        "name",
        "name",
    ]


def test_empty_header_is_rejected_by_default() -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessor().parse(
            "name,,age\nAda,X,36\n"
        )


def test_empty_header_can_be_allowed() -> None:
    processor = CsvProcessor(
        CsvProcessorConfig(
            reject_empty_headers=False
        )
    )

    document = processor.parse(
        "name,,age\nAda,X,36\n"
    )

    assert document.headers[1] == ""


# =============================================================================
# ROW VALIDATION
# =============================================================================
def test_short_row_is_rejected_by_default() -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessor().parse(
            "a,b,c\n1,2\n"
        )


def test_long_row_is_rejected_by_default() -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessor().parse(
            "a,b\n1,2,3\n"
        )


def test_variable_row_lengths_can_be_allowed() -> None:
    processor = CsvProcessor(
        CsvProcessorConfig(
            strict_row_length=False
        )
    )

    document = processor.parse(
        "a,b\n1\n2,3,4\n"
    )

    assert document.row_count == 2


def test_blank_rows_are_skipped_by_default() -> None:
    document = CsvProcessor().parse(
        "a,b\n\n1,2\n\n3,4\n"
    )

    assert document.row_count == 2


def test_blank_rows_can_be_preserved() -> None:
    processor = CsvProcessor(
        CsvProcessorConfig(
            skip_blank_rows=False,
            strict_row_length=False,
        )
    )

    document = processor.parse(
        "a,b\n\n1,2\n"
    )

    assert document.row_count == 2


# =============================================================================
# HEADERLESS CSV
# =============================================================================
def test_headerless_csv() -> None:
    processor = CsvProcessor(
        CsvProcessorConfig(
            has_header=False
        )
    )

    document = processor.parse(
        "Ada,36\nAlan,41\n"
    )

    assert document.headers == []

    assert document.rows == [
        ["Ada", "36"],
        ["Alan", "41"],
    ]


def test_headerless_csv_requires_consistent_rows_by_default() -> None:
    processor = CsvProcessor(
        CsvProcessorConfig(
            has_header=False
        )
    )

    with pytest.raises(
        CsvProcessingError
    ):
        processor.parse(
            "1,2\n3\n"
        )


# =============================================================================
# DOCUMENT API
# =============================================================================
def test_document_row_count() -> None:
    document = CsvDocument(
        headers=["a"],
        rows=[
            ["1"],
            ["2"],
        ],
    )

    assert document.row_count == 2


def test_document_column_count_from_headers() -> None:
    document = CsvDocument(
        headers=[
            "a",
            "b",
        ],
        rows=[],
    )

    assert document.column_count == 2


def test_document_column_count_without_headers() -> None:
    document = CsvDocument(
        headers=[],
        rows=[
            ["1", "2", "3"]
        ],
    )

    assert document.column_count == 3


def test_document_to_rows_returns_copy() -> None:
    document = CsvDocument(
        headers=["a"],
        rows=[["1"]],
    )

    rows = document.to_rows()

    rows[0][0] = "changed"

    assert document.rows == [
        ["1"]
    ]


def test_document_to_dicts() -> None:
    document = CsvProcessor().parse(
        "name,age\nAda,36\n"
    )

    assert document.to_dicts() == [
        {
            "name": "Ada",
            "age": "36",
        }
    ]


def test_headerless_document_cannot_convert_to_dicts() -> None:
    document = CsvDocument(
        headers=[],
        rows=[["1"]],
    )

    with pytest.raises(
        CsvProcessingError
    ):
        document.to_dicts()


def test_document_column() -> None:
    document = CsvProcessor().parse(
        "name,age\nAda,36\nAlan,41\n"
    )

    assert document.column(
        "name"
    ) == [
        "Ada",
        "Alan",
    ]


def test_unknown_document_column_is_rejected() -> None:
    document = CsvProcessor().parse(
        "name,age\nAda,36\n"
    )

    with pytest.raises(
        CsvProcessingError
    ):
        document.column(
            "missing"
        )


def test_empty_document_column_name_is_rejected() -> None:
    document = CsvProcessor().parse(
        "name\nAda\n"
    )

    with pytest.raises(
        CsvProcessingError
    ):
        document.column(
            ""
        )


def test_document_to_dict() -> None:
    document = CsvProcessor().parse(
        "a,b\n1,2\n"
    )

    payload = document.to_dict()

    assert payload["headers"] == [
        "a",
        "b",
    ]

    assert payload["row_count"] == 1
    assert payload["column_count"] == 2


# =============================================================================
# FROM DICTS
# =============================================================================
def test_from_dicts() -> None:
    document = (
        CsvProcessor().from_dicts(
            [
                {
                    "name": "Ada",
                    "age": 36,
                },
                {
                    "name": "Alan",
                    "age": 41,
                },
            ]
        )
    )

    assert document.headers == [
        "name",
        "age",
    ]

    assert document.rows == [
        ["Ada", "36"],
        ["Alan", "41"],
    ]


def test_from_dicts_preserves_explicit_header_order() -> None:
    document = (
        CsvProcessor().from_dicts(
            [
                {
                    "name": "Ada",
                    "age": 36,
                }
            ],
            headers=[
                "age",
                "name",
            ],
        )
    )

    assert document.rows == [
        ["36", "Ada"]
    ]


def test_from_dicts_missing_value_becomes_empty_string() -> None:
    document = (
        CsvProcessor().from_dicts(
            [
                {
                    "name": "Ada"
                }
            ],
            headers=[
                "name",
                "age",
            ],
        )
    )

    assert document.rows == [
        ["Ada", ""]
    ]


def test_from_dicts_none_becomes_empty_string() -> None:
    document = (
        CsvProcessor().from_dicts(
            [
                {
                    "name": None
                }
            ]
        )
    )

    assert document.rows == [
        [""]
    ]


def test_from_dicts_unknown_field_is_rejected() -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessor().from_dicts(
            [
                {
                    "a": 1,
                    "b": 2,
                }
            ],
            headers=["a"],
        )


def test_from_dicts_non_mapping_row_is_rejected() -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessor().from_dicts(
            [
                ["a", "b"]
            ]
        )


def test_from_dicts_non_string_key_is_rejected() -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessor().from_dicts(
            [
                {
                    1: "value"
                }
            ]
        )


def test_from_dicts_complex_value_is_rejected() -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessor().from_dicts(
            [
                {
                    "value": {
                        "nested": True
                    }
                }
            ]
        )


def test_from_empty_dict_sequence() -> None:
    document = (
        CsvProcessor().from_dicts(
            []
        )
    )

    assert document.headers == []
    assert document.rows == []


# =============================================================================
# SERIALIZATION
# =============================================================================
def test_serialize_csv() -> None:
    document = CsvDocument(
        headers=[
            "name",
            "age",
        ],
        rows=[
            ["Ada", "36"]
        ],
    )

    rendered = (
        CsvProcessor().serialize(
            document
        )
    )

    assert rendered == (
        "name,age\n"
        "Ada,36\n"
    )


def test_serialization_quotes_delimiter() -> None:
    document = CsvDocument(
        headers=["value"],
        rows=[
            ["hello,world"]
        ],
    )

    rendered = (
        CsvProcessor().serialize(
            document
        )
    )

    assert (
        '"hello,world"'
        in rendered
    )


def test_serialization_custom_delimiter() -> None:
    processor = CsvProcessor(
        CsvProcessorConfig(
            delimiter=";"
        )
    )

    document = CsvDocument(
        headers=["a", "b"],
        rows=[["1", "2"]],
    )

    assert (
        processor.serialize(
            document
        )
        == "a;b\n1;2\n"
    )


def test_serialize_bytes() -> None:
    document = CsvDocument(
        headers=["a"],
        rows=[["1"]],
    )

    rendered = (
        CsvProcessor().serialize_bytes(
            document
        )
    )

    assert rendered == (
        b"a\n1\n"
    )


def test_serialized_output_respects_size_limit() -> None:
    processor = CsvProcessor(
        CsvProcessorConfig(
            max_bytes=4
        )
    )

    document = CsvDocument(
        headers=["value"],
        rows=[["12345"]],
    )

    with pytest.raises(
        CsvProcessingError
    ):
        processor.serialize(
            document
        )


def test_serialize_rejects_non_document() -> None:
    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessor().serialize(
            {}
        )


@pytest.mark.parametrize(
    "value",
    [
        "",
        123,
        None,
    ],
)
def test_invalid_line_terminator_is_rejected(
    value,
) -> None:
    document = CsvDocument(
        headers=["a"],
        rows=[],
    )

    with pytest.raises(
        CsvProcessingError
    ):
        CsvProcessor().serialize(
            document,
            line_terminator=value,
        )


# =============================================================================
# HELPERS
# =============================================================================
def test_parse_csv_helper() -> None:
    document = parse_csv(
        "a,b\n1,2\n"
    )

    assert document.row_count == 1


def test_parse_csv_file_helper(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "data.csv"
    )

    path.write_text(
        "a,b\n1,2\n",
        encoding="utf-8",
    )

    document = parse_csv_file(
        path
    )

    assert document.row_count == 1


def test_serialize_csv_helper() -> None:
    document = CsvDocument(
        headers=["a"],
        rows=[["1"]],
    )

    assert serialize_csv(
        document
    ) == "a\n1\n"


# =============================================================================
# UNSUPPORTED TYPES
# =============================================================================
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
        CsvProcessingError
    ):
        CsvProcessor().parse(
            source
        )


# =============================================================================
# SNAPSHOT / REPR
# =============================================================================
def test_snapshot_contains_configuration() -> None:
    processor = CsvProcessor(
        CsvProcessorConfig(
            delimiter=";"
        )
    )

    snapshot = (
        processor.snapshot()
    )

    assert snapshot[
        "processor"
    ] == "CsvProcessor"

    assert snapshot[
        "config"
    ][
        "delimiter"
    ] == ";"


def test_repr_contains_configuration() -> None:
    processor = CsvProcessor(
        CsvProcessorConfig(
            delimiter=";"
        )
    )

    rendered = repr(
        processor
    )

    assert "CsvProcessor" in rendered
    assert "delimiter=';'" in rendered