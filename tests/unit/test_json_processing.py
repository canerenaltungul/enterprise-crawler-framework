from __future__ import annotations

from pathlib import Path

import pytest

from enterprise_crawler.exceptions import (
    ContractValidationError,
)
from enterprise_crawler.processing.json import (
    DEFAULT_JSON_ENCODING,
    DEFAULT_MAX_JSON_BYTES,
    JsonProcessingError,
    JsonProcessor,
    JsonProcessorConfig,
    parse_json,
    parse_json_array,
    parse_json_object,
    serialize_json,
)


# =============================================================================
# CONFIG
# =============================================================================
def test_default_config_is_valid() -> None:
    config = JsonProcessorConfig()

    assert (
        config.encoding
        == DEFAULT_JSON_ENCODING
    )

    assert (
        config.max_bytes
        == DEFAULT_MAX_JSON_BYTES
    )

    assert (
        config.reject_duplicate_keys
        is True
    )

    assert (
        config.allow_scalar_root
        is True
    )


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
        ContractValidationError
    ):
        JsonProcessorConfig(
            max_bytes=value
        )


def test_unlimited_max_bytes_is_allowed() -> None:
    config = JsonProcessorConfig(
        max_bytes=None
    )

    assert config.max_bytes is None


def test_empty_encoding_is_rejected() -> None:
    with pytest.raises(
        ContractValidationError
    ):
        JsonProcessorConfig(
            encoding=" "
        )


def test_invalid_duplicate_policy_is_rejected() -> None:
    with pytest.raises(
        ContractValidationError
    ):
        JsonProcessorConfig(
            reject_duplicate_keys=1
        )


def test_invalid_scalar_policy_is_rejected() -> None:
    with pytest.raises(
        ContractValidationError
    ):
        JsonProcessorConfig(
            allow_scalar_root=1
        )


def test_invalid_processor_config_type_is_rejected() -> None:
    with pytest.raises(
        ContractValidationError
    ):
        JsonProcessor(
            config={}
        )


# =============================================================================
# STRING PARSING
# =============================================================================
def test_parse_object_from_string() -> None:
    processor = JsonProcessor()

    result = processor.parse(
        '{"name":"crawler","ok":true}'
    )

    assert result == {
        "name": "crawler",
        "ok": True,
    }


def test_parse_array_from_string() -> None:
    processor = JsonProcessor()

    result = processor.parse(
        '[1,2,3]'
    )

    assert result == [
        1,
        2,
        3,
    ]


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ("null", None),
        ("true", True),
        ("false", False),
        ('"hello"', "hello"),
        ("123", 123),
        ("1.5", 1.5),
    ],
)
def test_scalar_roots_are_supported_by_default(
    payload,
    expected,
) -> None:
    processor = JsonProcessor()

    assert (
        processor.parse(payload)
        == expected
    )


def test_scalar_root_can_be_disabled() -> None:
    processor = JsonProcessor(
        config=JsonProcessorConfig(
            allow_scalar_root=False
        )
    )

    with pytest.raises(
        JsonProcessingError
    ):
        processor.parse(
            '"hello"'
        )


@pytest.mark.parametrize(
    "payload",
    [
        "",
        " ",
        "\n\t",
    ],
)
def test_empty_payload_is_rejected(
    payload,
) -> None:
    processor = JsonProcessor()

    with pytest.raises(
        JsonProcessingError
    ):
        processor.parse(
            payload
        )


@pytest.mark.parametrize(
    "payload",
    [
        "{",
        '{"a":}',
        "[1,",
        "hello",
    ],
)
def test_malformed_json_is_rejected(
    payload,
) -> None:
    processor = JsonProcessor()

    with pytest.raises(
        JsonProcessingError
    ):
        processor.parse(
            payload
        )


# =============================================================================
# BYTES
# =============================================================================
def test_parse_bytes() -> None:
    processor = JsonProcessor()

    result = processor.parse(
        b'{"ok":true}'
    )

    assert result == {
        "ok": True
    }


def test_parse_bytearray() -> None:
    processor = JsonProcessor()

    result = processor.parse(
        bytearray(
            b'{"value":5}'
        )
    )

    assert result == {
        "value": 5
    }


def test_parse_memoryview() -> None:
    processor = JsonProcessor()

    result = processor.parse(
        memoryview(
            b'[1,2]'
        )
    )

    assert result == [
        1,
        2,
    ]


def test_invalid_utf8_is_rejected() -> None:
    processor = JsonProcessor()

    with pytest.raises(
        JsonProcessingError
    ):
        processor.parse(
            b"\xff\xfe\xfa"
        )


def test_unicode_payload_is_preserved() -> None:
    processor = JsonProcessor()

    payload = (
        '{"city":"İstanbul",'
        '"district":"Beyoğlu"}'
    )

    result = processor.parse(
        payload.encode("utf-8")
    )

    assert result == {
        "city": "İstanbul",
        "district": "Beyoğlu",
    }


# =============================================================================
# PATH
# =============================================================================
def test_parse_json_file(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "payload.json"
    )

    path.write_text(
        '{"source":"file"}',
        encoding="utf-8",
    )

    result = (
        JsonProcessor().parse(
            path
        )
    )

    assert result == {
        "source": "file"
    }


def test_missing_json_file_is_rejected(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "missing.json"
    )

    with pytest.raises(
        JsonProcessingError
    ):
        JsonProcessor().parse(
            path
        )


def test_directory_is_rejected_as_json_file(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        JsonProcessingError
    ):
        JsonProcessor().parse(
            tmp_path
        )


def test_string_is_not_implicitly_treated_as_path(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "payload.json"
    )

    path.write_text(
        '{"ok":true}',
        encoding="utf-8",
    )

    with pytest.raises(
        JsonProcessingError
    ):
        JsonProcessor().parse(
            str(path)
        )


# =============================================================================
# ROOT CONTRACTS
# =============================================================================
def test_parse_object_accepts_object() -> None:
    result = (
        JsonProcessor()
        .parse_object(
            '{"id":1}'
        )
    )

    assert result == {
        "id": 1
    }


@pytest.mark.parametrize(
    "payload",
    [
        "[]",
        "1",
        "true",
        "null",
        '"hello"',
    ],
)
def test_parse_object_rejects_non_object(
    payload,
) -> None:
    with pytest.raises(
        JsonProcessingError
    ):
        JsonProcessor().parse_object(
            payload
        )


def test_parse_array_accepts_array() -> None:
    result = (
        JsonProcessor()
        .parse_array(
            '[{"id":1}]'
        )
    )

    assert result == [
        {
            "id": 1
        }
    ]


@pytest.mark.parametrize(
    "payload",
    [
        "{}",
        "1",
        "true",
        "null",
        '"hello"',
    ],
)
def test_parse_array_rejects_non_array(
    payload,
) -> None:
    with pytest.raises(
        JsonProcessingError
    ):
        JsonProcessor().parse_array(
            payload
        )


# =============================================================================
# STRICT JSON
# =============================================================================
@pytest.mark.parametrize(
    "payload",
    [
        '{"value":NaN}',
        '{"value":Infinity}',
        '{"value":-Infinity}',
    ],
)
def test_non_standard_numeric_constants_are_rejected(
    payload,
) -> None:
    with pytest.raises(
        JsonProcessingError
    ):
        JsonProcessor().parse(
            payload
        )


def test_duplicate_object_keys_are_rejected_by_default() -> None:
    with pytest.raises(
        JsonProcessingError
    ):
        JsonProcessor().parse(
            '{"id":1,"id":2}'
        )


def test_duplicate_object_keys_can_be_allowed() -> None:
    processor = JsonProcessor(
        config=JsonProcessorConfig(
            reject_duplicate_keys=False
        )
    )

    result = processor.parse(
        '{"id":1,"id":2}'
    )

    assert result == {
        "id": 2
    }


def test_nested_duplicate_object_keys_are_rejected() -> None:
    with pytest.raises(
        JsonProcessingError
    ):
        JsonProcessor().parse(
            '{"outer":{"id":1,"id":2}}'
        )


# =============================================================================
# SIZE POLICY
# =============================================================================
def test_text_over_size_limit_is_rejected() -> None:
    processor = JsonProcessor(
        config=JsonProcessorConfig(
            max_bytes=5
        )
    )

    with pytest.raises(
        JsonProcessingError
    ):
        processor.parse(
            '{"a":1}'
        )


def test_bytes_over_size_limit_is_rejected() -> None:
    processor = JsonProcessor(
        config=JsonProcessorConfig(
            max_bytes=5
        )
    )

    with pytest.raises(
        JsonProcessingError
    ):
        processor.parse(
            b'{"a":1}'
        )


def test_file_over_size_limit_is_rejected(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "large.json"
    )

    path.write_text(
        '{"long":"payload"}',
        encoding="utf-8",
    )

    processor = JsonProcessor(
        config=JsonProcessorConfig(
            max_bytes=5
        )
    )

    with pytest.raises(
        JsonProcessingError
    ):
        processor.parse(
            path
        )


def test_size_limit_can_be_disabled() -> None:
    processor = JsonProcessor(
        config=JsonProcessorConfig(
            max_bytes=None
        )
    )

    result = processor.parse(
        '{"long":"payload"}'
    )

    assert result == {
        "long": "payload"
    }


# =============================================================================
# SERIALIZATION
# =============================================================================
def test_serialize_json() -> None:
    result = (
        JsonProcessor()
        .serialize(
            {
                "a": 1,
                "b": True,
            }
        )
    )

    assert result == (
        '{"a":1,"b":true}'
    )


def test_serialization_is_deterministically_sorted() -> None:
    processor = JsonProcessor()

    left = processor.serialize(
        {
            "b": 2,
            "a": 1,
        }
    )

    right = processor.serialize(
        {
            "a": 1,
            "b": 2,
        }
    )

    assert left == right

    assert left == (
        '{"a":1,"b":2}'
    )


def test_sorting_can_be_disabled() -> None:
    processor = JsonProcessor()

    result = processor.serialize(
        {
            "b": 2,
            "a": 1,
        },
        sort_keys=False,
    )

    assert result == (
        '{"b":2,"a":1}'
    )


def test_pretty_serialization() -> None:
    result = (
        JsonProcessor()
        .serialize(
            {
                "a": 1
            },
            pretty=True,
        )
    )

    assert "\n" in result
    assert '  "a": 1' in result


def test_unicode_is_not_ascii_escaped() -> None:
    result = (
        JsonProcessor()
        .serialize(
            {
                "city": "İstanbul"
            }
        )
    )

    assert "İstanbul" in result


@pytest.mark.parametrize(
    "value",
    [
        float("nan"),
        float("inf"),
        float("-inf"),
    ],
)
def test_non_finite_numbers_cannot_be_serialized(
    value,
) -> None:
    with pytest.raises(
        ContractValidationError
    ):
        JsonProcessor().serialize(
            {
                "value": value
            }
        )


def test_non_string_mapping_key_is_rejected() -> None:
    with pytest.raises(
        ContractValidationError
    ):
        JsonProcessor().serialize(
            {
                1: "value"
            }
        )


def test_custom_object_is_rejected() -> None:
    class Custom:
        pass

    with pytest.raises(
        ContractValidationError
    ):
        JsonProcessor().serialize(
            {
                "custom": Custom()
            }
        )


def test_invalid_pretty_flag_is_rejected() -> None:
    with pytest.raises(
        ContractValidationError
    ):
        JsonProcessor().serialize(
            {},
            pretty=1,
        )


def test_invalid_sort_keys_flag_is_rejected() -> None:
    with pytest.raises(
        ContractValidationError
    ):
        JsonProcessor().serialize(
            {},
            sort_keys=1,
        )


def test_serialize_bytes() -> None:
    raw = (
        JsonProcessor()
        .serialize_bytes(
            {
                "ok": True
            }
        )
    )

    assert raw == (
        b'{"ok":true}'
    )


def test_serialized_output_respects_size_limit() -> None:
    processor = JsonProcessor(
        config=JsonProcessorConfig(
            max_bytes=5
        )
    )

    with pytest.raises(
        JsonProcessingError
    ):
        processor.serialize(
            {
                "a": 1
            }
        )


# =============================================================================
# CONVENIENCE API
# =============================================================================
def test_parse_json_helper() -> None:
    assert parse_json(
        '{"a":1}'
    ) == {
        "a": 1
    }


def test_parse_json_object_helper() -> None:
    assert parse_json_object(
        '{"a":1}'
    ) == {
        "a": 1
    }


def test_parse_json_array_helper() -> None:
    assert parse_json_array(
        '[1,2]'
    ) == [
        1,
        2,
    ]


def test_serialize_json_helper() -> None:
    assert serialize_json(
        {
            "b": 2,
            "a": 1,
        }
    ) == (
        '{"a":1,"b":2}'
    )


# =============================================================================
# SOURCE CONTRACT
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
    ],
)
def test_unsupported_source_type_is_rejected(
    source,
) -> None:
    with pytest.raises(
        ContractValidationError
    ):
        JsonProcessor().parse(
            source
        )


# =============================================================================
# SNAPSHOT / REPR
# =============================================================================
def test_snapshot_contains_configuration() -> None:
    processor = JsonProcessor(
        config=JsonProcessorConfig(
            encoding="utf-8",
            max_bytes=1024,
            reject_duplicate_keys=False,
            allow_scalar_root=False,
        )
    )

    assert processor.snapshot() == {
        "encoding": "utf-8",
        "max_bytes": 1024,
        "reject_duplicate_keys": False,
        "allow_scalar_root": False,
    }


def test_repr_contains_configuration() -> None:
    processor = JsonProcessor(
        config=JsonProcessorConfig(
            max_bytes=2048
        )
    )

    rendered = repr(
        processor
    )

    assert "JsonProcessor" in rendered
    assert "2048" in rendered
    assert "utf-8" in rendered