from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from enterprise_crawler.processing.pdf import (
    DEFAULT_PDF_MAX_BYTES,
    PDF_MAGIC,
    SUPPORTED_PDF_VERSIONS,
    PdfDocument,
    PdfProcessingError,
    PdfProcessor,
    PdfProcessorConfig,
    parse_pdf,
    parse_pdf_file,
)


# =============================================================================
# FIXTURES / HELPERS
# =============================================================================
def make_pdf(
    body: bytes = b"",
    *,
    version: str = "1.7",
    eof: bool = True,
) -> bytes:
    payload = (
        f"%PDF-{version}\n".encode(
            "ascii"
        )
        + b"1 0 obj\n"
        + b"<< /Type /Catalog >>\n"
        + b"endobj\n"
        + body
    )

    if eof:
        payload += (
            b"\nstartxref\n0\n%%EOF\n"
        )

    return payload


# =============================================================================
# CONFIG
# =============================================================================
def test_default_config_is_valid() -> None:
    config = (
        PdfProcessorConfig()
    )

    assert (
        config.max_bytes
        == DEFAULT_PDF_MAX_BYTES
    )

    assert (
        config.require_eof_marker
        is True
    )

    assert (
        config.reject_unsupported_version
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
        PdfProcessingError
    ):
        PdfProcessorConfig(
            max_bytes=value
        )


def test_unlimited_max_bytes_is_allowed() -> None:
    config = (
        PdfProcessorConfig(
            max_bytes=None
        )
    )

    assert (
        config.max_bytes
        is None
    )


@pytest.mark.parametrize(
    (
        "field_name",
        "value",
    ),
    [
        (
            "require_eof_marker",
            1,
        ),
        (
            "reject_unsupported_version",
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
        PdfProcessingError
    ):
        PdfProcessorConfig(
            **kwargs
        )


def test_config_to_dict() -> None:
    config = (
        PdfProcessorConfig(
            max_bytes=4096,
            require_eof_marker=False,
            reject_unsupported_version=False,
        )
    )

    assert config.to_dict() == {
        "max_bytes": 4096,
        "require_eof_marker": False,
        "reject_unsupported_version": False,
    }


def test_invalid_processor_config_type_is_rejected() -> None:
    with pytest.raises(
        PdfProcessingError
    ):
        PdfProcessor(
            config={}  # type: ignore[arg-type]
        )


# =============================================================================
# BINARY PARSING
# =============================================================================
def test_parse_pdf_from_bytes() -> None:
    payload = make_pdf()

    document = (
        PdfProcessor().parse(
            payload
        )
    )

    assert isinstance(
        document,
        PdfDocument,
    )

    assert (
        document.payload
        == payload
    )

    assert (
        document.byte_size
        == len(payload)
    )

    assert (
        document.version
        == "1.7"
    )

    assert (
        document.has_eof_marker
        is True
    )


def test_parse_pdf_from_bytearray() -> None:
    payload = make_pdf()

    document = (
        PdfProcessor().parse(
            bytearray(payload)
        )
    )

    assert (
        document.payload
        == payload
    )


def test_parse_pdf_from_memoryview() -> None:
    payload = make_pdf()

    document = (
        PdfProcessor().parse(
            memoryview(payload)
        )
    )

    assert (
        document.payload
        == payload
    )


def test_payload_sha256_is_calculated() -> None:
    payload = make_pdf()

    document = (
        PdfProcessor().parse(
            payload
        )
    )

    expected = hashlib.sha256(
        payload
    ).hexdigest()

    assert (
        document.sha256
        == expected
    )


def test_pdf_document_is_pdf() -> None:
    document = (
        PdfProcessor().parse(
            make_pdf()
        )
    )

    assert (
        document.is_pdf
        is True
    )


def test_document_to_bytes_returns_payload() -> None:
    payload = make_pdf()

    document = (
        PdfProcessor().parse(
            payload
        )
    )

    assert (
        document.to_bytes()
        == payload
    )


# =============================================================================
# INPUT VALIDATION
# =============================================================================
def test_empty_bytes_are_rejected() -> None:
    with pytest.raises(
        PdfProcessingError
    ):
        PdfProcessor().parse(
            b""
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
    source,
) -> None:
    with pytest.raises(
        PdfProcessingError
    ):
        PdfProcessor().parse(
            source
        )


def test_string_is_not_implicitly_treated_as_path() -> None:
    with pytest.raises(
        PdfProcessingError
    ):
        PdfProcessor().parse(
            "document.pdf"
        )


def test_path_is_not_accepted_by_parse() -> None:
    with pytest.raises(
        PdfProcessingError
    ):
        PdfProcessor().parse(
            Path(
                "document.pdf"
            )
        )


# =============================================================================
# HEADER VALIDATION
# =============================================================================
def test_pdf_magic_constant() -> None:
    assert (
        PDF_MAGIC
        == b"%PDF-"
    )


def test_non_pdf_payload_is_rejected() -> None:
    with pytest.raises(
        PdfProcessingError
    ):
        PdfProcessor().parse(
            b"hello world"
        )


def test_pdf_header_must_be_at_start() -> None:
    payload = (
        b"junk"
        + make_pdf()
    )

    with pytest.raises(
        PdfProcessingError
    ):
        PdfProcessor().parse(
            payload
        )


@pytest.mark.parametrize(
    "version",
    sorted(
        SUPPORTED_PDF_VERSIONS
    ),
)
def test_supported_pdf_versions_are_accepted(
    version: str,
) -> None:
    document = (
        PdfProcessor().parse(
            make_pdf(
                version=version
            )
        )
    )

    assert (
        document.version
        == version
    )


def test_invalid_pdf_version_header_is_rejected() -> None:
    payload = (
        b"%PDF-x.y\n"
        b"%%EOF"
    )

    with pytest.raises(
        PdfProcessingError
    ):
        PdfProcessor().parse(
            payload
        )


def test_unsupported_pdf_version_is_rejected_by_default() -> None:
    with pytest.raises(
        PdfProcessingError
    ):
        PdfProcessor().parse(
            make_pdf(
                version="9.9"
            )
        )


def test_unsupported_pdf_version_can_be_observed_when_enabled() -> None:
    processor = (
        PdfProcessor(
            PdfProcessorConfig(
                reject_unsupported_version=False
            )
        )
    )

    document = processor.parse(
        make_pdf(
            version="9.9"
        )
    )

    assert (
        document.version
        == "9.9"
    )


# =============================================================================
# EOF VALIDATION
# =============================================================================
def test_missing_eof_marker_is_rejected_by_default() -> None:
    with pytest.raises(
        PdfProcessingError
    ):
        PdfProcessor().parse(
            make_pdf(
                eof=False
            )
        )


def test_missing_eof_marker_can_be_allowed() -> None:
    processor = (
        PdfProcessor(
            PdfProcessorConfig(
                require_eof_marker=False
            )
        )
    )

    document = processor.parse(
        make_pdf(
            eof=False
        )
    )

    assert (
        document.has_eof_marker
        is False
    )


def test_eof_marker_near_end_is_detected() -> None:
    payload = (
        make_pdf()
        + b"\n"
        + b" " * 100
    )

    document = (
        PdfProcessor().parse(
            payload
        )
    )

    assert (
        document.has_eof_marker
        is True
    )


def test_eof_marker_far_from_end_is_not_accepted() -> None:
    payload = (
        make_pdf()
        + b"x" * 2048
    )

    with pytest.raises(
        PdfProcessingError
    ):
        PdfProcessor().parse(
            payload
        )


# =============================================================================
# SIZE LIMITS
# =============================================================================
def test_payload_over_size_limit_is_rejected() -> None:
    payload = make_pdf()

    processor = (
        PdfProcessor(
            PdfProcessorConfig(
                max_bytes=(
                    len(payload)
                    - 1
                )
            )
        )
    )

    with pytest.raises(
        PdfProcessingError
    ):
        processor.parse(
            payload
        )


def test_payload_exactly_at_size_limit_is_allowed() -> None:
    payload = make_pdf()

    processor = (
        PdfProcessor(
            PdfProcessorConfig(
                max_bytes=len(
                    payload
                )
            )
        )
    )

    document = processor.parse(
        payload
    )

    assert (
        document.byte_size
        == len(payload)
    )


def test_size_limit_can_be_disabled() -> None:
    payload = make_pdf(
        body=(
            b"x" * 10_000
        )
    )

    processor = (
        PdfProcessor(
            PdfProcessorConfig(
                max_bytes=None
            )
        )
    )

    document = processor.parse(
        payload
    )

    assert (
        document.byte_size
        == len(payload)
    )


# =============================================================================
# SECURITY OBSERVATION
# =============================================================================
def test_encrypted_pdf_marker_is_observed() -> None:
    document = (
        PdfProcessor().parse(
            make_pdf(
                body=(
                    b"<< /Encrypt 2 0 R >>"
                )
            )
        )
    )

    assert (
        document.encrypted
        is True
    )


def test_normal_pdf_is_not_marked_encrypted() -> None:
    document = (
        PdfProcessor().parse(
            make_pdf()
        )
    )

    assert (
        document.encrypted
        is False
    )


def test_javascript_marker_is_observed() -> None:
    document = (
        PdfProcessor().parse(
            make_pdf(
                body=(
                    b"<< /S /JavaScript "
                    b"/JS (alert) >>"
                )
            )
        )
    )

    assert (
        document.contains_javascript
        is True
    )


def test_js_short_marker_is_observed() -> None:
    document = (
        PdfProcessor().parse(
            make_pdf(
                body=(
                    b"<< /JS (hello) >>"
                )
            )
        )
    )

    assert (
        document.contains_javascript
        is True
    )


def test_embedded_file_marker_is_observed() -> None:
    document = (
        PdfProcessor().parse(
            make_pdf(
                body=(
                    b"<< /Type /EmbeddedFile >>"
                )
            )
        )
    )

    assert (
        document.contains_embedded_files
        is True
    )


def test_launch_action_marker_is_observed() -> None:
    document = (
        PdfProcessor().parse(
            make_pdf(
                body=(
                    b"<< /S /Launch >>"
                )
            )
        )
    )

    assert (
        document.contains_launch_action
        is True
    )


def test_security_markers_are_case_insensitive() -> None:
    document = (
        PdfProcessor().parse(
            make_pdf(
                body=(
                    b"/encrypt "
                    b"/javascript "
                    b"/embeddedfile "
                    b"/launch"
                )
            )
        )
    )

    assert (
        document.encrypted
        is True
    )

    assert (
        document.contains_javascript
        is True
    )

    assert (
        document.contains_embedded_files
        is True
    )

    assert (
        document.contains_launch_action
        is True
    )


# =============================================================================
# FILE PARSING
# =============================================================================
def test_parse_pdf_file(
    tmp_path: Path,
) -> None:
    payload = make_pdf()

    path = (
        tmp_path
        / "document.pdf"
    )

    path.write_bytes(
        payload
    )

    document = (
        PdfProcessor().parse_file(
            path
        )
    )

    assert (
        document.payload
        == payload
    )

    assert (
        document.source_path
        == path
    )


def test_missing_pdf_file_is_rejected(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        PdfProcessingError
    ):
        PdfProcessor().parse_file(
            tmp_path
            / "missing.pdf"
        )


def test_directory_is_rejected_as_pdf_file(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        PdfProcessingError
    ):
        PdfProcessor().parse_file(
            tmp_path
        )


def test_parse_file_requires_path_object() -> None:
    with pytest.raises(
        PdfProcessingError
    ):
        PdfProcessor().parse_file(
            "document.pdf"  # type: ignore[arg-type]
        )


def test_file_over_size_limit_is_rejected(
    tmp_path: Path,
) -> None:
    payload = make_pdf()

    path = (
        tmp_path
        / "large.pdf"
    )

    path.write_bytes(
        payload
    )

    processor = (
        PdfProcessor(
            PdfProcessorConfig(
                max_bytes=(
                    len(payload)
                    - 1
                )
            )
        )
    )

    with pytest.raises(
        PdfProcessingError
    ):
        processor.parse_file(
            path
        )


# =============================================================================
# DOCUMENT REPRESENTATION
# =============================================================================
def test_document_to_dict() -> None:
    payload = make_pdf()

    document = (
        PdfProcessor().parse(
            payload
        )
    )

    result = (
        document.to_dict()
    )

    assert (
        result["byte_size"]
        == len(payload)
    )

    assert (
        result["sha256"]
        == hashlib.sha256(
            payload
        ).hexdigest()
    )

    assert (
        result["version"]
        == "1.7"
    )

    assert (
        result["has_eof_marker"]
        is True
    )

    assert (
        result["source_path"]
        is None
    )

    assert (
        "payload"
        not in result
    )


def test_file_document_to_dict_serializes_path(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "document.pdf"
    )

    path.write_bytes(
        make_pdf()
    )

    result = (
        PdfProcessor()
        .parse_file(
            path
        )
        .to_dict()
    )

    assert (
        result["source_path"]
        == str(path)
    )


# =============================================================================
# HELPERS
# =============================================================================
def test_parse_pdf_helper() -> None:
    payload = make_pdf()

    document = parse_pdf(
        payload
    )

    assert (
        document.payload
        == payload
    )


def test_parse_pdf_file_helper(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "document.pdf"
    )

    path.write_bytes(
        make_pdf()
    )

    document = (
        parse_pdf_file(
            path
        )
    )

    assert (
        document.source_path
        == path
    )


def test_helper_respects_custom_config() -> None:
    payload = make_pdf(
        eof=False
    )

    document = parse_pdf(
        payload,
        config=(
            PdfProcessorConfig(
                require_eof_marker=False
            )
        ),
    )

    assert (
        document.has_eof_marker
        is False
    )


# =============================================================================
# PROCESSOR SNAPSHOT / REPR
# =============================================================================
def test_snapshot_contains_configuration() -> None:
    processor = (
        PdfProcessor(
            PdfProcessorConfig(
                max_bytes=4096
            )
        )
    )

    snapshot = (
        processor.snapshot()
    )

    assert (
        snapshot["processor"]
        == "PdfProcessor"
    )

    assert (
        snapshot["config"][
            "max_bytes"
        ]
        == 4096
    )

    assert (
        "1.7"
        in snapshot[
            "supported_versions"
        ]
    )


def test_repr_contains_configuration() -> None:
    processor = (
        PdfProcessor(
            PdfProcessorConfig(
                max_bytes=4096,
                require_eof_marker=False,
            )
        )
    )

    rendered = repr(
        processor
    )

    assert (
        "PdfProcessor"
        in rendered
    )

    assert (
        "4096"
        in rendered
    )

    assert (
        "require_eof_marker=False"
        in rendered
    )