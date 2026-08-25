from __future__ import annotations

"""
Enterprise Crawler Framework - Processing Public API

Framework'ün yerleşik veri işleme bileşenlerinin kararlı public import
yüzeyidir.

Desteklenen processing aileleri:

- JSON
- XML
- HTML
- CSV
- RSS / Atom Feed
- PDF
- Processing Pipeline

Örnek
-----
    from enterprise_crawler.processing import (
        JsonProcessor,
        XmlProcessor,
        HtmlProcessor,
        CsvProcessor,
        FeedProcessor,
        PdfProcessor,
        ProcessingPipeline,
    )
"""

# =============================================================================
# JSON
# =============================================================================
from enterprise_crawler.processing.json import (
    JsonProcessingError,
    JsonProcessor,
    JsonProcessorConfig,
    parse_json,
    parse_json_array,
    parse_json_object,
    serialize_json,
)


# =============================================================================
# XML
# =============================================================================
from enterprise_crawler.processing.xml import (
    XmlProcessingError,
    XmlProcessor,
    XmlProcessorConfig,
    parse_xml,
    parse_xml_file,
    serialize_xml,
)


# =============================================================================
# HTML
# =============================================================================
from enterprise_crawler.processing.html import (
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


# =============================================================================
# CSV
# =============================================================================
from enterprise_crawler.processing.csv import (
    CsvDocument,
    CsvProcessingError,
    CsvProcessor,
    CsvProcessorConfig,
    parse_csv,
    parse_csv_file,
    serialize_csv,
)


# =============================================================================
# RSS / ATOM FEED
# =============================================================================
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
# PDF
# =============================================================================
from enterprise_crawler.processing.pdf import (
    DEFAULT_PDF_MAX_BYTES,
    PDF_EOF_MARKER,
    PDF_EOF_SEARCH_BYTES,
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
# PROCESSING PIPELINE
# =============================================================================
from enterprise_crawler.processing.pipeline import (
    PipelineContext,
    PipelineResult,
    ProcessingPipeline,
)


# =============================================================================
# PUBLIC API
# =============================================================================
__all__ = [
    # -------------------------------------------------------------------------
    # JSON
    # -------------------------------------------------------------------------
    "JsonProcessingError",
    "JsonProcessor",
    "JsonProcessorConfig",
    "parse_json",
    "parse_json_array",
    "parse_json_object",
    "serialize_json",

    # -------------------------------------------------------------------------
    # XML
    # -------------------------------------------------------------------------
    "XmlProcessingError",
    "XmlProcessor",
    "XmlProcessorConfig",
    "parse_xml",
    "parse_xml_file",
    "serialize_xml",

    # -------------------------------------------------------------------------
    # HTML
    # -------------------------------------------------------------------------
    "HtmlDocument",
    "HtmlLink",
    "HtmlNode",
    "HtmlProcessingError",
    "HtmlProcessor",
    "HtmlProcessorConfig",
    "extract_html_text",
    "parse_html",
    "parse_html_file",

    # -------------------------------------------------------------------------
    # CSV
    # -------------------------------------------------------------------------
    "CsvDocument",
    "CsvProcessingError",
    "CsvProcessor",
    "CsvProcessorConfig",
    "parse_csv",
    "parse_csv_file",
    "serialize_csv",

    # -------------------------------------------------------------------------
    # RSS / ATOM FEED
    # -------------------------------------------------------------------------
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

    # -------------------------------------------------------------------------
    # PDF
    # -------------------------------------------------------------------------
    "DEFAULT_PDF_MAX_BYTES",
    "PDF_EOF_MARKER",
    "PDF_EOF_SEARCH_BYTES",
    "PDF_MAGIC",
    "SUPPORTED_PDF_VERSIONS",
    "PdfDocument",
    "PdfProcessingError",
    "PdfProcessor",
    "PdfProcessorConfig",
    "parse_pdf",
    "parse_pdf_file",

    # -------------------------------------------------------------------------
    # PROCESSING PIPELINE
    # -------------------------------------------------------------------------
    "PipelineContext",
    "PipelineResult",
    "ProcessingPipeline",
]