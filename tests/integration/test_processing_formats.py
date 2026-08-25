from __future__ import annotations

"""
Enterprise Crawler Framework
Processing Formats Integration Tests

Amaç
----
Processing katmanındaki format processor'larının gerçek BaseBot lifecycle
içinde çalışabildiğini doğrular.

Mevcut ``test_processing_bot.py`` zaten şu zinciri doğrular:

    Crawler
        ↓
    BaseBot
        ↓
    HttpClient
        ↓
    JSON payload
        ↓
    JsonProcessor
        ↓
    ExecutionResult

Bu dosya HTTP davranışını gereksiz yere tekrar test etmez. Bunun yerine diğer
processing formatlarının BaseBot execution/lifecycle sınırında doğru compose
edilebildiğini kanıtlar:

    BaseBot
        ↓
    Processor
        ↓
    normalized document
        ↓
    ExecutionResult metadata

Kapsanan formatlar
------------------
- JSON
- XML
- HTML
- CSV
- RSS/Atom Feed
- PDF

Bu testler network erişimine ihtiyaç duymaz.
"""

from typing import Any

from enterprise_crawler.contracts.enums import (
    ExecutionStatus,
)
from enterprise_crawler.core.base_bot import BaseBot
from enterprise_crawler.processing import (
    CsvProcessor,
    FeedProcessor,
    HtmlProcessor,
    JsonProcessor,
    PdfProcessor,
    XmlProcessor,
)


# =============================================================================
# FIXTURES
# =============================================================================
JSON_PAYLOAD = b"""
{
    "framework": "enterprise-crawler",
    "version": 1,
    "active": true
}
""".strip()


XML_PAYLOAD = b"""
<?xml version="1.0" encoding="UTF-8"?>
<root>
    <item id="1">hello</item>
    <item id="2">world</item>
</root>
""".strip()


HTML_PAYLOAD = b"""
<!doctype html>
<html>
    <head>
        <title>Enterprise Crawler</title>
    </head>
    <body>
        <main>
            <h1>Hello World</h1>
            <a href="https://example.com/document">
                Document
            </a>
        </main>
    </body>
</html>
""".strip()


CSV_PAYLOAD = b"""
id,name,status
1,alpha,active
2,beta,inactive
""".strip()


RSS_PAYLOAD = b"""
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
    <channel>
        <title>Enterprise Crawler Feed</title>
        <link>https://example.com/</link>
        <description>Integration feed</description>

        <item>
            <guid>entry-1</guid>
            <title>First Entry</title>
            <link>https://example.com/entry-1</link>
            <description>Hello World</description>
        </item>
    </channel>
</rss>
""".strip()


PDF_PAYLOAD = (
    b"%PDF-1.7\n"
    b"1 0 obj\n"
    b"<< /Type /Catalog >>\n"
    b"endobj\n"
    b"trailer\n"
    b"<< /Root 1 0 R >>\n"
    b"%%EOF\n"
)


# =============================================================================
# TEST BOT
# =============================================================================
class ProcessingFormatBot(BaseBot):
    """
    Tek processor çağrısını gerçek BaseBot lifecycle üzerinden çalıştıran
    integration-test botu.
    """

    def __init__(
        self,
        *,
        processor: Any,
        payload: bytes,
        format_name: str,
    ) -> None:
        super().__init__(
            bot_name=(
                f"processing-{format_name}-bot"
            )
        )

        self.processor = processor
        self.payload = payload
        self.format_name = format_name

        self.parsed_document: Any = None

    def execute(
        self,
    ) -> dict[str, Any]:
        self.raise_if_stopping()

        self.parsed_document = (
            self.processor.parse(
                self.payload
            )
        )

        self.mark_record_processed()

        self.set_runtime_metadata(
            "processing_format",
            self.format_name,
        )

        self.set_runtime_metadata(
            "processor_class",
            self.processor.__class__.__name__,
        )

        return {
            "status": (
                ExecutionStatus.COMPLETED
            ),
            "records_processed": 1,
            "errors": 0,
            "warnings": 0,
            "metadata": {
                "processing": {
                    "format": (
                        self.format_name
                    ),
                    "processor": (
                        self.processor
                        .__class__
                        .__name__
                    ),
                }
            },
        }


# =============================================================================
# HELPERS
# =============================================================================
def run_processing_bot(
    *,
    processor: Any,
    payload: bytes,
    format_name: str,
) -> tuple[
    ProcessingFormatBot,
    Any,
]:
    bot = ProcessingFormatBot(
        processor=processor,
        payload=payload,
        format_name=format_name,
    )

    try:
        result = bot.run()

        return (
            bot,
            result,
        )

    except Exception:
        bot.close()
        raise


def assert_successful_processing_run(
    bot: ProcessingFormatBot,
    result: Any,
    *,
    expected_format: str,
    expected_processor: str,
) -> None:
    assert (
        result.status
        is ExecutionStatus.COMPLETED
    )

    assert (
        result.records_processed
        == 1
    )

    assert (
        result.errors
        == 0
    )

    assert (
        bot.parsed_document
        is not None
    )

    assert (
        bot.run_count
        == 1
    )

    assert (
        bot.is_running
        is False
    )

    assert (
        bot.last_result
        is result
    )

    assert (
        result.metadata[
            "processing"
        ][
            "format"
        ]
        == expected_format
    )

    assert (
        result.metadata[
            "processing"
        ][
            "processor"
        ]
        == expected_processor
    )

    assert (
        result.metadata[
            "runtime"
        ][
            "processing_format"
        ]
        == expected_format
    )

    assert (
        result.metadata[
            "runtime"
        ][
            "processor_class"
        ]
        == expected_processor
    )

    assert (
        result.metadata[
            "bot"
        ][
            "bot_name"
        ]
        == bot.bot_name
    )


# =============================================================================
# JSON
# =============================================================================
def test_json_processor_runs_inside_basebot_lifecycle() -> None:
    bot, result = (
        run_processing_bot(
            processor=JsonProcessor(),
            payload=JSON_PAYLOAD,
            format_name="json",
        )
    )

    try:
        assert_successful_processing_run(
            bot,
            result,
            expected_format="json",
            expected_processor=(
                "JsonProcessor"
            ),
        )

        assert (
            bot.parsed_document[
                "framework"
            ]
            == "enterprise-crawler"
        )

        assert (
            bot.parsed_document[
                "version"
            ]
            == 1
        )

        assert (
            bot.parsed_document[
                "active"
            ]
            is True
        )

    finally:
        bot.close()


# =============================================================================
# XML
# =============================================================================
def test_xml_processor_runs_inside_basebot_lifecycle() -> None:
    bot, result = (
        run_processing_bot(
            processor=XmlProcessor(),
            payload=XML_PAYLOAD,
            format_name="xml",
        )
    )

    try:
        assert_successful_processing_run(
            bot,
            result,
            expected_format="xml",
            expected_processor=(
                "XmlProcessor"
            ),
        )

        root = bot.parsed_document

        assert (
            root.tag
            == "root"
        )

        items = root.findall(
            "item"
        )

        assert (
            len(items)
            == 2
        )

        assert (
            items[0].text
            == "hello"
        )

        assert (
            items[1].text
            == "world"
        )

    finally:
        bot.close()


# =============================================================================
# HTML
# =============================================================================
def test_html_processor_runs_inside_basebot_lifecycle() -> None:
    bot, result = (
        run_processing_bot(
            processor=HtmlProcessor(),
            payload=HTML_PAYLOAD,
            format_name="html",
        )
    )

    try:
        assert_successful_processing_run(
            bot,
            result,
            expected_format="html",
            expected_processor=(
                "HtmlProcessor"
            ),
        )

        document = (
            bot.parsed_document
        )

        assert (
            document.title
            == "Enterprise Crawler"
        )

        extracted_text = (
            document.text()
        )

        assert (
            "Hello World"
            in extracted_text
        )

        links = (
            document.links()
        )

        assert (
            len(links)
            == 1
        )

        assert (
            links[0].href
            == (
                "https://example.com/document"
            )
        )

    finally:
        bot.close()


# =============================================================================
# CSV
# =============================================================================
def test_csv_processor_runs_inside_basebot_lifecycle() -> None:
    bot, result = (
        run_processing_bot(
            processor=CsvProcessor(),
            payload=CSV_PAYLOAD,
            format_name="csv",
        )
    )

    try:
        assert_successful_processing_run(
            bot,
            result,
            expected_format="csv",
            expected_processor=(
                "CsvProcessor"
            ),
        )

        document = (
            bot.parsed_document
        )

        assert (
            document.headers
            == [
                "id",
                "name",
                "status",
            ]
        )

        assert (
            document.row_count
            == 2
        )

        rows = (
            document.to_dicts()
        )

        assert (
            rows[0]
            == {
                "id": "1",
                "name": "alpha",
                "status": "active",
            }
        )

        assert (
            rows[1]
            == {
                "id": "2",
                "name": "beta",
                "status": "inactive",
            }
        )

    finally:
        bot.close()


# =============================================================================
# FEED
# =============================================================================
def test_feed_processor_runs_inside_basebot_lifecycle() -> None:
    bot, result = (
        run_processing_bot(
            processor=FeedProcessor(),
            payload=RSS_PAYLOAD,
            format_name="feed",
        )
    )

    try:
        assert_successful_processing_run(
            bot,
            result,
            expected_format="feed",
            expected_processor=(
                "FeedProcessor"
            ),
        )

        document = (
            bot.parsed_document
        )

        assert (
            document.feed_type
            == "rss"
        )

        assert (
            document.title
            == (
                "Enterprise Crawler Feed"
            )
        )

        assert (
            document.entry_count
            == 1
        )

        entry = (
            document.entries[0]
        )

        assert (
            entry.identity
            == "entry-1"
        )

        assert (
            entry.title
            == "First Entry"
        )

        assert (
            entry.link
            == (
                "https://example.com/entry-1"
            )
        )

    finally:
        bot.close()


# =============================================================================
# PDF
# =============================================================================
def test_pdf_processor_runs_inside_basebot_lifecycle() -> None:
    bot, result = (
        run_processing_bot(
            processor=PdfProcessor(),
            payload=PDF_PAYLOAD,
            format_name="pdf",
        )
    )

    try:
        assert_successful_processing_run(
            bot,
            result,
            expected_format="pdf",
            expected_processor=(
                "PdfProcessor"
            ),
        )

        document = (
            bot.parsed_document
        )

        assert (
            document.is_pdf
            is True
        )

        assert (
            document.version
            == "1.7"
        )

        document_bytes = (
            document.to_bytes()
        )

        assert (
            len(document_bytes)
            == len(PDF_PAYLOAD)
        )

        assert (
            document_bytes
            == PDF_PAYLOAD
        )

    finally:
        bot.close()


# =============================================================================
# CROSS-FORMAT LIFECYCLE
# =============================================================================
def test_processing_formats_can_run_sequentially() -> None:
    cases = [
        (
            JsonProcessor(),
            JSON_PAYLOAD,
            "json",
        ),
        (
            XmlProcessor(),
            XML_PAYLOAD,
            "xml",
        ),
        (
            HtmlProcessor(),
            HTML_PAYLOAD,
            "html",
        ),
        (
            CsvProcessor(),
            CSV_PAYLOAD,
            "csv",
        ),
        (
            FeedProcessor(),
            RSS_PAYLOAD,
            "feed",
        ),
        (
            PdfProcessor(),
            PDF_PAYLOAD,
            "pdf",
        ),
    ]

    results = []

    for (
        processor,
        payload,
        format_name,
    ) in cases:
        bot = ProcessingFormatBot(
            processor=processor,
            payload=payload,
            format_name=format_name,
        )

        try:
            result = bot.run()

            assert (
                result.status
                is ExecutionStatus.COMPLETED
            )

            assert (
                result.records_processed
                == 1
            )

            assert (
                bot.parsed_document
                is not None
            )

            results.append(
                result
            )

        finally:
            bot.close()

    assert (
        len(results)
        == 6
    )

    assert all(
        result.status
        is ExecutionStatus.COMPLETED
        for result in results
    )