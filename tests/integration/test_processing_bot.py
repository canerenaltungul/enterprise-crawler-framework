from __future__ import annotations

"""
Enterprise Crawler Framework - Processing Integration Tests

Bu testler aşağıdaki gerçek framework composition zincirini doğrular:

Crawler
    ↓
BaseBot
    ↓
HttpClient
    ↓
HTTP response
    ↓
Processing layer
    ↓
ExecutionResult

Amaç
----
Processing modüllerini yalnızca izole unit testlerle değil, gerçek BaseBot
runtime ve lifecycle zinciri içinde de doğrulamaktır.

Bu testler gerçek internete çıkmaz.

HTTP transport, requests.Session benzeri kontrollü fake session ile sağlanır.
Böylece testler:

- deterministik,
- hızlı,
- network bağımsız,
- CI güvenli

kalır.
"""

from typing import Any

from enterprise_crawler.contracts.enums import ExecutionStatus
from enterprise_crawler.core.base_bot import BaseBot
from enterprise_crawler.core.crawler import Crawler
from enterprise_crawler.core.session import SessionManager
from enterprise_crawler.processing import (
    JsonProcessingError,
    JsonProcessor,
)


# =============================================================================
# TEST DOUBLES
# =============================================================================
class FakeResponse:
    """
    requests.Response için gereken minimum test double.

    HttpClient'in response üzerinde bekleyebileceği temel alanları sağlar.
    """

    def __init__(
        self,
        *,
        status_code: int = 200,
        text: str = "",
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
        url: str = "https://example.test/data.json",
    ) -> None:
        self.status_code = status_code

        self.text = text

        self.content = (
            content
            if content is not None
            else text.encode("utf-8")
        )

        self.headers = dict(
            headers
            or {
                "Content-Type": "application/json",
            }
        )

        self.url = url

        self.reason = (
            "OK"
            if 200 <= status_code < 300
            else "ERROR"
        )

        self.closed = False

    @property
    def ok(
        self,
    ) -> bool:
        return (
            200
            <= self.status_code
            < 400
        )

    def raise_for_status(
        self,
    ) -> None:
        if self.status_code >= 400:
            raise RuntimeError(
                "HTTP error "
                f"{self.status_code}"
            )

    def close(
        self,
    ) -> None:
        self.closed = True

    def __enter__(
        self,
    ) -> "FakeResponse":
        return self

    def __exit__(
        self,
        exc_type: Any,
        exc: Any,
        traceback: Any,
    ) -> None:
        self.close()


class FakeSession:
    """
    requests.Session compatible kontrollü test transport'u.

    HttpClient gerçek şekilde çalışır fakat dış network çağrısı yapılmaz.
    """

    def __init__(
        self,
        response: FakeResponse,
    ) -> None:
        self.response = response

        self.headers: dict[
            str,
            str,
        ] = {}

        self.proxies: dict[
            str,
            str,
        ] = {}

        self.verify = True

        self.closed = False

        self.request_count = 0

        self.last_method: str | None = None

        self.last_url: str | None = None

        self.last_kwargs: dict[
            str,
            Any,
        ] | None = None

    def request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> FakeResponse:
        self.request_count += 1

        self.last_method = method
        self.last_url = url

        self.last_kwargs = dict(
            kwargs
        )

        self.response.url = url

        return self.response

    def get(
        self,
        url: str,
        **kwargs: Any,
    ) -> FakeResponse:
        return self.request(
            "GET",
            url,
            **kwargs,
        )

    def post(
        self,
        url: str,
        **kwargs: Any,
    ) -> FakeResponse:
        return self.request(
            "POST",
            url,
            **kwargs,
        )

    def close(
        self,
    ) -> None:
        self.closed = True


# =============================================================================
# BOTS
# =============================================================================
class JsonProcessingBot(
    BaseBot
):
    """
    HTTP JSON payload'ını processing katmanından geçirir.
    """

    URL = (
        "https://example.test/"
        "records.json"
    )

    def __init__(
        self,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            **kwargs
        )

        self.processor = (
            JsonProcessor()
        )

        self.document: dict[
            str,
            Any,
        ] | None = None

    def execute(
        self,
    ) -> dict[
        str,
        Any,
    ]:
        response = self.http.get(
            self.URL
        )

        document = (
            self.processor.parse_object(
                response.content
            )
        )

        self.document = document

        records = document.get(
            "records",
            []
        )

        if not isinstance(
            records,
            list,
        ):
            raise JsonProcessingError(
                "'records' alanı liste "
                "olmalıdır."
            )

        self.mark_record_processed(
            len(records)
        )

        return {
            "status": (
                ExecutionStatus.COMPLETED
            ),
            "records_processed": (
                len(records)
            ),
            "metadata": {
                "source_url": (
                    response.url
                ),
                "record_count": (
                    len(records)
                ),
            },
        }


class BrokenJsonBot(
    BaseBot
):
    """
    HTTP başarılı olsa bile bozuk JSON processing failure üretmelidir.
    """

    URL = (
        "https://example.test/"
        "broken.json"
    )

    def __init__(
        self,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            **kwargs
        )

        self.processor = (
            JsonProcessor()
        )

    def execute(
        self,
    ) -> None:
        response = self.http.get(
            self.URL
        )

        self.processor.parse(
            response.content
        )


# =============================================================================
# HELPERS
# =============================================================================
def build_external_session_manager(
    response: FakeResponse,
) -> tuple[
    SessionManager,
    FakeSession,
]:
    fake_session = FakeSession(
        response
    )

    manager = SessionManager(
        session=fake_session,
    )

    return (
        manager,
        fake_session,
    )


# =============================================================================
# SUCCESS PIPELINE
# =============================================================================
def test_http_json_processing_pipeline() -> None:
    response = FakeResponse(
        text=(
            '{"records": ['
            '{"id": 1, "name": "alpha"},'
            '{"id": 2, "name": "beta"},'
            '{"id": 3, "name": "gamma"}'
            "]}"
        )
    )

    (
        session_manager,
        fake_session,
    ) = (
        build_external_session_manager(
            response
        )
    )

    bot = JsonProcessingBot(
        session_manager=(
            session_manager
        )
    )

    crawler = Crawler(
        bot
    )

    try:
        result = crawler.run()

        assert (
            result.status
            is ExecutionStatus.COMPLETED
        )

        assert (
            result.records_processed
            == 3
        )

        assert (
            result.errors
            == 0
        )

        assert (
            bot.records_processed
            == 3
        )

        assert (
            bot.document
            == {
                "records": [
                    {
                        "id": 1,
                        "name": "alpha",
                    },
                    {
                        "id": 2,
                        "name": "beta",
                    },
                    {
                        "id": 3,
                        "name": "gamma",
                    },
                ]
            }
        )

        assert (
            fake_session.request_count
            == 1
        )

        assert (
            fake_session.last_method
            == "GET"
        )

        assert (
            fake_session.last_url
            == JsonProcessingBot.URL
        )

        assert (
            result.metadata[
                "source_url"
            ]
            == JsonProcessingBot.URL
        )

        assert (
            result.metadata[
                "record_count"
            ]
            == 3
        )

    finally:
        bot.close()

        session_manager.close()


# =============================================================================
# PROCESSING FAILURE
# =============================================================================
def test_invalid_json_becomes_failed_execution_result() -> None:
    response = FakeResponse(
        text=(
            '{"records": ['
        )
    )

    (
        session_manager,
        fake_session,
    ) = (
        build_external_session_manager(
            response
        )
    )

    bot = BrokenJsonBot(
        session_manager=(
            session_manager
        )
    )

    crawler = Crawler(
        bot
    )

    try:
        result = crawler.run()

        assert (
            result.status
            is ExecutionStatus.FAILED
        )

        assert (
            result.errors
            >= 1
        )

        assert (
            fake_session.request_count
            == 1
        )

        failure = (
            result.metadata.get(
                "failure"
            )
        )

        assert isinstance(
            failure,
            dict,
        )

        assert (
            failure.get(
                "exception_type"
            )
            == "JsonProcessingError"
        )

        assert (
            failure.get(
                "message"
            )
        )

    finally:
        bot.close()

        session_manager.close()


# =============================================================================
# PROCESSING RESULT PRESERVATION
# =============================================================================
def test_processing_result_metadata_survives_lifecycle() -> None:
    response = FakeResponse(
        text=(
            '{"records": ['
            '{"id": 100}'
            "]}"
        )
    )

    (
        session_manager,
        _,
    ) = (
        build_external_session_manager(
            response
        )
    )

    bot = JsonProcessingBot(
        bot_name=(
            "integration-json-bot"
        ),
        session_manager=(
            session_manager
        ),
    )

    crawler = Crawler(
        bot
    )

    try:
        result = crawler.run()

        assert (
            result.status
            is ExecutionStatus.COMPLETED
        )

        assert (
            result.metadata[
                "record_count"
            ]
            == 1
        )

        assert (
            result.metadata[
                "source_url"
            ]
            == JsonProcessingBot.URL
        )

        assert (
            "bot"
            in result.metadata
        )

        bot_metadata = (
            result.metadata[
                "bot"
            ]
        )

        assert (
            bot_metadata[
                "bot_name"
            ]
            == "integration-json-bot"
        )

        assert (
            bot_metadata[
                "run_count"
            ]
            == 1
        )

        assert (
            bot_metadata[
                "started_at"
            ]
            is not None
        )

        assert (
            bot_metadata[
                "finished_at"
            ]
            is not None
        )

    finally:
        bot.close()

        session_manager.close()


# =============================================================================
# RESOURCE OWNERSHIP
# =============================================================================
def test_external_processing_session_is_not_owned_by_bot() -> None:
    response = FakeResponse(
        text=(
            '{"records": []}'
        )
    )

    (
        session_manager,
        fake_session,
    ) = (
        build_external_session_manager(
            response
        )
    )

    bot = JsonProcessingBot(
        session_manager=(
            session_manager
        )
    )

    crawler = Crawler(
        bot
    )

    result = crawler.run()

    assert (
        result.status
        is ExecutionStatus.COMPLETED
    )

    bot.close()

    # BaseBot'a inject edilen SessionManager
    # bot tarafından sahiplenilmez.
    assert (
        fake_session.closed
        is False
    )

    session_manager.close()

    # SessionManager'a inject edilen session da
    # SessionManager tarafından sahiplenilmiyorsa
    # açık kalmalıdır.
    #
    # Mevcut SessionManager ownership contract'ına
    # göre external session close edilmez.
    assert (
        fake_session.closed
        is False
    )