from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from enterprise_crawler.contracts import ExecutionResult
from enterprise_crawler.contracts.enums import ExecutionStatus
from enterprise_crawler.core.base_bot import BaseBot
from enterprise_crawler.core.crawler import Crawler
from enterprise_crawler.core.http_client import HttpClient
from enterprise_crawler.core.session import SessionManager


# =============================================================================
# TEST DOUBLES
# =============================================================================
PAYLOAD = b"enterprise-crawler-payload"

RECORD_KEY = hashlib.sha256(
    PAYLOAD
).hexdigest()


class FakeResponse:
    def __init__(self) -> None:
        self.status_code = 200

        self.headers = {
            "Content-Type": (
                "application/octet-stream"
            ),
            "Content-Length": str(
                len(PAYLOAD)
            ),
        }

        self.closed = False

    def iter_content(
        self,
        *,
        chunk_size: int,
    ):
        midpoint = (
            len(PAYLOAD)
            // 2
        )

        yield PAYLOAD[
            :midpoint
        ]

        yield PAYLOAD[
            midpoint:
        ]

    def close(self) -> None:
        self.closed = True


class FakeSession:
    def __init__(self) -> None:
        self.headers: dict[
            str,
            str,
        ] = {}

        self.calls: list[
            dict[str, Any]
        ] = []

        self.responses: list[
            FakeResponse
        ] = []

        self.closed = False

    def request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> FakeResponse:
        self.calls.append(
            {
                "method": method,
                "url": url,
                **kwargs,
            }
        )

        response = (
            FakeResponse()
        )

        self.responses.append(
            response
        )

        return response

    def close(self) -> None:
        self.closed = True


# =============================================================================
# BOT
# =============================================================================
class PersistentDownloadBot(
    BaseBot
):
    def execute(
        self,
    ) -> ExecutionResult:
        storage = (
            self.require_storage()
        )

        namespace = (
            "download-records"
        )

        # Aynı logical record daha önce başarıyla işlendi mi?
        if storage.has_seen(
            namespace,
            RECORD_KEY,
        ):
            return ExecutionResult(
                status=(
                    ExecutionStatus.COMPLETED
                ),
                records_processed=0,
                metadata={
                    "duplicate": True,
                    "record_key": (
                        RECORD_KEY
                    ),
                },
            )

        target = (
            storage.files.resolve(
                "downloads/payload.bin"
            )
        )

        download = (
            self.downloader.download(
                "https://example.com/payload.bin",
                target,
                overwrite=True,
                expected_sha256=(
                    RECORD_KEY
                ),
            )
        )

        # Downloader tarafından üretilen dosyayı storage API üzerinden tekrar
        # okuyabiliyoruz.
        persisted = (
            storage.load_bytes(
                "downloads/payload.bin"
            )
        )

        if persisted != PAYLOAD:
            raise RuntimeError(
                "Persist edilen payload "
                "beklenen içerikle eşleşmiyor."
            )

        # Idempotency yalnız download + persistence başarıyla tamamlandıktan
        # sonra commit edilir.
        first_seen = (
            storage.mark_seen(
                namespace,
                RECORD_KEY,
                metadata={
                    "sha256": (
                        download.sha256
                    ),
                    "size_bytes": (
                        download.size_bytes
                    ),
                    "path": (
                        "downloads/payload.bin"
                    ),
                },
            )
        )

        if not first_seen:
            raise RuntimeError(
                "Record beklenmedik biçimde "
                "önceden işlenmiş görünüyor."
            )

        self.mark_record_processed()

        return ExecutionResult(
            status=(
                ExecutionStatus.COMPLETED
            ),
            records_processed=1,
            metadata={
                "duplicate": False,
                "record_key": (
                    RECORD_KEY
                ),
                "download": (
                    download.to_dict()
                ),
            },
        )


# =============================================================================
# INTEGRATION
# =============================================================================
def test_http_download_storage_idempotency_pipeline(
    tmp_path: Path,
) -> None:
    fake_session = (
        FakeSession()
    )

    session_manager = (
        SessionManager(
            session=fake_session,  # type: ignore[arg-type]
        )
    )

    http_client = HttpClient(
        session=(
            session_manager.session
        ),
    )

    bot = PersistentDownloadBot(
        session_manager=(
            session_manager
        ),
        http_client=(
            http_client
        ),
        storage_root=(
            tmp_path
            / "crawler-storage"
        ),
    )

    crawler = Crawler(
        bot
    )

    try:
        # ---------------------------------------------------------------------
        # FIRST RUN
        # ---------------------------------------------------------------------
        first = crawler.run()

        assert (
            first.status
            is ExecutionStatus.COMPLETED
        )

        assert (
            first.records_processed
            == 1
        )

        assert (
            first.metadata[
                "duplicate"
            ]
            is False
        )

        assert (
            len(fake_session.calls)
            == 1
        )

        assert (
            fake_session.calls[0][
                "method"
            ]
            == "GET"
        )

        storage = (
            bot.require_storage()
        )

        assert (
            storage.load_bytes(
                "downloads/payload.bin"
            )
            == PAYLOAD
        )

        assert (
            storage.has_seen(
                "download-records",
                RECORD_KEY,
            )
            is True
        )

        seen = (
            storage.state.get_seen(
                "download-records",
                RECORD_KEY,
            )
        )

        assert seen is not None

        assert (
            seen.metadata[
                "sha256"
            ]
            == RECORD_KEY
        )

        assert (
            seen.metadata[
                "size_bytes"
            ]
            == len(PAYLOAD)
        )

        # ---------------------------------------------------------------------
        # SECOND RUN
        # ---------------------------------------------------------------------
        second = crawler.run()

        assert (
            second.status
            is ExecutionStatus.COMPLETED
        )

        assert (
            second.records_processed
            == 0
        )

        assert (
            second.metadata[
                "duplicate"
            ]
            is True
        )

        # En kritik invariant:
        # ikinci run duplicate olduğu için yeni HTTP request yok.
        assert (
            len(fake_session.calls)
            == 1
        )

        assert (
            bot.run_count
            == 2
        )

        assert (
            crawler.run_count
            == 2
        )

        assert (
            storage.state.seen_count(
                "download-records"
            )
            == 1
        )

    finally:
        bot.close()

        http_client.close()
        session_manager.close()


def test_basebot_storage_is_opt_in(
    tmp_path: Path,
) -> None:
    class StorageProbeBot(
        BaseBot
    ):
        def execute(
            self,
        ) -> ExecutionResult:
            return ExecutionResult(
                status=(
                    ExecutionStatus.COMPLETED
                )
            )

    without_storage = (
        StorageProbeBot()
    )

    try:
        assert (
            without_storage.storage
            is None
        )

        snapshot = (
            without_storage.runtime_snapshot()
        )

        assert (
            snapshot[
                "storage_enabled"
            ]
            is False
        )

    finally:
        without_storage.close()

    with_storage = (
        StorageProbeBot(
            storage_root=(
                tmp_path
                / "storage"
            )
        )
    )

    try:
        assert (
            with_storage.storage
            is not None
        )

        snapshot = (
            with_storage.runtime_snapshot()
        )

        assert (
            snapshot[
                "storage_enabled"
            ]
            is True
        )

        assert (
            snapshot["storage"]
            is not None
        )

    finally:
        with_storage.close()


def test_owned_storage_is_closed_with_bot(
    tmp_path: Path,
) -> None:
    class StorageProbeBot(
        BaseBot
    ):
        def execute(
            self,
        ) -> ExecutionResult:
            return ExecutionResult(
                status=(
                    ExecutionStatus.COMPLETED
                )
            )

    bot = StorageProbeBot(
        storage_root=(
            tmp_path
            / "storage"
        )
    )

    storage = (
        bot.require_storage()
    )

    assert (
        storage.is_closed
        is False
    )

    bot.close()

    assert (
        storage.is_closed
        is True
    )