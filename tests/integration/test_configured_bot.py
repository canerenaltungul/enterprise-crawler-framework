from __future__ import annotations

from pathlib import Path

from enterprise_crawler.config import (
    ConfigLoader,
    CrawlerSettings,
    DownloadSettings,
    HTTPSettings,
    StorageSettings,
)
from enterprise_crawler.contracts import (
    ExecutionResult,
)
from enterprise_crawler.contracts.enums import (
    ExecutionStatus,
)
from enterprise_crawler.core.base_bot import (
    BaseBot,
)
from enterprise_crawler.core.crawler import (
    Crawler,
)


class ConfiguredBot(
    BaseBot
):
    def execute(
        self,
    ) -> ExecutionResult:
        storage = (
            self.require_storage()
        )

        storage.save_json(
            "records/configured.json",
            {
                "configured": True,
            },
        )

        storage.mark_seen(
            "configured-bot",
            "record-1",
        )

        self.mark_record_processed()

        return ExecutionResult(
            status=(
                ExecutionStatus.COMPLETED
            ),
            records_processed=1,
        )


def test_crawler_settings_build_runtime_stack(
    tmp_path: Path,
) -> None:
    root = (
        tmp_path
        / "runtime-storage"
    )

    settings = CrawlerSettings(
        http=HTTPSettings(
            timeout_seconds=7,
            max_retries=1,
            backoff_factor=0.2,
            pool_connections=3,
            pool_maxsize=8,
            verify_tls=True,
        ),
        download=DownloadSettings(
            chunk_size=2048,
            max_bytes=50_000,
        ),
        storage=StorageSettings(
            enabled=True,
            root=root,
            state_path=(
                ".state/custom.db"
            ),
            sqlite_timeout_seconds=4,
        ),
    )

    bot = ConfiguredBot(
        settings=settings
    )

    crawler = Crawler(
        bot
    )

    try:
        assert (
            bot.settings
            is settings
        )

        assert (
            bot.storage
            is not None
        )

        storage = (
            bot.require_storage()
        )

        assert (
            storage.root
            == root.resolve()
        )

        assert (
            storage.state_db_path
            == (
                root
                / ".state"
                / "custom.db"
            ).resolve()
        )

        session_snapshot = (
            bot.session_manager.snapshot()
        )

        assert (
            session_snapshot[
                "max_retries"
            ]
            == 1
        )

        assert (
            session_snapshot[
                "backoff_factor"
            ]
            == 0.2
        )

        assert (
            session_snapshot[
                "pool_connections"
            ]
            == 3
        )

        assert (
            session_snapshot[
                "pool_maxsize"
            ]
            == 8
        )

        http_snapshot = (
            bot.http.snapshot()
        )

        assert (
            http_snapshot[
                "timeout_seconds"
            ]
            == 7.0
        )

        result = crawler.run()

        assert (
            result.status
            is ExecutionStatus.COMPLETED
        )

        assert (
            result.records_processed
            == 1
        )

        assert (
            storage.load_json(
                "records/configured.json"
            )
            == {
                "configured": True,
            }
        )

        assert (
            storage.has_seen(
                "configured-bot",
                "record-1",
            )
            is True
        )

        runtime = (
            bot.runtime_snapshot()
        )

        assert (
            runtime[
                "settings_configured"
            ]
            is True
        )

        assert (
            runtime[
                "settings"
            ][
                "http"
            ][
                "timeout_seconds"
            ]
            == 7.0
        )

        assert (
            runtime[
                "settings"
            ][
                "download"
            ][
                "chunk_size"
            ]
            == 2048
        )

        assert (
            runtime[
                "settings"
            ][
                "storage"
            ][
                "enabled"
            ]
            is True
        )

    finally:
        bot.close()


def test_config_loader_output_can_directly_configure_bot(
    tmp_path: Path,
) -> None:
    settings = (
        ConfigLoader.from_mapping(
            {
                "http": {
                    "timeout_seconds": 9,
                    "max_retries": 0,
                },
                "download": {
                    "chunk_size": 4096,
                    "max_bytes": 100_000,
                },
                "storage": {
                    "enabled": True,
                    "root": str(
                        tmp_path
                        / "storage"
                    ),
                },
            }
        )
    )

    bot = ConfiguredBot(
        settings=settings
    )

    try:
        result = (
            Crawler(
                bot
            ).run()
        )

        assert (
            result.status
            is ExecutionStatus.COMPLETED
        )

        assert (
            bot.require_storage().exists(
                "records/configured.json"
            )
            is True
        )

        assert (
            bot.http.snapshot()[
                "timeout_seconds"
            ]
            == 9.0
        )

    finally:
        bot.close()


def test_disabled_storage_settings_do_not_create_storage() -> None:
    settings = CrawlerSettings(
        storage=StorageSettings(
            enabled=False,
            root="preconfigured-data",
        )
    )

    class NoStorageBot(
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

    bot = NoStorageBot(
        settings=settings
    )

    try:
        assert (
            bot.storage
            is None
        )

        assert (
            bot.runtime_snapshot()[
                "storage_enabled"
            ]
            is False
        )

    finally:
        bot.close()


def test_explicit_runtime_arguments_override_settings(
    tmp_path: Path,
) -> None:
    settings = CrawlerSettings(
        http=HTTPSettings(
            timeout_seconds=20,
        ),
        download=DownloadSettings(
            chunk_size=1024,
            max_bytes=2000,
        ),
        storage=StorageSettings(
            enabled=True,
            root=(
                tmp_path
                / "settings-storage"
            ),
        ),
    )

    explicit_root = (
        tmp_path
        / "explicit-storage"
    )

    class OverrideBot(
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

    bot = OverrideBot(
        settings=settings,
        request_timeout_seconds=5,
        download_chunk_size=8192,
        max_download_bytes=None,
        storage_root=explicit_root,
    )

    try:
        assert (
            bot.http.snapshot()[
                "timeout_seconds"
            ]
            == 5.0
        )

        assert (
            bot.require_storage().root
            == explicit_root.resolve()
        )

    finally:
        bot.close()