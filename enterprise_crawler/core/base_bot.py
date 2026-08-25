from __future__ import annotations

"""
Enterprise Crawler Framework - BaseBot

Bütün framework botlarının ortak çalışma sözleşmesi.

Runtime
-------
Crawler
   ↓
BaseBot.run()
   ↓
LifecycleRunner

Composition
-----------
BaseBot
├── SessionManager
├── HttpClient
├── Downloader
├── StorageManager [opt-in]
└── PluginManager  [opt-in]

Configuration
-------------
CrawlerSettings kullanıcı-facing configuration modelidir.

Runtime precedence:

    injected component
        ↓
    explicit BaseBot argument
        ↓
    CrawlerSettings
        ↓
    framework default

Storage varsayılan olarak opt-in'dir.

Storage şu yollardan biriyle etkinleşir:

    storage_manager=StorageManager(...)

veya::

    storage_root="data"

veya::

    settings=CrawlerSettings(
        storage=StorageSettings(
            enabled=True,
            root="data",
        )
    )

Plugin sistemi de opt-in'dir.

PluginManager yalnızca açıkça inject edildiğinde etkinleşir::

    plugin_manager=PluginManager(...)

BaseBot inject edilmiş PluginManager'ın ownership'ini almaz ve
BaseBot.close() tarafından kapatmaz.

Basit/HTTP-only botlar disk, SQLite veya plugin side-effect üretmez.
"""

import threading
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Mapping, Optional

from enterprise_crawler.config import (
    CrawlerSettings,
)
from enterprise_crawler.contracts import ExecutionResult
from enterprise_crawler.contracts.enums import ExecutionStatus
from enterprise_crawler.core.downloader import Downloader
from enterprise_crawler.core.http_client import HttpClient
from enterprise_crawler.core.lifecycle import LifecycleRunner
from enterprise_crawler.core.session import (
    SessionConfig,
    SessionManager,
)
from enterprise_crawler.exceptions import (
    AlreadyRunningError,
    ConfigurationError,
    ContractValidationError,
    PluginError,
    ShutdownRequested,
    StorageError,
)
from enterprise_crawler.plugins import PluginManager
from enterprise_crawler.storage import StorageManager


UTC = timezone.utc

_UNSET = object()


# =============================================================================
# HELPERS
# =============================================================================
def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_now() -> str:
    return utc_now().isoformat()


def _safe_exception_message(
    error: BaseException,
) -> str:
    message = str(error).strip()

    if not message:
        message = error.__class__.__name__

    return message[:8_000]


def _normalize_non_negative_int(
    value: Any,
    *,
    field_name: str,
) -> int:
    if isinstance(value, bool):
        raise ContractValidationError(
            f"{field_name} boolean olamaz."
        )

    try:
        normalized = int(value)

    except (TypeError, ValueError) as exc:
        raise ContractValidationError(
            f"{field_name} tam sayı olmalıdır."
        ) from exc

    if normalized < 0:
        raise ContractValidationError(
            f"{field_name} negatif olamaz."
        )

    return normalized


def _normalize_status(
    value: Any,
) -> ExecutionStatus:
    if isinstance(
        value,
        ExecutionStatus,
    ):
        return value

    normalized = str(
        value or ""
    ).strip().lower()

    try:
        return ExecutionStatus(
            normalized
        )

    except ValueError as exc:
        raise ContractValidationError(
            f"Geçersiz execution status: {value!r}"
        ) from exc


# =============================================================================
# BASE BOT
# =============================================================================
class BaseBot:
    """
    Enterprise Crawler Framework botlarının temel sınıfı.

    Basit::

        class HelloBot(BaseBot):
            def execute(self):
                print("hello")

    HTTP::

        class ApiBot(BaseBot):
            def execute(self):
                response = self.http.get(
                    "https://example.com"
                )

    Storage::

        bot = MyBot(
            storage_root="data"
        )

    Plugins::

        bot = MyBot(
            plugin_manager=PluginManager()
        )

        plugins = bot.require_plugins()

    Configuration::

        bot = MyBot(
            settings=CrawlerSettings(...)
        )
    """

    ENGINE_VERSION = "0.5.0"

    # =========================================================================
    # SUBCLASS CONTRACT
    # =========================================================================
    def __init_subclass__(
        cls,
        **kwargs: Any,
    ) -> None:
        super().__init_subclass__(
            **kwargs
        )

        if "run" in cls.__dict__:
            raise TypeError(
                f"{cls.__name__} run() metodunu override edemez. "
                "İş mantığını execute() içine yaz."
            )

    # =========================================================================
    # CONSTRUCTION
    # =========================================================================
    def __init__(
        self,
        *,
        bot_name: Optional[str] = None,
        stop_event: Optional[Any] = None,

        # User-facing configuration
        settings: Optional[
            CrawlerSettings
        ] = None,

        # HTTP explicit overrides
        request_timeout_seconds: Optional[
            float
        ] = None,
        proxies: Optional[
            list[str]
        ] = None,
        user_agents: Optional[
            list[str] | tuple[str, ...]
        ] = None,
        allow_insecure_tls: Optional[
            bool
        ] = None,

        # Session explicit override
        session_config: Optional[
            SessionConfig
        ] = None,

        # Downloader explicit overrides
        download_chunk_size: Optional[
            int
        ] = None,
        max_download_bytes: Any = _UNSET,

        # Storage explicit overrides
        storage_root: Optional[
            str | Path
        ] = None,
        storage_state_db_path: Optional[
            str | Path
        ] = None,
        storage_state_timeout_seconds: Optional[
            float
        ] = None,

        # Dependency injection
        session_manager: Optional[
            SessionManager
        ] = None,
        http_client: Optional[
            HttpClient
        ] = None,
        downloader: Optional[
            Downloader
        ] = None,
        storage_manager: Optional[
            StorageManager
        ] = None,
        plugin_manager: Optional[
            PluginManager
        ] = None,
    ) -> None:
        # ---------------------------------------------------------------------
        # SETTINGS
        # ---------------------------------------------------------------------
        if (
            settings is not None
            and not isinstance(
                settings,
                CrawlerSettings,
            )
        ):
            raise ConfigurationError(
                "settings CrawlerSettings olmalıdır."
            )

        self.settings = settings

        effective_settings = (
            settings
            if settings is not None
            else CrawlerSettings()
        )

        # ---------------------------------------------------------------------
        # IDENTITY
        # ---------------------------------------------------------------------
        resolved_name = str(
            bot_name
            or self.__class__.__name__
        ).strip()

        if not resolved_name:
            raise ValueError(
                "bot_name boş olamaz."
            )

        self.bot_name = (
            resolved_name
        )

        # ---------------------------------------------------------------------
        # EFFECTIVE HTTP SETTINGS
        # ---------------------------------------------------------------------
        resolved_request_timeout = (
            request_timeout_seconds
            if request_timeout_seconds is not None
            else effective_settings.http.timeout_seconds
        )

        if allow_insecure_tls is None:
            resolved_allow_insecure_tls = (
                not effective_settings.http.verify_tls
            )

        else:
            resolved_allow_insecure_tls = (
                allow_insecure_tls
            )

        # ---------------------------------------------------------------------
        # EFFECTIVE DOWNLOAD SETTINGS
        # ---------------------------------------------------------------------
        resolved_download_chunk_size = (
            download_chunk_size
            if download_chunk_size is not None
            else effective_settings.download.chunk_size
        )

        if max_download_bytes is _UNSET:
            resolved_max_download_bytes = (
                effective_settings.download.max_bytes
            )

        else:
            resolved_max_download_bytes = (
                max_download_bytes
            )

        # ---------------------------------------------------------------------
        # STORAGE PRECEDENCE VALIDATION
        # ---------------------------------------------------------------------
        if (
            storage_manager is not None
            and storage_root is not None
        ):
            raise StorageError(
                "storage_manager ve storage_root "
                "aynı anda verilemez."
            )

        # ---------------------------------------------------------------------
        # PLUGIN VALIDATION
        # ---------------------------------------------------------------------
        if (
            plugin_manager is not None
            and not isinstance(
                plugin_manager,
                PluginManager,
            )
        ):
            raise PluginError(
                "plugin_manager PluginManager olmalıdır."
            )

        # ---------------------------------------------------------------------
        # STOP EVENT
        # ---------------------------------------------------------------------
        self.stop_event = (
            stop_event
            if stop_event is not None
            else threading.Event()
        )

        # ---------------------------------------------------------------------
        # RUN STATE
        # ---------------------------------------------------------------------
        self._run_lock = (
            threading.Lock()
        )

        self._state_lock = (
            threading.RLock()
        )

        self._is_running = False
        self._run_count = 0

        self._run_started_at: Optional[
            datetime
        ] = None

        self._run_finished_at: Optional[
            datetime
        ] = None

        self._last_result: Optional[
            ExecutionResult
        ] = None

        self._records_processed = 0
        self._error_count = 0
        self._warning_count = 0

        self._runtime_metadata: dict[
            str,
            Any,
        ] = {}

        # ---------------------------------------------------------------------
        # LIFECYCLE
        # ---------------------------------------------------------------------
        self._lifecycle_runner = (
            LifecycleRunner()
        )

        # ---------------------------------------------------------------------
        # SESSION
        #
        # Precedence:
        # injected manager
        #   > explicit SessionConfig
        #   > CrawlerSettings.http
        # ---------------------------------------------------------------------
        if session_manager is None:
            if session_config is None:
                resolved_session_config = (
                    SessionConfig(
                        max_retries=(
                            effective_settings.http.max_retries
                        ),
                        backoff_factor=(
                            effective_settings.http.backoff_factor
                        ),
                        pool_connections=(
                            effective_settings.http.pool_connections
                        ),
                        pool_maxsize=(
                            effective_settings.http.pool_maxsize
                        ),
                    )
                )

            else:
                resolved_session_config = (
                    session_config
                )

            self.session_manager = (
                SessionManager(
                    config=(
                        resolved_session_config
                    )
                )
            )

            self._owns_session_manager = True

        else:
            self.session_manager = (
                session_manager
            )

            self._owns_session_manager = False

        # ---------------------------------------------------------------------
        # HTTP
        #
        # Injected client wins.
        # Otherwise effective HTTP settings are applied.
        # ---------------------------------------------------------------------
        if http_client is None:
            self.http = HttpClient(
                session=(
                    self.session_manager.session
                ),
                timeout_seconds=(
                    resolved_request_timeout
                ),
                proxies=proxies,
                user_agents=user_agents,
                allow_insecure_tls=(
                    resolved_allow_insecure_tls
                ),
                stop_check=(
                    self.raise_if_stopping
                ),
            )

            self._owns_http_client = True

        else:
            self.http = (
                http_client
            )

            self._owns_http_client = False

        # ---------------------------------------------------------------------
        # DOWNLOADER
        # ---------------------------------------------------------------------
        if downloader is None:
            self.downloader = Downloader(
                self.http,
                chunk_size=(
                    resolved_download_chunk_size
                ),
                max_download_bytes=(
                    resolved_max_download_bytes
                ),
                stop_check=(
                    self.raise_if_stopping
                ),
            )

            self._owns_downloader = True

        else:
            self.downloader = (
                downloader
            )

            self._owns_downloader = False

        # ---------------------------------------------------------------------
        # STORAGE
        #
        # Precedence:
        # injected StorageManager
        #   > explicit storage_root
        #   > enabled StorageSettings
        # ---------------------------------------------------------------------
        if storage_manager is not None:
            self.storage: Optional[
                StorageManager
            ] = storage_manager

            self._owns_storage_manager = False

        elif storage_root is not None:
            resolved_storage_timeout = (
                storage_state_timeout_seconds
                if storage_state_timeout_seconds is not None
                else effective_settings.storage.sqlite_timeout_seconds
            )

            resolved_state_db_path = (
                storage_state_db_path
                if storage_state_db_path is not None
                else effective_settings.storage.state_path
            )

            self.storage = StorageManager(
                storage_root,
                state_db_path=(
                    resolved_state_db_path
                ),
                state_timeout_seconds=(
                    resolved_storage_timeout
                ),
            )

            self._owns_storage_manager = True

        elif effective_settings.storage.enabled:
            if effective_settings.storage.root is None:
                # StorageSettings zaten bunu validate eder.
                # Buradaki guard defense-in-depth amaçlıdır.
                raise ConfigurationError(
                    "Storage enabled fakat root tanımlı değil."
                )

            self.storage = StorageManager(
                effective_settings.storage.root,
                state_db_path=(
                    effective_settings.storage.state_path
                ),
                state_timeout_seconds=(
                    effective_settings.storage.sqlite_timeout_seconds
                ),
            )

            self._owns_storage_manager = True

        else:
            self.storage = None

            self._owns_storage_manager = False

        # ---------------------------------------------------------------------
        # PLUGINS
        #
        # PluginManager şu aşamada yalnız dependency injection ile
        # etkinleştirilir.
        #
        # BaseBot inject edilmiş manager'ın ownership'ini almaz.
        # ---------------------------------------------------------------------
        self.plugins: Optional[
            PluginManager
        ] = plugin_manager

        self._owns_plugin_manager = False

    # =========================================================================
    # PUBLIC STATE
    # =========================================================================
    @property
    def is_running(
        self,
    ) -> bool:
        with self._state_lock:
            return self._is_running

    @property
    def run_count(
        self,
    ) -> int:
        with self._state_lock:
            return self._run_count

    @property
    def last_result(
        self,
    ) -> Optional[ExecutionResult]:
        with self._state_lock:
            return self._last_result

    @property
    def records_processed(
        self,
    ) -> int:
        with self._state_lock:
            return self._records_processed

    @property
    def error_count(
        self,
    ) -> int:
        with self._state_lock:
            return self._error_count

    @property
    def warning_count(
        self,
    ) -> int:
        with self._state_lock:
            return self._warning_count

    # =========================================================================
    # STORAGE CONTRACT
    # =========================================================================
    def require_storage(
        self,
    ) -> StorageManager:
        """
        Storage-enabled botlarda kullanılacak fail-fast helper.
        """

        storage = self.storage

        if storage is None:
            raise StorageError(
                "Bu bot için storage yapılandırılmamış "
                f"| bot={self.bot_name}. "
                "storage_root, storage_manager veya "
                "enabled StorageSettings ver."
            )

        if storage.is_closed:
            raise StorageError(
                "Bot storage manager kapalı "
                f"| bot={self.bot_name}"
            )

        return storage

    # =========================================================================
    # PLUGIN CONTRACT
    # =========================================================================
    def require_plugins(
        self,
    ) -> PluginManager:
        """
        Plugin-enabled botlarda kullanılacak fail-fast helper.

        PluginManager configure edilmemişse framework seviyesinde
        anlaşılır hata üretir.

        Kapalı PluginManager da kullanılabilir bir runtime resource değildir.
        """

        plugins = self.plugins

        if plugins is None:
            raise PluginError(
                "Bu bot için PluginManager yapılandırılmamış "
                f"| bot={self.bot_name}. "
                "plugin_manager ver."
            )

        is_closed = getattr(
            plugins,
            "is_closed",
            None,
        )

        if callable(is_closed):
            closed = bool(
                is_closed()
            )

        elif is_closed is not None:
            closed = bool(
                is_closed
            )

        else:
            snapshot_method = getattr(
                plugins,
                "snapshot",
                None,
            )

            closed = False

            if callable(snapshot_method):
                try:
                    snapshot = (
                        snapshot_method()
                    )

                    if isinstance(
                        snapshot,
                        Mapping,
                    ):
                        closed = bool(
                            snapshot.get(
                                "closed",
                                False,
                            )
                        )

                except Exception:
                    closed = False

        if closed:
            raise PluginError(
                "Bot PluginManager kapalı "
                f"| bot={self.bot_name}"
            )

        return plugins

    # =========================================================================
    # COOPERATIVE SHUTDOWN
    # =========================================================================
    def should_stop(
        self,
    ) -> bool:
        event = self.stop_event

        is_set = getattr(
            event,
            "is_set",
            None,
        )

        if callable(
            is_set
        ):
            return bool(
                is_set()
            )

        return bool(event)

    def request_stop(
        self,
    ) -> None:
        setter = getattr(
            self.stop_event,
            "set",
            None,
        )

        if callable(
            setter
        ):
            setter()
            return

        self.stop_event = True

    def reset_stop_request(
        self,
    ) -> None:
        if self.is_running:
            raise AlreadyRunningError(
                "Çalışan bot'un stop sinyali resetlenemez "
                f"| bot={self.bot_name}"
            )

        clearer = getattr(
            self.stop_event,
            "clear",
            None,
        )

        if callable(
            clearer
        ):
            clearer()

        else:
            self.stop_event = False

    def raise_if_stopping(
        self,
    ) -> None:
        if self.should_stop():
            raise ShutdownRequested(
                "Shutdown requested "
                f"| bot={self.bot_name}"
            )

    # =========================================================================
    # COUNTERS
    # =========================================================================
    def mark_record_processed(
        self,
        count: int = 1,
    ) -> None:
        increment = (
            _normalize_non_negative_int(
                count,
                field_name="count",
            )
        )

        with self._state_lock:
            self._records_processed += (
                increment
            )

    def mark_error(
        self,
        count: int = 1,
    ) -> None:
        increment = (
            _normalize_non_negative_int(
                count,
                field_name="count",
            )
        )

        with self._state_lock:
            self._error_count += (
                increment
            )

    def mark_warning(
        self,
        count: int = 1,
    ) -> None:
        increment = (
            _normalize_non_negative_int(
                count,
                field_name="count",
            )
        )

        with self._state_lock:
            self._warning_count += (
                increment
            )

    def set_runtime_metadata(
        self,
        key: str,
        value: Any,
    ) -> None:
        normalized_key = str(
            key or ""
        ).strip()

        if not normalized_key:
            raise ValueError(
                "metadata key boş olamaz."
            )

        with self._state_lock:
            self._runtime_metadata[
                normalized_key
            ] = value

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================
    def initialize(
        self,
    ) -> None:
        pass

    def before_run(
        self,
    ) -> None:
        pass

    def after_run(
        self,
        result: ExecutionResult,
    ) -> Optional[
        ExecutionResult
    ]:
        return None

    def cleanup(
        self,
    ) -> None:
        """
        Run-scoped cleanup.

        Session, storage ve plugin manager burada kapatılmaz.

        Aynı bot instance'ının ardışık run'larda yeniden kullanılabilmesi için
        process/resource cleanup close() metoduna bırakılmıştır.
        """

    # =========================================================================
    # EXECUTION CONTRACT
    # =========================================================================
    def execute(
        self,
    ) -> Any:
        scrape_method = getattr(
            self,
            "scrape",
            None,
        )

        if not callable(
            scrape_method
        ):
            raise NotImplementedError(
                f"{self.__class__.__name__} "
                "execute() metodunu implement etmelidir."
            )

        return scrape_method()

    def parse(
        self,
        raw_data: Any,
    ) -> Any:
        return raw_data

    # =========================================================================
    # RESULT NORMALIZATION
    # =========================================================================
    def _normalize_execution_result(
        self,
        result: Any,
    ) -> ExecutionResult:
        if isinstance(
            result,
            ExecutionResult,
        ):
            return (
                self._validate_execution_result(
                    result
                )
            )

        if isinstance(
            result,
            Mapping,
        ):
            execution_result = ExecutionResult(
                status=_normalize_status(
                    result.get(
                        "status",
                        ExecutionStatus.COMPLETED,
                    )
                ),
                records_processed=(
                    _normalize_non_negative_int(
                        result.get(
                            "records_processed",
                            self.records_processed,
                        ),
                        field_name=(
                            "records_processed"
                        ),
                    )
                ),
                errors=(
                    _normalize_non_negative_int(
                        result.get(
                            "errors",
                            self.error_count,
                        ),
                        field_name="errors",
                    )
                ),
                warnings=(
                    _normalize_non_negative_int(
                        result.get(
                            "warnings",
                            self.warning_count,
                        ),
                        field_name="warnings",
                    )
                ),
                metadata=dict(
                    result.get(
                        "metadata"
                    )
                    or {}
                ),
            )

            return (
                self._validate_execution_result(
                    execution_result
                )
            )

        if result is None:
            return ExecutionResult(
                status=(
                    ExecutionStatus.COMPLETED
                ),
                records_processed=(
                    self.records_processed
                ),
                errors=self.error_count,
                warnings=(
                    self.warning_count
                ),
                metadata={},
            )

        raise ContractValidationError(
            "execute() ExecutionResult, Mapping veya None "
            "döndürmelidir; "
            f"actual={type(result).__name__}."
        )

    @staticmethod
    def _validate_execution_result(
        result: ExecutionResult,
    ) -> ExecutionResult:
        if not isinstance(
            result.status,
            ExecutionStatus,
        ):
            raise ContractValidationError(
                "ExecutionResult.status "
                "ExecutionStatus olmalıdır."
            )

        result.records_processed = (
            _normalize_non_negative_int(
                result.records_processed,
                field_name=(
                    "records_processed"
                ),
            )
        )

        result.errors = (
            _normalize_non_negative_int(
                result.errors,
                field_name="errors",
            )
        )

        result.warnings = (
            _normalize_non_negative_int(
                result.warnings,
                field_name="warnings",
            )
        )

        if not isinstance(
            result.metadata,
            dict,
        ):
            raise ContractValidationError(
                "ExecutionResult.metadata "
                "dict olmalıdır."
            )

        return result

    # =========================================================================
    # RUN STATE
    # =========================================================================
    def _begin_run(
        self,
    ) -> float:
        acquired = (
            self._run_lock.acquire(
                blocking=False
            )
        )

        if not acquired:
            raise AlreadyRunningError(
                "Bot zaten çalışıyor "
                f"| bot={self.bot_name}"
            )

        with self._state_lock:
            if self._is_running:
                self._run_lock.release()

                raise AlreadyRunningError(
                    "Bot zaten çalışıyor "
                    f"| bot={self.bot_name}"
                )

            self._is_running = True
            self._run_count += 1

            self._run_started_at = (
                utc_now()
            )

            self._run_finished_at = None
            self._last_result = None

            self._records_processed = 0
            self._error_count = 0
            self._warning_count = 0

            self._runtime_metadata = {}

        return monotonic()

    def _finish_run_state(
        self,
        result: ExecutionResult,
        *,
        started_monotonic: float,
    ) -> ExecutionResult:
        finished_at = utc_now()

        duration_seconds = max(
            0.0,
            monotonic()
            - started_monotonic,
        )

        with self._state_lock:
            self._run_finished_at = (
                finished_at
            )

            lifecycle_metadata = {
                "bot_name": self.bot_name,
                "engine_version": (
                    self.ENGINE_VERSION
                ),
                "run_count": (
                    self._run_count
                ),
                "started_at": (
                    self._run_started_at.isoformat()
                    if self._run_started_at
                    else None
                ),
                "finished_at": (
                    finished_at.isoformat()
                ),
                "duration_seconds": round(
                    duration_seconds,
                    6,
                ),
            }

            combined_metadata = dict(
                result.metadata
                or {}
            )

            combined_metadata.setdefault(
                "bot",
                lifecycle_metadata,
            )

            if self._runtime_metadata:
                combined_metadata.setdefault(
                    "runtime",
                    dict(
                        self._runtime_metadata
                    ),
                )

            result.metadata = (
                combined_metadata
            )

            self._last_result = result
            self._is_running = False

        return result

    def _release_run_lock(
        self,
    ) -> None:
        try:
            self._run_lock.release()

        except RuntimeError:
            pass

    # =========================================================================
    # RESULT BUILDERS
    # =========================================================================
    def _cancelled_result(
        self,
        error: BaseException,
    ) -> ExecutionResult:
        return ExecutionResult(
            status=(
                ExecutionStatus.CANCELLED
            ),
            records_processed=(
                self.records_processed
            ),
            errors=self.error_count,
            warnings=(
                self.warning_count
            ),
            metadata={
                "cancellation": {
                    "exception_type": (
                        error.__class__.__name__
                    ),
                    "message": (
                        _safe_exception_message(
                            error
                        )
                    ),
                }
            },
        )

    def _failed_result(
        self,
        error: BaseException,
    ) -> ExecutionResult:
        error_count = max(
            1,
            self.error_count,
        )

        return ExecutionResult(
            status=(
                ExecutionStatus.FAILED
            ),
            records_processed=(
                self.records_processed
            ),
            errors=error_count,
            warnings=(
                self.warning_count
            ),
            metadata={
                "failure": {
                    "exception_type": (
                        error.__class__.__name__
                    ),
                    "message": (
                        _safe_exception_message(
                            error
                        )
                    ),
                }
            },
        )

    # =========================================================================
    # RUN
    # =========================================================================
    def run(
        self,
    ) -> ExecutionResult:
        return (
            self._lifecycle_runner.run(
                self
            )
        )

    # =========================================================================
    # RESOURCE CLEANUP
    # =========================================================================
    def close(
        self,
    ) -> None:
        """
        BaseBot'un sahip olduğu framework resource'larını kapatır.

        Inject edilmiş resource'lar kapatılmaz.

        Şu aşamada PluginManager yalnız inject edilebildiği için
        BaseBot tarafından kapatılmaz.
        """

        errors: list[
            BaseException
        ] = []

        if (
            self._owns_plugin_manager
            and self.plugins is not None
        ):
            try:
                self.plugins.close()

            except Exception as exc:
                errors.append(
                    exc
                )

        if (
            self._owns_storage_manager
            and self.storage is not None
        ):
            try:
                self.storage.close()

            except Exception as exc:
                errors.append(
                    exc
                )

        if self._owns_http_client:
            try:
                self.http.close()

            except Exception as exc:
                errors.append(
                    exc
                )

        if self._owns_session_manager:
            try:
                self.session_manager.close()

            except Exception as exc:
                errors.append(
                    exc
                )

        if errors:
            raise RuntimeError(
                "BaseBot resource cleanup başarısız "
                f"| bot={self.bot_name} "
                f"| error_count={len(errors)} "
                f"| first_error={errors[0]}"
            )

    def __enter__(
        self,
    ) -> "BaseBot":
        return self

    def __exit__(
        self,
        exc_type: Any,
        exc: Any,
        traceback: Any,
    ) -> None:
        self.close()

    # =========================================================================
    # SNAPSHOT
    # =========================================================================
    def runtime_snapshot(
        self,
    ) -> dict[str, Any]:
        with self._state_lock:
            plugin_snapshot = None

            if self.plugins is not None:
                snapshot_method = getattr(
                    self.plugins,
                    "snapshot",
                    None,
                )

                if callable(
                    snapshot_method
                ):
                    try:
                        plugin_snapshot = (
                            snapshot_method()
                        )

                    except Exception:
                        plugin_snapshot = {
                            "snapshot_error": True,
                        }

            return {
                "bot_name": (
                    self.bot_name
                ),
                "engine_version": (
                    self.ENGINE_VERSION
                ),
                "is_running": (
                    self._is_running
                ),
                "run_count": (
                    self._run_count
                ),
                "stop_requested": (
                    self.should_stop()
                ),
                "started_at": (
                    self._run_started_at.isoformat()
                    if self._run_started_at
                    else None
                ),
                "finished_at": (
                    self._run_finished_at.isoformat()
                    if self._run_finished_at
                    else None
                ),
                "records_processed": (
                    self._records_processed
                ),
                "errors": (
                    self._error_count
                ),
                "warnings": (
                    self._warning_count
                ),
                "last_status": (
                    self._last_result.status.value
                    if self._last_result
                    else None
                ),
                "settings_configured": (
                    self.settings
                    is not None
                ),
                "settings": (
                    self.settings.to_dict()
                    if self.settings
                    is not None
                    else None
                ),
                "http": (
                    self.http.snapshot()
                    if hasattr(
                        self.http,
                        "snapshot",
                    )
                    else None
                ),
                "session": (
                    self.session_manager.snapshot()
                    if hasattr(
                        self.session_manager,
                        "snapshot",
                    )
                    else None
                ),
                "storage_enabled": (
                    self.storage
                    is not None
                ),
                "storage": (
                    self.storage.snapshot()
                    if (
                        self.storage
                        is not None
                        and not self.storage.is_closed
                    )
                    else None
                ),
                "plugins_enabled": (
                    self.plugins
                    is not None
                ),
                "plugins": (
                    plugin_snapshot
                ),
            }

    # =========================================================================
    # REPRESENTATION
    # =========================================================================
    def __repr__(
        self,
    ) -> str:
        return (
            f"{self.__class__.__name__}("
            f"bot_name={self.bot_name!r}, "
            f"is_running={self.is_running!r}, "
            f"run_count={self.run_count}, "
            f"settings_configured="
            f"{self.settings is not None}, "
            f"storage_enabled="
            f"{self.storage is not None}, "
            f"plugins_enabled="
            f"{self.plugins is not None}"
            f")"
        )