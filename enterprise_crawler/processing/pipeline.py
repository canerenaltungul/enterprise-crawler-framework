from __future__ import annotations

"""
Enterprise Crawler Framework - Processing Pipeline

Genel amaçlı deterministic processing / middleware pipeline.

Akış
----
source
    ↓
processor
    ↓
middleware(s)
    ↓
validator(s)
    ↓
sink [optional]
    ↓
PipelineResult

Amaç
----
Framework parser/processor katmanlarını birbirine bağlayan merkezi,
format-bağımsız bir execution zinciri sağlamak.

Pipeline bilinçli olarak belirli bir Record contract'ına bağlı değildir.
JSON, XML, HTML, CSV, Feed, PDF veya kullanıcı tanımlı processor çıktıları
aynı runtime üzerinden geçirilebilir.

Desteklenen processor biçimleri
--------------------------------
1. callable::

       processor(source)

2. parse() metoduna sahip nesne::

       processor.parse(source)

Desteklenen middleware biçimleri
--------------------------------
1. callable::

       middleware(value, context)

2. process() metoduna sahip nesne::

       middleware.process(value, context)

Middleware ``None`` döndürürse mevcut value korunur.

Validator
---------
Validator::

    validator(value, context)

şeklinde çağrılır.

Dönüş değeri:

- None -> valid
- True -> valid
- False -> validation failure

Diğer dönüş tipleri contract ihlali kabul edilir.

Sink
----
Optional sink::

    sink(value, context)

veya::

    sink.write(value, context)

şeklinde çalışır.

Sink sonucu PipelineResult.sink_result içinde saklanır.

Fail-closed
-----------
Processor, middleware, validator veya sink exception üretirse hata
PipelineStageError altında normalize edilir.

Pipeline aynı instance üzerinde ardışık çalıştırılabilir fakat aynı anda
iki thread tarafından çalıştırılamaz.
"""

import threading
from dataclasses import dataclass, field
from time import monotonic
from typing import Any, Callable, Mapping, Optional


# =============================================================================
# EXCEPTIONS
# =============================================================================
class ProcessingPipelineError(RuntimeError):
    """Processing pipeline temel hatası."""


class PipelineAlreadyRunningError(
    ProcessingPipelineError
):
    """Aynı pipeline instance eşzamanlı çalıştırılmaya çalışıldığında."""


class PipelineClosedError(
    ProcessingPipelineError
):
    """Kapatılmış pipeline kullanılmaya çalışıldığında."""


class PipelineConfigurationError(
    ProcessingPipelineError
):
    """Pipeline configuration hatası."""


class PipelineStageError(
    ProcessingPipelineError
):
    """
    Processor / middleware / validator / sink stage hatası.
    """

    def __init__(
        self,
        message: str,
        *,
        stage_type: str,
        stage_name: str,
        cause: Optional[
            BaseException
        ] = None,
    ) -> None:
        super().__init__(
            message
        )

        self.stage_type = (
            stage_type
        )

        self.stage_name = (
            stage_name
        )

        self.cause = cause


class PipelineValidationError(
    PipelineStageError
):
    """Validator veriyi reddettiğinde."""


# =============================================================================
# HELPERS
# =============================================================================
def _normalize_name(
    value: Any,
    *,
    field_name: str,
) -> str:
    if not isinstance(
        value,
        str,
    ):
        raise PipelineConfigurationError(
            f"{field_name} str olmalıdır."
        )

    normalized = value.strip()

    if not normalized:
        raise PipelineConfigurationError(
            f"{field_name} boş olamaz."
        )

    return normalized


def _safe_exception_message(
    error: BaseException,
) -> str:
    message = str(
        error
    ).strip()

    if not message:
        message = (
            error.__class__.__name__
        )

    return message[:8_000]


def _stage_name(
    stage: Any,
) -> str:
    explicit_name = getattr(
        stage,
        "name",
        None,
    )

    if (
        isinstance(
            explicit_name,
            str,
        )
        and explicit_name.strip()
    ):
        return (
            explicit_name.strip()
        )

    callable_name = getattr(
        stage,
        "__name__",
        None,
    )

    if (
        isinstance(
            callable_name,
            str,
        )
        and callable_name.strip()
    ):
        return (
            callable_name.strip()
        )

    return (
        stage.__class__.__name__
    )


def _resolve_callable(
    stage: Any,
    *,
    method_name: str,
    stage_type: str,
) -> Callable[..., Any]:
    if stage is None:
        raise PipelineConfigurationError(
            f"{stage_type} None olamaz."
        )

    method = getattr(
        stage,
        method_name,
        None,
    )

    if callable(
        method
    ):
        return method

    if callable(
        stage
    ):
        return stage

    raise PipelineConfigurationError(
        f"{stage_type} callable veya "
        f"{method_name}() metoduna sahip olmalıdır "
        f"| actual={type(stage).__name__}"
    )


# =============================================================================
# CONTEXT
# =============================================================================
@dataclass(slots=True)
class PipelineContext:
    """
    Tek pipeline run'ına ait mutable execution context.

    metadata:
        Kullanıcı / middleware metadata alanı.

    stage_history:
        Başarıyla tamamlanan stage'lerin deterministic sırası.

    state:
        Middleware'lerin run-scoped geçici state paylaşabileceği alan.
    """

    run_number: int

    metadata: dict[
        str,
        Any,
    ] = field(
        default_factory=dict
    )

    stage_history: list[
        str
    ] = field(
        default_factory=list
    )

    state: dict[
        str,
        Any,
    ] = field(
        default_factory=dict
    )

    def set_metadata(
        self,
        key: str,
        value: Any,
    ) -> None:
        normalized = (
            _normalize_name(
                key,
                field_name="metadata key",
            )
        )

        self.metadata[
            normalized
        ] = value

    def set_state(
        self,
        key: str,
        value: Any,
    ) -> None:
        normalized = (
            _normalize_name(
                key,
                field_name="state key",
            )
        )

        self.state[
            normalized
        ] = value

    def get_state(
        self,
        key: str,
        default: Any = None,
    ) -> Any:
        normalized = (
            _normalize_name(
                key,
                field_name="state key",
            )
        )

        return self.state.get(
            normalized,
            default,
        )

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "run_number": (
                self.run_number
            ),
            "metadata": dict(
                self.metadata
            ),
            "stage_history": list(
                self.stage_history
            ),
            "state": dict(
                self.state
            ),
        }


# =============================================================================
# RESULT
# =============================================================================
@dataclass(slots=True)
class PipelineResult:
    """
    Başarılı pipeline execution sonucu.
    """

    value: Any

    context: PipelineContext

    sink_result: Any = None

    duration_seconds: float = 0.0

    @property
    def stage_history(
        self,
    ) -> tuple[str, ...]:
        return tuple(
            self.context.stage_history
        )

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "value": self.value,
            "context": (
                self.context.to_dict()
            ),
            "sink_result": (
                self.sink_result
            ),
            "duration_seconds": (
                self.duration_seconds
            ),
        }


# =============================================================================
# PIPELINE
# =============================================================================
class ProcessingPipeline:
    """
    Deterministic processor → middleware → validation → sink pipeline.

    Örnek::

        pipeline = ProcessingPipeline(
            processor=JsonProcessor(),
            middlewares=[
                normalize_record,
                enrich_record,
            ],
            validators=[
                validate_record,
            ],
            sink=save_record,
        )

        result = pipeline.run(
            raw_json
        )
    """

    def __init__(
        self,
        *,
        processor: Any,
        middlewares: Optional[
            list[Any] | tuple[Any, ...]
        ] = None,
        validators: Optional[
            list[Any] | tuple[Any, ...]
        ] = None,
        sink: Optional[
            Any
        ] = None,
        name: str = (
            "processing-pipeline"
        ),
    ) -> None:
        self.name = (
            _normalize_name(
                name,
                field_name="name",
            )
        )

        # Validate processor immediately.
        _resolve_callable(
            processor,
            method_name="parse",
            stage_type="processor",
        )

        normalized_middlewares = (
            tuple(
                middlewares
                or ()
            )
        )

        normalized_validators = (
            tuple(
                validators
                or ()
            )
        )

        for middleware in (
            normalized_middlewares
        ):
            _resolve_callable(
                middleware,
                method_name="process",
                stage_type="middleware",
            )

        for validator in (
            normalized_validators
        ):
            _resolve_callable(
                validator,
                method_name="validate",
                stage_type="validator",
            )

        if sink is not None:
            _resolve_callable(
                sink,
                method_name="write",
                stage_type="sink",
            )

        self.processor = (
            processor
        )

        self.middlewares = (
            normalized_middlewares
        )

        self.validators = (
            normalized_validators
        )

        self.sink = sink

        self._run_lock = (
            threading.Lock()
        )

        self._state_lock = (
            threading.RLock()
        )

        self._run_count = 0

        self._is_running = False

        self._is_closed = False

        self._last_result: Optional[
            PipelineResult
        ] = None

        self._last_error: Optional[
            BaseException
        ] = None

    # =========================================================================
    # STATE
    # =========================================================================
    @property
    def run_count(
        self,
    ) -> int:
        with self._state_lock:
            return (
                self._run_count
            )

    @property
    def is_running(
        self,
    ) -> bool:
        with self._state_lock:
            return (
                self._is_running
            )

    @property
    def is_closed(
        self,
    ) -> bool:
        with self._state_lock:
            return (
                self._is_closed
            )

    @property
    def last_result(
        self,
    ) -> Optional[
        PipelineResult
    ]:
        with self._state_lock:
            return (
                self._last_result
            )

    @property
    def last_error(
        self,
    ) -> Optional[
        BaseException
    ]:
        with self._state_lock:
            return (
                self._last_error
            )

    # =========================================================================
    # EXECUTION HELPERS
    # =========================================================================
    def _ensure_open(
        self,
    ) -> None:
        if self.is_closed:
            raise PipelineClosedError(
                "ProcessingPipeline kapalı "
                f"| pipeline={self.name}"
            )

    def _run_processor(
        self,
        source: Any,
        context: PipelineContext,
    ) -> Any:
        stage = self.processor

        name = (
            _stage_name(
                stage
            )
        )

        operation = (
            _resolve_callable(
                stage,
                method_name="parse",
                stage_type="processor",
            )
        )

        try:
            value = operation(
                source
            )

        except Exception as exc:
            raise PipelineStageError(
                "Processor stage başarısız "
                f"| pipeline={self.name} "
                f"| stage={name} "
                f"| error={_safe_exception_message(exc)}",
                stage_type="processor",
                stage_name=name,
                cause=exc,
            ) from exc

        context.stage_history.append(
            f"processor:{name}"
        )

        return value

    def _run_middleware(
        self,
        middleware: Any,
        value: Any,
        context: PipelineContext,
    ) -> Any:
        name = (
            _stage_name(
                middleware
            )
        )

        operation = (
            _resolve_callable(
                middleware,
                method_name="process",
                stage_type="middleware",
            )
        )

        try:
            next_value = operation(
                value,
                context,
            )

        except Exception as exc:
            raise PipelineStageError(
                "Middleware stage başarısız "
                f"| pipeline={self.name} "
                f"| stage={name} "
                f"| error={_safe_exception_message(exc)}",
                stage_type="middleware",
                stage_name=name,
                cause=exc,
            ) from exc

        context.stage_history.append(
            f"middleware:{name}"
        )

        if next_value is None:
            return value

        return next_value

    def _run_validator(
        self,
        validator: Any,
        value: Any,
        context: PipelineContext,
    ) -> None:
        name = (
            _stage_name(
                validator
            )
        )

        operation = (
            _resolve_callable(
                validator,
                method_name="validate",
                stage_type="validator",
            )
        )

        try:
            validation_result = (
                operation(
                    value,
                    context,
                )
            )

        except PipelineValidationError:
            raise

        except Exception as exc:
            raise PipelineStageError(
                "Validator stage exception üretti "
                f"| pipeline={self.name} "
                f"| stage={name} "
                f"| error={_safe_exception_message(exc)}",
                stage_type="validator",
                stage_name=name,
                cause=exc,
            ) from exc

        if validation_result is False:
            raise PipelineValidationError(
                "Validator veriyi reddetti "
                f"| pipeline={self.name} "
                f"| stage={name}",
                stage_type="validator",
                stage_name=name,
            )

        if (
            validation_result is not None
            and validation_result is not True
        ):
            raise PipelineStageError(
                "Validator bool veya None döndürmelidir "
                f"| pipeline={self.name} "
                f"| stage={name} "
                f"| actual={type(validation_result).__name__}",
                stage_type="validator",
                stage_name=name,
            )

        context.stage_history.append(
            f"validator:{name}"
        )

    def _run_sink(
        self,
        value: Any,
        context: PipelineContext,
    ) -> Any:
        if self.sink is None:
            return None

        stage = self.sink

        name = (
            _stage_name(
                stage
            )
        )

        operation = (
            _resolve_callable(
                stage,
                method_name="write",
                stage_type="sink",
            )
        )

        try:
            result = operation(
                value,
                context,
            )

        except Exception as exc:
            raise PipelineStageError(
                "Sink stage başarısız "
                f"| pipeline={self.name} "
                f"| stage={name} "
                f"| error={_safe_exception_message(exc)}",
                stage_type="sink",
                stage_name=name,
                cause=exc,
            ) from exc

        context.stage_history.append(
            f"sink:{name}"
        )

        return result

    # =========================================================================
    # RUN
    # =========================================================================
    def run(
        self,
        source: Any,
        *,
        metadata: Optional[
            Mapping[str, Any]
        ] = None,
    ) -> PipelineResult:
        self._ensure_open()

        if (
            metadata is not None
            and not isinstance(
                metadata,
                Mapping,
            )
        ):
            raise PipelineConfigurationError(
                "metadata Mapping olmalıdır."
            )

        acquired = (
            self._run_lock.acquire(
                blocking=False
            )
        )

        if not acquired:
            raise PipelineAlreadyRunningError(
                "ProcessingPipeline zaten çalışıyor "
                f"| pipeline={self.name}"
            )

        started = monotonic()

        try:
            with self._state_lock:
                if self._is_closed:
                    raise PipelineClosedError(
                        "ProcessingPipeline kapalı "
                        f"| pipeline={self.name}"
                    )

                if self._is_running:
                    raise PipelineAlreadyRunningError(
                        "ProcessingPipeline zaten çalışıyor "
                        f"| pipeline={self.name}"
                    )

                self._is_running = True

                self._run_count += 1

                current_run = (
                    self._run_count
                )

                self._last_result = None
                self._last_error = None

            context = PipelineContext(
                run_number=(
                    current_run
                ),
                metadata=dict(
                    metadata
                    or {}
                ),
            )

            value = self._run_processor(
                source,
                context,
            )

            for middleware in (
                self.middlewares
            ):
                value = (
                    self._run_middleware(
                        middleware,
                        value,
                        context,
                    )
                )

            for validator in (
                self.validators
            ):
                self._run_validator(
                    validator,
                    value,
                    context,
                )

            sink_result = (
                self._run_sink(
                    value,
                    context,
                )
            )

            duration = max(
                0.0,
                monotonic()
                - started,
            )

            result = PipelineResult(
                value=value,
                context=context,
                sink_result=(
                    sink_result
                ),
                duration_seconds=round(
                    duration,
                    6,
                ),
            )

            with self._state_lock:
                self._last_result = (
                    result
                )

            return result

        except BaseException as exc:
            with self._state_lock:
                self._last_error = (
                    exc
                )

            raise

        finally:
            with self._state_lock:
                self._is_running = False

            try:
                self._run_lock.release()

            except RuntimeError:
                pass

    # =========================================================================
    # CLOSE
    # =========================================================================
    def close(
        self,
    ) -> None:
        with self._state_lock:
            if self._is_closed:
                return

            if self._is_running:
                raise PipelineAlreadyRunningError(
                    "Çalışan ProcessingPipeline kapatılamaz "
                    f"| pipeline={self.name}"
                )

            self._is_closed = True

    def __enter__(
        self,
    ) -> "ProcessingPipeline":
        self._ensure_open()

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
    def snapshot(
        self,
    ) -> dict[str, Any]:
        with self._state_lock:
            return {
                "name": (
                    self.name
                ),
                "run_count": (
                    self._run_count
                ),
                "is_running": (
                    self._is_running
                ),
                "is_closed": (
                    self._is_closed
                ),
                "processor": (
                    _stage_name(
                        self.processor
                    )
                ),
                "middlewares": [
                    _stage_name(
                        stage
                    )
                    for stage
                    in self.middlewares
                ],
                "validators": [
                    _stage_name(
                        stage
                    )
                    for stage
                    in self.validators
                ],
                "sink": (
                    _stage_name(
                        self.sink
                    )
                    if self.sink
                    is not None
                    else None
                ),
                "last_success": (
                    self._last_result
                    is not None
                ),
                "last_error_type": (
                    self._last_error.__class__.__name__
                    if self._last_error
                    is not None
                    else None
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
            f"name={self.name!r}, "
            f"middleware_count="
            f"{len(self.middlewares)}, "
            f"validator_count="
            f"{len(self.validators)}, "
            f"sink_configured="
            f"{self.sink is not None}, "
            f"run_count={self.run_count}, "
            f"closed={self.is_closed}"
            f")"
        )