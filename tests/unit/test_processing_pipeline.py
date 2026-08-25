from __future__ import annotations

import threading
from typing import Any

import pytest

from enterprise_crawler.processing.pipeline import (
    PipelineAlreadyRunningError,
    PipelineClosedError,
    PipelineConfigurationError,
    PipelineContext,
    PipelineResult,
    PipelineStageError,
    PipelineValidationError,
    ProcessingPipeline,
)


# =============================================================================
# FIXTURES / TEST DOUBLES
# =============================================================================
class ParseProcessor:
    def parse(
        self,
        source: Any,
    ) -> dict[str, Any]:
        return {
            "value": source,
        }


class CallableProcessor:
    def __call__(
        self,
        source: Any,
    ) -> Any:
        return source * 2


class ObjectMiddleware:
    name = "object-middleware"

    def process(
        self,
        value: dict[str, Any],
        context: PipelineContext,
    ) -> dict[str, Any]:
        output = dict(
            value
        )

        output[
            "middleware"
        ] = True

        context.set_metadata(
            "middleware_seen",
            True,
        )

        return output


class NoneMiddleware:
    def process(
        self,
        value: Any,
        context: PipelineContext,
    ) -> None:
        context.set_state(
            "executed",
            True,
        )

        return None


class ObjectValidator:
    name = "object-validator"

    def validate(
        self,
        value: Any,
        context: PipelineContext,
    ) -> bool:
        return value is not None


class RejectingValidator:
    def validate(
        self,
        value: Any,
        context: PipelineContext,
    ) -> bool:
        return False


class InvalidResultValidator:
    def validate(
        self,
        value: Any,
        context: PipelineContext,
    ) -> str:
        return "yes"


class ObjectSink:
    name = "object-sink"

    def __init__(
        self,
    ) -> None:
        self.values: list[
            Any
        ] = []

    def write(
        self,
        value: Any,
        context: PipelineContext,
    ) -> dict[str, Any]:
        self.values.append(
            value
        )

        return {
            "saved": True,
        }


# =============================================================================
# CONTEXT
# =============================================================================
def test_pipeline_context_metadata() -> None:
    context = PipelineContext(
        run_number=1
    )

    context.set_metadata(
        "source",
        "test",
    )

    assert (
        context.metadata[
            "source"
        ]
        == "test"
    )


def test_pipeline_context_state() -> None:
    context = PipelineContext(
        run_number=1
    )

    context.set_state(
        "seen",
        3,
    )

    assert (
        context.get_state(
            "seen"
        )
        == 3
    )


def test_pipeline_context_missing_state_default() -> None:
    context = PipelineContext(
        run_number=1
    )

    assert (
        context.get_state(
            "missing",
            "fallback",
        )
        == "fallback"
    )


@pytest.mark.parametrize(
    "key",
    [
        "",
        " ",
        "\n\t",
    ],
)
def test_context_empty_metadata_key_is_rejected(
    key: str,
) -> None:
    context = PipelineContext(
        run_number=1
    )

    with pytest.raises(
        PipelineConfigurationError
    ):
        context.set_metadata(
            key,
            1,
        )


def test_context_to_dict_returns_copies() -> None:
    context = PipelineContext(
        run_number=1,
        metadata={
            "a": 1,
        },
        stage_history=[
            "processor:test"
        ],
        state={
            "x": 2,
        },
    )

    payload = (
        context.to_dict()
    )

    payload[
        "metadata"
    ][
        "a"
    ] = 999

    payload[
        "stage_history"
    ].append(
        "changed"
    )

    assert (
        context.metadata[
            "a"
        ]
        == 1
    )

    assert (
        context.stage_history
        == [
            "processor:test"
        ]
    )


# =============================================================================
# CONFIGURATION
# =============================================================================
def test_pipeline_requires_processor() -> None:
    with pytest.raises(
        PipelineConfigurationError
    ):
        ProcessingPipeline(
            processor=None
        )


def test_invalid_processor_is_rejected() -> None:
    with pytest.raises(
        PipelineConfigurationError
    ):
        ProcessingPipeline(
            processor=object()
        )


def test_invalid_middleware_is_rejected() -> None:
    with pytest.raises(
        PipelineConfigurationError
    ):
        ProcessingPipeline(
            processor=lambda value: value,
            middlewares=[
                object()
            ],
        )


def test_invalid_validator_is_rejected() -> None:
    with pytest.raises(
        PipelineConfigurationError
    ):
        ProcessingPipeline(
            processor=lambda value: value,
            validators=[
                object()
            ],
        )


def test_invalid_sink_is_rejected() -> None:
    with pytest.raises(
        PipelineConfigurationError
    ):
        ProcessingPipeline(
            processor=lambda value: value,
            sink=object(),
        )


@pytest.mark.parametrize(
    "name",
    [
        "",
        " ",
        "\n",
    ],
)
def test_empty_pipeline_name_is_rejected(
    name: str,
) -> None:
    with pytest.raises(
        PipelineConfigurationError
    ):
        ProcessingPipeline(
            processor=lambda value: value,
            name=name,
        )


def test_non_string_pipeline_name_is_rejected() -> None:
    with pytest.raises(
        PipelineConfigurationError
    ):
        ProcessingPipeline(
            processor=lambda value: value,
            name=123,  # type: ignore[arg-type]
        )


# =============================================================================
# PROCESSOR
# =============================================================================
def test_parse_processor_is_supported() -> None:
    pipeline = (
        ProcessingPipeline(
            processor=(
                ParseProcessor()
            )
        )
    )

    result = pipeline.run(
        "hello"
    )

    assert (
        result.value
        == {
            "value": "hello",
        }
    )


def test_callable_processor_is_supported() -> None:
    pipeline = (
        ProcessingPipeline(
            processor=(
                CallableProcessor()
            )
        )
    )

    result = pipeline.run(
        5
    )

    assert (
        result.value
        == 10
    )


def test_processor_failure_is_wrapped() -> None:
    def failing_processor(
        source: Any,
    ) -> Any:
        raise ValueError(
            "boom"
        )

    pipeline = (
        ProcessingPipeline(
            processor=(
                failing_processor
            )
        )
    )

    with pytest.raises(
        PipelineStageError
    ) as exc_info:
        pipeline.run(
            "payload"
        )

    error = exc_info.value

    assert (
        error.stage_type
        == "processor"
    )

    assert (
        error.stage_name
        == "failing_processor"
    )

    assert isinstance(
        error.cause,
        ValueError,
    )


# =============================================================================
# MIDDLEWARE
# =============================================================================
def test_object_middleware_transforms_value() -> None:
    pipeline = (
        ProcessingPipeline(
            processor=(
                ParseProcessor()
            ),
            middlewares=[
                ObjectMiddleware()
            ],
        )
    )

    result = pipeline.run(
        "hello"
    )

    assert (
        result.value[
            "middleware"
        ]
        is True
    )

    assert (
        result.context.metadata[
            "middleware_seen"
        ]
        is True
    )


def test_callable_middleware_is_supported() -> None:
    def increment(
        value: int,
        context: PipelineContext,
    ) -> int:
        return value + 1

    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value,
            middlewares=[
                increment
            ],
        )
    )

    result = pipeline.run(
        10
    )

    assert (
        result.value
        == 11
    )


def test_none_middleware_preserves_value() -> None:
    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value,
            middlewares=[
                NoneMiddleware()
            ],
        )
    )

    result = pipeline.run(
        {
            "a": 1,
        }
    )

    assert (
        result.value
        == {
            "a": 1,
        }
    )

    assert (
        result.context.get_state(
            "executed"
        )
        is True
    )


def test_middlewares_run_in_order() -> None:
    history: list[
        str
    ] = []

    def first(
        value: int,
        context: PipelineContext,
    ) -> int:
        history.append(
            "first"
        )

        return value + 1

    def second(
        value: int,
        context: PipelineContext,
    ) -> int:
        history.append(
            "second"
        )

        return value * 2

    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value,
            middlewares=[
                first,
                second,
            ],
        )
    )

    result = pipeline.run(
        5
    )

    assert (
        history
        == [
            "first",
            "second",
        ]
    )

    assert (
        result.value
        == 12
    )


def test_middleware_failure_is_wrapped() -> None:
    def explode(
        value: Any,
        context: PipelineContext,
    ) -> Any:
        raise RuntimeError(
            "middleware boom"
        )

    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value,
            middlewares=[
                explode
            ],
        )
    )

    with pytest.raises(
        PipelineStageError
    ) as exc_info:
        pipeline.run(
            "payload"
        )

    assert (
        exc_info.value.stage_type
        == "middleware"
    )


# =============================================================================
# VALIDATION
# =============================================================================
def test_validator_true_accepts_value() -> None:
    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value,
            validators=[
                ObjectValidator()
            ],
        )
    )

    result = pipeline.run(
        "valid"
    )

    assert (
        result.value
        == "valid"
    )


def test_validator_none_accepts_value() -> None:
    def validator(
        value: Any,
        context: PipelineContext,
    ) -> None:
        return None

    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value,
            validators=[
                validator
            ],
        )
    )

    assert (
        pipeline.run(
            "valid"
        ).value
        == "valid"
    )


def test_validator_false_rejects_value() -> None:
    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value,
            validators=[
                RejectingValidator()
            ],
        )
    )

    with pytest.raises(
        PipelineValidationError
    ) as exc_info:
        pipeline.run(
            "invalid"
        )

    assert (
        exc_info.value.stage_type
        == "validator"
    )


def test_invalid_validator_return_type_is_rejected() -> None:
    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value,
            validators=[
                InvalidResultValidator()
            ],
        )
    )

    with pytest.raises(
        PipelineStageError
    ):
        pipeline.run(
            "payload"
        )


def test_validator_exception_is_wrapped() -> None:
    def validator(
        value: Any,
        context: PipelineContext,
    ) -> bool:
        raise ValueError(
            "invalid"
        )

    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value,
            validators=[
                validator
            ],
        )
    )

    with pytest.raises(
        PipelineStageError
    ) as exc_info:
        pipeline.run(
            "payload"
        )

    assert (
        exc_info.value.stage_type
        == "validator"
    )

    assert isinstance(
        exc_info.value.cause,
        ValueError,
    )


# =============================================================================
# SINK
# =============================================================================
def test_object_sink_is_supported() -> None:
    sink = ObjectSink()

    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value,
            sink=sink,
        )
    )

    result = pipeline.run(
        {
            "id": 1,
        }
    )

    assert (
        sink.values
        == [
            {
                "id": 1,
            }
        ]
    )

    assert (
        result.sink_result
        == {
            "saved": True,
        }
    )


def test_callable_sink_is_supported() -> None:
    saved: list[
        Any
    ] = []

    def sink(
        value: Any,
        context: PipelineContext,
    ) -> int:
        saved.append(
            value
        )

        return len(
            saved
        )

    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value,
            sink=sink,
        )
    )

    result = pipeline.run(
        "payload"
    )

    assert (
        saved
        == [
            "payload"
        ]
    )

    assert (
        result.sink_result
        == 1
    )


def test_sink_failure_is_wrapped() -> None:
    def sink(
        value: Any,
        context: PipelineContext,
    ) -> None:
        raise IOError(
            "disk failure"
        )

    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value,
            sink=sink,
        )
    )

    with pytest.raises(
        PipelineStageError
    ) as exc_info:
        pipeline.run(
            "payload"
        )

    assert (
        exc_info.value.stage_type
        == "sink"
    )


# =============================================================================
# ORDER / RESULT
# =============================================================================
def test_complete_stage_order_is_deterministic() -> None:
    def middleware(
        value: Any,
        context: PipelineContext,
    ) -> Any:
        return value

    def validator(
        value: Any,
        context: PipelineContext,
    ) -> bool:
        return True

    def sink(
        value: Any,
        context: PipelineContext,
    ) -> None:
        return None

    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value,
            middlewares=[
                middleware
            ],
            validators=[
                validator
            ],
            sink=sink,
        )
    )

    result = pipeline.run(
        "payload"
    )

    assert (
        result.stage_history
        == (
            "processor:<lambda>",
            "middleware:middleware",
            "validator:validator",
            "sink:sink",
        )
    )


def test_pipeline_result_type() -> None:
    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value
        )
    )

    result = pipeline.run(
        "payload"
    )

    assert isinstance(
        result,
        PipelineResult,
    )


def test_pipeline_result_duration_is_non_negative() -> None:
    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value
        )
    )

    result = pipeline.run(
        "payload"
    )

    assert (
        result.duration_seconds
        >= 0
    )


def test_pipeline_metadata_is_copied() -> None:
    metadata = {
        "source": "api",
    }

    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value
        )
    )

    result = pipeline.run(
        "payload",
        metadata=metadata,
    )

    metadata[
        "source"
    ] = "changed"

    assert (
        result.context.metadata[
            "source"
        ]
        == "api"
    )


def test_invalid_metadata_type_is_rejected() -> None:
    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value
        )
    )

    with pytest.raises(
        PipelineConfigurationError
    ):
        pipeline.run(
            "payload",
            metadata="invalid",  # type: ignore[arg-type]
        )


# =============================================================================
# REUSE / STATE
# =============================================================================
def test_pipeline_can_run_more_than_once() -> None:
    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value
        )
    )

    first = pipeline.run(
        "first"
    )

    second = pipeline.run(
        "second"
    )

    assert (
        first.context.run_number
        == 1
    )

    assert (
        second.context.run_number
        == 2
    )

    assert (
        pipeline.run_count
        == 2
    )


def test_last_result_tracks_latest_success() -> None:
    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value
        )
    )

    pipeline.run(
        "first"
    )

    second = pipeline.run(
        "second"
    )

    assert (
        pipeline.last_result
        is second
    )


def test_last_error_tracks_failure() -> None:
    def fail(
        source: Any,
    ) -> Any:
        raise RuntimeError(
            "boom"
        )

    pipeline = (
        ProcessingPipeline(
            processor=fail
        )
    )

    with pytest.raises(
        PipelineStageError
    ):
        pipeline.run(
            "payload"
        )

    assert isinstance(
        pipeline.last_error,
        PipelineStageError,
    )


def test_run_lock_is_released_after_failure() -> None:
    attempts = 0

    def processor(
        source: Any,
    ) -> Any:
        nonlocal attempts

        attempts += 1

        if attempts == 1:
            raise RuntimeError(
                "first failure"
            )

        return source

    pipeline = (
        ProcessingPipeline(
            processor=processor
        )
    )

    with pytest.raises(
        PipelineStageError
    ):
        pipeline.run(
            "first"
        )

    result = pipeline.run(
        "second"
    )

    assert (
        result.value
        == "second"
    )


# =============================================================================
# CONCURRENCY
# =============================================================================
def test_same_pipeline_cannot_run_concurrently() -> None:
    entered = (
        threading.Event()
    )

    release = (
        threading.Event()
    )

    errors: list[
        BaseException
    ] = []

    def processor(
        source: Any,
    ) -> Any:
        entered.set()

        release.wait(
            timeout=2
        )

        return source

    pipeline = (
        ProcessingPipeline(
            processor=processor
        )
    )

    def first_run() -> None:
        try:
            pipeline.run(
                "first"
            )

        except BaseException as exc:
            errors.append(
                exc
            )

    thread = threading.Thread(
        target=first_run
    )

    thread.start()

    assert entered.wait(
        timeout=2
    )

    try:
        with pytest.raises(
            PipelineAlreadyRunningError
        ):
            pipeline.run(
                "second"
            )

    finally:
        release.set()

        thread.join(
            timeout=2
        )

    assert (
        errors
        == []
    )


# =============================================================================
# CLOSE
# =============================================================================
def test_close_is_idempotent() -> None:
    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value
        )
    )

    pipeline.close()
    pipeline.close()

    assert (
        pipeline.is_closed
        is True
    )


def test_closed_pipeline_rejects_run() -> None:
    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value
        )
    )

    pipeline.close()

    with pytest.raises(
        PipelineClosedError
    ):
        pipeline.run(
            "payload"
        )


def test_context_manager_closes_pipeline() -> None:
    with ProcessingPipeline(
        processor=lambda value: value
    ) as pipeline:
        assert (
            pipeline.is_closed
            is False
        )

    assert (
        pipeline.is_closed
        is True
    )


# =============================================================================
# SNAPSHOT / REPR
# =============================================================================
def test_snapshot_reports_configuration() -> None:
    pipeline = (
        ProcessingPipeline(
            processor=(
                ParseProcessor()
            ),
            middlewares=[
                ObjectMiddleware()
            ],
            validators=[
                ObjectValidator()
            ],
            sink=ObjectSink(),
            name="records",
        )
    )

    snapshot = (
        pipeline.snapshot()
    )

    assert (
        snapshot[
            "name"
        ]
        == "records"
    )

    assert (
        snapshot[
            "processor"
        ]
        == "ParseProcessor"
    )

    assert (
        snapshot[
            "middlewares"
        ]
        == [
            "object-middleware"
        ]
    )

    assert (
        snapshot[
            "validators"
        ]
        == [
            "object-validator"
        ]
    )

    assert (
        snapshot[
            "sink"
        ]
        == "object-sink"
    )


def test_snapshot_reports_last_failure() -> None:
    def fail(
        source: Any,
    ) -> Any:
        raise RuntimeError(
            "boom"
        )

    pipeline = (
        ProcessingPipeline(
            processor=fail
        )
    )

    with pytest.raises(
        PipelineStageError
    ):
        pipeline.run(
            "payload"
        )

    snapshot = (
        pipeline.snapshot()
    )

    assert (
        snapshot[
            "last_success"
        ]
        is False
    )

    assert (
        snapshot[
            "last_error_type"
        ]
        == "PipelineStageError"
    )


def test_repr_contains_runtime_state() -> None:
    pipeline = (
        ProcessingPipeline(
            processor=lambda value: value,
            middlewares=[
                lambda value, context: value
            ],
            validators=[
                lambda value, context: True
            ],
            name="records",
        )
    )

    representation = repr(
        pipeline
    )

    assert (
        "ProcessingPipeline"
        in representation
    )

    assert (
        "records"
        in representation
    )

    assert (
        "middleware_count=1"
        in representation
    )

    assert (
        "validator_count=1"
        in representation
    )