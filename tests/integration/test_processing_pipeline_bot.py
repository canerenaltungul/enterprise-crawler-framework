from __future__ import annotations

from typing import Any

from enterprise_crawler.contracts.enums import (
    ExecutionStatus,
)
from enterprise_crawler.core.base_bot import (
    BaseBot,
)
from enterprise_crawler.core.crawler import (
    Crawler,
)
from enterprise_crawler.processing import (
    JsonProcessor,
    PipelineResult,
    ProcessingPipeline,
)


# =============================================================================
# FIXTURES / TEST COMPONENTS
# =============================================================================
JSON_PAYLOAD = (
    b'{"name":"enterprise-crawler","count":2}'
)


def enrich_payload(
    value: Any,
    context: Any,
) -> Any:
    """
    Parsed JSON document üzerinde deterministic enrichment.
    """

    enriched = dict(
        value
    )

    enriched["processed"] = True

    context.set_metadata(
        "middleware",
        "enrich_payload",
    )

    context.set_state(
        "enriched",
        True,
    )

    return enriched


def validate_payload(
    value: Any,
    context: Any,
) -> bool:
    """
    Pipeline validator.
    """

    return (
        isinstance(
            value,
            dict,
        )
        and value.get(
            "processed"
        )
        is True
    )


class MemorySink:
    """
    Pipeline sink davranışını gözlemlemek için küçük in-memory sink.
    """

    name = "memory-sink"

    def __init__(
        self,
    ) -> None:
        self.values: list[
            Any
        ] = []

        self.contexts: list[
            Any
        ] = []

    def write(
        self,
        value: Any,
        context: Any,
    ) -> dict[str, Any]:
        self.values.append(
            value
        )

        self.contexts.append(
            context
        )

        return {
            "stored": True,
            "count": len(
                self.values
            ),
        }


class RejectingValidator:
    name = "rejecting-validator"

    def validate(
        self,
        value: Any,
        context: Any,
    ) -> bool:
        return False


# =============================================================================
# BASEBOT + PROCESSING PIPELINE
# =============================================================================
class PipelineBot(
    BaseBot
):
    """
    ProcessingPipeline'i gerçek BaseBot lifecycle'ı içinde çalıştırır.
    """

    def __init__(
        self,
        pipeline: ProcessingPipeline,
        payload: Any = JSON_PAYLOAD,
    ) -> None:
        super().__init__(
            bot_name="pipeline-bot"
        )

        self.pipeline = (
            pipeline
        )

        self.payload = payload

        self.pipeline_result: (
            PipelineResult
            | None
        ) = None

    def execute(
        self,
    ) -> dict[str, Any]:
        result = (
            self.pipeline.run(
                self.payload,
                metadata={
                    "bot_name": (
                        self.bot_name
                    ),
                    "source": (
                        "integration-test"
                    ),
                },
            )
        )

        self.pipeline_result = (
            result
        )

        self.mark_record_processed(
            1
        )

        self.set_runtime_metadata(
            "processing_pipeline",
            {
                "name": (
                    self.pipeline.name
                ),
                "run_number": (
                    result.context.run_number
                ),
                "stage_history": list(
                    result.stage_history
                ),
            },
        )

        return {
            "status": (
                ExecutionStatus.COMPLETED
            ),
            "records_processed": (
                self.records_processed
            ),
            "errors": (
                self.error_count
            ),
            "warnings": (
                self.warning_count
            ),
            "metadata": {
                "processing": {
                    "pipeline": (
                        self.pipeline.name
                    ),
                    "run_number": (
                        result.context.run_number
                    ),
                    "stage_history": list(
                        result.stage_history
                    ),
                    "sink_result": (
                        result.sink_result
                    ),
                }
            },
        }


# =============================================================================
# HELPERS
# =============================================================================
def build_pipeline(
    *,
    sink: Any = None,
    validators: Any = None,
) -> ProcessingPipeline:
    return ProcessingPipeline(
        name="json-processing-pipeline",
        processor=JsonProcessor(),
        middlewares=[
            enrich_payload,
        ],
        validators=(
            validators
            if validators is not None
            else [
                validate_payload,
            ]
        ),
        sink=sink,
    )


# =============================================================================
# TESTS
# =============================================================================
def test_processing_pipeline_runs_inside_basebot_lifecycle() -> None:
    sink = MemorySink()

    pipeline = build_pipeline(
        sink=sink
    )

    bot = PipelineBot(
        pipeline
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
            result.errors
            == 0
        )

        assert (
            bot.pipeline_result
            is not None
        )

        pipeline_result = (
            bot.pipeline_result
        )

        assert (
            pipeline_result.value[
                "name"
            ]
            == "enterprise-crawler"
        )

        assert (
            pipeline_result.value[
                "count"
            ]
            == 2
        )

        assert (
            pipeline_result.value[
                "processed"
            ]
            is True
        )

        assert (
            pipeline_result.context.metadata[
                "bot_name"
            ]
            == "pipeline-bot"
        )

        assert (
            pipeline_result.context.metadata[
                "source"
            ]
            == "integration-test"
        )

        assert (
            pipeline_result.context.metadata[
                "middleware"
            ]
            == "enrich_payload"
        )

        assert (
            pipeline_result.context.get_state(
                "enriched"
            )
            is True
        )

        assert (
            pipeline_result.stage_history
            == (
                "processor:JsonProcessor",
                "middleware:enrich_payload",
                "validator:validate_payload",
                "sink:memory-sink",
            )
        )

        assert (
            pipeline_result.sink_result
            == {
                "stored": True,
                "count": 1,
            }
        )

        assert len(
            sink.values
        ) == 1

        assert (
            sink.values[0][
                "processed"
            ]
            is True
        )

    finally:
        bot.close()
        pipeline.close()


def test_processing_pipeline_runs_through_crawler() -> None:
    pipeline = build_pipeline()

    bot = PipelineBot(
        pipeline
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
            == 1
        )

        assert (
            bot.pipeline_result
            is not None
        )

        assert (
            pipeline.run_count
            == 1
        )

        assert (
            crawler.run_count
            == 1
        )

    finally:
        bot.close()
        pipeline.close()


def test_pipeline_metadata_survives_basebot_lifecycle() -> None:
    sink = MemorySink()

    pipeline = build_pipeline(
        sink=sink
    )

    bot = PipelineBot(
        pipeline
    )

    try:
        result = bot.run()

        processing_metadata = (
            result.metadata[
                "processing"
            ]
        )

        assert (
            processing_metadata[
                "pipeline"
            ]
            == "json-processing-pipeline"
        )

        assert (
            processing_metadata[
                "run_number"
            ]
            == 1
        )

        assert (
            processing_metadata[
                "stage_history"
            ]
            == [
                "processor:JsonProcessor",
                "middleware:enrich_payload",
                "validator:validate_payload",
                "sink:memory-sink",
            ]
        )

        assert (
            processing_metadata[
                "sink_result"
            ]
            == {
                "stored": True,
                "count": 1,
            }
        )

        assert (
            "bot"
            in result.metadata
        )

        assert (
            "runtime"
            in result.metadata
        )

        runtime_metadata = (
            result.metadata[
                "runtime"
            ][
                "processing_pipeline"
            ]
        )

        assert (
            runtime_metadata[
                "name"
            ]
            == "json-processing-pipeline"
        )

        assert (
            runtime_metadata[
                "run_number"
            ]
            == 1
        )

    finally:
        bot.close()
        pipeline.close()


def test_pipeline_failure_becomes_failed_execution_result() -> None:
    pipeline = build_pipeline(
        validators=[
            RejectingValidator(),
        ]
    )

    bot = PipelineBot(
        pipeline
    )

    try:
        result = bot.run()

        assert (
            result.status
            is ExecutionStatus.FAILED
        )

        assert (
            result.errors
            >= 1
        )

        assert (
            bot.pipeline_result
            is None
        )

        assert (
            pipeline.last_result
            is None
        )

        assert (
            pipeline.last_error
            is not None
        )

        assert (
            pipeline.last_error.__class__.__name__
            == "PipelineValidationError"
        )

        assert (
            "failure"
            in result.metadata
        )

        failure = (
            result.metadata[
                "failure"
            ]
        )

        assert (
            failure[
                "exception_type"
            ]
            == "PipelineValidationError"
        )

    finally:
        bot.close()
        pipeline.close()


def test_processing_pipeline_can_be_reused_across_bot_runs() -> None:
    sink = MemorySink()

    pipeline = build_pipeline(
        sink=sink
    )

    bot = PipelineBot(
        pipeline
    )

    try:
        first = bot.run()
        second = bot.run()

        assert (
            first.status
            is ExecutionStatus.COMPLETED
        )

        assert (
            second.status
            is ExecutionStatus.COMPLETED
        )

        assert (
            bot.run_count
            == 2
        )

        assert (
            pipeline.run_count
            == 2
        )

        assert len(
            sink.values
        ) == 2

        assert (
            bot.pipeline_result
            is not None
        )

        assert (
            bot.pipeline_result.context.run_number
            == 2
        )

        assert (
            bot.pipeline_result.sink_result
            == {
                "stored": True,
                "count": 2,
            }
        )

    finally:
        bot.close()
        pipeline.close()


def test_pipeline_context_is_run_scoped() -> None:
    observed_contexts: list[
        Any
    ] = []

    def capture_context(
        value: Any,
        context: Any,
    ) -> Any:
        observed_contexts.append(
            context
        )

        previous = (
            context.get_state(
                "visit_count",
                0,
            )
        )

        context.set_state(
            "visit_count",
            previous + 1,
        )

        return value

    pipeline = ProcessingPipeline(
        name="context-scope-pipeline",
        processor=JsonProcessor(),
        middlewares=[
            capture_context,
        ],
    )

    bot = PipelineBot(
        pipeline
    )

    try:
        first = bot.run()

        assert (
            first.status
            is ExecutionStatus.COMPLETED
        )

        first_pipeline_result = (
            bot.pipeline_result
        )

        assert (
            first_pipeline_result
            is not None
        )

        first_context = (
            first_pipeline_result.context
        )

        second = bot.run()

        assert (
            second.status
            is ExecutionStatus.COMPLETED
        )

        second_pipeline_result = (
            bot.pipeline_result
        )

        assert (
            second_pipeline_result
            is not None
        )

        second_context = (
            second_pipeline_result.context
        )

        assert (
            first_context
            is not second_context
        )

        assert (
            first_context.run_number
            == 1
        )

        assert (
            second_context.run_number
            == 2
        )

        assert (
            first_context.get_state(
                "visit_count"
            )
            == 1
        )

        assert (
            second_context.get_state(
                "visit_count"
            )
            == 1
        )

        assert len(
            observed_contexts
        ) == 2

        assert (
            observed_contexts[0]
            is first_context
        )

        assert (
            observed_contexts[1]
            is second_context
        )

    finally:
        bot.close()
        pipeline.close()


def test_basebot_close_does_not_implicitly_close_external_pipeline() -> None:
    pipeline = build_pipeline()

    bot = PipelineBot(
        pipeline
    )

    result = bot.run()

    assert (
        result.status
        is ExecutionStatus.COMPLETED
    )

    bot.close()

    assert (
        pipeline.is_closed
        is False
    )

    direct_result = (
        pipeline.run(
            JSON_PAYLOAD
        )
    )

    assert (
        direct_result.context.run_number
        == 2
    )

    pipeline.close()

    assert (
        pipeline.is_closed
        is True
    )


def test_pipeline_snapshot_reflects_bot_execution() -> None:
    pipeline = build_pipeline()

    bot = PipelineBot(
        pipeline
    )

    try:
        result = bot.run()

        assert (
            result.status
            is ExecutionStatus.COMPLETED
        )

        snapshot = (
            pipeline.snapshot()
        )

        assert (
            snapshot[
                "name"
            ]
            == "json-processing-pipeline"
        )

        assert (
            snapshot[
                "run_count"
            ]
            == 1
        )

        assert (
            snapshot[
                "is_running"
            ]
            is False
        )

        assert (
            snapshot[
                "is_closed"
            ]
            is False
        )

        assert (
            snapshot[
                "processor"
            ]
            == "JsonProcessor"
        )

        assert (
            snapshot[
                "middlewares"
            ]
            == [
                "enrich_payload"
            ]
        )

        assert (
            snapshot[
                "validators"
            ]
            == [
                "validate_payload"
            ]
        )

        assert (
            snapshot[
                "last_success"
            ]
            is True
        )

        assert (
            snapshot[
                "last_error_type"
            ]
            is None
        )

    finally:
        bot.close()
        pipeline.close()