from __future__ import annotations

from typing import Any

import pytest

from enterprise_crawler.events import (
    RetryAction,
    RetryDecision,
    RetryPolicy,
    RetryPolicyError,
    RetryPolicyValidationError,
)


# =============================================================================
# TEST EXCEPTIONS
# =============================================================================
class TemporaryError(
    RuntimeError
):
    pass


class PermanentError(
    RuntimeError
):
    pass


class DiscardError(
    RuntimeError
):
    pass


# =============================================================================
# TEST RANDOM SOURCES
# =============================================================================
class CountingRandomSource:
    def __init__(
        self,
        value: float,
    ) -> None:
        self.value = value
        self.call_count = 0

    def __call__(
        self,
    ) -> float:
        self.call_count += 1

        return self.value


class FailingRandomSource:
    def __call__(
        self,
    ) -> float:
        raise RuntimeError(
            "random unavailable"
        )


# =============================================================================
# CONSTRUCTION
# =============================================================================
def test_default_policy() -> None:
    policy = (
        RetryPolicy()
    )

    assert (
        policy.max_deliveries
        == 3
    )

    assert (
        policy.retry_exceptions
        == (
            Exception,
        )
    )

    assert (
        policy.discard_exceptions
        == ()
    )

    assert (
        policy.dead_letter_on_exhaustion
        is True
    )

    assert (
        policy.dead_letter_non_retryable
        is True
    )

    assert (
        policy.base_delay_seconds
        == 0.0
    )

    assert (
        policy.backoff_multiplier
        == 2.0
    )

    assert (
        policy.max_delay_seconds
        is None
    )

    assert (
        policy.jitter_ratio
        == 0.0
    )


def test_custom_policy() -> None:
    random_source = (
        CountingRandomSource(
            0.25
        )
    )

    policy = RetryPolicy(
        max_deliveries=5,
        retry_exceptions=(
            TemporaryError,
        ),
        discard_exceptions=(
            DiscardError,
        ),
        dead_letter_on_exhaustion=False,
        dead_letter_non_retryable=False,
        base_delay_seconds=2.5,
        backoff_multiplier=3.0,
        max_delay_seconds=30.0,
        jitter_ratio=0.4,
        random_source=(
            random_source
        ),
    )

    assert (
        policy.max_deliveries
        == 5
    )

    assert (
        policy.retry_exceptions
        == (
            TemporaryError,
        )
    )

    assert (
        policy.discard_exceptions
        == (
            DiscardError,
        )
    )

    assert (
        policy.base_delay_seconds
        == 2.5
    )

    assert (
        policy.backoff_multiplier
        == 3.0
    )

    assert (
        policy.max_delay_seconds
        == 30.0
    )

    assert (
        policy.jitter_ratio
        == 0.4
    )

    assert (
        policy.random_source
        is random_source
    )


@pytest.mark.parametrize(
    "value",
    [
        0,
        -1,
        True,
        1.5,
        "3",
        None,
    ],
)
def test_invalid_max_deliveries_is_rejected(
    value: Any,
) -> None:
    with pytest.raises(
        RetryPolicyValidationError
    ):
        RetryPolicy(
            max_deliveries=value,  # type: ignore[arg-type]
        )


def test_invalid_retry_exceptions_container_is_rejected() -> None:
    with pytest.raises(
        RetryPolicyValidationError
    ):
        RetryPolicy(
            retry_exceptions=Exception,  # type: ignore[arg-type]
        )


def test_invalid_retry_exception_item_is_rejected() -> None:
    with pytest.raises(
        RetryPolicyValidationError
    ):
        RetryPolicy(
            retry_exceptions=(
                "RuntimeError",  # type: ignore[arg-type]
            )
        )


def test_non_exception_class_is_rejected() -> None:
    with pytest.raises(
        RetryPolicyValidationError
    ):
        RetryPolicy(
            retry_exceptions=(
                str,  # type: ignore[arg-type]
            )
        )


def test_empty_retry_exception_sequence_is_allowed() -> None:
    policy = RetryPolicy(
        retry_exceptions=()
    )

    assert (
        policy.retry_exceptions
        == ()
    )


@pytest.mark.parametrize(
    "field_name,value",
    [
        (
            "dead_letter_on_exhaustion",
            1,
        ),
        (
            "dead_letter_non_retryable",
            "true",
        ),
    ],
)
def test_invalid_boolean_configuration_is_rejected(
    field_name: str,
    value: Any,
) -> None:
    kwargs = {
        field_name: value,
    }

    with pytest.raises(
        RetryPolicyValidationError
    ):
        RetryPolicy(
            **kwargs
        )


# =============================================================================
# RETRY DELAY CONFIGURATION
# =============================================================================
@pytest.mark.parametrize(
    "value",
    [
        -1,
        True,
        "1",
        float("inf"),
        float("nan"),
    ],
)
def test_invalid_base_delay_seconds_is_rejected(
    value: Any,
) -> None:
    with pytest.raises(
        RetryPolicyValidationError
    ):
        RetryPolicy(
            base_delay_seconds=value,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "value",
    [
        0,
        0.5,
        -1,
        True,
        "2",
        float("inf"),
        float("nan"),
    ],
)
def test_invalid_backoff_multiplier_is_rejected(
    value: Any,
) -> None:
    with pytest.raises(
        RetryPolicyValidationError
    ):
        RetryPolicy(
            backoff_multiplier=value,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "value",
    [
        -1,
        True,
        "10",
        float("inf"),
        float("nan"),
    ],
)
def test_invalid_max_delay_seconds_is_rejected(
    value: Any,
) -> None:
    with pytest.raises(
        RetryPolicyValidationError
    ):
        RetryPolicy(
            max_delay_seconds=value,  # type: ignore[arg-type]
        )


def test_zero_delay_configuration_preserves_immediate_retry() -> None:
    policy = RetryPolicy(
        base_delay_seconds=0.0,
        backoff_multiplier=10.0,
        max_delay_seconds=0.0,
    )

    assert (
        policy.retry_delay_for_delivery(
            1
        )
        == 0.0
    )

    assert (
        policy.retry_delay_for_delivery(
            100
        )
        == 0.0
    )


def test_first_retry_uses_base_delay() -> None:
    policy = RetryPolicy(
        base_delay_seconds=5.0,
        backoff_multiplier=2.0,
    )

    assert (
        policy.retry_delay_for_delivery(
            1
        )
        == 5.0
    )


def test_retry_delay_grows_exponentially() -> None:
    policy = RetryPolicy(
        base_delay_seconds=5.0,
        backoff_multiplier=2.0,
    )

    assert [
        policy.retry_delay_for_delivery(
            delivery_count
        )
        for delivery_count
        in (
            1,
            2,
            3,
            4,
        )
    ] == [
        5.0,
        10.0,
        20.0,
        40.0,
    ]


def test_retry_delay_is_capped_by_max_delay() -> None:
    policy = RetryPolicy(
        base_delay_seconds=5.0,
        backoff_multiplier=3.0,
        max_delay_seconds=20.0,
    )

    assert [
        policy.retry_delay_for_delivery(
            delivery_count
        )
        for delivery_count
        in (
            1,
            2,
            3,
        )
    ] == [
        5.0,
        15.0,
        20.0,
    ]


@pytest.mark.parametrize(
    "value",
    [
        0,
        -1,
        True,
        1.5,
        "1",
        None,
    ],
)
def test_retry_delay_requires_positive_delivery_count(
    value: Any,
) -> None:
    policy = RetryPolicy()

    with pytest.raises(
        RetryPolicyValidationError
    ):
        policy.retry_delay_for_delivery(
            value  # type: ignore[arg-type]
        )


# =============================================================================
# RETRY JITTER CONFIGURATION
# =============================================================================
@pytest.mark.parametrize(
    "value",
    [
        -0.1,
        1.1,
        True,
        "0.5",
        float("inf"),
        float("nan"),
    ],
)
def test_invalid_jitter_ratio_is_rejected(
    value: Any,
) -> None:
    with pytest.raises(
        RetryPolicyValidationError
    ):
        RetryPolicy(
            jitter_ratio=value,  # type: ignore[arg-type]
        )


def test_jitter_ratio_boundaries_are_allowed() -> None:
    assert (
        RetryPolicy(
            jitter_ratio=0.0
        ).jitter_ratio
        == 0.0
    )

    assert (
        RetryPolicy(
            jitter_ratio=1.0
        ).jitter_ratio
        == 1.0
    )


def test_non_callable_random_source_is_rejected() -> None:
    with pytest.raises(
        RetryPolicyValidationError
    ):
        RetryPolicy(
            random_source=123,  # type: ignore[arg-type]
        )


def test_jitter_disabled_does_not_call_random_source() -> None:
    random_source = (
        FailingRandomSource()
    )

    policy = RetryPolicy(
        base_delay_seconds=10.0,
        jitter_ratio=0.0,
        random_source=random_source,
    )

    assert (
        policy.retry_delay_for_delivery(
            1
        )
        == 10.0
    )


def test_zero_base_delay_does_not_call_random_source_even_when_jitter_enabled() -> None:
    random_source = (
        FailingRandomSource()
    )

    policy = RetryPolicy(
        base_delay_seconds=0.0,
        jitter_ratio=1.0,
        random_source=random_source,
    )

    assert (
        policy.retry_delay_for_delivery(
            1
        )
        == 0.0
    )


@pytest.mark.parametrize(
    "sample,expected",
    [
        (
            0.0,
            0.0,
        ),
        (
            0.25,
            10.0,
        ),
        (
            0.5,
            20.0,
        ),
        (
            0.75,
            30.0,
        ),
        (
            1.0,
            40.0,
        ),
    ],
)
def test_full_jitter_maps_unit_interval_to_full_delay_range(
    sample: float,
    expected: float,
) -> None:
    policy = RetryPolicy(
        base_delay_seconds=40.0,
        jitter_ratio=1.0,
        random_source=(
            lambda: sample
        ),
    )

    assert (
        policy.retry_delay_for_delivery(
            1
        )
        == expected
    )


def test_partial_jitter_preserves_configured_minimum_fraction() -> None:
    low = RetryPolicy(
        base_delay_seconds=100.0,
        jitter_ratio=0.25,
        random_source=(
            lambda: 0.0
        ),
    )

    high = RetryPolicy(
        base_delay_seconds=100.0,
        jitter_ratio=0.25,
        random_source=(
            lambda: 1.0
        ),
    )

    assert (
        low.retry_delay_for_delivery(
            1
        )
        == 75.0
    )

    assert (
        high.retry_delay_for_delivery(
            1
        )
        == 100.0
    )


def test_jitter_is_applied_after_max_delay_cap() -> None:
    policy = RetryPolicy(
        base_delay_seconds=10.0,
        backoff_multiplier=10.0,
        max_delay_seconds=50.0,
        jitter_ratio=1.0,
        random_source=(
            lambda: 0.5
        ),
    )

    assert (
        policy.retry_delay_for_delivery(
            2
        )
        == 25.0
    )


def test_jitter_never_exceeds_capped_deterministic_delay() -> None:
    policy = RetryPolicy(
        base_delay_seconds=10.0,
        backoff_multiplier=10.0,
        max_delay_seconds=50.0,
        jitter_ratio=0.75,
        random_source=(
            lambda: 1.0
        ),
    )

    assert (
        policy.retry_delay_for_delivery(
            2
        )
        == 50.0
    )


@pytest.mark.parametrize(
    "value",
    [
        -0.1,
        1.1,
        True,
        "0.5",
        float("inf"),
        float("nan"),
    ],
)
def test_invalid_random_source_output_fails_closed(
    value: Any,
) -> None:
    policy = RetryPolicy(
        base_delay_seconds=10.0,
        jitter_ratio=1.0,
        random_source=(
            lambda: value
        ),
    )

    with pytest.raises(
        RetryPolicyError
    ):
        policy.retry_delay_for_delivery(
            1
        )


def test_random_source_failure_is_wrapped() -> None:
    policy = RetryPolicy(
        base_delay_seconds=10.0,
        jitter_ratio=1.0,
        random_source=(
            FailingRandomSource()
        ),
    )

    with pytest.raises(
        RetryPolicyError,
        match="random_source",
    ) as exc_info:
        policy.retry_delay_for_delivery(
            1
        )

    assert isinstance(
        exc_info.value.__cause__,
        RuntimeError,
    )


def test_enabled_jitter_calls_random_source_once_per_delay_calculation() -> None:
    random_source = (
        CountingRandomSource(
            0.5
        )
    )

    policy = RetryPolicy(
        base_delay_seconds=10.0,
        jitter_ratio=1.0,
        random_source=random_source,
    )

    assert (
        policy.retry_delay_for_delivery(
            1
        )
        == 5.0
    )

    assert (
        random_source.call_count
        == 1
    )

    assert (
        policy.retry_delay_for_delivery(
            2
        )
        == 10.0
    )

    assert (
        random_source.call_count
        == 2
    )


# =============================================================================
# RETRY CLASSIFICATION
# =============================================================================
def test_retryable_exception_is_detected() -> None:
    policy = RetryPolicy(
        retry_exceptions=(
            TemporaryError,
        )
    )

    assert (
        policy.is_retryable(
            TemporaryError(
                "temporary"
            )
        )
        is True
    )


def test_non_retryable_exception_is_detected() -> None:
    policy = RetryPolicy(
        retry_exceptions=(
            TemporaryError,
        )
    )

    assert (
        policy.is_retryable(
            PermanentError(
                "permanent"
            )
        )
        is False
    )


def test_discardable_exception_is_detected() -> None:
    policy = RetryPolicy(
        discard_exceptions=(
            DiscardError,
        )
    )

    assert (
        policy.is_discardable(
            DiscardError()
        )
        is True
    )


def test_invalid_error_classification_input_is_rejected() -> None:
    policy = (
        RetryPolicy()
    )

    with pytest.raises(
        RetryPolicyValidationError
    ):
        policy.is_retryable(
            "boom"  # type: ignore[arg-type]
        )


# =============================================================================
# RETRY DECISION
# =============================================================================
def test_first_retryable_failure_is_retried() -> None:
    policy = RetryPolicy(
        max_deliveries=3,
        retry_exceptions=(
            TemporaryError,
        ),
    )

    decision = policy.decide(
        1,
        TemporaryError(
            "temporary"
        ),
    )

    assert isinstance(
        decision,
        RetryDecision,
    )

    assert (
        decision.action
        is RetryAction.RETRY
    )

    assert (
        decision.retryable
        is True
    )

    assert (
        decision.exhausted
        is False
    )

    assert (
        decision.should_retry
        is True
    )

    assert (
        decision.retry_delay_seconds
        == 0.0
    )


def test_second_failure_is_retried_before_limit() -> None:
    policy = RetryPolicy(
        max_deliveries=3,
        retry_exceptions=(
            TemporaryError,
        ),
    )

    decision = policy.decide(
        2,
        TemporaryError()
    )

    assert (
        decision.action
        is RetryAction.RETRY
    )


def test_retry_decision_contains_calculated_delay() -> None:
    policy = RetryPolicy(
        max_deliveries=5,
        retry_exceptions=(
            TemporaryError,
        ),
        base_delay_seconds=2.0,
        backoff_multiplier=3.0,
    )

    decision = policy.decide(
        3,
        TemporaryError(),
    )

    assert (
        decision.action
        is RetryAction.RETRY
    )

    assert (
        decision.retry_delay_seconds
        == 18.0
    )


def test_retry_decision_contains_jittered_delay() -> None:
    policy = RetryPolicy(
        max_deliveries=5,
        retry_exceptions=(
            TemporaryError,
        ),
        base_delay_seconds=20.0,
        backoff_multiplier=2.0,
        jitter_ratio=1.0,
        random_source=(
            lambda: 0.25
        ),
    )

    decision = policy.decide(
        2,
        TemporaryError(),
    )

    assert (
        decision.action
        is RetryAction.RETRY
    )

    assert (
        decision.retry_delay_seconds
        == 10.0
    )


def test_final_delivery_goes_to_dead_letter() -> None:
    policy = RetryPolicy(
        max_deliveries=3,
        retry_exceptions=(
            TemporaryError,
        ),
    )

    decision = policy.decide(
        3,
        TemporaryError(
            "still failing"
        ),
    )

    assert (
        decision.action
        is RetryAction.DEAD_LETTER
    )

    assert (
        decision.exhausted
        is True
    )

    assert (
        decision.should_dead_letter
        is True
    )

    assert (
        decision.reason
        == "max_deliveries_exhausted"
    )

    assert (
        decision.retry_delay_seconds
        == 0.0
    )


def test_exhausted_decision_does_not_consume_random_source() -> None:
    random_source = (
        FailingRandomSource()
    )

    policy = RetryPolicy(
        max_deliveries=2,
        base_delay_seconds=10.0,
        jitter_ratio=1.0,
        random_source=random_source,
    )

    decision = policy.decide(
        2,
        TemporaryError(),
    )

    assert (
        decision.action
        is RetryAction.DEAD_LETTER
    )

    assert (
        decision.retry_delay_seconds
        == 0.0
    )


def test_delivery_over_limit_remains_exhausted() -> None:
    policy = RetryPolicy(
        max_deliveries=3
    )

    decision = policy.decide(
        99,
        RuntimeError()
    )

    assert (
        decision.exhausted
        is True
    )

    assert (
        decision.action
        is RetryAction.DEAD_LETTER
    )


def test_exhausted_event_can_be_discarded() -> None:
    policy = RetryPolicy(
        max_deliveries=2,
        dead_letter_on_exhaustion=False,
    )

    decision = policy.decide(
        2,
        RuntimeError()
    )

    assert (
        decision.action
        is RetryAction.DISCARD
    )

    assert (
        decision.should_discard
        is True
    )


# =============================================================================
# EXPLICIT DISCARD
# =============================================================================
def test_explicit_discard_exception_wins_before_retry() -> None:
    policy = RetryPolicy(
        max_deliveries=10,
        retry_exceptions=(
            Exception,
        ),
        discard_exceptions=(
            DiscardError,
        ),
    )

    decision = policy.decide(
        1,
        DiscardError(
            "bad input"
        ),
    )

    assert (
        decision.action
        is RetryAction.DISCARD
    )

    assert (
        decision.reason
        == "error_matches_discard_exception"
    )

    assert (
        decision.retry_delay_seconds
        == 0.0
    )

    assert (
        decision.retryable
        is False
    )


def test_discard_decision_does_not_consume_random_source() -> None:
    policy = RetryPolicy(
        max_deliveries=10,
        retry_exceptions=(
            Exception,
        ),
        discard_exceptions=(
            DiscardError,
        ),
        base_delay_seconds=10.0,
        jitter_ratio=1.0,
        random_source=(
            FailingRandomSource()
        ),
    )

    decision = policy.decide(
        1,
        DiscardError(),
    )

    assert (
        decision.action
        is RetryAction.DISCARD
    )


def test_explicit_discard_wins_even_at_delivery_limit() -> None:
    policy = RetryPolicy(
        max_deliveries=3,
        retry_exceptions=(
            Exception,
        ),
        discard_exceptions=(
            DiscardError,
        ),
    )

    decision = policy.decide(
        3,
        DiscardError()
    )

    assert (
        decision.action
        is RetryAction.DISCARD
    )


# =============================================================================
# NON-RETRYABLE
# =============================================================================
def test_non_retryable_error_goes_to_dead_letter_by_default() -> None:
    policy = RetryPolicy(
        retry_exceptions=(
            TemporaryError,
        )
    )

    decision = policy.decide(
        1,
        PermanentError(
            "permanent"
        ),
    )

    assert (
        decision.action
        is RetryAction.DEAD_LETTER
    )

    assert (
        decision.retryable
        is False
    )

    assert (
        decision.exhausted
        is False
    )

    assert (
        decision.reason
        == "non_retryable_error"
    )

    assert (
        decision.retry_delay_seconds
        == 0.0
    )


def test_non_retryable_decision_does_not_consume_random_source() -> None:
    policy = RetryPolicy(
        retry_exceptions=(
            TemporaryError,
        ),
        base_delay_seconds=10.0,
        jitter_ratio=1.0,
        random_source=(
            FailingRandomSource()
        ),
    )

    decision = policy.decide(
        1,
        PermanentError(),
    )

    assert (
        decision.action
        is RetryAction.DEAD_LETTER
    )


def test_non_retryable_error_can_be_discarded() -> None:
    policy = RetryPolicy(
        retry_exceptions=(
            TemporaryError,
        ),
        dead_letter_non_retryable=False,
    )

    decision = policy.decide(
        1,
        PermanentError()
    )

    assert (
        decision.action
        is RetryAction.DISCARD
    )


# =============================================================================
# INPUT VALIDATION
# =============================================================================
@pytest.mark.parametrize(
    "value",
    [
        0,
        -1,
        True,
        1.5,
        "1",
        None,
    ],
)
def test_invalid_delivery_count_is_rejected(
    value: Any,
) -> None:
    policy = (
        RetryPolicy()
    )

    with pytest.raises(
        RetryPolicyValidationError
    ):
        policy.decide(
            value,  # type: ignore[arg-type]
            RuntimeError(),
        )


def test_decide_requires_exception() -> None:
    policy = (
        RetryPolicy()
    )

    with pytest.raises(
        RetryPolicyValidationError
    ):
        policy.decide(
            1,
            "error",  # type: ignore[arg-type]
        )


# =============================================================================
# ERROR METADATA
# =============================================================================
def test_decision_captures_error_metadata() -> None:
    policy = (
        RetryPolicy()
    )

    decision = policy.decide(
        1,
        RuntimeError(
            "something failed"
        ),
    )

    assert (
        decision.error_type
        == "RuntimeError"
    )

    assert (
        decision.error_message
        == "something failed"
    )


def test_empty_exception_message_uses_class_name() -> None:
    policy = (
        RetryPolicy()
    )

    decision = policy.decide(
        1,
        RuntimeError(),
    )

    assert (
        decision.error_message
        == "RuntimeError"
    )


# =============================================================================
# RESULT SERIALIZATION
# =============================================================================
def test_retry_decision_to_dict() -> None:
    policy = RetryPolicy(
        max_deliveries=3
    )

    decision = policy.decide(
        1,
        RuntimeError(
            "temporary"
        ),
    )

    payload = (
        decision.to_dict()
    )

    assert (
        payload[
            "action"
        ]
        == "retry"
    )

    assert (
        payload[
            "delivery_count"
        ]
        == 1
    )

    assert (
        payload[
            "max_deliveries"
        ]
        == 3
    )

    assert (
        payload[
            "retry_delay_seconds"
        ]
        == 0.0
    )


def test_retry_decision_to_dict_includes_backoff_delay() -> None:
    policy = RetryPolicy(
        max_deliveries=5,
        base_delay_seconds=4.0,
        backoff_multiplier=2.0,
    )

    payload = policy.decide(
        2,
        RuntimeError(
            "temporary"
        ),
    ).to_dict()

    assert (
        payload[
            "retry_delay_seconds"
        ]
        == 8.0
    )


def test_retry_decision_to_dict_includes_jittered_delay() -> None:
    policy = RetryPolicy(
        max_deliveries=5,
        base_delay_seconds=40.0,
        jitter_ratio=1.0,
        random_source=(
            lambda: 0.25
        ),
    )

    payload = policy.decide(
        1,
        RuntimeError(
            "temporary"
        ),
    ).to_dict()

    assert (
        payload[
            "retry_delay_seconds"
        ]
        == 10.0
    )


# =============================================================================
# SNAPSHOT
# =============================================================================
def test_policy_snapshot() -> None:
    policy = RetryPolicy(
        max_deliveries=5,
        retry_exceptions=(
            TemporaryError,
        ),
        discard_exceptions=(
            DiscardError,
        ),
        base_delay_seconds=3.0,
        backoff_multiplier=2.5,
        max_delay_seconds=60.0,
        jitter_ratio=0.4,
    )

    snapshot = (
        policy.snapshot()
    )

    assert (
        snapshot[
            "max_deliveries"
        ]
        == 5
    )

    assert (
        snapshot[
            "retry_exceptions"
        ]
        == [
            "TemporaryError"
        ]
    )

    assert (
        snapshot[
            "discard_exceptions"
        ]
        == [
            "DiscardError"
        ]
    )

    assert (
        snapshot[
            "base_delay_seconds"
        ]
        == 3.0
    )

    assert (
        snapshot[
            "backoff_multiplier"
        ]
        == 2.5
    )

    assert (
        snapshot[
            "max_delay_seconds"
        ]
        == 60.0
    )

    assert (
        snapshot[
            "jitter_ratio"
        ]
        == 0.4
    )

    assert (
        snapshot[
            "jitter_enabled"
        ]
        is True
    )


def test_policy_snapshot_reports_disabled_jitter() -> None:
    snapshot = (
        RetryPolicy().snapshot()
    )

    assert (
        snapshot[
            "jitter_ratio"
        ]
        == 0.0
    )

    assert (
        snapshot[
            "jitter_enabled"
        ]
        is False
    )


# =============================================================================
# REPRESENTATION
# =============================================================================
def test_repr_contains_configuration() -> None:
    policy = RetryPolicy(
        max_deliveries=7,
        base_delay_seconds=1.5,
        backoff_multiplier=4.0,
        max_delay_seconds=30.0,
        jitter_ratio=0.5,
    )

    rendered = repr(
        policy
    )

    assert (
        "RetryPolicy"
        in rendered
    )

    assert (
        "max_deliveries=7"
        in rendered
    )

    assert (
        "jitter_ratio=0.5"
        in rendered
    )