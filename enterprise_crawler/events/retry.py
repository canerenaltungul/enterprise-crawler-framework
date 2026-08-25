from __future__ import annotations

"""
Enterprise Crawler Framework - Event Retry Policy

Event delivery başarısızlıklarının nasıl ele alınacağını belirleyen
backend-independent retry karar katmanı.

Akış
----
handler failure
    ↓
RetryPolicy.decide(
    delivery_count,
    error,
)
    ↓
RetryDecision
    ├── RETRY
    ├── DISCARD
    └── DEAD_LETTER

delivery_count
--------------
Queue claim sayısını temsil eder.

İlk delivery:

    delivery_count = 1

İlk başarısızlık sonrası retry edilirse sonraki claim:

    delivery_count = 2

max_deliveries
--------------
Bir event'in toplam kaç delivery hakkı olduğunu belirler.

Örnek::

    max_deliveries = 3

    delivery 1 -> failure -> RETRY
    delivery 2 -> failure -> RETRY
    delivery 3 -> failure -> DEAD_LETTER

Retry Delay
-----------
RetryPolicy yalnız RETRY kararı için retry gecikmesi hesaplar.
Queue veya worker implementation'ına bağlı değildir.

Varsayılan ``base_delay_seconds=0.0`` mevcut immediate-retry davranışını korur.
Pozitif delay kullanıldığında exponential backoff uygulanır::

    delay = base_delay_seconds * backoff_multiplier ** (delivery_count - 1)

Opsiyonel ``max_delay_seconds`` deterministic gecikmeye üst sınır koyar.
Absolute retry zamanı queue katmanında hesaplanır; RetryPolicy wall-clock bilmez.

Retry Jitter
------------
Varsayılan ``jitter_ratio=0.0`` ile jitter kapalıdır ve deterministic backoff
davranışı aynen korunur.

Jitter açıldığında final RETRY delay'i deterministic/capped delay üzerinden
hesaplanır::

    minimum_factor = 1.0 - jitter_ratio
    factor = minimum_factor + jitter_ratio * random_sample
    final_delay = deterministic_delay * factor

``random_sample`` değeri [0.0, 1.0] aralığındadır.

Bu modelde:

- jitter_ratio=0.0 -> jitter yok
- jitter_ratio=1.0 -> full jitter, [0, deterministic_delay]
- final delay deterministic delay'i aşmaz
- max_delay_seconds hard upper-bound olarak korunur

Test edilebilirlik için random source inject edilebilir.
Jitter kapalıyken random source çağrılmaz.
"""

import math
import random
from dataclasses import dataclass
from enum import Enum
from numbers import Real
from typing import Any, Callable, Optional


# =============================================================================
# EXCEPTIONS
# =============================================================================
class RetryPolicyError(RuntimeError):
    """
    Retry policy hatalarının temel sınıfı.
    """


class RetryPolicyValidationError(
    RetryPolicyError
):
    """
    Retry configuration veya decision input contract hatası.
    """


# =============================================================================
# ACTION
# =============================================================================
class RetryAction(
    str,
    Enum,
):
    """
    Event failure sonrası worker'ın uygulaması gereken karar.
    """

    RETRY = "retry"

    DISCARD = "discard"

    DEAD_LETTER = "dead_letter"


# =============================================================================
# HELPERS
# =============================================================================
def _normalize_positive_int(
    value: Any,
    *,
    field_name: str,
) -> int:
    if (
        isinstance(
            value,
            bool,
        )
        or not isinstance(
            value,
            int,
        )
    ):
        raise RetryPolicyValidationError(
            f"{field_name} int olmalıdır."
        )

    if value <= 0:
        raise RetryPolicyValidationError(
            f"{field_name} sıfırdan büyük olmalıdır."
        )

    return value


def _normalize_bool(
    value: Any,
    *,
    field_name: str,
) -> bool:
    if not isinstance(
        value,
        bool,
    ):
        raise RetryPolicyValidationError(
            f"{field_name} bool olmalıdır."
        )

    return value


def _normalize_non_negative_float(
    value: Any,
    *,
    field_name: str,
) -> float:
    if (
        isinstance(
            value,
            bool,
        )
        or not isinstance(
            value,
            Real,
        )
    ):
        raise RetryPolicyValidationError(
            f"{field_name} sayı olmalıdır."
        )

    normalized = float(
        value
    )

    if (
        not math.isfinite(
            normalized
        )
        or normalized < 0
    ):
        raise RetryPolicyValidationError(
            f"{field_name} negatif olmayan sonlu sayı olmalıdır."
        )

    return normalized


def _normalize_ratio(
    value: Any,
    *,
    field_name: str,
) -> float:
    normalized = (
        _normalize_non_negative_float(
            value,
            field_name=field_name,
        )
    )

    if normalized > 1.0:
        raise RetryPolicyValidationError(
            f"{field_name} en fazla 1.0 olabilir."
        )

    return normalized


def _normalize_backoff_multiplier(
    value: Any,
) -> float:
    if (
        isinstance(
            value,
            bool,
        )
        or not isinstance(
            value,
            Real,
        )
    ):
        raise RetryPolicyValidationError(
            "backoff_multiplier sayı olmalıdır."
        )

    normalized = float(
        value
    )

    if (
        not math.isfinite(
            normalized
        )
        or normalized < 1.0
    ):
        raise RetryPolicyValidationError(
            "backoff_multiplier en az 1.0 olan sonlu sayı olmalıdır."
        )

    return normalized


def _normalize_optional_non_negative_float(
    value: Any,
    *,
    field_name: str,
) -> Optional[float]:
    if value is None:
        return None

    return _normalize_non_negative_float(
        value,
        field_name=field_name,
    )


def _normalize_random_source(
    value: Any,
) -> Callable[
    [],
    Real,
]:
    if not callable(
        value
    ):
        raise RetryPolicyValidationError(
            "random_source callable olmalıdır."
        )

    return value


def _normalize_exception_types(
    value: Any,
    *,
    field_name: str,
) -> tuple[
    type[BaseException],
    ...,
]:
    if not isinstance(
        value,
        (
            list,
            tuple,
        ),
    ):
        raise RetryPolicyValidationError(
            f"{field_name} list veya tuple olmalıdır."
        )

    normalized: list[
        type[BaseException]
    ] = []

    for item in value:
        if not isinstance(
            item,
            type,
        ):
            raise RetryPolicyValidationError(
                f"{field_name} yalnız exception "
                "class'ları içermelidir."
            )

        if not issubclass(
            item,
            BaseException,
        ):
            raise RetryPolicyValidationError(
                f"{field_name} yalnız BaseException "
                "subclass'ları içermelidir."
            )

        normalized.append(
            item
        )

    return tuple(
        normalized
    )


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


# =============================================================================
# DECISION
# =============================================================================
@dataclass(
    slots=True,
    frozen=True,
)
class RetryDecision:
    """
    RetryPolicy tarafından üretilen immutable karar.

    retryable:
        Error configured retry exception sınıflarından birine uyuyor mu?

    exhausted:
        delivery_count max_deliveries limitine ulaşmış mı?

    retry_delay_seconds:
        Yalnız RETRY action için uygulanacak final gecikme.
        Jitter açıksa jitter uygulanmış değerdir.
        DISCARD ve DEAD_LETTER kararlarında her zaman 0.0'dır.
    """

    action: RetryAction

    delivery_count: int

    max_deliveries: int

    retryable: bool

    exhausted: bool

    reason: str

    error_type: str

    error_message: str

    retry_delay_seconds: float = 0.0

    @property
    def should_retry(
        self,
    ) -> bool:
        return (
            self.action
            is RetryAction.RETRY
        )

    @property
    def should_discard(
        self,
    ) -> bool:
        return (
            self.action
            is RetryAction.DISCARD
        )

    @property
    def should_dead_letter(
        self,
    ) -> bool:
        return (
            self.action
            is RetryAction.DEAD_LETTER
        )

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "action": (
                self.action.value
            ),
            "delivery_count": (
                self.delivery_count
            ),
            "max_deliveries": (
                self.max_deliveries
            ),
            "retryable": (
                self.retryable
            ),
            "exhausted": (
                self.exhausted
            ),
            "reason": (
                self.reason
            ),
            "error_type": (
                self.error_type
            ),
            "error_message": (
                self.error_message
            ),
            "retry_delay_seconds": (
                self.retry_delay_seconds
            ),
        }


# =============================================================================
# POLICY
# =============================================================================
class RetryPolicy:
    """
    Backend-independent event retry policy.

    Varsayılan::

        RetryPolicy(
            max_deliveries=3,
            retry_exceptions=(Exception,),
            discard_exceptions=(),
            dead_letter_on_exhaustion=True,
            dead_letter_non_retryable=True,
            base_delay_seconds=0.0,
            backoff_multiplier=2.0,
            max_delay_seconds=None,
            jitter_ratio=0.0,
        )

    Karar sırası::

        explicit discard exception
            ↓
        delivery exhausted
            ↓
        retryable exception
            ↓
        non-retryable error

    Retry delay yalnız RETRY kararı üretildiğinde hesaplanır.
    """

    def __init__(
        self,
        *,
        max_deliveries: int = 3,
        retry_exceptions: tuple[
            type[BaseException],
            ...,
        ] = (
            Exception,
        ),
        discard_exceptions: tuple[
            type[BaseException],
            ...,
        ] = (),
        dead_letter_on_exhaustion: bool = True,
        dead_letter_non_retryable: bool = True,
        base_delay_seconds: float = 0.0,
        backoff_multiplier: float = 2.0,
        max_delay_seconds: Optional[
            float
        ] = None,
        jitter_ratio: float = 0.0,
        random_source: Optional[
            Callable[
                [],
                Real,
            ]
        ] = None,
    ) -> None:
        self.max_deliveries = (
            _normalize_positive_int(
                max_deliveries,
                field_name=(
                    "max_deliveries"
                ),
            )
        )

        self.retry_exceptions = (
            _normalize_exception_types(
                retry_exceptions,
                field_name=(
                    "retry_exceptions"
                ),
            )
        )

        self.discard_exceptions = (
            _normalize_exception_types(
                discard_exceptions,
                field_name=(
                    "discard_exceptions"
                ),
            )
        )

        self.dead_letter_on_exhaustion = (
            _normalize_bool(
                dead_letter_on_exhaustion,
                field_name=(
                    "dead_letter_on_exhaustion"
                ),
            )
        )

        self.dead_letter_non_retryable = (
            _normalize_bool(
                dead_letter_non_retryable,
                field_name=(
                    "dead_letter_non_retryable"
                ),
            )
        )

        self.base_delay_seconds = (
            _normalize_non_negative_float(
                base_delay_seconds,
                field_name=(
                    "base_delay_seconds"
                ),
            )
        )

        self.backoff_multiplier = (
            _normalize_backoff_multiplier(
                backoff_multiplier
            )
        )

        self.max_delay_seconds = (
            _normalize_optional_non_negative_float(
                max_delay_seconds,
                field_name=(
                    "max_delay_seconds"
                ),
            )
        )

        self.jitter_ratio = (
            _normalize_ratio(
                jitter_ratio,
                field_name=(
                    "jitter_ratio"
                ),
            )
        )

        self.random_source = (
            random.random
            if random_source is None
            else _normalize_random_source(
                random_source
            )
        )

    # =========================================================================
    # CLASSIFICATION
    # =========================================================================
    def is_retryable(
        self,
        error: BaseException,
    ) -> bool:
        if not isinstance(
            error,
            BaseException,
        ):
            raise RetryPolicyValidationError(
                "error BaseException olmalıdır."
            )

        if not self.retry_exceptions:
            return False

        return isinstance(
            error,
            self.retry_exceptions,
        )

    def is_discardable(
        self,
        error: BaseException,
    ) -> bool:
        if not isinstance(
            error,
            BaseException,
        ):
            raise RetryPolicyValidationError(
                "error BaseException olmalıdır."
            )

        if not self.discard_exceptions:
            return False

        return isinstance(
            error,
            self.discard_exceptions,
        )

    # =========================================================================
    # JITTER
    # =========================================================================
    def _random_unit_interval(
        self,
    ) -> float:
        try:
            raw_value = (
                self.random_source()
            )

        except Exception as exc:
            raise RetryPolicyError(
                "random_source çağrısı başarısız."
            ) from exc

        if (
            isinstance(
                raw_value,
                bool,
            )
            or not isinstance(
                raw_value,
                Real,
            )
        ):
            raise RetryPolicyError(
                "random_source [0.0, 1.0] "
                "aralığında sayı döndürmelidir."
            )

        normalized = float(
            raw_value
        )

        if (
            not math.isfinite(
                normalized
            )
            or normalized < 0.0
            or normalized > 1.0
        ):
            raise RetryPolicyError(
                "random_source [0.0, 1.0] "
                "aralığında sonlu sayı döndürmelidir."
            )

        return normalized

    def _apply_jitter(
        self,
        delay_seconds: float,
    ) -> float:
        if (
            delay_seconds == 0.0
            or self.jitter_ratio == 0.0
        ):
            return float(
                delay_seconds
            )

        sample = (
            self._random_unit_interval()
        )

        minimum_factor = (
            1.0
            - self.jitter_ratio
        )

        factor = (
            minimum_factor
            + (
                self.jitter_ratio
                * sample
            )
        )

        jittered = (
            delay_seconds
            * factor
        )

        if not math.isfinite(
            jittered
        ):
            raise RetryPolicyError(
                "Jitter uygulanmış retry delay sonlu değeri aştı."
            )

        return float(
            jittered
        )

    # =========================================================================
    # RETRY DELAY
    # =========================================================================
    def retry_delay_for_delivery(
        self,
        delivery_count: int,
    ) -> float:
        """
        Mevcut failed delivery için final retry delay hesaplar.

        delivery_count=1 ilk retry gecikmesini üretir.
        Absolute timestamp hesaplamaz.

        Jitter kapalıysa deterministic exponential backoff döner.
        Jitter açıksa deterministic/capped delay azaltıcı jitter ile dağıtılır.
        """
        resolved_delivery_count = (
            _normalize_positive_int(
                delivery_count,
                field_name=(
                    "delivery_count"
                ),
            )
        )

        if self.base_delay_seconds == 0.0:
            return 0.0

        exponent = (
            resolved_delivery_count
            - 1
        )

        try:
            calculated = (
                self.base_delay_seconds
                * (
                    self.backoff_multiplier
                    ** exponent
                )
            )

        except OverflowError:
            calculated = float(
                "inf"
            )

        if self.max_delay_seconds is not None:
            calculated = min(
                calculated,
                self.max_delay_seconds,
            )

        if not math.isfinite(
            calculated
        ):
            raise RetryPolicyError(
                "Retry delay sonlu değeri aştı; "
                "max_delay_seconds yapılandırın."
            )

        return (
            self._apply_jitter(
                float(
                    calculated
                )
            )
        )

    # =========================================================================
    # DECISION
    # =========================================================================
    def decide(
        self,
        delivery_count: int,
        error: BaseException,
    ) -> RetryDecision:
        resolved_delivery_count = (
            _normalize_positive_int(
                delivery_count,
                field_name=(
                    "delivery_count"
                ),
            )
        )

        if not isinstance(
            error,
            BaseException,
        ):
            raise RetryPolicyValidationError(
                "error BaseException olmalıdır."
            )

        retryable = (
            self.is_retryable(
                error
            )
        )

        discardable = (
            self.is_discardable(
                error
            )
        )

        exhausted = (
            resolved_delivery_count
            >= self.max_deliveries
        )

        error_type = (
            error.__class__.__name__
        )

        error_message = (
            _safe_exception_message(
                error
            )
        )

        # ---------------------------------------------------------------------
        # EXPLICIT DISCARD
        # ---------------------------------------------------------------------
        if discardable:
            return RetryDecision(
                action=(
                    RetryAction.DISCARD
                ),
                delivery_count=(
                    resolved_delivery_count
                ),
                max_deliveries=(
                    self.max_deliveries
                ),
                retryable=False,
                exhausted=(
                    exhausted
                ),
                reason=(
                    "error_matches_discard_exception"
                ),
                error_type=(
                    error_type
                ),
                error_message=(
                    error_message
                ),
                retry_delay_seconds=0.0,
            )

        # ---------------------------------------------------------------------
        # DELIVERY EXHAUSTION
        # ---------------------------------------------------------------------
        if exhausted:
            if (
                self.dead_letter_on_exhaustion
            ):
                action = (
                    RetryAction.DEAD_LETTER
                )

                reason = (
                    "max_deliveries_exhausted"
                )

            else:
                action = (
                    RetryAction.DISCARD
                )

                reason = (
                    "max_deliveries_exhausted_discard"
                )

            return RetryDecision(
                action=action,
                delivery_count=(
                    resolved_delivery_count
                ),
                max_deliveries=(
                    self.max_deliveries
                ),
                retryable=(
                    retryable
                ),
                exhausted=True,
                reason=reason,
                error_type=(
                    error_type
                ),
                error_message=(
                    error_message
                ),
                retry_delay_seconds=0.0,
            )

        # ---------------------------------------------------------------------
        # RETRYABLE
        # ---------------------------------------------------------------------
        if retryable:
            retry_delay_seconds = (
                self.retry_delay_for_delivery(
                    resolved_delivery_count
                )
            )

            return RetryDecision(
                action=(
                    RetryAction.RETRY
                ),
                delivery_count=(
                    resolved_delivery_count
                ),
                max_deliveries=(
                    self.max_deliveries
                ),
                retryable=True,
                exhausted=False,
                reason=(
                    "retryable_error"
                ),
                error_type=(
                    error_type
                ),
                error_message=(
                    error_message
                ),
                retry_delay_seconds=(
                    retry_delay_seconds
                ),
            )

        # ---------------------------------------------------------------------
        # NON-RETRYABLE
        # ---------------------------------------------------------------------
        if (
            self.dead_letter_non_retryable
        ):
            action = (
                RetryAction.DEAD_LETTER
            )

            reason = (
                "non_retryable_error"
            )

        else:
            action = (
                RetryAction.DISCARD
            )

            reason = (
                "non_retryable_error_discard"
            )

        return RetryDecision(
            action=action,
            delivery_count=(
                resolved_delivery_count
            ),
            max_deliveries=(
                self.max_deliveries
            ),
            retryable=False,
            exhausted=False,
            reason=reason,
            error_type=(
                error_type
            ),
            error_message=(
                error_message
            ),
            retry_delay_seconds=0.0,
        )

    # =========================================================================
    # SNAPSHOT
    # =========================================================================
    def snapshot(
        self,
    ) -> dict[str, Any]:
        return {
            "max_deliveries": (
                self.max_deliveries
            ),
            "retry_exceptions": [
                (
                    exception_type.__name__
                )
                for exception_type
                in self.retry_exceptions
            ],
            "discard_exceptions": [
                (
                    exception_type.__name__
                )
                for exception_type
                in self.discard_exceptions
            ],
            "dead_letter_on_exhaustion": (
                self.dead_letter_on_exhaustion
            ),
            "dead_letter_non_retryable": (
                self.dead_letter_non_retryable
            ),
            "base_delay_seconds": (
                self.base_delay_seconds
            ),
            "backoff_multiplier": (
                self.backoff_multiplier
            ),
            "max_delay_seconds": (
                self.max_delay_seconds
            ),
            "jitter_ratio": (
                self.jitter_ratio
            ),
            "jitter_enabled": (
                self.jitter_ratio
                > 0.0
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
            f"max_deliveries="
            f"{self.max_deliveries}, "
            f"retry_exception_count="
            f"{len(self.retry_exceptions)}, "
            f"discard_exception_count="
            f"{len(self.discard_exceptions)}, "
            f"dead_letter_on_exhaustion="
            f"{self.dead_letter_on_exhaustion}, "
            f"dead_letter_non_retryable="
            f"{self.dead_letter_non_retryable}, "
            f"base_delay_seconds="
            f"{self.base_delay_seconds}, "
            f"backoff_multiplier="
            f"{self.backoff_multiplier}, "
            f"max_delay_seconds="
            f"{self.max_delay_seconds}, "
            f"jitter_ratio="
            f"{self.jitter_ratio}"
            f")"
        )