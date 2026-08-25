from __future__ import annotations

from enum import Enum


class ExecutionStatus(str, Enum):
    """
    Bir crawler çalışmasının yaşam döngüsü durumu.
    """

    INITIALIZED = "initialized"

    RUNNING = "running"

    COMPLETED = "completed"

    FAILED = "failed"

    CANCELLED = "cancelled"

    DEGRADED = "degraded"

    SKIPPED = "skipped"


class HealthState(str, Enum):
    """
    Component sağlık durumu.
    """

    HEALTHY = "healthy"

    DEGRADED = "degraded"

    UNHEALTHY = "unhealthy"


class EventPriority(str, Enum):
    """
    Event önceliği.
    """

    LOW = "low"

    NORMAL = "normal"

    HIGH = "high"

    CRITICAL = "critical"