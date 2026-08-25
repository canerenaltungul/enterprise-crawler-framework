from dataclasses import dataclass, field
from typing import Any

from .enums import ExecutionStatus


@dataclass(slots=True)
class ExecutionResult:

    status: ExecutionStatus

    records_processed: int = 0

    errors: int = 0

    warnings: int = 0

    metadata: dict[str, Any] = field(default_factory=dict)