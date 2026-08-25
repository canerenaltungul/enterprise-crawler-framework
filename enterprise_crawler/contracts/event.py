from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class Event:

    event_type: str

    timestamp: datetime

    payload: dict[str, Any]

    metadata: dict[str, Any] = field(default_factory=dict)