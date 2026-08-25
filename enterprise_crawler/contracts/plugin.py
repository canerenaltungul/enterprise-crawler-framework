from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class PluginInfo:

    name: str

    version: str

    author: str = ""

    description: str = ""

    metadata: dict[str, Any] = field(default_factory=dict)