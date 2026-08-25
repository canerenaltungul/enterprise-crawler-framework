from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class CrawlerInfo:

    name: str

    version: str

    framework_version: str