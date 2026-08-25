"""
Enterprise Crawler Framework

Top-level public API.

The root package intentionally exposes only the small set of types required
for the primary framework workflow:

    BaseBot
        ↓
    Crawler
        ↓
    ExecutionResult

Subsystem-specific APIs remain available from their dedicated namespaces,
for example:

    enterprise_crawler.config
    enterprise_crawler.events
    enterprise_crawler.plugins
    enterprise_crawler.processing
    enterprise_crawler.storage
"""

from .version import (
    __version__,
    __title__,
    FRAMEWORK_NAME,
)
from .contracts import (
    ExecutionResult,
)
from .contracts.enums import (
    ExecutionStatus,
)
from .core.base_bot import (
    BaseBot,
)
from .core.crawler import (
    Crawler,
)


__all__ = [
    "__version__",
    "__title__",
    "FRAMEWORK_NAME",
    "BaseBot",
    "Crawler",
    "ExecutionResult",
    "ExecutionStatus",
]