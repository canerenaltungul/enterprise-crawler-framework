from __future__ import annotations

"""
Enterprise Crawler Framework integration-test plugin.

Bu paket gerçek Python entry-point metadata'sı üzerinden keşfedilmek üzere
integration test fixture olarak kullanılır.
"""

from typing import Any

from enterprise_crawler.contracts import (
    PluginInfo,
)


class FixturePlugin:
    """
    Gerçek packaging entry-point üzerinden yüklenen test plugin'i.
    """

    plugin_info = PluginInfo(
        name="fixture-runtime-plugin",
        version="0.1.0",
        author="Enterprise Crawler Framework",
        description=(
            "Real entry-point integration test plugin."
        ),
        metadata={
            "fixture": True,
            "source": "python-entry-point",
        },
    )

    def __init__(
        self,
    ) -> None:
        self.events: list[
            str
        ] = []

        self.manager: Any = None

    def on_load(
        self,
        manager: Any,
    ) -> None:
        self.manager = manager

        self.events.append(
            "load"
        )

    def on_enable(
        self,
    ) -> None:
        self.events.append(
            "enable"
        )

    def on_disable(
        self,
    ) -> None:
        self.events.append(
            "disable"
        )

    def on_unload(
        self,
    ) -> None:
        self.events.append(
            "unload"
        )

    def collect(
        self,
        value: str = "hello",
    ) -> dict[str, Any]:
        return {
            "plugin": (
                self.plugin_info.name
            ),
            "version": (
                self.plugin_info.version
            ),
            "value": value,
            "events": list(
                self.events
            ),
        }