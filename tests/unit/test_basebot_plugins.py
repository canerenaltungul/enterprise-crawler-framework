from __future__ import annotations

import pytest

from enterprise_crawler.contracts.enums import (
    ExecutionStatus,
)
from enterprise_crawler.core.base_bot import (
    BaseBot,
)
from enterprise_crawler.exceptions import (
    PluginError,
)
from enterprise_crawler.plugins import (
    PluginManager,
)


class SimpleBot(BaseBot):
    def execute(self):
        return None


class RuntimePlugin:
    def __init__(self) -> None:
        self.events: list[str] = []

    def on_load(
        self,
        manager,
    ) -> None:
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

    def ping(
        self,
    ) -> str:
        return "pong"


def test_plugins_are_disabled_by_default() -> None:
    bot = SimpleBot()

    try:
        assert (
            bot.plugins
            is None
        )

        assert (
            bot.runtime_snapshot()[
                "plugins_enabled"
            ]
            is False
        )

    finally:
        bot.close()


def test_plugin_manager_can_be_injected() -> None:
    manager = PluginManager()

    bot = SimpleBot(
        plugin_manager=manager
    )

    try:
        assert (
            bot.plugins
            is manager
        )

        assert (
            bot.require_plugins()
            is manager
        )

    finally:
        bot.close()
        manager.close()


def test_invalid_plugin_manager_is_rejected() -> None:
    with pytest.raises(
        PluginError
    ):
        SimpleBot(
            plugin_manager=object()
        )


def test_require_plugins_fails_when_not_configured() -> None:
    bot = SimpleBot()

    try:
        with pytest.raises(
            PluginError
        ):
            bot.require_plugins()

    finally:
        bot.close()


def test_injected_plugin_manager_is_not_owned_by_bot() -> None:
    manager = PluginManager()

    bot = SimpleBot(
        plugin_manager=manager
    )

    bot.close()

    assert (
        manager.snapshot()[
            "closed"
        ]
        is False
    )

    manager.close()


def test_closed_plugin_manager_is_rejected_by_require_plugins() -> None:
    manager = PluginManager()

    bot = SimpleBot(
        plugin_manager=manager
    )

    manager.close()

    try:
        with pytest.raises(
            PluginError
        ):
            bot.require_plugins()

    finally:
        bot.close()


def test_plugin_manager_snapshot_is_exposed() -> None:
    manager = PluginManager()

    bot = SimpleBot(
        plugin_manager=manager
    )

    try:
        snapshot = (
            bot.runtime_snapshot()
        )

        assert (
            snapshot[
                "plugins_enabled"
            ]
            is True
        )

        assert isinstance(
            snapshot[
                "plugins"
            ],
            dict,
        )

        assert (
            snapshot[
                "plugins"
            ][
                "closed"
            ]
            is False
        )

    finally:
        bot.close()
        manager.close()


def test_plugin_can_be_used_inside_execute() -> None:
    manager = PluginManager()

    plugin = RuntimePlugin()

    from enterprise_crawler.contracts import (
        PluginInfo,
    )

    manager.register(
        plugin,
        info=PluginInfo(
            name="runtime",
            version="1.0.0",
        ),
    )

    class PluginBot(BaseBot):
        def execute(self):
            plugins = (
                self.require_plugins()
            )

            result = plugins.invoke(
                "runtime",
                "ping",
            )

            if result != "pong":
                raise RuntimeError(
                    "unexpected plugin result"
                )

            self.mark_record_processed()

            return {
                "status": "completed",
                "records_processed": 1,
                "metadata": {
                    "plugin_result": (
                        result
                    ),
                },
            }

    bot = PluginBot(
        plugin_manager=manager
    )

    try:
        result = bot.run()

        assert (
            result.status
            == ExecutionStatus.COMPLETED
        )

        assert (
            result.records_processed
            == 1
        )

        assert (
            result.metadata[
                "plugin_result"
            ]
            == "pong"
        )

    finally:
        bot.close()
        manager.close()


def test_basebot_close_does_not_unload_injected_plugins() -> None:
    manager = PluginManager()

    plugin = RuntimePlugin()

    from enterprise_crawler.contracts import (
        PluginInfo,
    )

    manager.register(
        plugin,
        info=PluginInfo(
            name="runtime",
            version="1.0.0",
        ),
    )

    bot = SimpleBot(
        plugin_manager=manager
    )

    bot.close()

    assert (
        manager.snapshot()[
            "closed"
        ]
        is False
    )

    assert (
        "runtime"
        in manager
    )

    assert (
        plugin.events
        == [
            "load",
            "enable",
        ]
    )

    manager.close()

    assert (
        plugin.events
        == [
            "load",
            "enable",
            "disable",
            "unload",
        ]
    )


def test_bot_can_run_multiple_times_with_same_plugin_manager() -> None:
    manager = PluginManager()

    bot = SimpleBot(
        plugin_manager=manager
    )

    try:
        first = bot.run()
        second = bot.run()

        assert (
            first.status
            == ExecutionStatus.COMPLETED
        )

        assert (
            second.status
            == ExecutionStatus.COMPLETED
        )

        assert (
            bot.run_count
            == 2
        )

        assert (
            bot.require_plugins()
            is manager
        )

    finally:
        bot.close()
        manager.close()


def test_repr_reports_plugin_state() -> None:
    manager = PluginManager()

    bot = SimpleBot(
        plugin_manager=manager
    )

    try:
        representation = repr(
            bot
        )

        assert (
            "plugins_enabled=True"
            in representation
        )

    finally:
        bot.close()
        manager.close()