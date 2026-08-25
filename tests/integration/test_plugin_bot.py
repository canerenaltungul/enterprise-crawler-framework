from __future__ import annotations

from typing import Any

import pytest

from enterprise_crawler.contracts import (
    ExecutionResult,
    PluginInfo,
)
from enterprise_crawler.contracts.enums import (
    ExecutionStatus,
)
from enterprise_crawler.core.base_bot import (
    BaseBot,
)
from enterprise_crawler.core.crawler import (
    Crawler,
)
from enterprise_crawler.exceptions import (
    PluginError,
)
from enterprise_crawler.plugins import (
    PluginManager,
)


# =============================================================================
# TEST PLUGINS
# =============================================================================
class EchoPlugin:
    """
    Basit plugin.

    PluginManager lifecycle hook'larını ve public method invocation'ı
    gözlemlemek için kullanılır.
    """

    plugin_info = PluginInfo(
        name="echo",
        version="1.0.0",
        author="Enterprise Crawler Framework",
        description="Integration test echo plugin.",
    )

    def __init__(
        self,
    ) -> None:
        self.events: list[str] = []

    def on_load(
        self,
        manager: PluginManager,
    ) -> None:
        assert isinstance(
            manager,
            PluginManager,
        )

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

    def echo(
        self,
        value: Any,
    ) -> Any:
        self.events.append(
            "invoke"
        )

        return value


class FailingPlugin:
    plugin_info = PluginInfo(
        name="failing",
        version="1.0.0",
    )

    def explode(
        self,
    ) -> None:
        raise RuntimeError(
            "plugin exploded"
        )


# =============================================================================
# TEST BOTS
# =============================================================================
class PluginBot(
    BaseBot
):
    def __init__(
        self,
        *,
        plugin_manager: PluginManager,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            **kwargs
        )

        if not isinstance(
            plugin_manager,
            PluginManager,
        ):
            raise TypeError(
                "plugin_manager PluginManager olmalıdır."
            )

        self.plugin_manager = (
            plugin_manager
        )

        self.plugin_result: Any = None

    def execute(
        self,
    ) -> ExecutionResult:
        self.plugin_result = (
            self.plugin_manager.invoke(
                "echo",
                "echo",
                "hello plugin",
            )
        )

        self.mark_record_processed()

        return ExecutionResult(
            status=(
                ExecutionStatus.COMPLETED
            ),
            records_processed=(
                self.records_processed
            ),
            errors=0,
            warnings=0,
            metadata={
                "plugin": {
                    "name": "echo",
                    "result": (
                        self.plugin_result
                    ),
                }
            },
        )


class DisabledPluginBot(
    BaseBot
):
    def __init__(
        self,
        *,
        plugin_manager: PluginManager,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            **kwargs
        )

        self.plugin_manager = (
            plugin_manager
        )

    def execute(
        self,
    ) -> None:
        self.plugin_manager.invoke(
            "echo",
            "echo",
            "should not run",
        )


class FailingPluginBot(
    BaseBot
):
    def __init__(
        self,
        *,
        plugin_manager: PluginManager,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            **kwargs
        )

        self.plugin_manager = (
            plugin_manager
        )

    def execute(
        self,
    ) -> None:
        self.plugin_manager.invoke(
            "failing",
            "explode",
        )


# =============================================================================
# HELPERS
# =============================================================================
def build_echo_runtime(
    *,
    enabled: bool = True,
) -> tuple[
    PluginManager,
    EchoPlugin,
]:
    manager = PluginManager()

    plugin = EchoPlugin()

    manager.register(
        plugin,
        enabled=enabled,
    )

    return (
        manager,
        plugin,
    )


# =============================================================================
# BASEBOT + PLUGIN MANAGER
# =============================================================================
def test_plugin_runs_inside_basebot_lifecycle() -> None:
    manager, plugin = (
        build_echo_runtime()
    )

    bot = PluginBot(
        plugin_manager=manager,
    )

    try:
        result = bot.run()

        assert (
            result.status
            is ExecutionStatus.COMPLETED
        )

        assert (
            result.records_processed
            == 1
        )

        assert (
            result.errors
            == 0
        )

        assert (
            bot.plugin_result
            == "hello plugin"
        )

        assert (
            result.metadata[
                "plugin"
            ][
                "name"
            ]
            == "echo"
        )

        assert (
            result.metadata[
                "plugin"
            ][
                "result"
            ]
            == "hello plugin"
        )

        assert plugin.events == [
            "load",
            "enable",
            "invoke",
        ]

    finally:
        bot.close()
        manager.close()


# =============================================================================
# CRAWLER + BASEBOT + PLUGIN MANAGER
# =============================================================================
def test_plugin_runs_through_crawler() -> None:
    manager, plugin = (
        build_echo_runtime()
    )

    bot = PluginBot(
        plugin_manager=manager,
    )

    crawler = Crawler(
        bot
    )

    try:
        result = crawler.run()

        assert (
            result.status
            is ExecutionStatus.COMPLETED
        )

        assert (
            result.records_processed
            == 1
        )

        assert (
            bot.plugin_result
            == "hello plugin"
        )

        assert (
            crawler.last_result
            is result
        )

        assert plugin.events == [
            "load",
            "enable",
            "invoke",
        ]

    finally:
        bot.close()
        manager.close()


# =============================================================================
# DISABLED PLUGIN
# =============================================================================
def test_disabled_plugin_fails_closed_inside_bot() -> None:
    manager, plugin = (
        build_echo_runtime(
            enabled=False
        )
    )

    bot = DisabledPluginBot(
        plugin_manager=manager,
    )

    try:
        result = bot.run()

        assert (
            result.status
            is ExecutionStatus.FAILED
        )

        assert (
            result.errors
            >= 1
        )

        assert (
            "failure"
            in result.metadata
        )

        failure = (
            result.metadata[
                "failure"
            ]
        )

        assert (
            failure[
                "exception_type"
            ]
        )

        assert (
            "plugin"
            in failure[
                "message"
            ].lower()
        )

        assert plugin.events == [
            "load",
        ]

    finally:
        bot.close()
        manager.close()


# =============================================================================
# PLUGIN FAILURE → EXECUTION RESULT
# =============================================================================
def test_plugin_exception_becomes_failed_execution_result() -> None:
    manager = PluginManager()

    plugin = FailingPlugin()

    manager.register(
        plugin
    )

    bot = FailingPluginBot(
        plugin_manager=manager,
    )

    try:
        result = bot.run()

        assert (
            result.status
            is ExecutionStatus.FAILED
        )

        assert (
            result.errors
            >= 1
        )

        assert (
            "failure"
            in result.metadata
        )

        failure = (
            result.metadata[
                "failure"
            ]
        )

        assert (
            failure[
                "exception_type"
            ]
        )

        assert (
            failure[
                "message"
            ]
        )

    finally:
        bot.close()
        manager.close()


# =============================================================================
# SEQUENTIAL RUNS
# =============================================================================
def test_plugin_bot_can_run_more_than_once() -> None:
    manager, plugin = (
        build_echo_runtime()
    )

    bot = PluginBot(
        plugin_manager=manager,
    )

    try:
        first = bot.run()
        second = bot.run()

        assert (
            first.status
            is ExecutionStatus.COMPLETED
        )

        assert (
            second.status
            is ExecutionStatus.COMPLETED
        )

        assert (
            bot.run_count
            == 2
        )

        assert plugin.events == [
            "load",
            "enable",
            "invoke",
            "invoke",
        ]

    finally:
        bot.close()
        manager.close()


# =============================================================================
# EXTERNAL RESOURCE OWNERSHIP
# =============================================================================
def test_external_plugin_manager_is_not_closed_by_bot() -> None:
    manager, plugin = (
        build_echo_runtime()
    )

    bot = PluginBot(
        plugin_manager=manager,
    )

    result = bot.run()

    assert (
        result.status
        is ExecutionStatus.COMPLETED
    )

    bot.close()

    assert (
        manager.is_closed
        is False
    )

    assert (
        manager.contains(
            "echo"
        )
        is True
    )

    assert (
        manager.invoke(
            "echo",
            "echo",
            "after bot close",
        )
        == "after bot close"
    )

    assert plugin.events == [
        "load",
        "enable",
        "invoke",
        "invoke",
    ]

    manager.close()


# =============================================================================
# MANAGER CLEANUP
# =============================================================================
def test_plugin_manager_cleanup_is_deterministic() -> None:
    manager, plugin = (
        build_echo_runtime()
    )

    assert plugin.events == [
        "load",
        "enable",
    ]

    manager.close()

    assert plugin.events == [
        "load",
        "enable",
        "disable",
        "unload",
    ]

    assert (
        manager.is_closed
        is True
    )


# =============================================================================
# DISABLE → ENABLE → BOT EXECUTION
# =============================================================================
def test_plugin_can_be_reenabled_before_bot_run() -> None:
    manager, plugin = (
        build_echo_runtime()
    )

    manager.disable(
        "echo"
    )

    assert plugin.events == [
        "load",
        "enable",
        "disable",
    ]

    manager.enable(
        "echo"
    )

    assert plugin.events == [
        "load",
        "enable",
        "disable",
        "enable",
    ]

    bot = PluginBot(
        plugin_manager=manager,
    )

    try:
        result = bot.run()

        assert (
            result.status
            is ExecutionStatus.COMPLETED
        )

        assert (
            bot.plugin_result
            == "hello plugin"
        )

        assert plugin.events == [
            "load",
            "enable",
            "disable",
            "enable",
            "invoke",
        ]

    finally:
        bot.close()
        manager.close()


# =============================================================================
# PLUGIN SNAPSHOT SURVIVES BOT RUN
# =============================================================================
def test_plugin_runtime_snapshot_survives_bot_execution() -> None:
    manager, _ = (
        build_echo_runtime()
    )

    bot = PluginBot(
        plugin_manager=manager,
    )

    try:
        result = bot.run()

        assert (
            result.status
            is ExecutionStatus.COMPLETED
        )

        snapshot = (
            manager.snapshot()
        )

        assert (
            snapshot[
                "closed"
            ]
            is False
        )

        assert (
            snapshot[
                "plugin_count"
            ]
            == 1
        )

        assert (
            snapshot[
                "enabled_count"
            ]
            == 1
        )

        assert (
            snapshot[
                "plugins"
            ][0][
                "name"
            ]
            == "echo"
        )

    finally:
        bot.close()
        manager.close()