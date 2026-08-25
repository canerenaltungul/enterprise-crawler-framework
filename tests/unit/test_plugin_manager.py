from __future__ import annotations

from typing import Any

import pytest

from enterprise_crawler.contracts import (
    PluginInfo,
)
from enterprise_crawler.plugins import (
    PluginDisabledError,
    PluginInvocationError,
    PluginLifecycleError,
    PluginManager,
    PluginManagerClosedError,
    PluginRegistry,
)


# =============================================================================
# TEST PLUGINS
# =============================================================================
class SimplePlugin:
    plugin_info = PluginInfo(
        name="simple",
        version="1.0.0",
        author="Enterprise Crawler",
        description="Simple test plugin.",
    )

    def echo(
        self,
        value: Any,
    ) -> Any:
        return value


class CallableInfoPlugin:
    def plugin_info(
        self,
    ) -> PluginInfo:
        return PluginInfo(
            name="callable",
            version="2.0.0",
        )

    def value(
        self,
    ) -> int:
        return 42


class LifecyclePlugin:
    plugin_info = PluginInfo(
        name="lifecycle",
        version="1.0.0",
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
        manager: PluginManager,
    ) -> None:
        self.events.append(
            "load"
        )

        self.manager = manager

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

    def execute(
        self,
        value: int,
    ) -> int:
        self.events.append(
            "execute"
        )

        return value * 2


class FailingInvocationPlugin:
    plugin_info = PluginInfo(
        name="failing-invocation",
        version="1.0.0",
    )

    def explode(
        self,
    ) -> None:
        raise ValueError(
            "boom"
        )


class FailingLoadPlugin:
    plugin_info = PluginInfo(
        name="failing-load",
        version="1.0.0",
    )

    def on_load(
        self,
        manager: PluginManager,
    ) -> None:
        raise RuntimeError(
            "load failed"
        )


class FailingEnablePlugin:
    plugin_info = PluginInfo(
        name="failing-enable",
        version="1.0.0",
    )

    def on_enable(
        self,
    ) -> None:
        raise RuntimeError(
            "enable failed"
        )


class InvalidHookPlugin:
    plugin_info = PluginInfo(
        name="invalid-hook",
        version="1.0.0",
    )

    on_load = "not-callable"


class AttributePlugin:
    plugin_info = PluginInfo(
        name="attribute",
        version="1.0.0",
    )

    value = 123


# =============================================================================
# CONSTRUCTION
# =============================================================================
def test_manager_creates_registry() -> None:
    manager = PluginManager()

    assert isinstance(
        manager.registry,
        PluginRegistry,
    )

    assert (
        manager.plugin_count
        == 0
    )

    assert (
        manager.enabled_count
        == 0
    )

    assert (
        manager.disabled_count
        == 0
    )


def test_injected_registry_is_reused() -> None:
    registry = (
        PluginRegistry()
    )

    manager = PluginManager(
        registry=registry
    )

    assert (
        manager.registry
        is registry
    )


def test_invalid_registry_is_rejected() -> None:
    with pytest.raises(
        TypeError
    ):
        PluginManager(
            registry=object()
        )


# =============================================================================
# REGISTRATION
# =============================================================================
def test_register_plugin() -> None:
    manager = PluginManager()

    plugin = SimplePlugin()

    returned = manager.register(
        plugin
    )

    assert returned is plugin

    assert (
        manager.get(
            "simple"
        )
        is plugin
    )

    assert (
        manager.plugin_count
        == 1
    )


def test_register_plugin_enabled_by_default() -> None:
    manager = PluginManager()

    manager.register(
        SimplePlugin()
    )

    assert (
        manager.is_enabled(
            "simple"
        )
        is True
    )

    assert (
        manager.enabled_count
        == 1
    )


def test_plugin_can_start_disabled() -> None:
    manager = PluginManager()

    manager.register(
        SimplePlugin(),
        enabled=False,
    )

    assert (
        manager.is_enabled(
            "simple"
        )
        is False
    )

    assert (
        manager.disabled_count
        == 1
    )


def test_invalid_enabled_flag_is_rejected() -> None:
    manager = PluginManager()

    with pytest.raises(
        TypeError
    ):
        manager.register(
            SimplePlugin(),
            enabled=1,
        )


def test_explicit_plugin_info_is_supported() -> None:
    manager = PluginManager()

    plugin = object()

    info = PluginInfo(
        name="explicit",
        version="3.0.0",
    )

    manager.register(
        plugin,
        info=info,
    )

    assert (
        manager.get(
            "explicit"
        )
        is plugin
    )

    assert (
        manager.get_info(
            "explicit"
        ).version
        == "3.0.0"
    )


def test_invalid_explicit_info_is_rejected() -> None:
    manager = PluginManager()

    with pytest.raises(
        TypeError
    ):
        manager.register(
            object(),
            info="invalid",
        )


# =============================================================================
# LIFECYCLE
# =============================================================================
def test_registration_runs_load_and_enable_hooks() -> None:
    manager = PluginManager()

    plugin = (
        LifecyclePlugin()
    )

    manager.register(
        plugin
    )

    assert plugin.events == [
        "load",
        "enable",
    ]

    assert (
        plugin.manager
        is manager
    )


def test_disabled_registration_only_runs_load() -> None:
    manager = PluginManager()

    plugin = (
        LifecyclePlugin()
    )

    manager.register(
        plugin,
        enabled=False,
    )

    assert plugin.events == [
        "load",
    ]


def test_missing_lifecycle_hooks_are_allowed() -> None:
    manager = PluginManager()

    manager.register(
        SimplePlugin()
    )

    assert (
        manager.contains(
            "simple"
        )
        is True
    )


def test_non_callable_lifecycle_hook_is_rejected() -> None:
    manager = PluginManager()

    with pytest.raises(
        PluginLifecycleError
    ):
        manager.register(
            InvalidHookPlugin()
        )


def test_failing_load_rolls_back_registration() -> None:
    manager = PluginManager()

    with pytest.raises(
        PluginLifecycleError
    ):
        manager.register(
            FailingLoadPlugin()
        )

    assert (
        manager.plugin_count
        == 0
    )

    assert (
        manager.contains(
            "failing-load"
        )
        is False
    )


def test_failing_enable_rolls_back_registration() -> None:
    manager = PluginManager()

    with pytest.raises(
        PluginLifecycleError
    ):
        manager.register(
            FailingEnablePlugin()
        )

    assert (
        manager.plugin_count
        == 0
    )

    assert (
        manager.contains(
            "failing-enable"
        )
        is False
    )


# =============================================================================
# LOOKUP
# =============================================================================
def test_get_plugin() -> None:
    manager = PluginManager()

    plugin = SimplePlugin()

    manager.register(
        plugin
    )

    assert (
        manager.get(
            "simple"
        )
        is plugin
    )


def test_lookup_is_case_insensitive() -> None:
    manager = PluginManager()

    manager.register(
        SimplePlugin()
    )

    assert (
        manager.get(
            "SIMPLE"
        ).plugin_info.name
        == "simple"
    )


def test_contains_plugin() -> None:
    manager = PluginManager()

    manager.register(
        SimplePlugin()
    )

    assert (
        manager.contains(
            "simple"
        )
        is True
    )

    assert (
        manager.contains(
            "missing"
        )
        is False
    )


def test_contains_operator() -> None:
    manager = PluginManager()

    manager.register(
        SimplePlugin()
    )

    assert (
        "simple"
        in manager
    )

    assert (
        "missing"
        not in manager
    )


def test_contains_operator_handles_non_string() -> None:
    manager = PluginManager()

    assert (
        123 not in manager
    )


# =============================================================================
# ENABLE / DISABLE
# =============================================================================
def test_disable_plugin() -> None:
    manager = PluginManager()

    plugin = (
        LifecyclePlugin()
    )

    manager.register(
        plugin
    )

    changed = manager.disable(
        "lifecycle"
    )

    assert changed is True

    assert (
        manager.is_enabled(
            "lifecycle"
        )
        is False
    )

    assert plugin.events == [
        "load",
        "enable",
        "disable",
    ]


def test_disabling_disabled_plugin_is_idempotent() -> None:
    manager = PluginManager()

    plugin = (
        LifecyclePlugin()
    )

    manager.register(
        plugin,
        enabled=False,
    )

    assert (
        manager.disable(
            "lifecycle"
        )
        is False
    )

    assert plugin.events == [
        "load",
    ]


def test_enable_plugin() -> None:
    manager = PluginManager()

    plugin = (
        LifecyclePlugin()
    )

    manager.register(
        plugin,
        enabled=False,
    )

    changed = manager.enable(
        "lifecycle"
    )

    assert changed is True

    assert (
        manager.is_enabled(
            "lifecycle"
        )
        is True
    )

    assert plugin.events == [
        "load",
        "enable",
    ]


def test_enabling_enabled_plugin_is_idempotent() -> None:
    manager = PluginManager()

    plugin = (
        LifecyclePlugin()
    )

    manager.register(
        plugin
    )

    assert (
        manager.enable(
            "lifecycle"
        )
        is False
    )

    assert plugin.events == [
        "load",
        "enable",
    ]


# =============================================================================
# INVOCATION
# =============================================================================
def test_invoke_plugin_method() -> None:
    manager = PluginManager()

    manager.register(
        SimplePlugin()
    )

    result = manager.invoke(
        "simple",
        "echo",
        "hello",
    )

    assert result == "hello"


def test_invoke_forwards_args() -> None:
    manager = PluginManager()

    manager.register(
        LifecyclePlugin()
    )

    assert (
        manager.invoke(
            "lifecycle",
            "execute",
            21,
        )
        == 42
    )


def test_disabled_plugin_cannot_be_invoked() -> None:
    manager = PluginManager()

    manager.register(
        SimplePlugin(),
        enabled=False,
    )

    with pytest.raises(
        PluginDisabledError
    ):
        manager.invoke(
            "simple",
            "echo",
            "hello",
        )


def test_missing_method_is_rejected() -> None:
    manager = PluginManager()

    manager.register(
        SimplePlugin()
    )

    with pytest.raises(
        PluginInvocationError
    ):
        manager.invoke(
            "simple",
            "missing",
        )


def test_non_callable_attribute_is_rejected() -> None:
    manager = PluginManager()

    manager.register(
        AttributePlugin()
    )

    with pytest.raises(
        PluginInvocationError
    ):
        manager.invoke(
            "attribute",
            "value",
        )


def test_private_method_invocation_is_rejected() -> None:
    manager = PluginManager()

    manager.register(
        SimplePlugin()
    )

    with pytest.raises(
        PluginInvocationError
    ):
        manager.invoke(
            "simple",
            "__repr__",
        )


def test_empty_method_name_is_rejected() -> None:
    manager = PluginManager()

    manager.register(
        SimplePlugin()
    )

    with pytest.raises(
        ValueError
    ):
        manager.invoke(
            "simple",
            " ",
        )


def test_non_string_method_name_is_rejected() -> None:
    manager = PluginManager()

    manager.register(
        SimplePlugin()
    )

    with pytest.raises(
        TypeError
    ):
        manager.invoke(
            "simple",
            123,
        )


def test_plugin_exception_becomes_invocation_error() -> None:
    manager = PluginManager()

    manager.register(
        FailingInvocationPlugin()
    )

    with pytest.raises(
        PluginInvocationError
    ) as exc_info:
        manager.invoke(
            "failing-invocation",
            "explode",
        )

    assert (
        "ValueError"
        in str(
            exc_info.value
        )
    )

    assert (
        "boom"
        in str(
            exc_info.value
        )
    )


# =============================================================================
# UNREGISTER
# =============================================================================
def test_unregister_plugin() -> None:
    manager = PluginManager()

    plugin = (
        LifecyclePlugin()
    )

    manager.register(
        plugin
    )

    manager.unregister(
        "lifecycle"
    )

    assert plugin.events == [
        "load",
        "enable",
        "disable",
        "unload",
    ]

    assert (
        manager.contains(
            "lifecycle"
        )
        is False
    )

    assert (
        manager.plugin_count
        == 0
    )


def test_unregister_disabled_plugin_does_not_disable_twice() -> None:
    manager = PluginManager()

    plugin = (
        LifecyclePlugin()
    )

    manager.register(
        plugin
    )

    manager.disable(
        "lifecycle"
    )

    manager.unregister(
        "lifecycle"
    )

    assert plugin.events == [
        "load",
        "enable",
        "disable",
        "unload",
    ]


# =============================================================================
# NAMES
# =============================================================================
def test_names_are_deterministic() -> None:
    manager = PluginManager()

    manager.register(
        object(),
        info=PluginInfo(
            name="zeta",
            version="1",
        ),
    )

    manager.register(
        object(),
        info=PluginInfo(
            name="alpha",
            version="1",
        ),
    )

    assert manager.names() == [
        "alpha",
        "zeta",
    ]


def test_enabled_only_names() -> None:
    manager = PluginManager()

    manager.register(
        object(),
        info=PluginInfo(
            name="alpha",
            version="1",
        ),
    )

    manager.register(
        object(),
        info=PluginInfo(
            name="beta",
            version="1",
        ),
        enabled=False,
    )

    assert manager.names(
        enabled_only=True
    ) == [
        "alpha",
    ]


def test_invalid_enabled_only_flag_is_rejected() -> None:
    manager = PluginManager()

    with pytest.raises(
        TypeError
    ):
        manager.names(
            enabled_only=1
        )


# =============================================================================
# SNAPSHOT
# =============================================================================
def test_plugin_snapshot() -> None:
    manager = PluginManager()

    manager.register(
        SimplePlugin()
    )

    manager.invoke(
        "simple",
        "echo",
        "value",
    )

    snapshot = (
        manager.plugin_snapshot(
            "simple"
        )
    )

    assert (
        snapshot["name"]
        == "simple"
    )

    assert (
        snapshot["version"]
        == "1.0.0"
    )

    assert (
        snapshot["runtime"][
            "enabled"
        ]
        is True
    )

    assert (
        snapshot["runtime"][
            "invocation_count"
        ]
        == 1
    )


def test_manager_snapshot() -> None:
    manager = PluginManager()

    manager.register(
        SimplePlugin()
    )

    manager.register(
        CallableInfoPlugin(),
        enabled=False,
    )

    snapshot = (
        manager.snapshot()
    )

    assert (
        snapshot[
            "plugin_count"
        ]
        == 2
    )

    assert (
        snapshot[
            "enabled_count"
        ]
        == 1
    )

    assert (
        snapshot[
            "disabled_count"
        ]
        == 1
    )

    assert [
        plugin["name"]
        for plugin
        in snapshot["plugins"]
    ] == [
        "callable",
        "simple",
    ]


# =============================================================================
# CLOSE
# =============================================================================
def test_close_unloads_plugins() -> None:
    manager = PluginManager()

    first = LifecyclePlugin()

    second = LifecyclePlugin()

    manager.register(
        first,
        info=PluginInfo(
            name="first",
            version="1",
        ),
    )

    manager.register(
        second,
        info=PluginInfo(
            name="second",
            version="1",
        ),
    )

    manager.close()

    assert (
        manager.is_closed
        is True
    )

    assert (
        manager.plugin_count
        == 0
    )

    assert first.events == [
        "load",
        "enable",
        "disable",
        "unload",
    ]

    assert second.events == [
        "load",
        "enable",
        "disable",
        "unload",
    ]


def test_close_is_idempotent() -> None:
    manager = PluginManager()

    manager.register(
        SimplePlugin()
    )

    manager.close()
    manager.close()

    assert (
        manager.is_closed
        is True
    )


def test_closed_manager_rejects_registration() -> None:
    manager = PluginManager()

    manager.close()

    with pytest.raises(
        PluginManagerClosedError
    ):
        manager.register(
            SimplePlugin()
        )


def test_closed_manager_rejects_lookup() -> None:
    manager = PluginManager()

    manager.close()

    with pytest.raises(
        PluginManagerClosedError
    ):
        manager.get(
            "simple"
        )


def test_context_manager_closes_manager() -> None:
    with PluginManager() as manager:
        manager.register(
            SimplePlugin()
        )

        assert (
            manager.is_closed
            is False
        )

    assert (
        manager.is_closed
        is True
    )


# =============================================================================
# REPRESENTATION
# =============================================================================
def test_repr_contains_runtime_state() -> None:
    manager = PluginManager()

    manager.register(
        SimplePlugin()
    )

    representation = repr(
        manager
    )

    assert (
        "PluginManager"
        in representation
    )

    assert (
        "plugin_count=1"
        in representation
    )

    assert (
        "enabled_count=1"
        in representation
    )