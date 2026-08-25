from __future__ import annotations

import sys
import types
from collections.abc import Iterator
from typing import Any

import pytest

from enterprise_crawler.contracts import (
    PluginInfo,
)
from enterprise_crawler.plugins.autoload import (
    AutoLoadedPlugin,
    PluginAutoLoader,
    PluginAutoLoadError,
    UnsupportedDiscoveredPluginError,
    discover_and_register_plugins,
)
from enterprise_crawler.plugins.discovery import (
    DEFAULT_PLUGIN_ENTRY_POINT_GROUP,
    DiscoveredPlugin,
    PluginDiscovery,
)
from enterprise_crawler.plugins.loader import (
    PluginLoader,
)
from enterprise_crawler.plugins.manager import (
    PluginManager,
)


# =============================================================================
# TEST DOUBLES
# =============================================================================
class FakeEntryPoint:
    def __init__(
        self,
        *,
        name: str,
        value: str,
        group: str = (
            DEFAULT_PLUGIN_ENTRY_POINT_GROUP
        ),
    ) -> None:
        self.name = name
        self.value = value
        self.group = group


class ExamplePlugin:
    plugin_info = PluginInfo(
        name="example-plugin",
        version="1.2.3",
        author="Enterprise Crawler",
        description="Test plugin.",
        metadata={
            "kind": "test",
        },
    )

    def __init__(
        self,
    ) -> None:
        self.events: list[
            str
        ] = []

    def on_load(
        self,
        manager: PluginManager,
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

    def collect(
        self,
    ) -> str:
        return "collected"


class SecondPlugin:
    plugin_info = PluginInfo(
        name="second-plugin",
        version="2.0.0",
    )

    def ping(
        self,
    ) -> str:
        return "pong"


# =============================================================================
# MODULE FIXTURE
# =============================================================================
@pytest.fixture
def plugin_module() -> Iterator[str]:
    module_name = (
        "_enterprise_crawler_test_autoload_plugins"
    )

    module = types.ModuleType(
        module_name
    )

    module.ExamplePlugin = (
        ExamplePlugin
    )

    module.SecondPlugin = (
        SecondPlugin
    )

    sys.modules[
        module_name
    ] = module

    try:
        yield module_name

    finally:
        sys.modules.pop(
            module_name,
            None,
        )


# =============================================================================
# HELPERS
# =============================================================================
def make_discovered(
    *,
    name: str,
    value: str,
) -> DiscoveredPlugin:
    entry_point = FakeEntryPoint(
        name=name,
        value=value,
    )

    module_name: str
    attribute_name: str | None

    if ":" in value:
        module_name, attribute_name = (
            value.split(
                ":",
                1,
            )
        )

    else:
        module_name = value
        attribute_name = None

    return DiscoveredPlugin(
        name=name,
        value=value,
        group=(
            DEFAULT_PLUGIN_ENTRY_POINT_GROUP
        ),
        module=module_name,
        attribute=attribute_name,
        entry_point=entry_point,
    )


# =============================================================================
# CONSTRUCTION
# =============================================================================
def test_manager_is_required() -> None:
    with pytest.raises(
        TypeError
    ):
        PluginAutoLoader(
            manager=object()  # type: ignore[arg-type]
        )


def test_default_discovery_and_loader_are_created() -> None:
    manager = PluginManager()

    auto_loader = (
        PluginAutoLoader(
            manager=manager
        )
    )

    try:
        assert isinstance(
            auto_loader.discovery,
            PluginDiscovery,
        )

        assert isinstance(
            auto_loader.loader,
            PluginLoader,
        )

    finally:
        manager.close()


def test_injected_components_are_reused() -> None:
    manager = PluginManager()

    discovery = PluginDiscovery(
        entry_points_provider=lambda: ()
    )

    loader = PluginLoader()

    auto_loader = PluginAutoLoader(
        manager=manager,
        discovery=discovery,
        loader=loader,
    )

    try:
        assert (
            auto_loader.discovery
            is discovery
        )

        assert (
            auto_loader.loader
            is loader
        )

    finally:
        manager.close()


def test_invalid_discovery_is_rejected() -> None:
    manager = PluginManager()

    try:
        with pytest.raises(
            TypeError
        ):
            PluginAutoLoader(
                manager=manager,
                discovery=object(),  # type: ignore[arg-type]
            )

    finally:
        manager.close()


def test_invalid_loader_is_rejected() -> None:
    manager = PluginManager()

    try:
        with pytest.raises(
            TypeError
        ):
            PluginAutoLoader(
                manager=manager,
                loader=object(),  # type: ignore[arg-type]
            )

    finally:
        manager.close()


# =============================================================================
# SINGLE DISCOVERED PLUGIN
# =============================================================================
def test_load_discovered_plugin(
    plugin_module: str,
) -> None:
    manager = PluginManager()

    auto_loader = (
        PluginAutoLoader(
            manager=manager
        )
    )

    discovered = (
        make_discovered(
            name="entry-example",
            value=(
                f"{plugin_module}:"
                "ExamplePlugin"
            ),
        )
    )

    try:
        result = (
            auto_loader.load_discovered(
                discovered
            )
        )

        assert isinstance(
            result,
            AutoLoadedPlugin,
        )

        assert (
            result.discovered
            is discovered
        )

        assert (
            result.name
            == "example-plugin"
        )

        assert (
            result.version
            == "1.2.3"
        )

        assert (
            result.reference
            == (
                f"{plugin_module}:"
                "ExamplePlugin"
            )
        )

        assert (
            result.enabled
            is True
        )

        assert (
            manager.contains(
                "example-plugin"
            )
            is True
        )

        assert (
            manager.is_enabled(
                "example-plugin"
            )
            is True
        )

    finally:
        manager.close()


def test_load_discovered_runs_plugin_lifecycle(
    plugin_module: str,
) -> None:
    manager = PluginManager()

    auto_loader = (
        PluginAutoLoader(
            manager=manager
        )
    )

    discovered = (
        make_discovered(
            name="entry-example",
            value=(
                f"{plugin_module}:"
                "ExamplePlugin"
            ),
        )
    )

    try:
        result = (
            auto_loader.load_discovered(
                discovered
            )
        )

        plugin = result.loaded.plugin

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

    finally:
        if not manager.is_closed:
            manager.close()


def test_discovered_name_does_not_override_plugin_info_name(
    plugin_module: str,
) -> None:
    manager = PluginManager()

    auto_loader = (
        PluginAutoLoader(
            manager=manager
        )
    )

    discovered = (
        make_discovered(
            name="packaging-entry-name",
            value=(
                f"{plugin_module}:"
                "ExamplePlugin"
            ),
        )
    )

    try:
        result = (
            auto_loader.load_discovered(
                discovered
            )
        )

        assert (
            result.discovered.name
            == "packaging-entry-name"
        )

        assert (
            result.loaded.info.name
            == "example-plugin"
        )

        assert (
            manager.contains(
                "example-plugin"
            )
            is True
        )

        assert (
            manager.contains(
                "packaging-entry-name"
            )
            is False
        )

    finally:
        manager.close()


def test_plugin_can_start_disabled(
    plugin_module: str,
) -> None:
    manager = PluginManager()

    auto_loader = (
        PluginAutoLoader(
            manager=manager
        )
    )

    discovered = (
        make_discovered(
            name="example",
            value=(
                f"{plugin_module}:"
                "ExamplePlugin"
            ),
        )
    )

    try:
        result = (
            auto_loader.load_discovered(
                discovered,
                enabled=False,
            )
        )

        assert (
            result.enabled
            is False
        )

        assert (
            manager.is_enabled(
                "example-plugin"
            )
            is False
        )

        assert (
            result.loaded.plugin.events
            == [
                "load",
            ]
        )

    finally:
        manager.close()


def test_invalid_enabled_flag_is_rejected(
    plugin_module: str,
) -> None:
    manager = PluginManager()

    auto_loader = (
        PluginAutoLoader(
            manager=manager
        )
    )

    discovered = (
        make_discovered(
            name="example",
            value=(
                f"{plugin_module}:"
                "ExamplePlugin"
            ),
        )
    )

    try:
        with pytest.raises(
            TypeError
        ):
            auto_loader.load_discovered(
                discovered,
                enabled=1,  # type: ignore[arg-type]
            )

    finally:
        manager.close()


def test_invalid_discovered_type_is_rejected() -> None:
    manager = PluginManager()

    auto_loader = (
        PluginAutoLoader(
            manager=manager
        )
    )

    try:
        with pytest.raises(
            TypeError
        ):
            auto_loader.load_discovered(
                object()  # type: ignore[arg-type]
            )

    finally:
        manager.close()


# =============================================================================
# MODULE-ONLY BOUNDARY
# =============================================================================
def test_module_only_discovered_plugin_is_rejected(
    plugin_module: str,
) -> None:
    manager = PluginManager()

    auto_loader = (
        PluginAutoLoader(
            manager=manager
        )
    )

    discovered = (
        make_discovered(
            name="module-only",
            value=plugin_module,
        )
    )

    try:
        with pytest.raises(
            UnsupportedDiscoveredPluginError
        ):
            auto_loader.load_discovered(
                discovered
            )

        assert (
            manager.plugin_count
            == 0
        )

    finally:
        manager.close()


# =============================================================================
# CLOSED MANAGER
# =============================================================================
def test_closed_manager_is_rejected(
    plugin_module: str,
) -> None:
    manager = PluginManager()

    manager.close()

    auto_loader = (
        PluginAutoLoader(
            manager=manager
        )
    )

    discovered = (
        make_discovered(
            name="example",
            value=(
                f"{plugin_module}:"
                "ExamplePlugin"
            ),
        )
    )

    with pytest.raises(
        PluginAutoLoadError
    ):
        auto_loader.load_discovered(
            discovered
        )


# =============================================================================
# LOAD MANY
# =============================================================================
def test_load_many_preserves_order(
    plugin_module: str,
) -> None:
    manager = PluginManager()

    auto_loader = (
        PluginAutoLoader(
            manager=manager
        )
    )

    first = make_discovered(
        name="first-entry",
        value=(
            f"{plugin_module}:"
            "ExamplePlugin"
        ),
    )

    second = make_discovered(
        name="second-entry",
        value=(
            f"{plugin_module}:"
            "SecondPlugin"
        ),
    )

    try:
        result = (
            auto_loader.load_many(
                [
                    first,
                    second,
                ]
            )
        )

        assert [
            item.discovered.name
            for item
            in result
        ] == [
            "first-entry",
            "second-entry",
        ]

        assert [
            item.name
            for item
            in result
        ] == [
            "example-plugin",
            "second-plugin",
        ]

        assert (
            manager.plugin_count
            == 2
        )

    finally:
        manager.close()


def test_load_many_empty_sequence() -> None:
    manager = PluginManager()

    auto_loader = (
        PluginAutoLoader(
            manager=manager
        )
    )

    try:
        result = (
            auto_loader.load_many(
                []
            )
        )

        assert result == ()

        assert (
            auto_loader.last_result
            == ()
        )

        assert (
            manager.plugin_count
            == 0
        )

    finally:
        manager.close()


def test_load_many_rejects_string() -> None:
    manager = PluginManager()

    auto_loader = (
        PluginAutoLoader(
            manager=manager
        )
    )

    try:
        with pytest.raises(
            TypeError
        ):
            auto_loader.load_many(
                "plugin"  # type: ignore[arg-type]
            )

    finally:
        manager.close()


def test_load_many_rejects_non_iterable() -> None:
    manager = PluginManager()

    auto_loader = (
        PluginAutoLoader(
            manager=manager
        )
    )

    try:
        with pytest.raises(
            TypeError
        ):
            auto_loader.load_many(
                123  # type: ignore[arg-type]
            )

    finally:
        manager.close()


# =============================================================================
# DISCOVER + REGISTER
# =============================================================================
def test_discover_and_register(
    plugin_module: str,
) -> None:
    entry_points = [
        FakeEntryPoint(
            name="example-entry",
            value=(
                f"{plugin_module}:"
                "ExamplePlugin"
            ),
        ),
        FakeEntryPoint(
            name="second-entry",
            value=(
                f"{plugin_module}:"
                "SecondPlugin"
            ),
        ),
    ]

    discovery = PluginDiscovery(
        entry_points_provider=lambda: (
            entry_points
        )
    )

    manager = PluginManager()

    auto_loader = (
        PluginAutoLoader(
            manager=manager,
            discovery=discovery,
        )
    )

    try:
        result = (
            auto_loader.discover_and_register()
        )

        assert len(result) == 2

        assert (
            manager.plugin_count
            == 2
        )

        assert (
            manager.contains(
                "example-plugin"
            )
            is True
        )

        assert (
            manager.contains(
                "second-plugin"
            )
            is True
        )

        assert (
            discovery.discover_count
            == 1
        )

    finally:
        manager.close()


def test_discover_and_register_empty_discovery() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: ()
    )

    manager = PluginManager()

    auto_loader = (
        PluginAutoLoader(
            manager=manager,
            discovery=discovery,
        )
    )

    try:
        result = (
            auto_loader.discover_and_register()
        )

        assert result == ()

        assert (
            manager.plugin_count
            == 0
        )

    finally:
        manager.close()


def test_discovery_order_controls_registration_order(
    plugin_module: str,
) -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            FakeEntryPoint(
                name="zeta-entry",
                value=(
                    f"{plugin_module}:"
                    "SecondPlugin"
                ),
            ),
            FakeEntryPoint(
                name="alpha-entry",
                value=(
                    f"{plugin_module}:"
                    "ExamplePlugin"
                ),
            ),
        ]
    )

    manager = PluginManager()

    auto_loader = PluginAutoLoader(
        manager=manager,
        discovery=discovery,
    )

    try:
        result = (
            auto_loader.discover_and_register()
        )

        assert [
            item.discovered.name
            for item
            in result
        ] == [
            "alpha-entry",
            "zeta-entry",
        ]

    finally:
        manager.close()


# =============================================================================
# DISCOVER ONE
# =============================================================================
def test_discover_and_register_one(
    plugin_module: str,
) -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            FakeEntryPoint(
                name="example-entry",
                value=(
                    f"{plugin_module}:"
                    "ExamplePlugin"
                ),
            )
        ]
    )

    manager = PluginManager()

    auto_loader = PluginAutoLoader(
        manager=manager,
        discovery=discovery,
    )

    try:
        result = (
            auto_loader.discover_and_register_one(
                "EXAMPLE-ENTRY"
            )
        )

        assert (
            result.name
            == "example-plugin"
        )

        assert (
            manager.plugin_count
            == 1
        )

    finally:
        manager.close()


# =============================================================================
# INVOCATION AFTER AUTOLOAD
# =============================================================================
def test_auto_loaded_plugin_can_be_invoked(
    plugin_module: str,
) -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            FakeEntryPoint(
                name="example-entry",
                value=(
                    f"{plugin_module}:"
                    "ExamplePlugin"
                ),
            )
        ]
    )

    manager = PluginManager()

    auto_loader = PluginAutoLoader(
        manager=manager,
        discovery=discovery,
    )

    try:
        auto_loader.discover_and_register()

        result = manager.invoke(
            "example-plugin",
            "collect",
        )

        assert (
            result
            == "collected"
        )

        snapshot = (
            manager.plugin_snapshot(
                "example-plugin"
            )
        )

        assert (
            snapshot[
                "runtime"
            ][
                "invocation_count"
            ]
            == 1
        )

    finally:
        manager.close()


# =============================================================================
# COUNTERS / STATE
# =============================================================================
def test_load_count_tracks_successful_plugins(
    plugin_module: str,
) -> None:
    manager = PluginManager()

    auto_loader = (
        PluginAutoLoader(
            manager=manager
        )
    )

    first = make_discovered(
        name="first",
        value=(
            f"{plugin_module}:"
            "ExamplePlugin"
        ),
    )

    second = make_discovered(
        name="second",
        value=(
            f"{plugin_module}:"
            "SecondPlugin"
        ),
    )

    try:
        assert (
            auto_loader.load_count
            == 0
        )

        auto_loader.load_discovered(
            first
        )

        assert (
            auto_loader.load_count
            == 1
        )

        auto_loader.load_discovered(
            second
        )

        assert (
            auto_loader.load_count
            == 2
        )

    finally:
        manager.close()


def test_last_result_tracks_batch(
    plugin_module: str,
) -> None:
    manager = PluginManager()

    auto_loader = (
        PluginAutoLoader(
            manager=manager
        )
    )

    discovered = (
        make_discovered(
            name="example",
            value=(
                f"{plugin_module}:"
                "ExamplePlugin"
            ),
        )
    )

    try:
        result = (
            auto_loader.load_many(
                [
                    discovered
                ]
            )
        )

        assert (
            auto_loader.last_result
            == result
        )

    finally:
        manager.close()


# =============================================================================
# RESULT SERIALIZATION
# =============================================================================
def test_auto_loaded_plugin_to_dict(
    plugin_module: str,
) -> None:
    manager = PluginManager()

    auto_loader = (
        PluginAutoLoader(
            manager=manager
        )
    )

    discovered = (
        make_discovered(
            name="entry-example",
            value=(
                f"{plugin_module}:"
                "ExamplePlugin"
            ),
        )
    )

    try:
        result = (
            auto_loader.load_discovered(
                discovered
            )
        )

        payload = (
            result.to_dict()
        )

        assert (
            payload[
                "discovered"
            ][
                "name"
            ]
            == "entry-example"
        )

        assert (
            payload[
                "loaded"
            ][
                "plugin_name"
            ]
            == "example-plugin"
        )

        assert (
            payload[
                "enabled"
            ]
            is True
        )

    finally:
        manager.close()


# =============================================================================
# CONVENIENCE FUNCTION
# =============================================================================
def test_discover_and_register_plugins_helper(
    plugin_module: str,
) -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            FakeEntryPoint(
                name="example-entry",
                value=(
                    f"{plugin_module}:"
                    "ExamplePlugin"
                ),
            )
        ]
    )

    manager = PluginManager()

    try:
        result = (
            discover_and_register_plugins(
                manager,
                discovery=discovery,
            )
        )

        assert len(result) == 1

        assert (
            result[0].name
            == "example-plugin"
        )

        assert (
            manager.plugin_count
            == 1
        )

    finally:
        manager.close()


# =============================================================================
# SNAPSHOT
# =============================================================================
def test_snapshot_before_loading() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: ()
    )

    manager = PluginManager()

    auto_loader = PluginAutoLoader(
        manager=manager,
        discovery=discovery,
    )

    try:
        snapshot = (
            auto_loader.snapshot()
        )

        assert (
            snapshot[
                "load_count"
            ]
            == 0
        )

        assert (
            snapshot[
                "last_plugin_count"
            ]
            == 0
        )

        assert (
            snapshot[
                "last_plugins"
            ]
            == []
        )

        assert (
            snapshot[
                "manager_plugin_count"
            ]
            == 0
        )

        assert (
            snapshot[
                "manager_closed"
            ]
            is False
        )

    finally:
        manager.close()


def test_snapshot_after_loading(
    plugin_module: str,
) -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            FakeEntryPoint(
                name="example-entry",
                value=(
                    f"{plugin_module}:"
                    "ExamplePlugin"
                ),
            )
        ]
    )

    manager = PluginManager()

    auto_loader = PluginAutoLoader(
        manager=manager,
        discovery=discovery,
    )

    try:
        auto_loader.discover_and_register()

        snapshot = (
            auto_loader.snapshot()
        )

        assert (
            snapshot[
                "load_count"
            ]
            == 1
        )

        assert (
            snapshot[
                "last_plugin_count"
            ]
            == 1
        )

        assert (
            snapshot[
                "manager_plugin_count"
            ]
            == 1
        )

        assert (
            snapshot[
                "last_plugins"
            ][0][
                "loaded"
            ][
                "plugin_name"
            ]
            == "example-plugin"
        )

    finally:
        manager.close()


# =============================================================================
# REPR
# =============================================================================
def test_repr_contains_runtime_state() -> None:
    manager = PluginManager()

    discovery = PluginDiscovery(
        entry_points_provider=lambda: ()
    )

    auto_loader = PluginAutoLoader(
        manager=manager,
        discovery=discovery,
    )

    try:
        representation = repr(
            auto_loader
        )

        assert (
            "PluginAutoLoader"
            in representation
        )

        assert (
            "load_count=0"
            in representation
        )

        assert (
            "manager_plugin_count=0"
            in representation
        )

    finally:
        manager.close()