from __future__ import annotations

from typing import Any

import pytest

from enterprise_crawler.plugins.discovery import (
    DEFAULT_PLUGIN_ENTRY_POINT_GROUP,
    DiscoveredPlugin,
    DuplicateDiscoveredPluginError,
    PluginDiscovery,
    PluginDiscoveryError,
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


class SelectableEntryPoints:
    def __init__(
        self,
        *entry_points: FakeEntryPoint,
    ) -> None:
        self.entry_points = (
            tuple(
                entry_points
            )
        )

        self.selected_groups: list[
            str
        ] = []

    def select(
        self,
        *,
        group: str,
    ) -> tuple[
        FakeEntryPoint,
        ...
    ]:
        self.selected_groups.append(
            group
        )

        return tuple(
            entry_point
            for entry_point
            in self.entry_points
            if entry_point.group
            == group
        )


# =============================================================================
# DEFAULT CONFIGURATION
# =============================================================================
def test_default_group() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: ()
    )

    assert (
        discovery.group
        == DEFAULT_PLUGIN_ENTRY_POINT_GROUP
    )


@pytest.mark.parametrize(
    "group",
    [
        "",
        " ",
        "\n\t",
    ],
)
def test_empty_group_is_rejected(
    group: str,
) -> None:
    with pytest.raises(
        PluginDiscoveryError
    ):
        PluginDiscovery(
            group=group
        )


def test_non_string_group_is_rejected() -> None:
    with pytest.raises(
        PluginDiscoveryError
    ):
        PluginDiscovery(
            group=123  # type: ignore[arg-type]
        )


def test_invalid_provider_is_rejected() -> None:
    with pytest.raises(
        TypeError
    ):
        PluginDiscovery(
            entry_points_provider=123  # type: ignore[arg-type]
        )


# =============================================================================
# EMPTY DISCOVERY
# =============================================================================
def test_empty_discovery() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: ()
    )

    result = (
        discovery.discover()
    )

    assert result == ()
    assert (
        discovery.discover_count
        == 1
    )


# =============================================================================
# LIST / ITERABLE DISCOVERY
# =============================================================================
def test_discover_from_iterable() -> None:
    entry_point = FakeEntryPoint(
        name="example",
        value=(
            "example_package.plugin:"
            "ExamplePlugin"
        ),
    )

    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            entry_point
        ]
    )

    result = (
        discovery.discover()
    )

    assert len(result) == 1

    plugin = result[0]

    assert isinstance(
        plugin,
        DiscoveredPlugin,
    )

    assert (
        plugin.name
        == "example"
    )

    assert (
        plugin.value
        == (
            "example_package.plugin:"
            "ExamplePlugin"
        )
    )

    assert (
        plugin.module
        == "example_package.plugin"
    )

    assert (
        plugin.attribute
        == "ExamplePlugin"
    )

    assert (
        plugin.entry_point
        is entry_point
    )


def test_unrelated_group_is_ignored() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            FakeEntryPoint(
                name="other",
                value="other.plugin:Plugin",
                group="other.group",
            ),
        ]
    )

    assert (
        discovery.discover()
        == ()
    )


# =============================================================================
# MODERN SELECT API
# =============================================================================
def test_select_api_is_supported() -> None:
    entry_points = (
        SelectableEntryPoints(
            FakeEntryPoint(
                name="example",
                value="pkg.plugin:Plugin",
            ),
            FakeEntryPoint(
                name="other",
                value="other.plugin:Plugin",
                group="other.group",
            ),
        )
    )

    discovery = PluginDiscovery(
        entry_points_provider=lambda: (
            entry_points
        )
    )

    result = (
        discovery.discover()
    )

    assert len(result) == 1

    assert (
        result[0].name
        == "example"
    )

    assert (
        entry_points.selected_groups
        == [
            DEFAULT_PLUGIN_ENTRY_POINT_GROUP
        ]
    )


# =============================================================================
# LEGACY MAPPING API
# =============================================================================
def test_mapping_api_is_supported() -> None:
    entry_point = FakeEntryPoint(
        name="example",
        value="pkg.plugin:Plugin",
    )

    discovery = PluginDiscovery(
        entry_points_provider=lambda: {
            DEFAULT_PLUGIN_ENTRY_POINT_GROUP: [
                entry_point
            ],
            "other.group": [
                FakeEntryPoint(
                    name="other",
                    value="other:Plugin",
                    group="other.group",
                )
            ],
        }
    )

    result = (
        discovery.discover()
    )

    assert len(result) == 1

    assert (
        result[0].name
        == "example"
    )


# =============================================================================
# TARGET PARSING
# =============================================================================
def test_module_only_target_is_supported() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            FakeEntryPoint(
                name="module-plugin",
                value="package.plugin",
            )
        ]
    )

    plugin = (
        discovery.discover()[0]
    )

    assert (
        plugin.module
        == "package.plugin"
    )

    assert (
        plugin.attribute
        is None
    )


def test_module_and_attribute_target() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            FakeEntryPoint(
                name="attribute-plugin",
                value="package.plugin:Plugin",
            )
        ]
    )

    plugin = (
        discovery.discover()[0]
    )

    assert (
        plugin.module
        == "package.plugin"
    )

    assert (
        plugin.attribute
        == "Plugin"
    )


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        ":Plugin",
        "package.plugin:",
    ],
)
def test_invalid_target_is_rejected(
    value: str,
) -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            FakeEntryPoint(
                name="invalid",
                value=value,
            )
        ]
    )

    with pytest.raises(
        PluginDiscoveryError
    ):
        discovery.discover()


# =============================================================================
# ENTRY POINT VALIDATION
# =============================================================================
@pytest.mark.parametrize(
    "name",
    [
        "",
        " ",
        "\n",
    ],
)
def test_empty_entry_point_name_is_rejected(
    name: str,
) -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            FakeEntryPoint(
                name=name,
                value="pkg.plugin:Plugin",
            )
        ]
    )

    with pytest.raises(
        PluginDiscoveryError
    ):
        discovery.discover()


def test_none_entry_point_is_rejected() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            None
        ]
    )

    with pytest.raises(
        PluginDiscoveryError
    ):
        discovery.discover()


def test_wrong_group_inside_select_result_is_rejected() -> None:
    class BrokenSelector:
        def select(
            self,
            *,
            group: str,
        ) -> list[
            FakeEntryPoint
        ]:
            return [
                FakeEntryPoint(
                    name="broken",
                    value="pkg.plugin:Plugin",
                    group="wrong.group",
                )
            ]

    discovery = PluginDiscovery(
        entry_points_provider=lambda: (
            BrokenSelector()
        )
    )

    with pytest.raises(
        PluginDiscoveryError
    ):
        discovery.discover()


# =============================================================================
# DETERMINISM
# =============================================================================
def test_results_are_sorted_deterministically() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            FakeEntryPoint(
                name="zeta",
                value="zeta:Plugin",
            ),
            FakeEntryPoint(
                name="Beta",
                value="beta:Plugin",
            ),
            FakeEntryPoint(
                name="alpha",
                value="alpha:Plugin",
            ),
        ]
    )

    result = (
        discovery.discover()
    )

    assert [
        plugin.name
        for plugin
        in result
    ] == [
        "alpha",
        "Beta",
        "zeta",
    ]


def test_duplicate_names_are_rejected() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            FakeEntryPoint(
                name="Example",
                value="first:Plugin",
            ),
            FakeEntryPoint(
                name="example",
                value="second:Plugin",
            ),
        ]
    )

    with pytest.raises(
        DuplicateDiscoveredPluginError
    ):
        discovery.discover()


def test_duplicate_detection_trims_name() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            FakeEntryPoint(
                name=" example ",
                value="first:Plugin",
            ),
            FakeEntryPoint(
                name="example",
                value="second:Plugin",
            ),
        ]
    )

    with pytest.raises(
        DuplicateDiscoveredPluginError
    ):
        discovery.discover()


# =============================================================================
# SIDE EFFECT BOUNDARY
# =============================================================================
def test_discovery_does_not_load_entry_point() -> None:
    class LoadAwareEntryPoint(
        FakeEntryPoint
    ):
        def __init__(
            self,
        ) -> None:
            super().__init__(
                name="example",
                value="pkg.plugin:Plugin",
            )

            self.load_called = False

        def load(
            self,
        ) -> Any:
            self.load_called = True

            raise AssertionError(
                "Discovery load() çağırmamalıdır."
            )

    entry_point = (
        LoadAwareEntryPoint()
    )

    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            entry_point
        ]
    )

    result = (
        discovery.discover()
    )

    assert len(result) == 1

    assert (
        entry_point.load_called
        is False
    )


# =============================================================================
# LOOKUP
# =============================================================================
def test_find_plugin() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            FakeEntryPoint(
                name="example",
                value="pkg.plugin:Plugin",
            )
        ]
    )

    plugin = discovery.find(
        "example"
    )

    assert plugin is not None

    assert (
        plugin.name
        == "example"
    )


def test_find_is_case_insensitive() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            FakeEntryPoint(
                name="Example",
                value="pkg.plugin:Plugin",
            )
        ]
    )

    plugin = discovery.find(
        " example "
    )

    assert plugin is not None

    assert (
        plugin.name
        == "Example"
    )


def test_find_unknown_plugin_returns_none() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: ()
    )

    assert (
        discovery.find(
            "missing"
        )
        is None
    )


def test_require_plugin() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            FakeEntryPoint(
                name="example",
                value="pkg.plugin:Plugin",
            )
        ]
    )

    plugin = discovery.require(
        "example"
    )

    assert (
        plugin.name
        == "example"
    )


def test_require_unknown_plugin_is_rejected() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: ()
    )

    with pytest.raises(
        PluginDiscoveryError
    ):
        discovery.require(
            "missing"
        )


# =============================================================================
# STATE
# =============================================================================
def test_discovery_count_increments() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: ()
    )

    discovery.discover()
    discovery.discover()

    assert (
        discovery.discover_count
        == 2
    )


def test_last_result_tracks_latest_discovery() -> None:
    calls = 0

    def provider() -> list[
        FakeEntryPoint
    ]:
        nonlocal calls

        calls += 1

        if calls == 1:
            return [
                FakeEntryPoint(
                    name="first",
                    value="first:Plugin",
                )
            ]

        return [
            FakeEntryPoint(
                name="second",
                value="second:Plugin",
            )
        ]

    discovery = PluginDiscovery(
        entry_points_provider=provider
    )

    first = (
        discovery.discover()
    )

    assert (
        first[0].name
        == "first"
    )

    second = (
        discovery.discover()
    )

    assert (
        second[0].name
        == "second"
    )

    assert (
        discovery.last_result[0].name
        == "second"
    )


def test_last_result_is_tuple_copy() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            FakeEntryPoint(
                name="example",
                value="pkg:Plugin",
            )
        ]
    )

    discovery.discover()

    first = discovery.last_result
    second = discovery.last_result

    assert (
        first
        == second
    )

    assert isinstance(
        first,
        tuple,
    )


# =============================================================================
# DESCRIPTOR
# =============================================================================
def test_discovered_plugin_to_dict() -> None:
    entry_point = FakeEntryPoint(
        name=" example ",
        value=" package.plugin:Plugin ",
    )

    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            entry_point
        ]
    )

    plugin = (
        discovery.discover()[0]
    )

    assert (
        plugin.to_dict()
        == {
            "name": "example",
            "value": (
                "package.plugin:Plugin"
            ),
            "group": (
                DEFAULT_PLUGIN_ENTRY_POINT_GROUP
            ),
            "module": (
                "package.plugin"
            ),
            "attribute": (
                "Plugin"
            ),
        }
    )


def test_discovered_plugin_key_is_casefolded() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            FakeEntryPoint(
                name="Example",
                value="pkg:Plugin",
            )
        ]
    )

    plugin = (
        discovery.discover()[0]
    )

    assert (
        plugin.key
        == "example"
    )


# =============================================================================
# PROVIDER FAILURE
# =============================================================================
def test_provider_failure_is_wrapped() -> None:
    def provider() -> Any:
        raise RuntimeError(
            "provider failed"
        )

    discovery = PluginDiscovery(
        entry_points_provider=provider
    )

    with pytest.raises(
        PluginDiscoveryError
    ) as exc_info:
        discovery.discover()

    assert (
        "provider failed"
        in str(
            exc_info.value
        )
    )


def test_non_iterable_provider_result_is_rejected() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: 123
    )

    with pytest.raises(
        PluginDiscoveryError
    ):
        discovery.discover()


# =============================================================================
# SNAPSHOT / REPR
# =============================================================================
def test_snapshot_before_discovery() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: ()
    )

    snapshot = (
        discovery.snapshot()
    )

    assert (
        snapshot[
            "group"
        ]
        == DEFAULT_PLUGIN_ENTRY_POINT_GROUP
    )

    assert (
        snapshot[
            "discover_count"
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


def test_snapshot_after_discovery() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: [
            FakeEntryPoint(
                name="example",
                value="pkg:Plugin",
            )
        ]
    )

    discovery.discover()

    snapshot = (
        discovery.snapshot()
    )

    assert (
        snapshot[
            "discover_count"
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
            "last_plugins"
        ][0][
            "name"
        ]
        == "example"
    )


def test_repr_contains_runtime_state() -> None:
    discovery = PluginDiscovery(
        entry_points_provider=lambda: ()
    )

    representation = repr(
        discovery
    )

    assert (
        "PluginDiscovery"
        in representation
    )

    assert (
        DEFAULT_PLUGIN_ENTRY_POINT_GROUP
        in representation
    )

    assert (
        "discover_count=0"
        in representation
    )