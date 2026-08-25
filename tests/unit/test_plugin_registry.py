from __future__ import annotations

import threading
from typing import Any

import pytest

from enterprise_crawler.contracts import (
    PluginInfo,
)
from enterprise_crawler.exceptions.plugin import (
    PluginError,
    PluginRegistrationError,
    PluginValidationError,
)
from enterprise_crawler.plugins import (
    PluginRegistry,
    RegisteredPlugin,
)


# =============================================================================
# FIXTURES / TEST PLUGINS
# =============================================================================
class ExamplePlugin:
    plugin_info = PluginInfo(
        name="example",
        version="1.0.0",
        author="Test Author",
        description="Example plugin.",
        metadata={
            "kind": "test",
        },
    )


class CallableInfoPlugin:
    def plugin_info(
        self,
    ) -> PluginInfo:
        return PluginInfo(
            name="callable",
            version="2.0.0",
        )


class MissingInfoPlugin:
    pass


# =============================================================================
# BASIC REGISTRY
# =============================================================================
def test_empty_registry() -> None:
    registry = PluginRegistry()

    assert len(registry) == 0
    assert registry.names() == ()
    assert registry.snapshot() == {
        "plugin_count": 0,
        "plugins": [],
    }


def test_register_plugin_from_attribute() -> None:
    registry = PluginRegistry()
    plugin = ExamplePlugin()

    info = registry.register(
        plugin
    )

    assert (
        info.name
        == "example"
    )

    assert (
        info.version
        == "1.0.0"
    )

    assert len(registry) == 1


def test_register_plugin_from_callable_info() -> None:
    registry = PluginRegistry()
    plugin = CallableInfoPlugin()

    info = registry.register(
        plugin
    )

    assert (
        info.name
        == "callable"
    )

    assert (
        registry.get(
            "callable"
        )
        is plugin
    )


def test_explicit_plugin_info_is_supported() -> None:
    registry = PluginRegistry()
    plugin = object()

    info = registry.register(
        plugin,
        info=PluginInfo(
            name="explicit",
            version="3.0.0",
        ),
    )

    assert (
        info.name
        == "explicit"
    )

    assert (
        registry.get(
            "explicit"
        )
        is plugin
    )


def test_explicit_info_overrides_plugin_attribute() -> None:
    registry = PluginRegistry()
    plugin = ExamplePlugin()

    registry.register(
        plugin,
        info=PluginInfo(
            name="override",
            version="9.0.0",
        ),
    )

    assert (
        registry.contains(
            "override"
        )
        is True
    )

    assert (
        registry.contains(
            "example"
        )
        is False
    )


# =============================================================================
# VALIDATION
# =============================================================================
def test_none_plugin_is_rejected() -> None:
    registry = PluginRegistry()

    with pytest.raises(
        PluginValidationError
    ):
        registry.register(
            None,
            info=PluginInfo(
                name="none",
                version="1.0.0",
            ),
        )


def test_missing_plugin_info_is_rejected() -> None:
    registry = PluginRegistry()

    with pytest.raises(
        PluginValidationError
    ):
        registry.register(
            MissingInfoPlugin()
        )


def test_non_plugin_info_contract_is_rejected() -> None:
    class InvalidPlugin:
        plugin_info = {
            "name": "invalid",
            "version": "1.0.0",
        }

    registry = PluginRegistry()

    with pytest.raises(
        PluginValidationError
    ):
        registry.register(
            InvalidPlugin()
        )


@pytest.mark.parametrize(
    "name",
    [
        "",
        " ",
        "\n\t",
    ],
)
def test_empty_plugin_name_is_rejected(
    name: str,
) -> None:
    registry = PluginRegistry()

    with pytest.raises(
        PluginValidationError
    ):
        registry.register(
            object(),
            info=PluginInfo(
                name=name,
                version="1.0.0",
            ),
        )


def test_non_string_plugin_name_is_rejected() -> None:
    registry = PluginRegistry()

    info = PluginInfo(
        name="valid",
        version="1.0.0",
    )

    info.name = 123  # type: ignore[assignment]

    with pytest.raises(
        PluginValidationError
    ):
        registry.register(
            object(),
            info=info,
        )


@pytest.mark.parametrize(
    "version",
    [
        "",
        " ",
        "\n",
    ],
)
def test_empty_version_is_rejected(
    version: str,
) -> None:
    registry = PluginRegistry()

    with pytest.raises(
        PluginValidationError
    ):
        registry.register(
            object(),
            info=PluginInfo(
                name="plugin",
                version=version,
            ),
        )


def test_non_string_version_is_rejected() -> None:
    registry = PluginRegistry()

    info = PluginInfo(
        name="plugin",
        version="1.0.0",
    )

    info.version = 1  # type: ignore[assignment]

    with pytest.raises(
        PluginValidationError
    ):
        registry.register(
            object(),
            info=info,
        )


def test_non_string_author_is_rejected() -> None:
    registry = PluginRegistry()

    info = PluginInfo(
        name="plugin",
        version="1.0.0",
    )

    info.author = 123  # type: ignore[assignment]

    with pytest.raises(
        PluginValidationError
    ):
        registry.register(
            object(),
            info=info,
        )


def test_non_string_description_is_rejected() -> None:
    registry = PluginRegistry()

    info = PluginInfo(
        name="plugin",
        version="1.0.0",
    )

    info.description = []  # type: ignore[assignment]

    with pytest.raises(
        PluginValidationError
    ):
        registry.register(
            object(),
            info=info,
        )


def test_metadata_must_be_dict() -> None:
    registry = PluginRegistry()

    info = PluginInfo(
        name="plugin",
        version="1.0.0",
    )

    info.metadata = []  # type: ignore[assignment]

    with pytest.raises(
        PluginValidationError
    ):
        registry.register(
            object(),
            info=info,
        )


# =============================================================================
# NORMALIZATION
# =============================================================================
def test_plugin_info_strings_are_trimmed() -> None:
    registry = PluginRegistry()

    info = registry.register(
        object(),
        info=PluginInfo(
            name="  example  ",
            version="  1.2.3  ",
            author="  Author  ",
            description=(
                "  Description  "
            ),
        ),
    )

    assert info.name == "example"
    assert info.version == "1.2.3"
    assert info.author == "Author"
    assert (
        info.description
        == "Description"
    )


def test_metadata_is_copied_on_registration() -> None:
    registry = PluginRegistry()

    metadata = {
        "nested": {
            "enabled": True,
        }
    }

    info = PluginInfo(
        name="copy-test",
        version="1.0.0",
        metadata=metadata,
    )

    registry.register(
        object(),
        info=info,
    )

    metadata[
        "nested"
    ][
        "enabled"
    ] = False

    stored = registry.get_info(
        "copy-test"
    )

    assert (
        stored.metadata[
            "nested"
        ][
            "enabled"
        ]
        is True
    )


def test_get_info_returns_independent_copy() -> None:
    registry = PluginRegistry()

    registry.register(
        object(),
        info=PluginInfo(
            name="copy-test",
            version="1.0.0",
            metadata={
                "value": 1,
            },
        ),
    )

    first = registry.get_info(
        "copy-test"
    )

    first.metadata[
        "value"
    ] = 999

    second = registry.get_info(
        "copy-test"
    )

    assert (
        second.metadata[
            "value"
        ]
        == 1
    )


# =============================================================================
# LOOKUP
# =============================================================================
def test_get_returns_registered_plugin() -> None:
    registry = PluginRegistry()
    plugin = ExamplePlugin()

    registry.register(
        plugin
    )

    assert (
        registry.get(
            "example"
        )
        is plugin
    )


def test_lookup_is_case_insensitive() -> None:
    registry = PluginRegistry()
    plugin = ExamplePlugin()

    registry.register(
        plugin
    )

    assert (
        registry.get(
            "EXAMPLE"
        )
        is plugin
    )

    assert (
        registry.get(
            "ExAmPlE"
        )
        is plugin
    )


def test_lookup_trims_plugin_name() -> None:
    registry = PluginRegistry()
    plugin = ExamplePlugin()

    registry.register(
        plugin
    )

    assert (
        registry.get(
            "  example  "
        )
        is plugin
    )


def test_contains_reports_registration() -> None:
    registry = PluginRegistry()

    registry.register(
        ExamplePlugin()
    )

    assert (
        registry.contains(
            "example"
        )
        is True
    )

    assert (
        registry.contains(
            "missing"
        )
        is False
    )


def test_contains_operator() -> None:
    registry = PluginRegistry()

    registry.register(
        ExamplePlugin()
    )

    assert (
        "example"
        in registry
    )

    assert (
        "EXAMPLE"
        in registry
    )

    assert (
        "missing"
        not in registry
    )


def test_contains_operator_rejects_non_string_safely() -> None:
    registry = PluginRegistry()

    assert (
        123
        not in registry
    )


def test_get_unknown_plugin_is_rejected() -> None:
    registry = PluginRegistry()

    with pytest.raises(
        PluginError
    ):
        registry.get(
            "missing"
        )


def test_get_info_unknown_plugin_is_rejected() -> None:
    registry = PluginRegistry()

    with pytest.raises(
        PluginError
    ):
        registry.get_info(
            "missing"
        )


# =============================================================================
# DUPLICATES
# =============================================================================
def test_duplicate_plugin_is_rejected() -> None:
    registry = PluginRegistry()

    registry.register(
        ExamplePlugin()
    )

    with pytest.raises(
        PluginRegistrationError
    ):
        registry.register(
            ExamplePlugin()
        )


def test_duplicate_detection_is_case_insensitive() -> None:
    registry = PluginRegistry()

    registry.register(
        object(),
        info=PluginInfo(
            name="Example",
            version="1.0.0",
        ),
    )

    with pytest.raises(
        PluginRegistrationError
    ):
        registry.register(
            object(),
            info=PluginInfo(
                name="example",
                version="2.0.0",
            ),
        )


# =============================================================================
# REGISTRATION MODEL
# =============================================================================
def test_get_registration() -> None:
    registry = PluginRegistry()
    plugin = ExamplePlugin()

    registry.register(
        plugin
    )

    registration = (
        registry.get_registration(
            "example"
        )
    )

    assert isinstance(
        registration,
        RegisteredPlugin,
    )

    assert (
        registration.plugin
        is plugin
    )

    assert (
        registration.name
        == "example"
    )

    assert (
        registration.version
        == "1.0.0"
    )


def test_registered_plugin_to_dict() -> None:
    registry = PluginRegistry()

    registry.register(
        ExamplePlugin()
    )

    registration = (
        registry.get_registration(
            "example"
        )
    )

    payload = (
        registration.to_dict()
    )

    assert (
        payload["name"]
        == "example"
    )

    assert (
        payload["version"]
        == "1.0.0"
    )

    assert (
        payload["metadata"]
        == {
            "kind": "test",
        }
    )


# =============================================================================
# UNREGISTER
# =============================================================================
def test_unregister_returns_plugin() -> None:
    registry = PluginRegistry()
    plugin = ExamplePlugin()

    registry.register(
        plugin
    )

    removed = registry.unregister(
        "example"
    )

    assert removed is plugin
    assert len(registry) == 0


def test_unregister_is_case_insensitive() -> None:
    registry = PluginRegistry()
    plugin = ExamplePlugin()

    registry.register(
        plugin
    )

    assert (
        registry.unregister(
            "EXAMPLE"
        )
        is plugin
    )


def test_unregister_unknown_plugin_is_rejected() -> None:
    registry = PluginRegistry()

    with pytest.raises(
        PluginRegistrationError
    ):
        registry.unregister(
            "missing"
        )


# =============================================================================
# ENUMERATION / SNAPSHOT
# =============================================================================
def test_names_are_deterministically_sorted() -> None:
    registry = PluginRegistry()

    registry.register(
        object(),
        info=PluginInfo(
            name="zeta",
            version="1",
        ),
    )

    registry.register(
        object(),
        info=PluginInfo(
            name="Alpha",
            version="1",
        ),
    )

    registry.register(
        object(),
        info=PluginInfo(
            name="beta",
            version="1",
        ),
    )

    assert (
        registry.names()
        == (
            "Alpha",
            "beta",
            "zeta",
        )
    )


def test_snapshot_is_deterministic() -> None:
    registry = PluginRegistry()

    registry.register(
        object(),
        info=PluginInfo(
            name="zeta",
            version="2.0.0",
        ),
    )

    registry.register(
        object(),
        info=PluginInfo(
            name="alpha",
            version="1.0.0",
        ),
    )

    snapshot = (
        registry.snapshot()
    )

    assert (
        snapshot[
            "plugin_count"
        ]
        == 2
    )

    assert [
        item["name"]
        for item
        in snapshot[
            "plugins"
        ]
    ] == [
        "alpha",
        "zeta",
    ]


def test_snapshot_metadata_is_independent() -> None:
    registry = PluginRegistry()

    registry.register(
        object(),
        info=PluginInfo(
            name="plugin",
            version="1.0.0",
            metadata={
                "enabled": True,
            },
        ),
    )

    snapshot = (
        registry.snapshot()
    )

    snapshot[
        "plugins"
    ][0][
        "metadata"
    ][
        "enabled"
    ] = False

    assert (
        registry.get_info(
            "plugin"
        ).metadata[
            "enabled"
        ]
        is True
    )


# =============================================================================
# CONCURRENCY
# =============================================================================
def test_concurrent_registration_allows_only_one_duplicate_name() -> None:
    registry = PluginRegistry()

    successes: list[
        object
    ] = []

    failures: list[
        BaseException
    ] = []

    result_lock = (
        threading.Lock()
    )

    def worker() -> None:
        plugin = object()

        try:
            registry.register(
                plugin,
                info=PluginInfo(
                    name="shared",
                    version="1.0.0",
                ),
            )

            with result_lock:
                successes.append(
                    plugin
                )

        except BaseException as exc:
            with result_lock:
                failures.append(
                    exc
                )

    threads = [
        threading.Thread(
            target=worker
        )
        for _ in range(10)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert (
        len(successes)
        == 1
    )

    assert (
        len(failures)
        == 9
    )

    assert all(
        isinstance(
            error,
            PluginRegistrationError,
        )
        for error
        in failures
    )

    assert len(registry) == 1


def test_concurrent_distinct_registrations_are_safe() -> None:
    registry = PluginRegistry()

    errors: list[
        BaseException
    ] = []

    result_lock = (
        threading.Lock()
    )

    def worker(
        index: int,
    ) -> None:
        try:
            registry.register(
                object(),
                info=PluginInfo(
                    name=(
                        f"plugin-{index}"
                    ),
                    version="1.0.0",
                ),
            )

        except BaseException as exc:
            with result_lock:
                errors.append(
                    exc
                )

    threads = [
        threading.Thread(
            target=worker,
            args=(index,),
        )
        for index
        in range(20)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert errors == []
    assert len(registry) == 20


# =============================================================================
# REPRESENTATION
# =============================================================================
def test_registry_repr_contains_plugin_count() -> None:
    registry = PluginRegistry()

    registry.register(
        ExamplePlugin()
    )

    representation = repr(
        registry
    )

    assert (
        "PluginRegistry"
        in representation
    )

    assert (
        "plugin_count=1"
        in representation
    )


def test_registered_plugin_repr_contains_identity() -> None:
    registry = PluginRegistry()

    registry.register(
        ExamplePlugin()
    )

    representation = repr(
        registry.get_registration(
            "example"
        )
    )

    assert (
        "RegisteredPlugin"
        in representation
    )

    assert (
        "example"
        in representation
    )

    assert (
        "1.0.0"
        in representation
    )