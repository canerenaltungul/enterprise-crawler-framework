from __future__ import annotations

import sys
import types

import pytest

from enterprise_crawler.contracts.plugin import (
    PluginInfo,
)
from enterprise_crawler.plugins.loader import (
    LoadedPlugin,
    PluginLoader,
    PluginLoadError,
    load_and_register_plugin,
    load_plugin,
)
from enterprise_crawler.plugins.manager import (
    PluginManager,
)


# =============================================================================
# FIXTURE PLUGINS
# =============================================================================
class BasicPlugin:

    plugin_info = PluginInfo(
        name="basic",
        version="1.0.0",
        author="Test",
        description="Basic plugin",
        metadata={
            "kind": "fixture",
        },
    )

    def ping(
        self,
    ) -> str:
        return "pong"


class CallableInfoPlugin:

    def plugin_info(
        self,
    ) -> PluginInfo:
        return PluginInfo(
            name="callable",
            version="2.0.0",
        )


class ConstantInfoPlugin:

    PLUGIN_INFO = PluginInfo(
        name="constant",
        version="3.0.0",
    )


class ConstructorFailurePlugin:

    plugin_info = PluginInfo(
        name="constructor-failure",
        version="1.0.0",
    )

    def __init__(
        self,
    ) -> None:
        raise RuntimeError(
            "constructor failed"
        )


class InvalidInfoPlugin:

    plugin_info = {
        "name": "invalid"
    }


class MissingInfoPlugin:
    pass


class FailingInfoProviderPlugin:

    def plugin_info(
        self,
    ) -> PluginInfo:
        raise RuntimeError(
            "info failed"
        )


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

    def ping(
        self,
    ) -> str:
        return "pong"


@pytest.fixture
def plugin_module():
    name = (
        "tests_fixture_plugin_module"
    )

    module = types.ModuleType(
        name
    )

    module.BasicPlugin = (
        BasicPlugin
    )

    module.CallableInfoPlugin = (
        CallableInfoPlugin
    )

    module.ConstantInfoPlugin = (
        ConstantInfoPlugin
    )

    module.ConstructorFailurePlugin = (
        ConstructorFailurePlugin
    )

    module.InvalidInfoPlugin = (
        InvalidInfoPlugin
    )

    module.MissingInfoPlugin = (
        MissingInfoPlugin
    )

    module.FailingInfoProviderPlugin = (
        FailingInfoProviderPlugin
    )

    module.LifecyclePlugin = (
        LifecyclePlugin
    )

    module.instance_plugin = (
        BasicPlugin()
    )

    module._PrivatePlugin = (
        BasicPlugin
    )

    sys.modules[
        name
    ] = module

    try:
        yield module

    finally:
        sys.modules.pop(
            name,
            None,
        )


# =============================================================================
# CONSTRUCTION
# =============================================================================
def test_default_loader_config() -> None:
    loader = PluginLoader()

    assert (
        loader.instantiate_classes
        is True
    )


def test_class_instantiation_can_be_disabled() -> None:
    loader = PluginLoader(
        instantiate_classes=False
    )

    assert (
        loader.instantiate_classes
        is False
    )


def test_invalid_instantiate_flag_is_rejected() -> None:
    with pytest.raises(
        TypeError
    ):
        PluginLoader(
            instantiate_classes=1
        )


# =============================================================================
# REFERENCE VALIDATION
# =============================================================================
@pytest.mark.parametrize(
    "reference",
    [
        "",
        " ",
        "\n\t",
    ],
)
def test_empty_reference_is_rejected(
    reference: str,
) -> None:
    with pytest.raises(
        PluginLoadError
    ):
        PluginLoader().load(
            reference
        )


@pytest.mark.parametrize(
    "reference",
    [
        "module",
        "module:",
        ":Plugin",
        "module:Plugin:Extra",
    ],
)
def test_invalid_reference_format_is_rejected(
    reference: str,
) -> None:
    with pytest.raises(
        PluginLoadError
    ):
        PluginLoader().load(
            reference
        )


def test_non_string_reference_is_rejected() -> None:
    with pytest.raises(
        PluginLoadError
    ):
        PluginLoader().load(
            123
        )


def test_reference_is_trimmed(
    plugin_module,
) -> None:
    loaded = PluginLoader().load(
        " tests_fixture_plugin_module"
        ":BasicPlugin "
    )

    assert (
        loaded.reference
        == (
            "tests_fixture_plugin_module"
            ":BasicPlugin"
        )
    )


# =============================================================================
# MODULE IMPORT
# =============================================================================
def test_unknown_module_is_rejected() -> None:
    with pytest.raises(
        PluginLoadError
    ):
        PluginLoader().load(
            "definitely_missing_plugin_module"
            ":Plugin"
        )


def test_empty_module_name_is_rejected() -> None:
    loader = PluginLoader()

    with pytest.raises(
        PluginLoadError
    ):
        loader.import_module(
            " "
        )


def test_non_string_module_name_is_rejected() -> None:
    loader = PluginLoader()

    with pytest.raises(
        PluginLoadError
    ):
        loader.import_module(
            123
        )


# =============================================================================
# ATTRIBUTE RESOLUTION
# =============================================================================
def test_missing_attribute_is_rejected(
    plugin_module,
) -> None:
    with pytest.raises(
        PluginLoadError
    ):
        PluginLoader().load(
            "tests_fixture_plugin_module"
            ":MissingAttribute"
        )


def test_private_attribute_is_rejected(
    plugin_module,
) -> None:
    with pytest.raises(
        PluginLoadError
    ):
        PluginLoader().load(
            "tests_fixture_plugin_module"
            ":_PrivatePlugin"
        )


def test_none_module_is_rejected() -> None:
    with pytest.raises(
        PluginLoadError
    ):
        PluginLoader().resolve_attribute(
            None,
            "Plugin",
        )


def test_empty_attribute_name_is_rejected(
    plugin_module,
) -> None:
    with pytest.raises(
        PluginLoadError
    ):
        PluginLoader().resolve_attribute(
            plugin_module,
            " ",
        )


def test_non_string_attribute_name_is_rejected(
    plugin_module,
) -> None:
    with pytest.raises(
        PluginLoadError
    ):
        PluginLoader().resolve_attribute(
            plugin_module,
            123,
        )


# =============================================================================
# LOAD
# =============================================================================
def test_load_class_plugin(
    plugin_module,
) -> None:
    loaded = PluginLoader().load(
        "tests_fixture_plugin_module"
        ":BasicPlugin"
    )

    assert isinstance(
        loaded,
        LoadedPlugin,
    )

    assert isinstance(
        loaded.plugin,
        BasicPlugin,
    )

    assert (
        loaded.instantiated
        is True
    )

    assert (
        loaded.info.name
        == "basic"
    )


def test_load_instance_plugin(
    plugin_module,
) -> None:
    loaded = PluginLoader().load(
        "tests_fixture_plugin_module"
        ":instance_plugin"
    )

    assert (
        loaded.plugin
        is plugin_module.instance_plugin
    )

    assert (
        loaded.instantiated
        is False
    )


def test_class_can_be_returned_without_instantiation(
    plugin_module,
) -> None:
    loader = PluginLoader(
        instantiate_classes=False
    )

    loaded = loader.load(
        "tests_fixture_plugin_module"
        ":BasicPlugin"
    )

    assert (
        loaded.plugin
        is BasicPlugin
    )

    assert (
        loaded.instantiated
        is False
    )


def test_callable_plugin_info_is_supported(
    plugin_module,
) -> None:
    loaded = PluginLoader().load(
        "tests_fixture_plugin_module"
        ":CallableInfoPlugin"
    )

    assert (
        loaded.info.name
        == "callable"
    )


def test_constant_plugin_info_is_supported(
    plugin_module,
) -> None:
    loaded = PluginLoader().load(
        "tests_fixture_plugin_module"
        ":ConstantInfoPlugin"
    )

    assert (
        loaded.info.name
        == "constant"
    )


def test_constructor_failure_is_wrapped(
    plugin_module,
) -> None:
    with pytest.raises(
        PluginLoadError
    ):
        PluginLoader().load(
            "tests_fixture_plugin_module"
            ":ConstructorFailurePlugin"
        )


def test_missing_plugin_info_is_rejected(
    plugin_module,
) -> None:
    with pytest.raises(
        PluginLoadError
    ):
        PluginLoader().load(
            "tests_fixture_plugin_module"
            ":MissingInfoPlugin"
        )


def test_invalid_plugin_info_is_rejected(
    plugin_module,
) -> None:
    with pytest.raises(
        PluginLoadError
    ):
        PluginLoader().load(
            "tests_fixture_plugin_module"
            ":InvalidInfoPlugin"
        )


def test_failing_info_provider_is_wrapped(
    plugin_module,
) -> None:
    with pytest.raises(
        PluginLoadError
    ):
        PluginLoader().load(
            "tests_fixture_plugin_module"
            ":FailingInfoProviderPlugin"
        )


# =============================================================================
# LOADED RESULT
# =============================================================================
def test_loaded_plugin_to_dict(
    plugin_module,
) -> None:
    loaded = PluginLoader().load(
        "tests_fixture_plugin_module"
        ":BasicPlugin"
    )

    payload = (
        loaded.to_dict()
    )

    assert (
        payload[
            "reference"
        ]
        == (
            "tests_fixture_plugin_module"
            ":BasicPlugin"
        )
    )

    assert (
        payload[
            "plugin_name"
        ]
        == "basic"
    )

    assert (
        payload[
            "plugin_version"
        ]
        == "1.0.0"
    )

    assert (
        payload[
            "instantiated"
        ]
        is True
    )


# =============================================================================
# REGISTRATION
# =============================================================================
def test_load_and_register_plugin(
    plugin_module,
) -> None:
    manager = PluginManager()

    loaded = (
        PluginLoader().load_and_register(
            "tests_fixture_plugin_module"
            ":BasicPlugin",
            manager,
        )
    )

    try:
        assert (
            manager.get(
                "basic"
            )
            is loaded.plugin
        )

    finally:
        manager.close()


def test_load_and_register_runs_lifecycle_hooks(
    plugin_module,
) -> None:
    manager = PluginManager()

    loaded = (
        PluginLoader().load_and_register(
            "tests_fixture_plugin_module"
            ":LifecyclePlugin",
            manager,
        )
    )

    try:
        assert (
            loaded.plugin.events
            == [
                "load",
                "enable",
            ]
        )

    finally:
        manager.close()


def test_load_and_register_can_start_disabled(
    plugin_module,
) -> None:
    manager = PluginManager()

    loaded = (
        PluginLoader().load_and_register(
            "tests_fixture_plugin_module"
            ":LifecyclePlugin",
            manager,
            enabled=False,
        )
    )

    try:
        assert (
            loaded.plugin.events
            == [
                "load",
            ]
        )

        assert (
            manager.is_enabled(
                "lifecycle"
            )
            is False
        )

    finally:
        manager.close()


def test_invalid_manager_is_rejected(
    plugin_module,
) -> None:
    with pytest.raises(
        TypeError
    ):
        PluginLoader().load_and_register(
            "tests_fixture_plugin_module"
            ":BasicPlugin",
            object(),
        )


def test_invalid_enabled_flag_is_rejected(
    plugin_module,
) -> None:
    manager = PluginManager()

    try:
        with pytest.raises(
            TypeError
        ):
            PluginLoader().load_and_register(
                "tests_fixture_plugin_module"
                ":BasicPlugin",
                manager,
                enabled=1,
            )

    finally:
        manager.close()


def test_duplicate_registration_is_wrapped(
    plugin_module,
) -> None:
    manager = PluginManager()

    try:
        PluginLoader().load_and_register(
            "tests_fixture_plugin_module"
            ":BasicPlugin",
            manager,
        )

        with pytest.raises(
            PluginLoadError
        ):
            PluginLoader().load_and_register(
                "tests_fixture_plugin_module"
                ":BasicPlugin",
                manager,
            )

    finally:
        manager.close()


# =============================================================================
# CONVENIENCE API
# =============================================================================
def test_load_plugin_helper(
    plugin_module,
) -> None:
    loaded = load_plugin(
        "tests_fixture_plugin_module"
        ":BasicPlugin"
    )

    assert isinstance(
        loaded.plugin,
        BasicPlugin,
    )


def test_load_and_register_plugin_helper(
    plugin_module,
) -> None:
    manager = PluginManager()

    try:
        loaded = (
            load_and_register_plugin(
                "tests_fixture_plugin_module"
                ":BasicPlugin",
                manager,
            )
        )

        assert (
            manager.get(
                "basic"
            )
            is loaded.plugin
        )

    finally:
        manager.close()


# =============================================================================
# SNAPSHOT / REPR
# =============================================================================
def test_loader_snapshot() -> None:
    snapshot = (
        PluginLoader().snapshot()
    )

    assert (
        snapshot[
            "instantiate_classes"
        ]
        is True
    )

    assert (
        snapshot[
            "reference_format"
        ]
        == (
            "package.module:attribute"
        )
    )


def test_loader_repr() -> None:
    representation = repr(
        PluginLoader()
    )

    assert (
        "PluginLoader"
        in representation
    )

    assert (
        "instantiate_classes=True"
        in representation
    )