from __future__ import annotations

import csv
import hashlib
import importlib
import io
import subprocess
import sys
import zipfile
from base64 import urlsafe_b64encode
from collections.abc import Iterator
from importlib import metadata as importlib_metadata
from pathlib import Path

import pytest

from enterprise_crawler.plugins import (
    DEFAULT_PLUGIN_ENTRY_POINT_GROUP,
    AutoLoadedPlugin,
    PluginAutoLoader,
    PluginDiscovery,
    PluginManager,
)


# =============================================================================
# CONSTANTS
# =============================================================================
FIXTURE_DISTRIBUTION_NAME = (
    "enterprise-crawler-test-entrypoint-plugin"
)

FIXTURE_NORMALIZED_DISTRIBUTION_NAME = (
    "enterprise_crawler_test_entrypoint_plugin"
)

FIXTURE_VERSION = (
    "0.1.0"
)

FIXTURE_ENTRY_POINT_NAME = (
    "fixture-entry"
)

FIXTURE_PLUGIN_NAME = (
    "fixture-runtime-plugin"
)

FIXTURE_MODULE_NAME = (
    "enterprise_crawler_test_entrypoint_plugin"
)

FIXTURE_PLUGIN_ATTRIBUTE = (
    "FixturePlugin"
)

FIXTURE_ENTRY_POINT_VALUE = (
    f"{FIXTURE_MODULE_NAME}:"
    f"{FIXTURE_PLUGIN_ATTRIBUTE}"
)

FIXTURE_WHEEL_FILENAME = (
    f"{FIXTURE_NORMALIZED_DISTRIBUTION_NAME}-"
    f"{FIXTURE_VERSION}-py3-none-any.whl"
)

FIXTURE_DIST_INFO_DIRECTORY = (
    f"{FIXTURE_NORMALIZED_DISTRIBUTION_NAME}-"
    f"{FIXTURE_VERSION}.dist-info"
)


# =============================================================================
# PATH HELPERS
# =============================================================================
def repository_root() -> Path:
    return (
        Path(__file__)
        .resolve()
        .parents[2]
    )


def fixture_package_root() -> Path:
    return (
        repository_root()
        / "tests"
        / "fixtures"
        / "entrypoint_plugin"
    )


def fixture_source_file() -> Path:
    return (
        fixture_package_root()
        / "src"
        / FIXTURE_MODULE_NAME
        / "__init__.py"
    )


# =============================================================================
# WHEEL HELPERS
# =============================================================================
def _sha256_record_value(
    payload: bytes,
) -> str:
    digest = hashlib.sha256(
        payload
    ).digest()

    encoded = (
        urlsafe_b64encode(
            digest
        )
        .rstrip(
            b"="
        )
        .decode(
            "ascii"
        )
    )

    return (
        f"sha256={encoded}"
    )


def _metadata_payload() -> bytes:
    return (
        "\n".join(
            [
                "Metadata-Version: 2.1",
                (
                    "Name: "
                    f"{FIXTURE_DISTRIBUTION_NAME}"
                ),
                (
                    "Version: "
                    f"{FIXTURE_VERSION}"
                ),
                (
                    "Summary: "
                    "Integration-test plugin for "
                    "Enterprise Crawler Framework."
                ),
                "Requires-Python: >=3.11",
                "",
            ]
        )
        .encode(
            "utf-8"
        )
    )


def _wheel_payload() -> bytes:
    return (
        "\n".join(
            [
                "Wheel-Version: 1.0",
                (
                    "Generator: "
                    "enterprise-crawler-framework-tests"
                ),
                "Root-Is-Purelib: true",
                "Tag: py3-none-any",
                "",
            ]
        )
        .encode(
            "utf-8"
        )
    )


def _entry_points_payload() -> bytes:
    return (
        "\n".join(
            [
                (
                    "["
                    f"{DEFAULT_PLUGIN_ENTRY_POINT_GROUP}"
                    "]"
                ),
                (
                    f"{FIXTURE_ENTRY_POINT_NAME} = "
                    f"{FIXTURE_ENTRY_POINT_VALUE}"
                ),
                "",
            ]
        )
        .encode(
            "utf-8"
        )
    )


def _build_record_payload(
    files: dict[
        str,
        bytes,
    ],
) -> bytes:
    output = io.StringIO(
        newline=""
    )

    writer = csv.writer(
        output,
        lineterminator="\n",
    )

    for path in sorted(
        files
    ):
        payload = files[
            path
        ]

        writer.writerow(
            [
                path,
                _sha256_record_value(
                    payload
                ),
                str(
                    len(
                        payload
                    )
                ),
            ]
        )

    record_path = (
        f"{FIXTURE_DIST_INFO_DIRECTORY}/"
        "RECORD"
    )

    writer.writerow(
        [
            record_path,
            "",
            "",
        ]
    )

    return (
        output
        .getvalue()
        .encode(
            "utf-8"
        )
    )


def build_fixture_wheel(
    destination: Path,
) -> Path:
    """
    Fixture source paketinden minimal fakat gerçek bir wheel üretir.

    Build backend kullanılmaz.

    Böylece integration testi:

    - setuptools
    - wheel package
    - internet
    - build isolation

    bağımlılığı olmadan gerçek pip wheel installation sınırını test eder.
    """

    source_file = (
        fixture_source_file()
    )

    assert (
        source_file.is_file()
    ), (
        "Fixture plugin source bulunamadı "
        f"| path={source_file}"
    )

    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    wheel_path = (
        destination
        / FIXTURE_WHEEL_FILENAME
    )

    package_path = (
        f"{FIXTURE_MODULE_NAME}/"
        "__init__.py"
    )

    metadata_path = (
        f"{FIXTURE_DIST_INFO_DIRECTORY}/"
        "METADATA"
    )

    wheel_metadata_path = (
        f"{FIXTURE_DIST_INFO_DIRECTORY}/"
        "WHEEL"
    )

    entry_points_path = (
        f"{FIXTURE_DIST_INFO_DIRECTORY}/"
        "entry_points.txt"
    )

    files: dict[
        str,
        bytes,
    ] = {
        package_path: (
            source_file.read_bytes()
        ),
        metadata_path: (
            _metadata_payload()
        ),
        wheel_metadata_path: (
            _wheel_payload()
        ),
        entry_points_path: (
            _entry_points_payload()
        ),
    }

    record_payload = (
        _build_record_payload(
            files
        )
    )

    record_path = (
        f"{FIXTURE_DIST_INFO_DIRECTORY}/"
        "RECORD"
    )

    with zipfile.ZipFile(
        wheel_path,
        mode="w",
        compression=(
            zipfile.ZIP_DEFLATED
        ),
    ) as archive:
        for path, payload in (
            files.items()
        ):
            archive.writestr(
                path,
                payload,
            )

        archive.writestr(
            record_path,
            record_payload,
        )

    assert (
        wheel_path.is_file()
    )

    return wheel_path


# =============================================================================
# INSTALL FIXTURE
# =============================================================================
@pytest.fixture
def installed_entrypoint_plugin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    """
    Fixture plugin için gerçek wheel üretir ve gerçek pip install --target
    işlemiyle izole temporary site-packages dizinine kurar.

    Host Python environment değiştirilmez.

    Build backend kullanılmaz; wheel doğrudan kurulur.
    """

    wheel_directory = (
        tmp_path
        / "wheel"
    )

    wheel_path = (
        build_fixture_wheel(
            wheel_directory
        )
    )

    target = (
        tmp_path
        / "site-packages"
    )

    target.mkdir(
        parents=True,
        exist_ok=True,
    )

    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
        "--no-input",
        "--no-deps",
        "--no-index",
        "--target",
        str(target),
        str(wheel_path),
    ]

    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )

    if completed.returncode != 0:
        pytest.fail(
            "Fixture plugin wheel kurulumu başarısız.\n"
            f"command={' '.join(command)}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )

    monkeypatch.syspath_prepend(
        str(target)
    )

    importlib.invalidate_caches()

    sys.modules.pop(
        FIXTURE_MODULE_NAME,
        None,
    )

    try:
        yield target

    finally:
        sys.modules.pop(
            FIXTURE_MODULE_NAME,
            None,
        )

        importlib.invalidate_caches()


# =============================================================================
# PACKAGING METADATA
# =============================================================================
def test_fixture_distribution_is_really_installed(
    installed_entrypoint_plugin: Path,
) -> None:
    distributions = list(
        importlib_metadata.distributions(
            path=[
                str(
                    installed_entrypoint_plugin
                )
            ]
        )
    )

    names = {
        distribution.metadata.get(
            "Name",
            "",
        ).casefold()
        for distribution
        in distributions
    }

    assert (
        FIXTURE_DISTRIBUTION_NAME.casefold()
        in names
    )


def test_fixture_distribution_contains_real_entry_point(
    installed_entrypoint_plugin: Path,
) -> None:
    distributions = list(
        importlib_metadata.distributions(
            path=[
                str(
                    installed_entrypoint_plugin
                )
            ]
        )
    )

    matching_distribution = next(
        (
            distribution
            for distribution
            in distributions
            if (
                distribution.metadata.get(
                    "Name",
                    "",
                ).casefold()
                == FIXTURE_DISTRIBUTION_NAME.casefold()
            )
        ),
        None,
    )

    assert (
        matching_distribution
        is not None
    )

    matching_entry_points = [
        entry_point
        for entry_point
        in matching_distribution.entry_points
        if (
            entry_point.group
            == DEFAULT_PLUGIN_ENTRY_POINT_GROUP
        )
    ]

    assert (
        len(
            matching_entry_points
        )
        == 1
    )

    entry_point = (
        matching_entry_points[0]
    )

    assert (
        entry_point.name
        == FIXTURE_ENTRY_POINT_NAME
    )

    assert (
        entry_point.value
        == FIXTURE_ENTRY_POINT_VALUE
    )


# =============================================================================
# REAL DISCOVERY
# =============================================================================
def test_default_plugin_discovery_finds_installed_package(
    installed_entrypoint_plugin: Path,
) -> None:
    discovery = (
        PluginDiscovery()
    )

    discovered = (
        discovery.require(
            FIXTURE_ENTRY_POINT_NAME
        )
    )

    assert (
        discovered.name
        == FIXTURE_ENTRY_POINT_NAME
    )

    assert (
        discovered.group
        == DEFAULT_PLUGIN_ENTRY_POINT_GROUP
    )

    assert (
        discovered.module
        == FIXTURE_MODULE_NAME
    )

    assert (
        discovered.attribute
        == FIXTURE_PLUGIN_ATTRIBUTE
    )

    assert (
        discovered.value
        == FIXTURE_ENTRY_POINT_VALUE
    )


def test_discovery_does_not_import_installed_plugin(
    installed_entrypoint_plugin: Path,
) -> None:
    sys.modules.pop(
        FIXTURE_MODULE_NAME,
        None,
    )

    discovery = (
        PluginDiscovery()
    )

    discovered = (
        discovery.require(
            FIXTURE_ENTRY_POINT_NAME
        )
    )

    assert (
        discovered.name
        == FIXTURE_ENTRY_POINT_NAME
    )

    assert (
        FIXTURE_MODULE_NAME
        not in sys.modules
    )


# =============================================================================
# REAL AUTOLOAD
# =============================================================================
def test_installed_entrypoint_plugin_can_be_autoloaded(
    installed_entrypoint_plugin: Path,
) -> None:
    manager = (
        PluginManager()
    )

    discovery = (
        PluginDiscovery()
    )

    auto_loader = (
        PluginAutoLoader(
            manager=manager,
            discovery=discovery,
        )
    )

    try:
        result = (
            auto_loader.discover_and_register_one(
                FIXTURE_ENTRY_POINT_NAME
            )
        )

        assert isinstance(
            result,
            AutoLoadedPlugin,
        )

        assert (
            result.discovered.name
            == FIXTURE_ENTRY_POINT_NAME
        )

        assert (
            result.name
            == FIXTURE_PLUGIN_NAME
        )

        assert (
            result.version
            == FIXTURE_VERSION
        )

        assert (
            manager.contains(
                FIXTURE_PLUGIN_NAME
            )
            is True
        )

        assert (
            manager.is_enabled(
                FIXTURE_PLUGIN_NAME
            )
            is True
        )

    finally:
        manager.close()


def test_real_entrypoint_autoload_runs_lifecycle(
    installed_entrypoint_plugin: Path,
) -> None:
    manager = (
        PluginManager()
    )

    auto_loader = (
        PluginAutoLoader(
            manager=manager,
            discovery=(
                PluginDiscovery()
            ),
        )
    )

    result = (
        auto_loader.discover_and_register_one(
            FIXTURE_ENTRY_POINT_NAME
        )
    )

    plugin = (
        result.loaded.plugin
    )

    try:
        assert (
            plugin.events
            == [
                "load",
                "enable",
            ]
        )

        assert (
            plugin.manager
            is manager
        )

    finally:
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


# =============================================================================
# REAL INVOCATION
# =============================================================================
def test_real_entrypoint_plugin_can_be_invoked(
    installed_entrypoint_plugin: Path,
) -> None:
    manager = (
        PluginManager()
    )

    auto_loader = (
        PluginAutoLoader(
            manager=manager,
            discovery=(
                PluginDiscovery()
            ),
        )
    )

    try:
        auto_loader.discover_and_register_one(
            FIXTURE_ENTRY_POINT_NAME
        )

        result = manager.invoke(
            FIXTURE_PLUGIN_NAME,
            "collect",
            "real-entry-point",
        )

        assert (
            result[
                "plugin"
            ]
            == FIXTURE_PLUGIN_NAME
        )

        assert (
            result[
                "version"
            ]
            == FIXTURE_VERSION
        )

        assert (
            result[
                "value"
            ]
            == "real-entry-point"
        )

        assert (
            result[
                "events"
            ]
            == [
                "load",
                "enable",
            ]
        )

        snapshot = (
            manager.plugin_snapshot(
                FIXTURE_PLUGIN_NAME
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
# END-TO-END SNAPSHOT
# =============================================================================
def test_real_entrypoint_pipeline_snapshots(
    installed_entrypoint_plugin: Path,
) -> None:
    manager = (
        PluginManager()
    )

    discovery = (
        PluginDiscovery()
    )

    auto_loader = (
        PluginAutoLoader(
            manager=manager,
            discovery=discovery,
        )
    )

    try:
        auto_loader.discover_and_register_one(
            FIXTURE_ENTRY_POINT_NAME
        )

        discovery_snapshot = (
            discovery.snapshot()
        )

        auto_loader_snapshot = (
            auto_loader.snapshot()
        )

        manager_snapshot = (
            manager.snapshot()
        )

        assert (
            discovery_snapshot[
                "discover_count"
            ]
            >= 1
        )

        assert any(
            plugin[
                "name"
            ]
            == FIXTURE_ENTRY_POINT_NAME
            for plugin
            in discovery_snapshot[
                "last_plugins"
            ]
        )

        assert (
            auto_loader_snapshot[
                "load_count"
            ]
            == 1
        )

        assert (
            auto_loader_snapshot[
                "last_plugin_count"
            ]
            == 1
        )

        assert (
            auto_loader_snapshot[
                "manager_plugin_count"
            ]
            == 1
        )

        assert (
            manager_snapshot[
                "plugin_count"
            ]
            == 1
        )

        assert (
            manager_snapshot[
                "enabled_count"
            ]
            == 1
        )

        assert (
            manager_snapshot[
                "plugins"
            ][0][
                "name"
            ]
            == FIXTURE_PLUGIN_NAME
        )

    finally:
        manager.close()