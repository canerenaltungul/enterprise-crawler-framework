from __future__ import annotations

import importlib
from io import StringIO
from typing import Any

import pytest

from enterprise_crawler import __version__
from enterprise_crawler.cli.main import (
    DoctorCheck,
    PROGRAM_NAME,
    build_parser,
    main,
    run_doctor,
    run_plugins_inspect,
    run_plugins_list,
    run_version,
)
from enterprise_crawler.plugins import (
    DEFAULT_PLUGIN_ENTRY_POINT_GROUP,
    PluginDiscovery,
)


# =============================================================================
# MODULE REFERENCES
# =============================================================================
CLI_MAIN_MODULE = importlib.import_module(
    "enterprise_crawler.cli.main"
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


def make_plugin_discovery(
    *entry_points: Any,
) -> PluginDiscovery:
    return PluginDiscovery(
        entry_points_provider=lambda: (
            list(
                entry_points
            )
        )
    )


# =============================================================================
# DOCTOR CHECK MODEL
# =============================================================================
def test_doctor_check_renders_success() -> None:
    check = DoctorCheck(
        name="core",
        passed=True,
        detail="ready",
    )

    assert (
        check.render()
        == "[OK] core: ready"
    )


def test_doctor_check_renders_failure() -> None:
    check = DoctorCheck(
        name="core",
        passed=False,
        detail="broken",
    )

    assert (
        check.render()
        == "[FAIL] core: broken"
    )


# =============================================================================
# PARSER
# =============================================================================
def test_parser_uses_expected_program_name() -> None:
    parser = build_parser()

    assert (
        parser.prog
        == PROGRAM_NAME
    )


def test_parser_accepts_version_command() -> None:
    parser = build_parser()

    args = parser.parse_args(
        [
            "version",
        ]
    )

    assert (
        args.command
        == "version"
    )

    assert (
        args.handler
        == "version"
    )


def test_parser_accepts_doctor_command() -> None:
    parser = build_parser()

    args = parser.parse_args(
        [
            "doctor",
        ]
    )

    assert (
        args.command
        == "doctor"
    )

    assert (
        args.handler
        == "doctor"
    )


def test_parser_accepts_plugins_list_command() -> None:
    parser = build_parser()

    args = parser.parse_args(
        [
            "plugins",
            "list",
        ]
    )

    assert (
        args.command
        == "plugins"
    )

    assert (
        args.plugin_command
        == "list"
    )

    assert (
        args.handler
        == "plugins-list"
    )


def test_parser_accepts_plugins_inspect_command() -> None:
    parser = build_parser()

    args = parser.parse_args(
        [
            "plugins",
            "inspect",
            "audit-plugin",
        ]
    )

    assert (
        args.command
        == "plugins"
    )

    assert (
        args.plugin_command
        == "inspect"
    )

    assert (
        args.handler
        == "plugins-inspect"
    )

    assert (
        args.name
        == "audit-plugin"
    )


# =============================================================================
# VERSION
# =============================================================================
def test_run_version_prints_framework_version() -> None:
    stdout = StringIO()

    exit_code = run_version(
        stdout=stdout
    )

    assert exit_code == 0

    assert (
        stdout.getvalue().strip()
        == __version__
    )


def test_main_version_command() -> None:
    stdout = StringIO()

    exit_code = main(
        [
            "version",
        ],
        stdout=stdout,
    )

    assert exit_code == 0

    assert (
        stdout.getvalue().strip()
        == __version__
    )


def test_dash_dash_version_uses_argparse_version_action(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(
        SystemExit
    ) as exc_info:
        main(
            [
                "--version",
            ]
        )

    assert (
        exc_info.value.code
        == 0
    )

    captured = (
        capsys.readouterr()
    )

    assert (
        PROGRAM_NAME
        in captured.out
    )

    assert (
        __version__
        in captured.out
    )


# =============================================================================
# DOCTOR
# =============================================================================
def test_doctor_returns_success() -> None:
    stdout = StringIO()

    exit_code = run_doctor(
        stdout=stdout
    )

    assert (
        exit_code
        == 0
    )


def test_doctor_output_contains_framework_version() -> None:
    stdout = StringIO()

    run_doctor(
        stdout=stdout
    )

    rendered = (
        stdout.getvalue()
    )

    assert (
        __version__
        in rendered
    )


def test_doctor_checks_configuration() -> None:
    stdout = StringIO()

    run_doctor(
        stdout=stdout
    )

    rendered = (
        stdout.getvalue()
    )

    assert (
        "[OK] configuration:"
        in rendered
    )


def test_doctor_checks_core_runtime() -> None:
    stdout = StringIO()

    run_doctor(
        stdout=stdout
    )

    rendered = (
        stdout.getvalue()
    )

    assert (
        "[OK] core:"
        in rendered
    )


def test_doctor_reports_all_checks_passed() -> None:
    stdout = StringIO()

    run_doctor(
        stdout=stdout
    )

    rendered = (
        stdout.getvalue()
    )

    assert (
        "Result: 3/3 passed"
        in rendered
    )


def test_main_doctor_command() -> None:
    stdout = StringIO()

    exit_code = main(
        [
            "doctor",
        ],
        stdout=stdout,
    )

    assert exit_code == 0

    assert (
        "Doctor"
        in stdout.getvalue()
    )


# =============================================================================
# PLUGINS LIST
# =============================================================================
def test_plugins_list_empty_discovery() -> None:
    discovery = (
        make_plugin_discovery()
    )

    stdout = StringIO()

    exit_code = (
        run_plugins_list(
            stdout=stdout,
            discovery=discovery,
        )
    )

    assert (
        exit_code
        == 0
    )

    assert (
        stdout.getvalue().strip()
        == "Discovered plugins: 0"
    )


def test_plugins_list_renders_discovered_plugins() -> None:
    discovery = (
        make_plugin_discovery(
            FakeEntryPoint(
                name="zeta",
                value=(
                    "example.plugins:"
                    "ZetaPlugin"
                ),
            ),
            FakeEntryPoint(
                name="alpha",
                value=(
                    "example.plugins:"
                    "AlphaPlugin"
                ),
            ),
        )
    )

    stdout = StringIO()

    exit_code = (
        run_plugins_list(
            stdout=stdout,
            discovery=discovery,
        )
    )

    rendered = (
        stdout.getvalue()
    )

    assert (
        exit_code
        == 0
    )

    assert (
        "Discovered plugins: 2"
        in rendered
    )

    assert (
        "NAME\tTARGET"
        in rendered
    )

    assert (
        "alpha\texample.plugins:AlphaPlugin"
        in rendered
    )

    assert (
        "zeta\texample.plugins:ZetaPlugin"
        in rendered
    )

    assert (
        rendered.index(
            "alpha\texample.plugins:AlphaPlugin"
        )
        < rendered.index(
            "zeta\texample.plugins:ZetaPlugin"
        )
    )


def test_plugins_list_does_not_load_plugin() -> None:
    loaded = False

    class EntryPoint:
        name = "safe-plugin"
        value = (
            "package.module:"
            "Plugin"
        )
        group = (
            DEFAULT_PLUGIN_ENTRY_POINT_GROUP
        )

        def load(
            self,
        ) -> None:
            nonlocal loaded

            loaded = True

    discovery = (
        make_plugin_discovery(
            EntryPoint()
        )
    )

    stdout = StringIO()

    exit_code = (
        run_plugins_list(
            stdout=stdout,
            discovery=discovery,
        )
    )

    assert (
        exit_code
        == 0
    )

    assert (
        loaded
        is False
    )


# =============================================================================
# PLUGINS INSPECT
# =============================================================================
def test_plugins_inspect_renders_metadata() -> None:
    discovery = (
        make_plugin_discovery(
            FakeEntryPoint(
                name="audit-plugin",
                value=(
                    "company.plugins:"
                    "AuditPlugin"
                ),
            )
        )
    )

    stdout = StringIO()

    exit_code = (
        run_plugins_inspect(
            "audit-plugin",
            stdout=stdout,
            discovery=discovery,
        )
    )

    rendered = (
        stdout.getvalue()
    )

    assert (
        exit_code
        == 0
    )

    assert (
        "Plugin: audit-plugin"
        in rendered
    )

    assert (
        (
            "Group: "
            f"{DEFAULT_PLUGIN_ENTRY_POINT_GROUP}"
        )
        in rendered
    )

    assert (
        (
            "Target: "
            "company.plugins:AuditPlugin"
        )
        in rendered
    )

    assert (
        "Module: company.plugins"
        in rendered
    )

    assert (
        "Attribute: AuditPlugin"
        in rendered
    )


def test_plugins_inspect_is_case_insensitive() -> None:
    discovery = (
        make_plugin_discovery(
            FakeEntryPoint(
                name="audit-plugin",
                value=(
                    "company.plugins:"
                    "AuditPlugin"
                ),
            )
        )
    )

    stdout = StringIO()

    exit_code = (
        run_plugins_inspect(
            "AUDIT-PLUGIN",
            stdout=stdout,
            discovery=discovery,
        )
    )

    assert (
        exit_code
        == 0
    )

    assert (
        "Plugin: audit-plugin"
        in stdout.getvalue()
    )


def test_plugins_inspect_unknown_plugin_returns_failure() -> None:
    discovery = (
        make_plugin_discovery()
    )

    stdout = StringIO()

    exit_code = (
        run_plugins_inspect(
            "missing-plugin",
            stdout=stdout,
            discovery=discovery,
        )
    )

    rendered = (
        stdout.getvalue()
    )

    assert (
        exit_code
        == 1
    )

    assert (
        "Plugin not found: missing-plugin"
        in rendered
    )


def test_plugins_inspect_module_only_target() -> None:
    discovery = (
        make_plugin_discovery(
            FakeEntryPoint(
                name="module-plugin",
                value="company.plugin_module",
            )
        )
    )

    stdout = StringIO()

    exit_code = (
        run_plugins_inspect(
            "module-plugin",
            stdout=stdout,
            discovery=discovery,
        )
    )

    rendered = (
        stdout.getvalue()
    )

    assert (
        exit_code
        == 0
    )

    assert (
        "Module: company.plugin_module"
        in rendered
    )

    assert (
        "Attribute: -"
        in rendered
    )


# =============================================================================
# MAIN PLUGIN DISPATCH
# =============================================================================
def test_main_plugins_list_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeDiscovery:
        def discover(
            self,
        ) -> tuple[Any, ...]:
            return ()

    monkeypatch.setattr(
        CLI_MAIN_MODULE,
        "PluginDiscovery",
        FakeDiscovery,
    )

    stdout = StringIO()

    exit_code = main(
        [
            "plugins",
            "list",
        ],
        stdout=stdout,
    )

    assert (
        exit_code
        == 0
    )

    assert (
        "Discovered plugins: 0"
        in stdout.getvalue()
    )


def test_main_plugins_inspect_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    discovery = (
        make_plugin_discovery(
            FakeEntryPoint(
                name="audit-plugin",
                value=(
                    "company.plugins:"
                    "AuditPlugin"
                ),
            )
        )
    )

    monkeypatch.setattr(
        CLI_MAIN_MODULE,
        "PluginDiscovery",
        lambda: discovery,
    )

    stdout = StringIO()

    exit_code = main(
        [
            "plugins",
            "inspect",
            "audit-plugin",
        ],
        stdout=stdout,
    )

    assert (
        exit_code
        == 0
    )

    assert (
        "Plugin: audit-plugin"
        in stdout.getvalue()
    )


# =============================================================================
# HELP
# =============================================================================
def test_no_command_prints_help() -> None:
    stdout = StringIO()

    exit_code = main(
        [],
        stdout=stdout,
    )

    rendered = (
        stdout.getvalue()
    )

    assert exit_code == 0

    assert (
        "usage:"
        in rendered
    )

    assert (
        "version"
        in rendered
    )

    assert (
        "doctor"
        in rendered
    )

    assert (
        "plugins"
        in rendered
    )


def test_plugins_without_subcommand_prints_main_help() -> None:
    stdout = StringIO()

    exit_code = main(
        [
            "plugins",
        ],
        stdout=stdout,
    )

    rendered = (
        stdout.getvalue()
    )

    assert (
        exit_code
        == 0
    )

    assert (
        "usage:"
        in rendered
    )


def test_help_option_exits_successfully(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(
        SystemExit
    ) as exc_info:
        main(
            [
                "--help",
            ]
        )

    assert (
        exc_info.value.code
        == 0
    )

    captured = (
        capsys.readouterr()
    )

    assert (
        PROGRAM_NAME
        in captured.out
    )


def test_plugins_help_exits_successfully(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(
        SystemExit
    ) as exc_info:
        main(
            [
                "plugins",
                "--help",
            ]
        )

    assert (
        exc_info.value.code
        == 0
    )

    captured = (
        capsys.readouterr()
    )

    assert (
        "list"
        in captured.out
    )

    assert (
        "inspect"
        in captured.out
    )


def test_unknown_command_exits_with_argparse_error() -> None:
    with pytest.raises(
        SystemExit
    ) as exc_info:
        main(
            [
                "unknown",
            ]
        )

    assert (
        exc_info.value.code
        == 2
    )


def test_unknown_plugin_command_exits_with_argparse_error() -> None:
    with pytest.raises(
        SystemExit
    ) as exc_info:
        main(
            [
                "plugins",
                "unknown",
            ]
        )

    assert (
        exc_info.value.code
        == 2
    )