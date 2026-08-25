from __future__ import annotations

"""
Enterprise Crawler Framework - CLI

Framework'ün command-line giriş noktası.

Desteklenen komutlar
--------------------
enterprise-crawler version
enterprise-crawler doctor
enterprise-crawler plugins list
enterprise-crawler plugins inspect <name>
enterprise-crawler --version

Ayrıca::

    python -m enterprise_crawler.cli

şeklinde de çalıştırılabilir.

Plugin CLI sınırı
-----------------
Plugin komutları yalnız discovery metadata'sını okur.

``plugins list`` ve ``plugins inspect``:

* plugin instantiate etmez,
* plugin lifecycle çalıştırmaz,
* PluginManager oluşturmaz,
* plugin invoke etmez.

Bu nedenle discovery operasyonları side-effect üretmeyen metadata
inspection komutları olarak kalır.
"""

import argparse
import sys
from dataclasses import dataclass
from typing import Optional, Sequence, TextIO

from enterprise_crawler import __version__
from enterprise_crawler.config import (
    ConfigLoader,
    CrawlerSettings,
)
from enterprise_crawler.core.base_bot import BaseBot
from enterprise_crawler.core.crawler import Crawler
from enterprise_crawler.plugins import (
    PluginDiscovery,
    PluginDiscoveryError,
)


PROGRAM_NAME = "enterprise-crawler"


# =============================================================================
# DOCTOR RESULT
# =============================================================================
@dataclass(frozen=True)
class DoctorCheck:
    """
    Tek bir CLI doctor kontrolünün sonucu.
    """

    name: str
    passed: bool
    detail: str

    def render(self) -> str:
        marker = (
            "OK"
            if self.passed
            else "FAIL"
        )

        return (
            f"[{marker}] "
            f"{self.name}: "
            f"{self.detail}"
        )


# =============================================================================
# OUTPUT HELPERS
# =============================================================================
def _write_line(
    stream: TextIO,
    value: str = "",
) -> None:
    stream.write(
        f"{value}\n"
    )


def _render_optional(
    value: Optional[str],
) -> str:
    if value is None:
        return "-"

    normalized = (
        value.strip()
    )

    return (
        normalized
        if normalized
        else "-"
    )


# =============================================================================
# DOCTOR
# =============================================================================
def _doctor_checks() -> tuple[
    DoctorCheck,
    ...,
]:
    """
    Side-effect üretmeyen temel runtime kontrollerini çalıştırır.

    Doctor:
    * network çağrısı yapmaz,
    * storage oluşturmaz,
    * SQLite dosyası üretmez,
    * bot çalıştırmaz.

    Ama framework'ün temel import/configuration zincirinin sağlıklı olduğunu
    doğrular.
    """

    checks: list[
        DoctorCheck
    ] = []

    # -------------------------------------------------------------------------
    # VERSION
    # -------------------------------------------------------------------------
    version_ok = bool(
        str(
            __version__
        ).strip()
    )

    checks.append(
        DoctorCheck(
            name="version",
            passed=version_ok,
            detail=(
                str(
                    __version__
                )
                if version_ok
                else "version boş"
            ),
        )
    )

    # -------------------------------------------------------------------------
    # CONFIGURATION
    # -------------------------------------------------------------------------
    try:
        settings = (
            ConfigLoader.from_mapping(
                {}
            )
        )

        config_ok = isinstance(
            settings,
            CrawlerSettings,
        )

        checks.append(
            DoctorCheck(
                name="configuration",
                passed=config_ok,
                detail=(
                    "default configuration valid"
                    if config_ok
                    else (
                        "unexpected settings type "
                        f"{type(settings).__name__}"
                    )
                ),
            )
        )

    except Exception as exc:
        checks.append(
            DoctorCheck(
                name="configuration",
                passed=False,
                detail=(
                    f"{exc.__class__.__name__}: "
                    f"{exc}"
                ),
            )
        )

    # -------------------------------------------------------------------------
    # CORE IMPORTS
    # -------------------------------------------------------------------------
    core_ok = (
        isinstance(
            BaseBot,
            type,
        )
        and isinstance(
            Crawler,
            type,
        )
    )

    checks.append(
        DoctorCheck(
            name="core",
            passed=core_ok,
            detail=(
                "BaseBot and Crawler available"
                if core_ok
                else "core runtime unavailable"
            ),
        )
    )

    return tuple(
        checks
    )


def run_doctor(
    *,
    stdout: TextIO,
) -> int:
    checks = (
        _doctor_checks()
    )

    _write_line(
        stdout,
        (
            "Enterprise Crawler Framework "
            f"{__version__}"
        ),
    )

    _write_line(
        stdout,
        "Doctor",
    )

    _write_line(
        stdout,
    )

    for check in checks:
        _write_line(
            stdout,
            check.render(),
        )

    passed_count = sum(
        1
        for check in checks
        if check.passed
    )

    total_count = len(
        checks
    )

    _write_line(
        stdout,
    )

    _write_line(
        stdout,
        (
            f"Result: "
            f"{passed_count}/{total_count} passed"
        ),
    )

    return (
        0
        if passed_count
        == total_count
        else 1
    )


# =============================================================================
# VERSION
# =============================================================================
def run_version(
    *,
    stdout: TextIO,
) -> int:
    _write_line(
        stdout,
        __version__,
    )

    return 0


# =============================================================================
# PLUGINS
# =============================================================================
def run_plugins_list(
    *,
    stdout: TextIO,
    discovery: Optional[
        PluginDiscovery
    ] = None,
) -> int:
    """
    Kurulu plugin entry-point'lerini listeler.

    Discovery dışında hiçbir plugin runtime işlemi yapılmaz.
    """

    resolved_discovery = (
        discovery
        if discovery is not None
        else PluginDiscovery()
    )

    try:
        plugins = (
            resolved_discovery.discover()
        )

    except PluginDiscoveryError as exc:
        _write_line(
            stdout,
            (
                "Plugin discovery failed: "
                f"{exc}"
            ),
        )

        return 1

    _write_line(
        stdout,
        (
            "Discovered plugins: "
            f"{len(plugins)}"
        ),
    )

    if not plugins:
        return 0

    _write_line(
        stdout,
    )

    _write_line(
        stdout,
        (
            "NAME\tTARGET"
        ),
    )

    for plugin in plugins:
        _write_line(
            stdout,
            (
                f"{plugin.name}\t"
                f"{plugin.value}"
            ),
        )

    return 0


def run_plugins_inspect(
    name: str,
    *,
    stdout: TextIO,
    discovery: Optional[
        PluginDiscovery
    ] = None,
) -> int:
    """
    Tek plugin entry-point metadata'sını gösterir.

    Plugin yüklenmez veya instantiate edilmez.
    """

    if not isinstance(
        name,
        str,
    ):
        _write_line(
            stdout,
            "Plugin name must be a string."
        )

        return 2

    normalized_name = (
        name.strip()
    )

    if not normalized_name:
        _write_line(
            stdout,
            "Plugin name cannot be empty."
        )

        return 2

    resolved_discovery = (
        discovery
        if discovery is not None
        else PluginDiscovery()
    )

    try:
        plugin = (
            resolved_discovery.require(
                normalized_name
            )
        )

    except PluginDiscoveryError as exc:
        _write_line(
            stdout,
            (
                "Plugin not found: "
                f"{normalized_name}"
            ),
        )

        _write_line(
            stdout,
            (
                "Detail: "
                f"{exc}"
            ),
        )

        return 1

    _write_line(
        stdout,
        (
            f"Plugin: "
            f"{plugin.name}"
        ),
    )

    _write_line(
        stdout,
        (
            "Group: "
            f"{plugin.group}"
        ),
    )

    _write_line(
        stdout,
        (
            "Target: "
            f"{plugin.value}"
        ),
    )

    _write_line(
        stdout,
        (
            "Module: "
            f"{plugin.module}"
        ),
    )

    _write_line(
        stdout,
        (
            "Attribute: "
            f"{_render_optional(plugin.attribute)}"
        ),
    )

    return 0


# =============================================================================
# PARSER
# =============================================================================
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=PROGRAM_NAME,
        description=(
            "Enterprise-grade crawling runtime "
            "and data collection framework."
        ),
    )

    parser.add_argument(
        "--version",
        action="version",
        version=(
            f"%(prog)s {__version__}"
        ),
    )

    subparsers = (
        parser.add_subparsers(
            dest="command",
            metavar="COMMAND",
        )
    )

    # -------------------------------------------------------------------------
    # VERSION
    # -------------------------------------------------------------------------
    version_parser = (
        subparsers.add_parser(
            "version",
            help=(
                "Show framework version."
            ),
        )
    )

    version_parser.set_defaults(
        handler="version"
    )

    # -------------------------------------------------------------------------
    # DOCTOR
    # -------------------------------------------------------------------------
    doctor_parser = (
        subparsers.add_parser(
            "doctor",
            help=(
                "Run local framework health checks."
            ),
        )
    )

    doctor_parser.set_defaults(
        handler="doctor"
    )

    # -------------------------------------------------------------------------
    # PLUGINS
    # -------------------------------------------------------------------------
    plugins_parser = (
        subparsers.add_parser(
            "plugins",
            help=(
                "Inspect installed framework plugins."
            ),
        )
    )

    plugin_subparsers = (
        plugins_parser.add_subparsers(
            dest="plugin_command",
            metavar="PLUGIN_COMMAND",
        )
    )

    # -------------------------------------------------------------------------
    # PLUGINS LIST
    # -------------------------------------------------------------------------
    plugins_list_parser = (
        plugin_subparsers.add_parser(
            "list",
            help=(
                "List discovered plugin entry-points."
            ),
        )
    )

    plugins_list_parser.set_defaults(
        handler="plugins-list"
    )

    # -------------------------------------------------------------------------
    # PLUGINS INSPECT
    # -------------------------------------------------------------------------
    plugins_inspect_parser = (
        plugin_subparsers.add_parser(
            "inspect",
            help=(
                "Inspect a discovered plugin entry-point."
            ),
        )
    )

    plugins_inspect_parser.add_argument(
        "name",
        help=(
            "Plugin entry-point name."
        ),
    )

    plugins_inspect_parser.set_defaults(
        handler="plugins-inspect"
    )

    return parser


# =============================================================================
# MAIN
# =============================================================================
def main(
    argv: Optional[
        Sequence[str]
    ] = None,
    *,
    stdout: Optional[
        TextIO
    ] = None,
) -> int:
    """
    CLI public entry point.

    ``argv`` verilmezse ``sys.argv[1:]`` kullanılır.

    Test edilebilirlik için stdout inject edilebilir.
    """

    resolved_stdout = (
        stdout
        if stdout is not None
        else sys.stdout
    )

    parser = (
        build_parser()
    )

    args = parser.parse_args(
        list(
            argv
        )
        if argv is not None
        else None
    )

    handler = getattr(
        args,
        "handler",
        None,
    )

    if handler == "version":
        return run_version(
            stdout=resolved_stdout
        )

    if handler == "doctor":
        return run_doctor(
            stdout=resolved_stdout
        )

    if handler == "plugins-list":
        return run_plugins_list(
            stdout=resolved_stdout
        )

    if handler == "plugins-inspect":
        return run_plugins_inspect(
            args.name,
            stdout=resolved_stdout,
        )

    parser.print_help(
        file=resolved_stdout
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(
        main()
    )