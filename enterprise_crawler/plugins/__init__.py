from __future__ import annotations

"""
Enterprise Crawler Framework - Plugins Public API

Plugin subsystem için kararlı public import yüzeyi.

Önerilen kullanım::

    from enterprise_crawler.plugins import (
        PluginRegistry,
        PluginManager,
        PluginLoader,
        PluginDiscovery,
        PluginAutoLoader,
    )

Alt modüllerin doğrudan import edilmesi mümkündür, ancak framework
kullanıcıları için desteklenen public API bu modül üzerinden sunulur.
"""

# =============================================================================
# REGISTRY
# =============================================================================
from enterprise_crawler.plugins.registry import (
    PluginRegistry,
    RegisteredPlugin,
)


# =============================================================================
# MANAGER
# =============================================================================
from enterprise_crawler.plugins.manager import (
    PluginDisabledError,
    PluginInvocationError,
    PluginLifecycleError,
    PluginManager,
    PluginManagerClosedError,
    PluginManagerError,
)


# =============================================================================
# LOADER
# =============================================================================
from enterprise_crawler.plugins.loader import (
    LoadedPlugin,
    PluginLoadError,
    PluginLoader,
    load_and_register_plugin,
    load_plugin,
)


# =============================================================================
# DISCOVERY
# =============================================================================
from enterprise_crawler.plugins.discovery import (
    DEFAULT_PLUGIN_ENTRY_POINT_GROUP,
    DiscoveredPlugin,
    DuplicateDiscoveredPluginError,
    PluginDiscovery,
    PluginDiscoveryError,
    discover_plugins,
)


# =============================================================================
# AUTOLOAD
# =============================================================================
from enterprise_crawler.plugins.autoload import (
    AutoLoadedPlugin,
    PluginAutoLoader,
    PluginAutoLoadError,
    UnsupportedDiscoveredPluginError,
    discover_and_register_plugins,
)


# =============================================================================
# PUBLIC API
# =============================================================================
__all__ = [
    # -------------------------------------------------------------------------
    # Registry
    # -------------------------------------------------------------------------
    "PluginRegistry",
    "RegisteredPlugin",

    # -------------------------------------------------------------------------
    # Manager
    # -------------------------------------------------------------------------
    "PluginManager",
    "PluginManagerError",
    "PluginDisabledError",
    "PluginInvocationError",
    "PluginLifecycleError",
    "PluginManagerClosedError",

    # -------------------------------------------------------------------------
    # Loader
    # -------------------------------------------------------------------------
    "PluginLoader",
    "LoadedPlugin",
    "PluginLoadError",
    "load_plugin",
    "load_and_register_plugin",

    # -------------------------------------------------------------------------
    # Discovery
    # -------------------------------------------------------------------------
    "DEFAULT_PLUGIN_ENTRY_POINT_GROUP",
    "PluginDiscovery",
    "PluginDiscoveryError",
    "DuplicateDiscoveredPluginError",
    "DiscoveredPlugin",
    "discover_plugins",

    # -------------------------------------------------------------------------
    # Autoload
    # -------------------------------------------------------------------------
    "PluginAutoLoader",
    "PluginAutoLoadError",
    "UnsupportedDiscoveredPluginError",
    "AutoLoadedPlugin",
    "discover_and_register_plugins",
]