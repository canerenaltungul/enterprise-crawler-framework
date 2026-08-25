from __future__ import annotations

"""
Enterprise Crawler Framework - Plugin Auto Loader

PluginDiscovery tarafından bulunan entry-point plugin descriptor'larını
mevcut PluginLoader ve PluginManager zincirine bağlayan orchestration
katmanıdır.

Akış
----
PluginDiscovery
    ↓
DiscoveredPlugin
    ↓
PluginAutoLoader
    ↓
PluginLoader
    ↓
PluginManager
    ↓
plugin lifecycle

Sorumluluk ayrımı
-----------------
PluginDiscovery:
    Plugin entry-point'lerini bulur. Import veya registration yapmaz.

PluginLoader:
    ``package.module:attribute`` referansını import eder, resolve eder,
    gerekiyorsa class instantiate eder ve PluginInfo sözleşmesini doğrular.

PluginManager:
    Plugin registration, enable/disable, invocation ve lifecycle yönetir.

PluginAutoLoader:
    Yukarıdaki üç bileşeni deterministic ve fail-closed biçimde birbirine
    bağlar.

Önemli
------
PluginLoader yalnız ``module:attribute`` biçimini destekler.

PluginDiscovery module-only entry-point descriptor üretebildiği için
autoload katmanı attribute bulunmayan descriptor'ları açıkça reddeder.
Sessiz fallback yapılmaz.
"""

from dataclasses import dataclass
from threading import RLock
from typing import Any, Iterable, Optional

from enterprise_crawler.plugins.discovery import (
    DiscoveredPlugin,
    PluginDiscovery,
    PluginDiscoveryError,
)
from enterprise_crawler.plugins.loader import (
    LoadedPlugin,
    PluginLoadError,
    PluginLoader,
)
from enterprise_crawler.plugins.manager import (
    PluginManager,
)


# =============================================================================
# EXCEPTIONS
# =============================================================================
class PluginAutoLoadError(RuntimeError):
    """
    Plugin discovery → load → registration orchestration hatası.
    """


class UnsupportedDiscoveredPluginError(
    PluginAutoLoadError
):
    """
    Discovery sonucu mevcut PluginLoader sözleşmesine dönüştürülemediğinde.
    """


# =============================================================================
# RESULT CONTRACT
# =============================================================================
@dataclass(
    frozen=True,
    slots=True,
)
class AutoLoadedPlugin:
    """
    Discovery descriptor ile LoadedPlugin sonucunu birlikte taşır.
    """

    discovered: DiscoveredPlugin
    loaded: LoadedPlugin
    enabled: bool

    @property
    def name(
        self,
    ) -> str:
        return self.loaded.info.name

    @property
    def version(
        self,
    ) -> str:
        return self.loaded.info.version

    @property
    def reference(
        self,
    ) -> str:
        return self.loaded.reference

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "discovered": (
                self.discovered.to_dict()
            ),
            "loaded": (
                self.loaded.to_dict()
            ),
            "enabled": (
                self.enabled
            ),
        }


# =============================================================================
# HELPERS
# =============================================================================
def _validate_discovered_plugin(
    plugin: Any,
) -> DiscoveredPlugin:
    if not isinstance(
        plugin,
        DiscoveredPlugin,
    ):
        raise TypeError(
            "plugin DiscoveredPlugin olmalıdır."
        )

    return plugin


def _reference_from_discovered(
    plugin: DiscoveredPlugin,
) -> str:
    """
    Discovery descriptor'ını mevcut PluginLoader referansına dönüştürür.

    PluginLoader contract::

        package.module:attribute

    Attribute bulunmayan descriptor fail-closed reddedilir.
    """

    if plugin.attribute is None:
        raise UnsupportedDiscoveredPluginError(
            "Discovered plugin module-only target kullanıyor; "
            "PluginLoader module:attribute referansı gerektiriyor "
            f"| plugin={plugin.name!r} "
            f"| value={plugin.value!r}"
        )

    module_name = (
        plugin.module.strip()
    )

    attribute_name = (
        plugin.attribute.strip()
    )

    if not module_name:
        raise UnsupportedDiscoveredPluginError(
            "Discovered plugin module adı boş "
            f"| plugin={plugin.name!r}"
        )

    if not attribute_name:
        raise UnsupportedDiscoveredPluginError(
            "Discovered plugin attribute adı boş "
            f"| plugin={plugin.name!r}"
        )

    return (
        f"{module_name}:"
        f"{attribute_name}"
    )


# =============================================================================
# AUTO LOADER
# =============================================================================
class PluginAutoLoader:
    """
    Discovery sonuçlarını PluginLoader + PluginManager zincirine bağlar.

    Default kullanım::

        manager = PluginManager()

        auto_loader = PluginAutoLoader(
            manager=manager
        )

        loaded = auto_loader.discover_and_register()

    Injection::

        auto_loader = PluginAutoLoader(
            manager=manager,
            discovery=my_discovery,
            loader=my_loader,
        )
    """

    def __init__(
        self,
        *,
        manager: PluginManager,
        discovery: Optional[
            PluginDiscovery
        ] = None,
        loader: Optional[
            PluginLoader
        ] = None,
    ) -> None:
        if not isinstance(
            manager,
            PluginManager,
        ):
            raise TypeError(
                "manager PluginManager olmalıdır."
            )

        if (
            discovery is not None
            and not isinstance(
                discovery,
                PluginDiscovery,
            )
        ):
            raise TypeError(
                "discovery PluginDiscovery olmalıdır."
            )

        if (
            loader is not None
            and not isinstance(
                loader,
                PluginLoader,
            )
        ):
            raise TypeError(
                "loader PluginLoader olmalıdır."
            )

        self.manager = (
            manager
        )

        self.discovery = (
            discovery
            if discovery is not None
            else PluginDiscovery()
        )

        self.loader = (
            loader
            if loader is not None
            else PluginLoader()
        )

        self._lock = (
            RLock()
        )

        self._load_count = 0

        self._last_result: tuple[
            AutoLoadedPlugin,
            ...
        ] = ()

    # =========================================================================
    # STATE
    # =========================================================================
    @property
    def load_count(
        self,
    ) -> int:
        with self._lock:
            return (
                self._load_count
            )

    @property
    def last_result(
        self,
    ) -> tuple[
        AutoLoadedPlugin,
        ...
    ]:
        with self._lock:
            return tuple(
                self._last_result
            )

    # =========================================================================
    # SINGLE PLUGIN
    # =========================================================================
    def load_discovered(
        self,
        plugin: DiscoveredPlugin,
        *,
        enabled: bool = True,
    ) -> AutoLoadedPlugin:
        """
        Tek bir DiscoveredPlugin'i yükleyip manager'a register eder.
        """

        discovered = (
            _validate_discovered_plugin(
                plugin
            )
        )

        if not isinstance(
            enabled,
            bool,
        ):
            raise TypeError(
                "enabled bool olmalıdır."
            )

        if self.manager.is_closed:
            raise PluginAutoLoadError(
                "PluginManager kapalı."
            )

        reference = (
            _reference_from_discovered(
                discovered
            )
        )

        try:
            loaded = (
                self.loader.load_and_register(
                    reference,
                    self.manager,
                    enabled=enabled,
                )
            )

        except PluginLoadError:
            raise

        except Exception as exc:
            raise PluginAutoLoadError(
                "Discovered plugin yüklenemedi "
                f"| discovery_name="
                f"{discovered.name!r} "
                f"| reference={reference!r} "
                f"| error={exc}"
            ) from exc

        result = AutoLoadedPlugin(
            discovered=discovered,
            loaded=loaded,
            enabled=enabled,
        )

        with self._lock:
            self._load_count += 1

        return result

    # =========================================================================
    # BATCH
    # =========================================================================
    def load_many(
        self,
        plugins: Iterable[
            DiscoveredPlugin
        ],
        *,
        enabled: bool = True,
    ) -> tuple[
        AutoLoadedPlugin,
        ...
    ]:
        """
        Verilen descriptor'ları sırayla yükler.

        Input order korunur.

        Bir plugin başarısız olursa fail-closed davranılır ve exception
        yükseltilir.

        Önceden başarıyla register edilmiş plugin'ler otomatik rollback
        edilmez. PluginManager lifecycle ownership nedeniyle batch-level
        transaction semantiği burada varsayılmaz.
        """

        if not isinstance(
            enabled,
            bool,
        ):
            raise TypeError(
                "enabled bool olmalıdır."
            )

        if isinstance(
            plugins,
            (
                str,
                bytes,
                bytearray,
            ),
        ):
            raise TypeError(
                "plugins DiscoveredPlugin iterable olmalıdır."
            )

        try:
            normalized_plugins = tuple(
                plugins
            )

        except TypeError as exc:
            raise TypeError(
                "plugins iterable olmalıdır."
            ) from exc

        results: list[
            AutoLoadedPlugin
        ] = []

        for plugin in (
            normalized_plugins
        ):
            results.append(
                self.load_discovered(
                    plugin,
                    enabled=enabled,
                )
            )

        result = tuple(
            results
        )

        with self._lock:
            self._last_result = (
                result
            )

        return result

    # =========================================================================
    # DISCOVER + REGISTER
    # =========================================================================
    def discover_and_register(
        self,
        *,
        enabled: bool = True,
    ) -> tuple[
        AutoLoadedPlugin,
        ...
    ]:
        """
        Configured PluginDiscovery ile tüm plugin'leri bulur ve register eder.
        """

        if not isinstance(
            enabled,
            bool,
        ):
            raise TypeError(
                "enabled bool olmalıdır."
            )

        try:
            discovered = (
                self.discovery.discover()
            )

        except PluginDiscoveryError:
            raise

        except Exception as exc:
            raise PluginAutoLoadError(
                "Plugin discovery başarısız "
                f"| error={exc}"
            ) from exc

        return self.load_many(
            discovered,
            enabled=enabled,
        )

    # =========================================================================
    # LOOKUP + REGISTER
    # =========================================================================
    def discover_and_register_one(
        self,
        name: str,
        *,
        enabled: bool = True,
    ) -> AutoLoadedPlugin:
        """
        Discovery üzerinden adı verilen tek plugin'i bulur ve register eder.
        """

        if not isinstance(
            enabled,
            bool,
        ):
            raise TypeError(
                "enabled bool olmalıdır."
            )

        discovered = (
            self.discovery.require(
                name
            )
        )

        result = (
            self.load_discovered(
                discovered,
                enabled=enabled,
            )
        )

        with self._lock:
            self._last_result = (
                result,
            )

        return result

    # =========================================================================
    # SNAPSHOT
    # =========================================================================
    def snapshot(
        self,
    ) -> dict[str, Any]:
        with self._lock:
            return {
                "load_count": (
                    self._load_count
                ),
                "last_plugin_count": (
                    len(
                        self._last_result
                    )
                ),
                "last_plugins": [
                    item.to_dict()
                    for item
                    in self._last_result
                ],
                "discovery": (
                    self.discovery.snapshot()
                ),
                "loader": (
                    self.loader.snapshot()
                ),
                "manager_closed": (
                    self.manager.is_closed
                ),
                "manager_plugin_count": (
                    self.manager.plugin_count
                ),
            }

    # =========================================================================
    # REPRESENTATION
    # =========================================================================
    def __repr__(
        self,
    ) -> str:
        return (
            f"{self.__class__.__name__}("
            f"load_count={self.load_count}, "
            f"last_plugin_count="
            f"{len(self.last_result)}, "
            f"manager_plugin_count="
            f"{self.manager.plugin_count}, "
            f"manager_closed="
            f"{self.manager.is_closed}"
            f")"
        )


# =============================================================================
# CONVENIENCE API
# =============================================================================
def discover_and_register_plugins(
    manager: PluginManager,
    *,
    enabled: bool = True,
    discovery: Optional[
        PluginDiscovery
    ] = None,
    loader: Optional[
        PluginLoader
    ] = None,
) -> tuple[
    AutoLoadedPlugin,
    ...
]:
    """
    One-shot discovery + load + registration helper.
    """

    return PluginAutoLoader(
        manager=manager,
        discovery=discovery,
        loader=loader,
    ).discover_and_register(
        enabled=enabled
    )