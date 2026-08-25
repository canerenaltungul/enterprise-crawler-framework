from __future__ import annotations

"""
Enterprise Crawler Framework - Plugin Loader

String tabanlı plugin referanslarını güvenli ve deterministik biçimde
Python objelerine dönüştürür.

Desteklenen referans biçimi::

    package.module:attribute

Örnekler::

    my_plugins.audit:AuditPlugin

    company.crawlers.plugins:plugin_instance

    acme.plugins.security:SecurityPlugin

Loader'ın görevi yalnızca discovery/loading sınırını yönetmektir.

Akış::

    import reference
        ↓
    module import
        ↓
    attribute resolve
        ↓
    optional class instantiation
        ↓
    PluginInfo validation
        ↓
    optional PluginManager registration

Bu modül filesystem scanning veya packaging entry-point discovery yapmaz.
Bunlar daha üst seviye discovery katmanlarının sorumluluğudur.
"""

from dataclasses import dataclass
from importlib import import_module
from inspect import isclass
from typing import Any, Optional

from enterprise_crawler.contracts.plugin import PluginInfo
from enterprise_crawler.exceptions import (
    PluginError,
)
from enterprise_crawler.plugins.manager import (
    PluginManager,
)


# =============================================================================
# EXCEPTIONS
# =============================================================================
class PluginLoadError(
    PluginError
):
    """
    Plugin import/resolve/instantiate işlemleri başarısız olduğunda üretilir.
    """


# =============================================================================
# RESULT CONTRACT
# =============================================================================
@dataclass(
    slots=True,
    frozen=True,
)
class LoadedPlugin:
    """
    Resolve edilmiş plugin objesini ve kaynak referansını taşır.
    """

    reference: str

    module_name: str

    attribute_name: str

    plugin: Any

    info: PluginInfo

    instantiated: bool

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "reference": (
                self.reference
            ),
            "module_name": (
                self.module_name
            ),
            "attribute_name": (
                self.attribute_name
            ),
            "plugin_name": (
                self.info.name
            ),
            "plugin_version": (
                self.info.version
            ),
            "instantiated": (
                self.instantiated
            ),
            "plugin_type": (
                type(
                    self.plugin
                ).__name__
            ),
        }


# =============================================================================
# HELPERS
# =============================================================================
def _normalize_reference(
    reference: Any,
) -> str:
    if not isinstance(
        reference,
        str,
    ):
        raise PluginLoadError(
            "Plugin reference string olmalıdır."
        )

    normalized = (
        reference.strip()
    )

    if not normalized:
        raise PluginLoadError(
            "Plugin reference boş olamaz."
        )

    if normalized.count(
        ":"
    ) != 1:
        raise PluginLoadError(
            "Plugin reference "
            "'package.module:attribute' "
            "biçiminde olmalıdır."
        )

    module_name, attribute_name = (
        normalized.split(
            ":",
            1,
        )
    )

    module_name = (
        module_name.strip()
    )

    attribute_name = (
        attribute_name.strip()
    )

    if not module_name:
        raise PluginLoadError(
            "Plugin module adı boş olamaz."
        )

    if not attribute_name:
        raise PluginLoadError(
            "Plugin attribute adı boş olamaz."
        )

    return (
        f"{module_name}:"
        f"{attribute_name}"
    )


def _split_reference(
    reference: str,
) -> tuple[
    str,
    str,
]:
    module_name, attribute_name = (
        reference.split(
            ":",
            1,
        )
    )

    return (
        module_name,
        attribute_name,
    )


def _resolve_plugin_info(
    plugin: Any,
) -> PluginInfo:
    """
    Plugin objesinden PluginInfo sözleşmesini çıkarır.

    Desteklenen kaynaklar::

        plugin.plugin_info

    veya::

        plugin.plugin_info()

    veya::

        plugin.PLUGIN_INFO
    """

    candidates = (
        "plugin_info",
        "PLUGIN_INFO",
    )

    found = False

    value: Any = None

    for name in candidates:
        if not hasattr(
            plugin,
            name,
        ):
            continue

        found = True

        value = getattr(
            plugin,
            name,
        )

        if callable(
            value
        ):
            try:
                value = value()

            except Exception as exc:
                raise PluginLoadError(
                    "PluginInfo sağlayıcısı "
                    "çalıştırılamadı "
                    f"| attribute={name} "
                    f"| error={exc}"
                ) from exc

        break

    if not found:
        raise PluginLoadError(
            "Plugin PluginInfo sağlamıyor. "
            "plugin_info veya PLUGIN_INFO "
            "tanımlanmalıdır."
        )

    if not isinstance(
        value,
        PluginInfo,
    ):
        raise PluginLoadError(
            "Plugin info contract geçersiz "
            f"| actual="
            f"{type(value).__name__}"
        )

    name = value.name

    version = value.version

    if not isinstance(
        name,
        str,
    ):
        raise PluginLoadError(
            "PluginInfo.name string olmalıdır."
        )

    if not isinstance(
        version,
        str,
    ):
        raise PluginLoadError(
            "PluginInfo.version string olmalıdır."
        )

    normalized_name = (
        name.strip()
    )

    normalized_version = (
        version.strip()
    )

    if not normalized_name:
        raise PluginLoadError(
            "PluginInfo.name boş olamaz."
        )

    if not normalized_version:
        raise PluginLoadError(
            "PluginInfo.version boş olamaz."
        )

    if not isinstance(
        value.author,
        str,
    ):
        raise PluginLoadError(
            "PluginInfo.author string olmalıdır."
        )

    if not isinstance(
        value.description,
        str,
    ):
        raise PluginLoadError(
            "PluginInfo.description string olmalıdır."
        )

    if not isinstance(
        value.metadata,
        dict,
    ):
        raise PluginLoadError(
            "PluginInfo.metadata dict olmalıdır."
        )

    return PluginInfo(
        name=normalized_name,
        version=normalized_version,
        author=(
            value.author.strip()
        ),
        description=(
            value.description.strip()
        ),
        metadata=dict(
            value.metadata
        ),
    )


# =============================================================================
# LOADER
# =============================================================================
class PluginLoader:
    """
    Python import reference tabanlı plugin loader.

    Varsayılan davranış class referanslarını instantiate etmektir::

        loader = PluginLoader()

        loaded = loader.load(
            "my_package.plugins:AuditPlugin"
        )

    Instance referansları doğrudan korunur.
    """

    def __init__(
        self,
        *,
        instantiate_classes: bool = True,
    ) -> None:
        if not isinstance(
            instantiate_classes,
            bool,
        ):
            raise TypeError(
                "instantiate_classes "
                "bool olmalıdır."
            )

        self.instantiate_classes = (
            instantiate_classes
        )

    # =========================================================================
    # MODULE IMPORT
    # =========================================================================
    def import_module(
        self,
        module_name: str,
    ) -> Any:
        if not isinstance(
            module_name,
            str,
        ):
            raise PluginLoadError(
                "module_name string olmalıdır."
            )

        normalized = (
            module_name.strip()
        )

        if not normalized:
            raise PluginLoadError(
                "module_name boş olamaz."
            )

        try:
            return import_module(
                normalized
            )

        except Exception as exc:
            raise PluginLoadError(
                "Plugin module import edilemedi "
                f"| module={normalized} "
                f"| error={exc}"
            ) from exc

    # =========================================================================
    # ATTRIBUTE RESOLUTION
    # =========================================================================
    def resolve_attribute(
        self,
        module: Any,
        attribute_name: str,
    ) -> Any:
        if module is None:
            raise PluginLoadError(
                "module None olamaz."
            )

        if not isinstance(
            attribute_name,
            str,
        ):
            raise PluginLoadError(
                "attribute_name string olmalıdır."
            )

        normalized = (
            attribute_name.strip()
        )

        if not normalized:
            raise PluginLoadError(
                "attribute_name boş olamaz."
            )

        if normalized.startswith(
            "_"
        ):
            raise PluginLoadError(
                "Private plugin attribute "
                "yüklenemez "
                f"| attribute={normalized}"
            )

        if not hasattr(
            module,
            normalized,
        ):
            raise PluginLoadError(
                "Plugin attribute bulunamadı "
                f"| module="
                f"{getattr(module, '__name__', module)!r} "
                f"| attribute={normalized}"
            )

        return getattr(
            module,
            normalized,
        )

    # =========================================================================
    # INSTANTIATION
    # =========================================================================
    def instantiate(
        self,
        target: Any,
    ) -> tuple[
        Any,
        bool,
    ]:
        if (
            isclass(
                target
            )
            and self.instantiate_classes
        ):
            try:
                instance = target()

            except Exception as exc:
                raise PluginLoadError(
                    "Plugin class instantiate "
                    "edilemedi "
                    f"| class="
                    f"{target.__module__}."
                    f"{target.__qualname__} "
                    f"| error={exc}"
                ) from exc

            return (
                instance,
                True,
            )

        return (
            target,
            False,
        )

    # =========================================================================
    # LOAD
    # =========================================================================
    def load(
        self,
        reference: str,
    ) -> LoadedPlugin:
        normalized_reference = (
            _normalize_reference(
                reference
            )
        )

        (
            module_name,
            attribute_name,
        ) = _split_reference(
            normalized_reference
        )

        module = (
            self.import_module(
                module_name
            )
        )

        target = (
            self.resolve_attribute(
                module,
                attribute_name,
            )
        )

        plugin, instantiated = (
            self.instantiate(
                target
            )
        )

        info = (
            _resolve_plugin_info(
                plugin
            )
        )

        return LoadedPlugin(
            reference=(
                normalized_reference
            ),
            module_name=(
                module_name
            ),
            attribute_name=(
                attribute_name
            ),
            plugin=plugin,
            info=info,
            instantiated=instantiated,
        )

    # =========================================================================
    # REGISTER
    # =========================================================================
    def load_and_register(
        self,
        reference: str,
        manager: PluginManager,
        *,
        enabled: bool = True,
    ) -> LoadedPlugin:
        if not isinstance(
            manager,
            PluginManager,
        ):
            raise TypeError(
                "manager PluginManager "
                "olmalıdır."
            )

        if not isinstance(
            enabled,
            bool,
        ):
            raise TypeError(
                "enabled bool olmalıdır."
            )

        loaded = (
            self.load(
                reference
            )
        )

        try:
            manager.register(
                loaded.plugin,
                info=loaded.info,
                enabled=enabled,
            )

        except Exception as exc:
            raise PluginLoadError(
                "Plugin manager registration "
                "başarısız "
                f"| reference="
                f"{loaded.reference} "
                f"| plugin="
                f"{loaded.info.name} "
                f"| error={exc}"
            ) from exc

        return loaded

    # =========================================================================
    # SNAPSHOT
    # =========================================================================
    def snapshot(
        self,
    ) -> dict[str, Any]:
        return {
            "instantiate_classes": (
                self.instantiate_classes
            ),
            "reference_format": (
                "package.module:attribute"
            ),
        }

    # =========================================================================
    # REPRESENTATION
    # =========================================================================
    def __repr__(
        self,
    ) -> str:
        return (
            "PluginLoader("
            f"instantiate_classes="
            f"{self.instantiate_classes!r}"
            ")"
        )


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================
def load_plugin(
    reference: str,
    *,
    instantiate_classes: bool = True,
) -> LoadedPlugin:
    return PluginLoader(
        instantiate_classes=(
            instantiate_classes
        )
    ).load(
        reference
    )


def load_and_register_plugin(
    reference: str,
    manager: PluginManager,
    *,
    enabled: bool = True,
    instantiate_classes: bool = True,
) -> LoadedPlugin:
    return PluginLoader(
        instantiate_classes=(
            instantiate_classes
        )
    ).load_and_register(
        reference,
        manager,
        enabled=enabled,
    )