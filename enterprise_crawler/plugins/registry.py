from __future__ import annotations

"""
Enterprise Crawler Framework - Plugin Registry

PluginRegistry, framework içindeki plugin nesnelerini güvenli ve deterministik
biçimde kaydetmek için kullanılan düşük seviyeli registry katmanıdır.

Bu modül bilinçli olarak şunları YAPMAZ:

- dynamic import
- setuptools entry-point discovery
- plugin lifecycle yönetimi
- dependency resolution
- plugin execution
- plugin enable/disable policy

Bunlar daha sonra PluginManager katmanına eklenecektir.

Registry'nin sorumlulukları:

- plugin sözleşmesini doğrulamak
- duplicate isimleri engellemek
- case-insensitive lookup sağlamak
- register / get / unregister işlemlerini yürütmek
- deterministic snapshot üretmek
"""

from copy import deepcopy
import threading
from typing import Any, Optional

from enterprise_crawler.contracts import PluginInfo
from enterprise_crawler.exceptions.plugin import (
    PluginError,
    PluginRegistrationError,
    PluginValidationError,
)


# =============================================================================
# INTERNAL SENTINELS
# =============================================================================
_MISSING = object()


# =============================================================================
# HELPERS
# =============================================================================
def _normalize_plugin_name(
    value: Any,
) -> str:
    if not isinstance(
        value,
        str,
    ):
        raise PluginValidationError(
            "Plugin adı string olmalıdır."
        )

    normalized = value.strip()

    if not normalized:
        raise PluginValidationError(
            "Plugin adı boş olamaz."
        )

    return normalized


def _plugin_lookup_key(
    value: Any,
) -> str:
    return (
        _normalize_plugin_name(
            value
        ).casefold()
    )


def _normalize_required_string(
    value: Any,
    *,
    field_name: str,
) -> str:
    if not isinstance(
        value,
        str,
    ):
        raise PluginValidationError(
            f"PluginInfo.{field_name} "
            "string olmalıdır."
        )

    normalized = value.strip()

    if not normalized:
        raise PluginValidationError(
            f"PluginInfo.{field_name} "
            "boş olamaz."
        )

    return normalized


def _normalize_optional_string(
    value: Any,
    *,
    field_name: str,
) -> str:
    if not isinstance(
        value,
        str,
    ):
        raise PluginValidationError(
            f"PluginInfo.{field_name} "
            "string olmalıdır."
        )

    return value.strip()


def _copy_metadata(
    value: Any,
) -> dict[str, Any]:
    if not isinstance(
        value,
        dict,
    ):
        raise PluginValidationError(
            "PluginInfo.metadata dict "
            "olmalıdır."
        )

    try:
        copied = deepcopy(
            value
        )

    except Exception as exc:
        raise PluginValidationError(
            "PluginInfo.metadata güvenli "
            "biçimde kopyalanamadı."
        ) from exc

    return copied


def _normalize_plugin_info(
    info: Any,
) -> PluginInfo:
    if not isinstance(
        info,
        PluginInfo,
    ):
        raise PluginValidationError(
            "Plugin bilgisi PluginInfo "
            "olmalıdır."
        )

    return PluginInfo(
        name=(
            _normalize_required_string(
                info.name,
                field_name="name",
            )
        ),
        version=(
            _normalize_required_string(
                info.version,
                field_name="version",
            )
        ),
        author=(
            _normalize_optional_string(
                info.author,
                field_name="author",
            )
        ),
        description=(
            _normalize_optional_string(
                info.description,
                field_name="description",
            )
        ),
        metadata=(
            _copy_metadata(
                info.metadata
            )
        ),
    )


def _plugin_info_to_dict(
    info: PluginInfo,
) -> dict[str, Any]:
    return {
        "name": info.name,
        "version": info.version,
        "author": info.author,
        "description": (
            info.description
        ),
        "metadata": (
            _copy_metadata(
                info.metadata
            )
        ),
    }


# =============================================================================
# REGISTERED PLUGIN
# =============================================================================
class RegisteredPlugin:
    """
    Registry içinde saklanan immutable-benzeri plugin kaydı.

    Plugin nesnesinin kendisi değiştirilmez. PluginInfo ise registry'ye
    kaydedilirken normalize edilip bağımsız kopya olarak tutulur.
    """

    __slots__ = (
        "_plugin",
        "_info",
    )

    def __init__(
        self,
        *,
        plugin: Any,
        info: PluginInfo,
    ) -> None:
        if plugin is None:
            raise PluginValidationError(
                "Plugin nesnesi None olamaz."
            )

        self._plugin = plugin
        self._info = (
            _normalize_plugin_info(
                info
            )
        )

    @property
    def plugin(
        self,
    ) -> Any:
        return self._plugin

    @property
    def info(
        self,
    ) -> PluginInfo:
        return PluginInfo(
            name=self._info.name,
            version=self._info.version,
            author=self._info.author,
            description=(
                self._info.description
            ),
            metadata=(
                _copy_metadata(
                    self._info.metadata
                )
            ),
        )

    @property
    def name(
        self,
    ) -> str:
        return self._info.name

    @property
    def version(
        self,
    ) -> str:
        return self._info.version

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return (
            _plugin_info_to_dict(
                self._info
            )
        )

    def __repr__(
        self,
    ) -> str:
        return (
            "RegisteredPlugin("
            f"name={self.name!r}, "
            f"version={self.version!r}, "
            f"plugin_type="
            f"{type(self._plugin).__name__!r}"
            ")"
        )


# =============================================================================
# PLUGIN REGISTRY
# =============================================================================
class PluginRegistry:
    """
    Thread-safe plugin registry.

    Plugin isimleri lookup ve duplicate detection sırasında case-insensitive
    değerlendirilir.

    Örnek::

        class MyPlugin:
            plugin_info = PluginInfo(
                name="example",
                version="1.0.0",
            )

        registry = PluginRegistry()
        registry.register(
            MyPlugin()
        )

        plugin = registry.get(
            "example"
        )
    """

    def __init__(
        self,
    ) -> None:
        self._lock = (
            threading.RLock()
        )

        self._plugins: dict[
            str,
            RegisteredPlugin,
        ] = {}

    # =========================================================================
    # INFO RESOLUTION
    # =========================================================================
    @staticmethod
    def _resolve_plugin_info(
        plugin: Any,
        explicit_info: Optional[
            PluginInfo
        ],
    ) -> PluginInfo:
        if plugin is None:
            raise PluginValidationError(
                "Plugin nesnesi None olamaz."
            )

        if explicit_info is not None:
            return (
                _normalize_plugin_info(
                    explicit_info
                )
            )

        raw_info = getattr(
            plugin,
            "plugin_info",
            _MISSING,
        )

        if raw_info is _MISSING:
            raise PluginValidationError(
                "Plugin PluginInfo sağlamıyor. "
                "Plugin nesnesinde plugin_info "
                "alanı tanımla veya register(..., "
                "info=PluginInfo(...)) kullan."
            )

        if callable(
            raw_info
        ):
            try:
                raw_info = (
                    raw_info()
                )

            except Exception as exc:
                raise PluginValidationError(
                    "Plugin plugin_info() "
                    "çağrısı başarısız."
                ) from exc

        return (
            _normalize_plugin_info(
                raw_info
            )
        )

    # =========================================================================
    # REGISTER
    # =========================================================================
    def register(
        self,
        plugin: Any,
        *,
        info: Optional[
            PluginInfo
        ] = None,
    ) -> PluginInfo:
        """
        Plugin'i registry'ye kaydeder.

        Duplicate plugin isimleri, harf büyüklüğünden bağımsız biçimde
        reddedilir.
        """

        resolved_info = (
            self._resolve_plugin_info(
                plugin,
                info,
            )
        )

        lookup_key = (
            _plugin_lookup_key(
                resolved_info.name
            )
        )

        record = RegisteredPlugin(
            plugin=plugin,
            info=resolved_info,
        )

        with self._lock:
            existing = (
                self._plugins.get(
                    lookup_key
                )
            )

            if existing is not None:
                raise PluginRegistrationError(
                    "Plugin zaten kayıtlı "
                    f"| requested="
                    f"{resolved_info.name!r} "
                    f"| existing="
                    f"{existing.name!r}"
                )

            self._plugins[
                lookup_key
            ] = record

        return record.info

    # =========================================================================
    # LOOKUP
    # =========================================================================
    def contains(
        self,
        name: str,
    ) -> bool:
        lookup_key = (
            _plugin_lookup_key(
                name
            )
        )

        with self._lock:
            return (
                lookup_key
                in self._plugins
            )

    def get(
        self,
        name: str,
    ) -> Any:
        lookup_key = (
            _plugin_lookup_key(
                name
            )
        )

        with self._lock:
            record = (
                self._plugins.get(
                    lookup_key
                )
            )

        if record is None:
            raise PluginError(
                "Plugin bulunamadı "
                f"| name={name!r}"
            )

        return record.plugin

    def get_info(
        self,
        name: str,
    ) -> PluginInfo:
        lookup_key = (
            _plugin_lookup_key(
                name
            )
        )

        with self._lock:
            record = (
                self._plugins.get(
                    lookup_key
                )
            )

        if record is None:
            raise PluginError(
                "Plugin bulunamadı "
                f"| name={name!r}"
            )

        return record.info

    def get_registration(
        self,
        name: str,
    ) -> RegisteredPlugin:
        lookup_key = (
            _plugin_lookup_key(
                name
            )
        )

        with self._lock:
            record = (
                self._plugins.get(
                    lookup_key
                )
            )

        if record is None:
            raise PluginError(
                "Plugin bulunamadı "
                f"| name={name!r}"
            )

        return record

    # =========================================================================
    # UNREGISTER
    # =========================================================================
    def unregister(
        self,
        name: str,
    ) -> Any:
        lookup_key = (
            _plugin_lookup_key(
                name
            )
        )

        with self._lock:
            record = (
                self._plugins.pop(
                    lookup_key,
                    None,
                )
            )

        if record is None:
            raise PluginRegistrationError(
                "Kayıtlı plugin bulunamadı "
                f"| name={name!r}"
            )

        return record.plugin

    # =========================================================================
    # ENUMERATION
    # =========================================================================
    def names(
        self,
    ) -> tuple[str, ...]:
        with self._lock:
            records = list(
                self._plugins.values()
            )

        ordered = sorted(
            records,
            key=lambda item: (
                item.name.casefold(),
                item.name,
            ),
        )

        return tuple(
            item.name
            for item in ordered
        )

    def registrations(
        self,
    ) -> tuple[
        RegisteredPlugin,
        ...,
    ]:
        with self._lock:
            records = list(
                self._plugins.values()
            )

        records.sort(
            key=lambda item: (
                item.name.casefold(),
                item.name,
            )
        )

        return tuple(
            records
        )

    # =========================================================================
    # SNAPSHOT
    # =========================================================================
    def snapshot(
        self,
    ) -> dict[str, Any]:
        registrations = (
            self.registrations()
        )

        return {
            "plugin_count": len(
                registrations
            ),
            "plugins": [
                registration.to_dict()
                for registration
                in registrations
            ],
        }

    # =========================================================================
    # COLLECTION PROTOCOL
    # =========================================================================
    def __contains__(
        self,
        name: object,
    ) -> bool:
        if not isinstance(
            name,
            str,
        ):
            return False

        try:
            return self.contains(
                name
            )

        except PluginValidationError:
            return False

    def __len__(
        self,
    ) -> int:
        with self._lock:
            return len(
                self._plugins
            )

    # =========================================================================
    # REPRESENTATION
    # =========================================================================
    def __repr__(
        self,
    ) -> str:
        return (
            "PluginRegistry("
            f"plugin_count={len(self)}"
            ")"
        )