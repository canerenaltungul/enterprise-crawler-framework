from __future__ import annotations

"""
Enterprise Crawler Framework - Plugin Discovery

Python package entry-point sistemi üzerinden framework plugin'lerini
deterministic ve side-effect üretmeden keşfeder.

Discovery yalnız plugin tanımlarını bulur.

Bu katman plugin'i:

- import etmez
- instantiate etmez
- register etmez
- lifecycle hook çalıştırmaz

Bu ayrım bilinçlidir.

Akış
----
installed distributions
    ↓
importlib.metadata.entry_points()
    ↓
PluginDiscovery
    ↓
DiscoveredPlugin
    ↓
PluginLoader / PluginManager   [sonraki katman]

Varsayılan entry-point group::

    enterprise_crawler.plugins

Örnek pyproject.toml::

    [project.entry-points."enterprise_crawler.plugins"]
    example = "example_package.plugin:ExamplePlugin"
"""

from dataclasses import dataclass
from importlib import metadata as importlib_metadata
from threading import RLock
from typing import Any, Callable, Optional


DEFAULT_PLUGIN_ENTRY_POINT_GROUP = (
    "enterprise_crawler.plugins"
)


# =============================================================================
# EXCEPTIONS
# =============================================================================
class PluginDiscoveryError(RuntimeError):
    """Plugin discovery temel hatası."""


class DuplicateDiscoveredPluginError(
    PluginDiscoveryError
):
    """Aynı isimle birden fazla plugin keşfedildiğinde."""


# =============================================================================
# HELPERS
# =============================================================================
def _normalize_non_empty_string(
    value: Any,
    *,
    field_name: str,
) -> str:
    if not isinstance(
        value,
        str,
    ):
        raise PluginDiscoveryError(
            f"{field_name} str olmalıdır."
        )

    normalized = value.strip()

    if not normalized:
        raise PluginDiscoveryError(
            f"{field_name} boş olamaz."
        )

    return normalized


def _normalize_plugin_key(
    value: str,
) -> str:
    return value.strip().casefold()


def _parse_entry_point_value(
    value: str,
) -> tuple[
    str,
    Optional[str],
]:
    """
    Entry-point target'ını parçalar.

    Örnek::

        package.module:PluginClass

    veya::

        package.module
    """

    normalized = (
        _normalize_non_empty_string(
            value,
            field_name=(
                "entry point value"
            ),
        )
    )

    if ":" not in normalized:
        return (
            normalized,
            None,
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
        raise PluginDiscoveryError(
            "Entry point module adı boş olamaz "
            f"| value={value!r}"
        )

    if not attribute_name:
        raise PluginDiscoveryError(
            "Entry point attribute adı boş olamaz "
            f"| value={value!r}"
        )

    return (
        module_name,
        attribute_name,
    )


# =============================================================================
# DISCOVERED PLUGIN
# =============================================================================
@dataclass(
    frozen=True,
    slots=True,
)
class DiscoveredPlugin:
    """
    Side-effect üretmeden keşfedilmiş plugin descriptor.

    ``entry_point`` ham importlib.metadata EntryPoint nesnesidir.
    """

    name: str
    value: str
    group: str

    module: str
    attribute: Optional[str]

    entry_point: Any

    def __post_init__(
        self,
    ) -> None:
        normalized_name = (
            _normalize_non_empty_string(
                self.name,
                field_name="name",
            )
        )

        normalized_value = (
            _normalize_non_empty_string(
                self.value,
                field_name="value",
            )
        )

        normalized_group = (
            _normalize_non_empty_string(
                self.group,
                field_name="group",
            )
        )

        normalized_module = (
            _normalize_non_empty_string(
                self.module,
                field_name="module",
            )
        )

        normalized_attribute = (
            self.attribute
        )

        if normalized_attribute is not None:
            normalized_attribute = (
                _normalize_non_empty_string(
                    normalized_attribute,
                    field_name="attribute",
                )
            )

        object.__setattr__(
            self,
            "name",
            normalized_name,
        )

        object.__setattr__(
            self,
            "value",
            normalized_value,
        )

        object.__setattr__(
            self,
            "group",
            normalized_group,
        )

        object.__setattr__(
            self,
            "module",
            normalized_module,
        )

        object.__setattr__(
            self,
            "attribute",
            normalized_attribute,
        )

    @property
    def key(
        self,
    ) -> str:
        return (
            _normalize_plugin_key(
                self.name
            )
        )

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "name": self.name,
            "value": self.value,
            "group": self.group,
            "module": self.module,
            "attribute": (
                self.attribute
            ),
        }


# =============================================================================
# DISCOVERY
# =============================================================================
class PluginDiscovery:
    """
    Installed Python distributions içinden framework plugin entry-point'lerini
    keşfeder.

    ``entry_points_provider`` dependency injection amacıyla vardır.
    Normal kullanımda ``importlib.metadata.entry_points`` kullanılır.
    """

    def __init__(
        self,
        *,
        group: str = (
            DEFAULT_PLUGIN_ENTRY_POINT_GROUP
        ),
        entry_points_provider: Optional[
            Callable[[], Any]
        ] = None,
    ) -> None:
        self.group = (
            _normalize_non_empty_string(
                group,
                field_name="group",
            )
        )

        if (
            entry_points_provider
            is not None
            and not callable(
                entry_points_provider
            )
        ):
            raise TypeError(
                "entry_points_provider callable "
                "olmalıdır."
            )

        self._entry_points_provider = (
            entry_points_provider
            or importlib_metadata.entry_points
        )

        self._state_lock = (
            RLock()
        )

        self._discover_count = 0

        self._last_result: tuple[
            DiscoveredPlugin,
            ...
        ] = ()

    # =========================================================================
    # STATE
    # =========================================================================
    @property
    def discover_count(
        self,
    ) -> int:
        with self._state_lock:
            return (
                self._discover_count
            )

    @property
    def last_result(
        self,
    ) -> tuple[
        DiscoveredPlugin,
        ...
    ]:
        with self._state_lock:
            return tuple(
                self._last_result
            )

    # =========================================================================
    # ENTRY POINT NORMALIZATION
    # =========================================================================
    def _select_entry_points(
        self,
        raw_entry_points: Any,
    ) -> tuple[Any, ...]:
        """
        Provider çıktısından yalnız configured group'a ait entry-point'leri
        seçer.

        Desteklenen provider biçimleri:

        - modern EntryPoints.select(group=...)
        - legacy mapping[group]
        - generic iterable

        Generic iterable içindeki malformed üyeler sessizce atlanmaz.
        None veya entry-point benzeri contract taşımayan nesne fail-closed
        davranışla reddedilir.
        """

        if raw_entry_points is None:
            return ()

        # ---------------------------------------------------------------------
        # MODERN importlib.metadata EntryPoints API
        # ---------------------------------------------------------------------
        selector = getattr(
            raw_entry_points,
            "select",
            None,
        )

        if callable(
            selector
        ):
            try:
                selected = selector(
                    group=self.group
                )

            except Exception as exc:
                raise PluginDiscoveryError(
                    "Entry-point group selection başarısız "
                    f"| group={self.group} "
                    f"| error={exc}"
                ) from exc

            try:
                return tuple(
                    selected
                )

            except TypeError as exc:
                raise PluginDiscoveryError(
                    "Entry-point select() iterable "
                    "döndürmelidir "
                    f"| group={self.group}"
                ) from exc

        # ---------------------------------------------------------------------
        # LEGACY MAPPING API
        # ---------------------------------------------------------------------
        if isinstance(
            raw_entry_points,
            dict,
        ):
            selected = (
                raw_entry_points.get(
                    self.group,
                    (),
                )
            )

            try:
                return tuple(
                    selected
                )

            except TypeError as exc:
                raise PluginDiscoveryError(
                    "Entry-point mapping group değeri "
                    "iterable olmalıdır "
                    f"| group={self.group}"
                ) from exc

        # ---------------------------------------------------------------------
        # GENERIC ITERABLE
        # ---------------------------------------------------------------------
        try:
            iterable = tuple(
                raw_entry_points
            )

        except TypeError as exc:
            raise PluginDiscoveryError(
                "entry_points provider iterable, mapping "
                "veya select() destekleyen nesne "
                "döndürmelidir."
            ) from exc

        selected: list[Any] = []

        for index, entry_point in enumerate(
            iterable
        ):
            # Malformed member'ı unrelated group gibi sessizce
            # görmezden gelmek güvenli değildir.
            if entry_point is None:
                raise PluginDiscoveryError(
                    "Entry point None olamaz "
                    f"| index={index}"
                )

            if not hasattr(
                entry_point,
                "name",
            ):
                raise PluginDiscoveryError(
                    "Entry point name alanına sahip olmalıdır "
                    f"| index={index} "
                    f"| actual={type(entry_point).__name__}"
                )

            if not hasattr(
                entry_point,
                "value",
            ):
                raise PluginDiscoveryError(
                    "Entry point value alanına sahip olmalıdır "
                    f"| index={index} "
                    f"| actual={type(entry_point).__name__}"
                )

            if not hasattr(
                entry_point,
                "group",
            ):
                raise PluginDiscoveryError(
                    "Entry point group alanına sahip olmalıdır "
                    f"| index={index} "
                    f"| actual={type(entry_point).__name__}"
                )

            entry_group = getattr(
                entry_point,
                "group",
            )

            if (
                entry_group
                == self.group
            ):
                selected.append(
                    entry_point
                )

        return tuple(
            selected
        )

    def _descriptor_from_entry_point(
        self,
        entry_point: Any,
    ) -> DiscoveredPlugin:
        if entry_point is None:
            raise PluginDiscoveryError(
                "Entry point None olamaz."
            )

        name = (
            _normalize_non_empty_string(
                getattr(
                    entry_point,
                    "name",
                    None,
                ),
                field_name=(
                    "entry point name"
                ),
            )
        )

        value = (
            _normalize_non_empty_string(
                getattr(
                    entry_point,
                    "value",
                    None,
                ),
                field_name=(
                    "entry point value"
                ),
            )
        )

        group = getattr(
            entry_point,
            "group",
            self.group,
        )

        group = (
            _normalize_non_empty_string(
                group,
                field_name=(
                    "entry point group"
                ),
            )
        )

        if group != self.group:
            raise PluginDiscoveryError(
                "Entry point yanlış group içinde "
                f"| expected={self.group!r} "
                f"| actual={group!r} "
                f"| plugin={name!r}"
            )

        module_name, attribute_name = (
            _parse_entry_point_value(
                value
            )
        )

        return DiscoveredPlugin(
            name=name,
            value=value,
            group=group,
            module=module_name,
            attribute=attribute_name,
            entry_point=entry_point,
        )

    # =========================================================================
    # DISCOVER
    # =========================================================================
    def discover(
        self,
    ) -> tuple[
        DiscoveredPlugin,
        ...
    ]:
        """
        Plugin entry-point'lerini keşfeder.

        Sonuç plugin adına göre case-insensitive deterministic sıralanır.

        Aynı normalized isim iki kez görülürse fail-closed davranılır.
        """

        try:
            raw_entry_points = (
                self._entry_points_provider()
            )

        except Exception as exc:
            raise PluginDiscoveryError(
                "Entry-point provider başarısız "
                f"| group={self.group} "
                f"| error={exc}"
            ) from exc

        selected = (
            self._select_entry_points(
                raw_entry_points
            )
        )

        discovered: dict[
            str,
            DiscoveredPlugin,
        ] = {}

        for entry_point in selected:
            descriptor = (
                self._descriptor_from_entry_point(
                    entry_point
                )
            )

            key = descriptor.key

            if key in discovered:
                existing = (
                    discovered[
                        key
                    ]
                )

                raise (
                    DuplicateDiscoveredPluginError(
                        "Duplicate plugin entry point "
                        f"| name={descriptor.name!r} "
                        f"| first={existing.value!r} "
                        f"| second={descriptor.value!r}"
                    )
                )

            discovered[
                key
            ] = descriptor

        result = tuple(
            sorted(
                discovered.values(),
                key=lambda item: (
                    item.name.casefold(),
                    item.name,
                    item.value,
                ),
            )
        )

        with self._state_lock:
            self._discover_count += 1

            self._last_result = (
                result
            )

        return result

    # =========================================================================
    # LOOKUP
    # =========================================================================
    def find(
        self,
        name: str,
    ) -> Optional[
        DiscoveredPlugin
    ]:
        normalized_name = (
            _normalize_non_empty_string(
                name,
                field_name="name",
            )
        )

        lookup_key = (
            _normalize_plugin_key(
                normalized_name
            )
        )

        for plugin in (
            self.discover()
        ):
            if plugin.key == lookup_key:
                return plugin

        return None

    def require(
        self,
        name: str,
    ) -> DiscoveredPlugin:
        plugin = self.find(
            name
        )

        if plugin is None:
            raise PluginDiscoveryError(
                "Plugin entry point bulunamadı "
                f"| name={name!r} "
                f"| group={self.group!r}"
            )

        return plugin

    # =========================================================================
    # SNAPSHOT
    # =========================================================================
    def snapshot(
        self,
    ) -> dict[str, Any]:
        with self._state_lock:
            return {
                "group": (
                    self.group
                ),
                "discover_count": (
                    self._discover_count
                ),
                "last_plugin_count": (
                    len(
                        self._last_result
                    )
                ),
                "last_plugins": [
                    plugin.to_dict()
                    for plugin
                    in self._last_result
                ],
            }

    # =========================================================================
    # REPRESENTATION
    # =========================================================================
    def __repr__(
        self,
    ) -> str:
        return (
            f"{self.__class__.__name__}("
            f"group={self.group!r}, "
            f"discover_count="
            f"{self.discover_count}, "
            f"last_plugin_count="
            f"{len(self.last_result)}"
            f")"
        )


# =============================================================================
# CONVENIENCE API
# =============================================================================
def discover_plugins(
    *,
    group: str = (
        DEFAULT_PLUGIN_ENTRY_POINT_GROUP
    ),
) -> tuple[
    DiscoveredPlugin,
    ...
]:
    """
    Default importlib.metadata provider ile plugin discovery helper.
    """

    return PluginDiscovery(
        group=group
    ).discover()