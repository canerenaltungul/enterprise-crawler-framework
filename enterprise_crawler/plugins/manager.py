from __future__ import annotations

"""
Enterprise Crawler Framework - Plugin Manager

PluginRegistry plugin kimliği ve kayıt işlemlerinden sorumludur.

PluginManager ise registry'nin üstünde çalışma zamanı davranışını yönetir:

    PluginManager
        │
        ├── PluginRegistry
        │     ├── register
        │     ├── get
        │     ├── get_info
        │     └── unregister
        │
        ├── enable / disable
        ├── optional lifecycle hooks
        ├── controlled invocation
        └── deterministic cleanup

Plugin lifecycle hook'ları bilinçli olarak opsiyoneldir.

Desteklenen hook'lar:

    on_load(manager)
    on_enable()
    on_disable()
    on_unload()

PluginInfo sözleşmesi lifecycle metodlarını zorunlu kılmadığı için plugin
nesneleri bu hook'ların hiçbirini implement etmek zorunda değildir.
"""

import threading
from dataclasses import dataclass
from typing import Any, Optional

from enterprise_crawler.contracts import PluginInfo
from enterprise_crawler.plugins.registry import PluginRegistry


# =============================================================================
# EXCEPTIONS
# =============================================================================
class PluginManagerError(RuntimeError):
    """
    PluginManager çalışma zamanı hatalarının temel sınıfı.
    """


class PluginDisabledError(PluginManagerError):
    """
    Disabled plugin üzerinde çalışma yapılmak istendiğinde üretilir.
    """


class PluginInvocationError(PluginManagerError):
    """
    Plugin metodunun çağrılması başarısız olduğunda üretilir.
    """


class PluginLifecycleError(PluginManagerError):
    """
    Plugin lifecycle hook'u başarısız olduğunda üretilir.
    """


class PluginManagerClosedError(PluginManagerError):
    """
    Kapatılmış PluginManager kullanılmaya çalışıldığında üretilir.
    """


# =============================================================================
# INTERNAL STATE
# =============================================================================
@dataclass(slots=True)
class _PluginRuntimeState:
    """
    PluginManager tarafından tutulan çalışma zamanı state'i.

    PluginRegistry metadata/identity kaynağı olmaya devam eder.
    """

    enabled: bool = True
    loaded: bool = False
    enable_count: int = 0
    disable_count: int = 0
    invocation_count: int = 0

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "loaded": self.loaded,
            "enable_count": self.enable_count,
            "disable_count": self.disable_count,
            "invocation_count": self.invocation_count,
        }


# =============================================================================
# HELPERS
# =============================================================================
def _normalize_plugin_name(
    name: str,
) -> str:
    if not isinstance(
        name,
        str,
    ):
        raise TypeError(
            "Plugin name str olmalıdır."
        )

    normalized = name.strip()

    if not normalized:
        raise ValueError(
            "Plugin name boş olamaz."
        )

    return normalized


def _normalized_key(
    name: str,
) -> str:
    return _normalize_plugin_name(
        name
    ).casefold()


def _safe_exception_message(
    error: BaseException,
) -> str:
    message = str(
        error
    ).strip()

    if not message:
        message = error.__class__.__name__

    return message[:8_000]


# =============================================================================
# PLUGIN MANAGER
# =============================================================================
class PluginManager:
    """
    PluginRegistry üzerinde runtime plugin yönetimi sağlar.

    Örnek::

        manager = PluginManager()

        manager.register(
            MyPlugin()
        )

        manager.invoke(
            "my-plugin",
            "collect",
        )

        manager.disable(
            "my-plugin"
        )

        manager.close()

    Lifecycle hook'ları opsiyoneldir.

    Bir plugin::

        class MyPlugin:
            plugin_info = PluginInfo(
                name="example",
                version="1.0.0",
            )

            def on_load(self, manager):
                ...

            def on_enable(self):
                ...

            def on_disable(self):
                ...

            def on_unload(self):
                ...

            def collect(self):
                return [...]
    """

    def __init__(
        self,
        *,
        registry: Optional[
            PluginRegistry
        ] = None,
    ) -> None:
        if (
            registry is not None
            and not isinstance(
                registry,
                PluginRegistry,
            )
        ):
            raise TypeError(
                "registry PluginRegistry olmalıdır."
            )

        if registry is None:
            self.registry = PluginRegistry()
            self._owns_registry = True

        else:
            self.registry = registry
            self._owns_registry = False

        self._lock = threading.RLock()

        self._runtime_states: dict[
            str,
            _PluginRuntimeState,
        ] = {}

        self._registration_order: list[
            str
        ] = []

        self._closed = False

    # =========================================================================
    # PUBLIC STATE
    # =========================================================================
    @property
    def is_closed(
        self,
    ) -> bool:
        with self._lock:
            return self._closed

    @property
    def plugin_count(
        self,
    ) -> int:
        with self._lock:
            return len(
                self._runtime_states
            )

    @property
    def enabled_count(
        self,
    ) -> int:
        with self._lock:
            return sum(
                1
                for state
                in self._runtime_states.values()
                if state.enabled
            )

    @property
    def disabled_count(
        self,
    ) -> int:
        with self._lock:
            return (
                len(
                    self._runtime_states
                )
                - sum(
                    1
                    for state
                    in self._runtime_states.values()
                    if state.enabled
                )
            )

    # =========================================================================
    # INTERNAL GUARDS
    # =========================================================================
    def _ensure_open(
        self,
    ) -> None:
        if self.is_closed:
            raise PluginManagerClosedError(
                "PluginManager kapalı."
            )

    def _require_runtime_state(
        self,
        name: str,
    ) -> _PluginRuntimeState:
        key = _normalized_key(
            name
        )

        with self._lock:
            try:
                return self._runtime_states[
                    key
                ]

            except KeyError as exc:
                raise PluginManagerError(
                    "Plugin manager state bulunamadı "
                    f"| plugin={name!r}"
                ) from exc

    # =========================================================================
    # OPTIONAL LIFECYCLE
    # =========================================================================
    @staticmethod
    def _optional_hook(
        plugin: Any,
        hook_name: str,
        *args: Any,
    ) -> bool:
        hook = getattr(
            plugin,
            hook_name,
            None,
        )

        if hook is None:
            return False

        if not callable(
            hook
        ):
            raise PluginLifecycleError(
                "Plugin lifecycle hook callable değil "
                f"| hook={hook_name}"
            )

        try:
            hook(
                *args
            )

        except Exception as exc:
            raise PluginLifecycleError(
                "Plugin lifecycle hook başarısız "
                f"| hook={hook_name} "
                f"| exception_type={exc.__class__.__name__} "
                f"| message={_safe_exception_message(exc)}"
            ) from exc

        return True

    # =========================================================================
    # REGISTRATION
    # =========================================================================
    def register(
        self,
        plugin: Any,
        *,
        info: Optional[
            PluginInfo
        ] = None,
        enabled: bool = True,
    ) -> Any:
        """
        Plugin'i registry'ye ve manager runtime'ına kaydeder.

        Sıra::

            registry.register()
                ↓
            runtime state
                ↓
            on_load(manager)
                ↓
            on_enable() [enabled=True]

        Lifecycle başarısız olursa kayıt rollback edilir.
        """

        self._ensure_open()

        if not isinstance(
            enabled,
            bool,
        ):
            raise TypeError(
                "enabled bool olmalıdır."
            )

        if info is None:
            registered = self.registry.register(
                plugin
            )

        else:
            if not isinstance(
                info,
                PluginInfo,
            ):
                raise TypeError(
                    "info PluginInfo olmalıdır."
                )

            registered = self.registry.register(
                plugin,
                info=info,
            )

        plugin_info = self.registry.get_info(
            self._resolve_registered_name(
                registered,
                plugin,
                info,
            )
        )

        plugin_name = plugin_info.name

        key = _normalized_key(
            plugin_name
        )

        state = _PluginRuntimeState(
            enabled=False,
            loaded=False,
        )

        with self._lock:
            self._runtime_states[
                key
            ] = state

            self._registration_order.append(
                key
            )

        try:
            self._optional_hook(
                plugin,
                "on_load",
                self,
            )

            with self._lock:
                state.loaded = True

            if enabled:
                self._optional_hook(
                    plugin,
                    "on_enable",
                )

                with self._lock:
                    state.enabled = True
                    state.enable_count += 1

            return plugin

        except Exception:
            with self._lock:
                self._runtime_states.pop(
                    key,
                    None,
                )

                try:
                    self._registration_order.remove(
                        key
                    )

                except ValueError:
                    pass

            try:
                self.registry.unregister(
                    plugin_name
                )

            except Exception:
                pass

            raise

    @staticmethod
    def _resolve_registered_name(
        registered: Any,
        plugin: Any,
        explicit_info: Optional[
            PluginInfo
        ],
    ) -> str:
        """
        Registry implementasyonunun register() dönüş tipine
        bağımlılığı azaltır.

        İsim çözüm sırası:

            explicit PluginInfo
            registered.info
            registered.name
            plugin.plugin_info
        """

        if explicit_info is not None:
            return explicit_info.name

        registered_info = getattr(
            registered,
            "info",
            None,
        )

        if isinstance(
            registered_info,
            PluginInfo,
        ):
            return registered_info.name

        registered_name = getattr(
            registered,
            "name",
            None,
        )

        if (
            isinstance(
                registered_name,
                str,
            )
            and registered_name.strip()
        ):
            return registered_name

        plugin_info = getattr(
            plugin,
            "plugin_info",
            None,
        )

        if callable(
            plugin_info
        ):
            plugin_info = plugin_info()

        if isinstance(
            plugin_info,
            PluginInfo,
        ):
            return plugin_info.name

        raise PluginManagerError(
            "Registered plugin adı çözümlenemedi."
        )

    # =========================================================================
    # LOOKUP
    # =========================================================================
    def get(
        self,
        name: str,
    ) -> Any:
        self._ensure_open()

        return self.registry.get(
            _normalize_plugin_name(
                name
            )
        )

    def get_info(
        self,
        name: str,
    ) -> PluginInfo:
        self._ensure_open()

        return self.registry.get_info(
            _normalize_plugin_name(
                name
            )
        )

    def contains(
        self,
        name: str,
    ) -> bool:
        self._ensure_open()

        if not isinstance(
            name,
            str,
        ):
            return False

        normalized = name.strip()

        if not normalized:
            return False

        return self.registry.contains(
            normalized
        )

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

        except PluginManagerClosedError:
            return False

    # =========================================================================
    # ENABLE / DISABLE
    # =========================================================================
    def is_enabled(
        self,
        name: str,
    ) -> bool:
        self._ensure_open()

        state = self._require_runtime_state(
            name
        )

        with self._lock:
            return state.enabled

    def enable(
        self,
        name: str,
    ) -> bool:
        """
        Plugin'i enable eder.

        Zaten enable ise False döner.
        State değişmeden önce on_enable çağrılır.
        """

        self._ensure_open()

        normalized = _normalize_plugin_name(
            name
        )

        plugin = self.registry.get(
            normalized
        )

        state = self._require_runtime_state(
            normalized
        )

        with self._lock:
            if state.enabled:
                return False

        self._optional_hook(
            plugin,
            "on_enable",
        )

        with self._lock:
            state.enabled = True
            state.enable_count += 1

        return True

    def disable(
        self,
        name: str,
    ) -> bool:
        """
        Plugin'i disable eder.

        Zaten disabled ise False döner.
        State değişmeden önce on_disable çağrılır.
        """

        self._ensure_open()

        normalized = _normalize_plugin_name(
            name
        )

        plugin = self.registry.get(
            normalized
        )

        state = self._require_runtime_state(
            normalized
        )

        with self._lock:
            if not state.enabled:
                return False

        self._optional_hook(
            plugin,
            "on_disable",
        )

        with self._lock:
            state.enabled = False
            state.disable_count += 1

        return True

    # =========================================================================
    # INVOCATION
    # =========================================================================
    def invoke(
        self,
        name: str,
        method_name: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Enabled plugin üzerindeki public callable'ı çalıştırır.

        Plugin'in arbitrary metodlarına kontrollü erişim sağlar.
        """

        self._ensure_open()

        normalized_name = _normalize_plugin_name(
            name
        )

        if not isinstance(
            method_name,
            str,
        ):
            raise TypeError(
                "method_name str olmalıdır."
            )

        normalized_method = method_name.strip()

        if not normalized_method:
            raise ValueError(
                "method_name boş olamaz."
            )

        if normalized_method.startswith(
            "_"
        ):
            raise PluginInvocationError(
                "Private plugin metodları invoke edilemez "
                f"| method={normalized_method!r}"
            )

        plugin = self.registry.get(
            normalized_name
        )

        state = self._require_runtime_state(
            normalized_name
        )

        with self._lock:
            if not state.enabled:
                raise PluginDisabledError(
                    "Plugin disabled "
                    f"| plugin={normalized_name!r}"
                )

        method = getattr(
            plugin,
            normalized_method,
            None,
        )

        if method is None:
            raise PluginInvocationError(
                "Plugin metodu bulunamadı "
                f"| plugin={normalized_name!r} "
                f"| method={normalized_method!r}"
            )

        if not callable(
            method
        ):
            raise PluginInvocationError(
                "Plugin attribute callable değil "
                f"| plugin={normalized_name!r} "
                f"| method={normalized_method!r}"
            )

        try:
            result = method(
                *args,
                **kwargs,
            )

        except Exception as exc:
            raise PluginInvocationError(
                "Plugin invocation başarısız "
                f"| plugin={normalized_name!r} "
                f"| method={normalized_method!r} "
                f"| exception_type={exc.__class__.__name__} "
                f"| message={_safe_exception_message(exc)}"
            ) from exc

        with self._lock:
            state.invocation_count += 1

        return result

    # =========================================================================
    # UNREGISTER
    # =========================================================================
    def unregister(
        self,
        name: str,
    ) -> Any:
        """
        Plugin'i manager ve registry'den kaldırır.

        Lifecycle sırası::

            on_disable() [enabled]
                ↓
            on_unload()
                ↓
            registry.unregister()
        """

        self._ensure_open()

        normalized = _normalize_plugin_name(
            name
        )

        plugin = self.registry.get(
            normalized
        )

        state = self._require_runtime_state(
            normalized
        )

        key = _normalized_key(
            normalized
        )

        with self._lock:
            enabled = state.enabled

        if enabled:
            self._optional_hook(
                plugin,
                "on_disable",
            )

            with self._lock:
                state.enabled = False
                state.disable_count += 1

        self._optional_hook(
            plugin,
            "on_unload",
        )

        removed = self.registry.unregister(
            normalized
        )

        with self._lock:
            self._runtime_states.pop(
                key,
                None,
            )

            try:
                self._registration_order.remove(
                    key
                )

            except ValueError:
                pass

        return removed

    # =========================================================================
    # NAMES
    # =========================================================================
    def names(
        self,
        *,
        enabled_only: bool = False,
    ) -> list[str]:
        self._ensure_open()

        if not isinstance(
            enabled_only,
            bool,
        ):
            raise TypeError(
                "enabled_only bool olmalıdır."
            )

        names = self.registry.names()

        if not enabled_only:
            return list(
                names
            )

        enabled_names: list[
            str
        ] = []

        for name in names:
            if self.is_enabled(
                name
            ):
                enabled_names.append(
                    name
                )

        return enabled_names

    # =========================================================================
    # SNAPSHOT
    # =========================================================================
    def plugin_snapshot(
        self,
        name: str,
    ) -> dict[str, Any]:
        self._ensure_open()

        info = self.get_info(
            name
        )

        state = self._require_runtime_state(
            name
        )

        return {
            "name": info.name,
            "version": info.version,
            "author": info.author,
            "description": info.description,
            "metadata": dict(
                info.metadata
            ),
            "runtime": state.to_dict(),
        }

    def snapshot(
        self,
    ) -> dict[str, Any]:
        """
        Deterministic manager snapshot.
        """

        self._ensure_open()

        plugin_names = self.names()

        return {
            "closed": False,
            "plugin_count": self.plugin_count,
            "enabled_count": self.enabled_count,
            "disabled_count": self.disabled_count,
            "plugins": [
                self.plugin_snapshot(
                    name
                )
                for name
                in plugin_names
            ],
        }

    # =========================================================================
    # CLOSE
    # =========================================================================
    def close(
        self,
    ) -> None:
        """
        PluginManager'ı deterministik biçimde kapatır.

        Plugin'ler ters registration sırasıyla unload edilir.

        Bir lifecycle hook hata verse bile diğer plugin'lerin cleanup işlemi
        devam eder. Sonunda ilk hata PluginLifecycleError olarak yükseltilir.

        Inject edilmiş registry kapatılmaz veya sahiplenilmez.
        """

        with self._lock:
            if self._closed:
                return

            order = list(
                reversed(
                    self._registration_order
                )
            )

        errors: list[
            BaseException
        ] = []

        for key in order:
            with self._lock:
                state = self._runtime_states.get(
                    key
                )

            if state is None:
                continue

            try:
                canonical_name = (
                    self._canonical_name_for_key(
                        key
                    )
                )

            except Exception as exc:
                errors.append(
                    exc
                )
                continue

            try:
                self.unregister(
                    canonical_name
                )

            except Exception as exc:
                errors.append(
                    exc
                )

        with self._lock:
            self._closed = True

        if errors:
            first = errors[0]

            raise PluginLifecycleError(
                "PluginManager cleanup sırasında "
                "bir veya daha fazla hata oluştu "
                f"| error_count={len(errors)} "
                f"| first_error="
                f"{_safe_exception_message(first)}"
            ) from first

    def _canonical_name_for_key(
        self,
        key: str,
    ) -> str:
        for name in self.registry.names():
            if name.casefold() == key:
                return name

        raise PluginManagerError(
            "Plugin canonical adı çözümlenemedi "
            f"| key={key!r}"
        )

    # =========================================================================
    # CONTEXT MANAGER
    # =========================================================================
    def __enter__(
        self,
    ) -> "PluginManager":
        self._ensure_open()

        return self

    def __exit__(
        self,
        exc_type: Any,
        exc: Any,
        traceback: Any,
    ) -> None:
        self.close()

    # =========================================================================
    # REPRESENTATION
    # =========================================================================
    def __repr__(
        self,
    ) -> str:
        return (
            "PluginManager("
            f"plugin_count={self.plugin_count}, "
            f"enabled_count={self.enabled_count}, "
            f"closed={self.is_closed}"
            ")"
        )