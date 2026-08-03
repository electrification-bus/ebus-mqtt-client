from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ebus_mqtt_client.client import AUTH_TYPE_USER_PASS, MqttClient

if TYPE_CHECKING:
    # For type checkers only: bind the name so `from ebus_mqtt_client import
    # AsyncioMqttDriver` resolves to the concrete class. At runtime the name is
    # served lazily by __getattr__ below (no import on package import).
    from ebus_mqtt_client.asyncio_driver import AsyncioMqttDriver as AsyncioMqttDriver

__version__ = "0.4.0"

__all__ = ["MqttClient", "AUTH_TYPE_USER_PASS", "AsyncioMqttDriver"]


def __getattr__(name: str) -> Any:
    # Lazy export (PEP 562): importing the package must NOT import
    # asyncio_driver, which a thread-mode consumer never needs. The module (and
    # its asyncio machinery) loads only when AsyncioMqttDriver is first accessed.
    if name == "AsyncioMqttDriver":
        from ebus_mqtt_client.asyncio_driver import AsyncioMqttDriver

        return AsyncioMqttDriver
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    # Surface the lazily-exported name in dir()/autocomplete alongside globals.
    return sorted(set(globals()) | set(__all__))
