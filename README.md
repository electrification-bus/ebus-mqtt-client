# ebus-mqtt-client

[![PyPI version](https://img.shields.io/pypi/v/ebus-mqtt-client.svg)](https://pypi.org/project/ebus-mqtt-client/)
[![Python versions](https://img.shields.io/pypi/pyversions/ebus-mqtt-client.svg)](https://pypi.org/project/ebus-mqtt-client/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![CI](https://github.com/electrification-bus/ebus-mqtt-client/actions/workflows/lint.yml/badge.svg)](https://github.com/electrification-bus/ebus-mqtt-client/actions/workflows/lint.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Standalone MQTT client wrapper around [paho-mqtt](https://pypi.org/project/paho-mqtt/).

## Features

- TLS support (secure with CA verification, insecure, or plaintext)
- Resilient connect: construction never blocks or raises on a down broker (`connect_async`); the connection is established and retried on the network loop started by `start()`
- Publishes issued while the link is down are held rather than handed to paho (retained at any QoS: newest value per topic; non-retained QoS 1 and 2: in order; non-retained QoS 0: dropped), bounded, and flushed on connect before `on_connect_callback`
- Automatic reconnection with configurable backoff
- Subscription recovery on reconnect
- Topic pattern matching via paho's `MQTTMatcher`
- Last Will and Testament (LWT)
- MQTTv3 and MQTTv5 protocol support
- Factory method for dict-based configuration
- Bounded, broker-independent shutdown: `stop(timeout=...)` returns promptly even against a dead broker
- Bounded publish flush: `publish_and_flush(...)` lands a final message before a clean disconnect, no fixed sleep
- Optional loop-native driving: `AsyncioMqttDriver` runs the network loop on your asyncio event loop instead of a background thread, for hosts that already own a loop

## Install

```bash
pip install ebus-mqtt-client
```

## Quick start

```python
from ebus_mqtt_client import MqttClient

client = MqttClient(
    client_id="my-client",
    endpoint="broker.example.com",
    port=1883,
)
client.start()

client.subscribe("sensors/#", callback_param)
client.publish("sensors/temp", "22.5")

client.stop()
```

### Seeing the retain flag

A subscription callback receives `(topic, payload)`. Pass `with_retain=True` to `subscribe()` and it receives `(topic, payload, retained)` instead, where `retained` is paho's `msg.retain` as a `bool`:

```python
def on_set(topic, payload, retained):
    if retained:
        return  # a stored retained message replayed at subscribe, not a live command
    apply(payload)

client.subscribe("devices/my-client/+/set", on_set, with_retain=True)
```

Under MQTT 3.1.1 a broker sets the flag on delivery only when it replays a stored retained message to a new subscription, including the resubscribe after every reconnect. A message published while the subscription is in place arrives with `retained=False`, even when it was published retained. The flag is per subscription and is kept across reconnects. When the client was constructed with a `callback`, that callback receives `(topic, payload, param, retained)` for such a subscription.

### Graceful shutdown

Publish a final retained message and flush it (bounded) before disconnecting, then stop within a time bound even when the broker is unreachable:

```python
# Land a final state update, waiting up to 1s for it to actually be sent.
# Returns True on flush; False (without blocking or raising) if not connected,
# the publish fails, or the flush exceeds the timeout.
client.publish_and_flush(
    "devices/my-client/state", "disconnected", retain=True, timeout=1.0
)

# Returns within ~timeout seconds even if the broker is gone.
client.stop(timeout=2.0)
```

`publish()` also returns paho's `MQTTMessageInfo` (or `None` if there is no client), so you can wait for a single message yourself: `client.publish(topic, data).wait_for_publish(1.0)`.

### Publishing before the connection is up

Construction registers the broker with `connect_async` and returns; CONNACK does not arrive until the network loop started by `start()` receives it. A publish issued while the link is down is an expected transient of startup (or of a dropped link), not a fault, so it logs at debug level and is held here rather than handed to paho. The link counts as down from the disconnect until the flush on the next connect takes the hold, so a publish issued after CONNACK but before the flush joins the hold behind the older values instead of overtaking them, and one issued during the flush waits for it.

- **Retained, any QoS**: held, newest value per topic. Retained state is last-value-wins, so only the latest value is worth delivering, and a queue that replayed every attempt could write a stale value on top of a newer one.
- **Not retained, QoS 1 or 2**: held in order, one entry per publish, so each event is delivered once.
- **Not retained, QoS 0**: dropped. A fire-and-forget event delivered after an arbitrary delay announces something that was true once.

The hold is flushed on connect, before subscription recovery and before your `on_connect_callback`. The flush publishes on a live link, where paho applies `max_inflight_messages` and queues the excess. What the callback publishes reaches paho after the flush, but a broker may apply a QoS 2 message only when the handshake reaches PUBREL (mosquitto does), so a retained value the callback republishes is guaranteed to win over a flushed value for the same topic only at the same or a higher QoS. The hold is bounded by `client.pending_limit` (default 512 entries, evicting the oldest) so a client that never connects cannot grow without limit; entries left unsent by a flush the link dropped in the middle of widen that bound until the next flush rather than being evicted by it. `stop()` (or the asyncio driver's `stop()`) discards anything still held, and nothing is held from then until the next start.

paho is kept out of this because of what it does with a QoS 1 or 2 publish it refuses: it stores the message and, on CONNACK, replays its whole queue in one burst that ignores `max_inflight_messages` and lands after `on_connect`, so an older value can overwrite the one `on_connect` just published. At QoS 2 the burst can also exceed mosquitto's limit on a client's unacknowledged incoming QoS 2 messages (`max_inflight_messages`, default 20), and for an MQTT 3.1.1 client mosquitto acknowledges the excess and discards it, so retained state ended on whichever value survived ([#20](https://github.com/electrification-bus/ebus-mqtt-client/issues/20)). paho still replays, in the same way, the QoS 1 and 2 messages it accepted on a live link and had not finished delivering when the link dropped, and a QoS 1 or 2 publish it refused in the moment between the drop and `on_disconnect` ([#21](https://github.com/electrification-bus/ebus-mqtt-client/issues/21)).

A held publish returns an `MQTTMessageInfo` with `rc == MQTT_ERR_NO_CONN`, as paho reports a refused publish; the message is published afresh when the hold is flushed, so that info never completes. Arguments paho would reject (a non-`str` or wildcard topic, an empty topic under MQTT 3.1.1, a QoS outside 0-2, a payload it cannot encode) raise from `publish()` whether or not the publish is held. Any other publish failure still logs a warning, with the paho result code.

`publish_and_flush()` never holds: it returns `False` immediately while the link is down (including after CONNACK until the flush begins; a call made during the flush waits for it, then publishes), since its whole purpose is to confirm a message reached the broker now.

### From a config dict

```python
cfg = {
    "host": "broker.example.com",
    "port": 8883,
    "use_tls": True,
    "tls_insecure": False,
    "tls_ca_cert": "/path/to/ca.pem",
    "authentication": {
        "type": "USER_PASS",
        "username": "user",
        "password": "secret",
    },
}

client = MqttClient.from_config(cfg, client_id="my-client")
client.start()
```

### mTLS (client-certificate authentication)

When the broker authenticates the client via the TLS handshake (no username/password), supply a client cert and key. File-path form:

```python
cfg = {
    "host": "broker.example.com",
    "port": 8883,
    "use_tls": True,
    "tls_insecure": False,
    "tls_ca_cert": "/path/to/ca.pem",
    "tls_client_cert": "/path/to/client.crt",
    "tls_client_key": "/path/to/client.key",
    # "tls_client_key_password": "...",  # only if the key is encrypted
}

client = MqttClient.from_config(cfg, client_id="my-client")
client.start()
```

In-memory form — useful when the cert/key are fetched from a secret store rather than the filesystem. If both the path and `*_data` forms are supplied for the same item, the `*_data` form wins and a warning is logged:

```python
cfg = {
    "host": "broker.example.com",
    "port": 8883,
    "use_tls": True,
    "tls_insecure": False,
    "tls_ca_data": ca_pem_str,
    "tls_client_cert_data": client_cert_pem_str,
    "tls_client_key_data": client_key_pem_str,
}

client = MqttClient.from_config(cfg, client_id="my-client")
client.start()
```

### Loop-native driving (asyncio)

By default `start()` runs paho's network loop on a background thread. If your program already owns an asyncio event loop (for example a Home Assistant integration), you can drive the same client on that loop with no extra thread, via the optional `AsyncioMqttDriver`:

```python
import asyncio
from ebus_mqtt_client import AsyncioMqttDriver, MqttClient

async def main():
    client = MqttClient.from_config(cfg, client_id="my-client")
    driver = client.asyncio_driver()      # or: AsyncioMqttDriver(client, loop=my_loop)
    await driver.start()                   # instead of client.start()
    client.subscribe("sensors/#", callback_param)
    # ... all MQTT I/O now runs on this event loop ...
    await driver.stop()

asyncio.run(main())
```

Thread mode (`client.start()`) and the driver are mutually exclusive per client: pick one. The driver module is imported lazily (only when you reference `AsyncioMqttDriver` or call `asyncio_driver()`), so a thread-only consumer never loads the asyncio machinery.

If you inject the client into `ebus_sdk.Controller(mqttc=client)` as a bring-your-own transport, wire `Controller.resync` onto the on-connect callback (`client.on_connect_callback = controller.resync`) so the retained tree re-walks after a reconnect; the SDK does that automatically only for a client it creates itself.

## Releasing

The version lives in exactly one place: `__version__` in `src/ebus_mqtt_client/__init__.py`. `pyproject.toml` reads it dynamically, the `setup.py` legacy shim reads it by regex, and the publish workflow refuses to release a tag that disagrees with it. To cut a release:

1. Bump `__version__` in `src/ebus_mqtt_client/__init__.py` (the only place).
2. Move the CHANGELOG's `[Unreleased]` entries under a new version heading.
3. Commit: `git commit -am "Release X.Y.Z"`.
4. Tag it to match, `v`-prefixed: `git tag vX.Y.Z`.
5. Push the tag: `git push --tags` (a plain `git push` does not trigger a release).

Pushing a `v*` tag runs the publish workflow, which verifies the tag equals `v$__version__` (a mismatch fails before anything is built), builds the sdist and wheel, and publishes to PyPI via Trusted Publishing (OIDC, no stored token).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to file Discussions, Issues, and pull requests. The library is intentionally a thin MQTT-only layer — Homie / eBus features belong in [`ebus-sdk`](https://github.com/electrification-bus/python-sdk).

## License

[MIT License](LICENSE) — Copyright (c) 2026 Clark Communications Corporation
