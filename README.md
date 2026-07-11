# ebus-mqtt-client

[![PyPI version](https://img.shields.io/pypi/v/ebus-mqtt-client.svg)](https://pypi.org/project/ebus-mqtt-client/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Standalone MQTT client wrapper around [paho-mqtt](https://pypi.org/project/paho-mqtt/) v2.

## Features

- TLS support (secure with CA verification, insecure, or plaintext)
- Automatic reconnection with configurable backoff
- Subscription recovery on reconnect
- Topic pattern matching via paho's `MQTTMatcher`
- Last Will and Testament (LWT)
- MQTTv3 and MQTTv5 protocol support
- Factory method for dict-based configuration
- Bounded, broker-independent shutdown: `stop(timeout=...)` returns promptly even against a dead broker
- Bounded publish flush: `publish_and_flush(...)` lands a final message before a clean disconnect, no fixed sleep

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

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to file Discussions, Issues, and pull requests. The library is intentionally a thin MQTT-only layer — Homie / eBus features belong in [`ebus-sdk`](https://github.com/electrification-bus/python-sdk).

## License

[MIT License](LICENSE) — Copyright (c) 2026 Clark Communications Corporation
