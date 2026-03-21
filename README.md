# ebus-mqtt-client

Standalone MQTT client wrapper around [paho-mqtt](https://pypi.org/project/paho-mqtt/) v2.

## Features

- TLS support (secure with CA verification, insecure, or plaintext)
- Automatic reconnection with configurable backoff
- Subscription recovery on reconnect
- Topic pattern matching via paho's `MQTTMatcher`
- Last Will and Testament (LWT)
- MQTTv3 and MQTTv5 protocol support
- Factory method for dict-based configuration

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

## License

[MIT License](LICENSE) — Copyright (c) 2026 Clark Communications Corporation
