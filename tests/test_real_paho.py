"""Un-mocked smoke tests that exercise the REAL paho-mqtt library.

The rest of the suite (``test_client.py``) patches ``paho.mqtt.client.Client``,
so it never touches the real transport. These tests deliberately do not, so
CI's paho-version matrix actually verifies the wrapper works across the
``paho-mqtt`` 1.x / 2.x boundary it claims to support (``paho-mqtt>=1.5.0``).

They are hermetic: construction calls ``connect_async``, which is non-blocking
and needs no broker, and the network loop is never started, so nothing touches
the network.
"""

import paho.mqtt.client as mqtt
import pytest

from ebus_mqtt_client import MqttClient

# The wrapper targets paho's VERSION1 callback API. On paho 2.x, constructing a
# Client without callback_api_version defaults to VERSION1 and emits this
# DeprecationWarning; that is expected and intentional. If a future paho drops
# VERSION1, construction here starts failing, which is exactly the signal we
# want the matrix to surface.
pytestmark = pytest.mark.filterwarnings("ignore:Callback API version 1 is deprecated")


def _make(**kw):
    return MqttClient(client_id="probe", endpoint="127.0.0.1", port=1883, **kw)


def test_construction_creates_real_paho_client():
    # If mqtt.Client() had raised (e.g. a mandatory-argument change across a
    # paho major), the constructor swallows it and .mqttc is never set (see the
    # hasattr(self, "mqttc") guard in publish()). Asserting the attribute exists
    # and is a real Client is the cross-major construction guarantee.
    c = _make()
    assert isinstance(c.mqttc, mqtt.Client)
    assert c.is_running is False


def test_callbacks_are_wired_to_the_wrapper():
    c = _make()
    assert c.mqttc.on_connect == c._on_connect
    assert c.mqttc.on_disconnect == c._on_disconnect
    assert c.mqttc.on_message == c._on_message


def test_v5_construction_creates_real_paho_client():
    c = _make(v5=True)
    assert isinstance(c.mqttc, mqtt.Client)


def test_publish_before_connect_returns_real_message_info():
    # Real paho returns an MQTTMessageInfo even when not connected (its rc
    # reflects the no-connection state); the wrapper returns it unchanged.
    c = _make()
    info = c.publish("t/x", "payload")
    assert isinstance(info, mqtt.MQTTMessageInfo)
    assert hasattr(info, "rc")


def test_publish_and_flush_unconnected_is_false():
    c = _make()
    assert c.publish_and_flush("t/x", "payload", timeout=0.1) is False
