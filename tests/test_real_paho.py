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


def test_real_paho_refuses_a_disconnected_publish_with_no_conn():
    # The linchpin of the hold-until-connected behavior (tests/test_pending_
    # publishes.py): the wrapper distinguishes "not connected yet" from a real
    # failure purely by this result code, so if a paho major ever reported
    # something else here, retained state would go back to being dropped and
    # logged as a failure. Assert the code itself, un-mocked, on both majors.
    c = _make()
    assert c.publish("t/x", "payload").rc == mqtt.MQTT_ERR_NO_CONN


def test_a_qos0_retained_publish_before_connect_is_held_not_lost():
    c = _make()
    c.publish("t/state", "ready", qos=0, retain=True)
    assert c._pending == {"t/state": ("ready", 0, True)}
    # A non-retained publish is an event, not state, so it is not held.
    c.publish("t/event", "happened", qos=0)
    assert "t/event" not in c._pending


def test_real_paho_keeps_qos1_itself_which_is_why_it_is_not_held():
    # The hold is scoped to QoS 0 because that is the only QoS at which paho
    # actually loses a refused message. At QoS 1 and 2 it stores the message in
    # its own out-queue before returning MQTT_ERR_NO_CONN and re-sends it from
    # _handle_connack, so holding it here too would publish it twice per
    # connect. That is a claim about paho's internals across the 1.x/2.x
    # boundary, so assert it against the real library: if a future paho stops
    # queueing, this fails and the scoping has to be revisited.
    c = _make()
    info = c.publish("t/state", "ready", qos=1, retain=True)
    assert info.rc == mqtt.MQTT_ERR_NO_CONN
    assert c._pending == {}, "QoS 1 is paho's to re-send, not ours to hold"
    queued = c.mqttc._out_messages
    assert len(queued) == 1
    assert next(iter(queued.values())).payload == b"ready"


def test_real_paho_keeps_nothing_at_qos0_which_is_why_it_is_held():
    # The other half of the same claim, and the reason the hold exists at all.
    c = _make()
    c.publish("t/state", "ready", qos=0, retain=True)
    assert c.mqttc._out_messages == {}


def test_publish_and_flush_unconnected_is_false():
    c = _make()
    assert c.publish_and_flush("t/x", "payload", timeout=0.1) is False
