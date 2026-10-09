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


def test_a_publish_before_connect_returns_a_message_info():
    # Not paho's: a not-ready publish never reaches paho, and the wrapper returns
    # a plain MQTTMessageInfo of its own with rc=MQTT_ERR_NO_CONN, the code paho
    # reports for a refused publish.
    c = _make()
    info = c.publish("t/x", "payload")
    assert isinstance(info, mqtt.MQTTMessageInfo)
    assert info.rc == mqtt.MQTT_ERR_NO_CONN


def test_real_paho_refuses_a_disconnected_publish_with_no_conn():
    # publish() and _flush_pending tell a lost link from a real failure by this
    # result code alone: at QoS 0 they hold the message, and at QoS 1 and 2 they
    # leave it to paho, which has already stored it. Assert both against the
    # real library on both majors.
    c = _make()
    info = c.mqttc.publish("t/x", "payload", 1, False)
    assert info.rc == mqtt.MQTT_ERR_NO_CONN
    assert info.mid in c.mqttc._out_messages


def test_a_qos0_retained_publish_before_connect_is_held_not_lost():
    c = _make()
    c.publish("t/state", "ready", qos=0, retain=True)
    assert [e[:4] for e in c._pending.values()] == [("t/state", "ready", 0, True)]
    # A non-retained QoS 0 publish is a fire-and-forget event, so it is not held.
    c.publish("t/event", "happened", qos=0)
    assert len(c._pending) == 1


@pytest.mark.parametrize("qos", [1, 2])
def test_a_higher_qos_publish_before_connect_is_held_and_never_reaches_paho(qos):
    # paho would store a refused QoS 1/2 publish and replay it on CONNACK in one
    # burst that ignores max_inflight and lands after on_connect (GH #20). The
    # wrapper therefore keeps it out of paho entirely; assert that against the
    # real library on both majors.
    c = _make()
    info = c.publish("t/state", "ready", qos=qos, retain=True)
    assert info.rc == mqtt.MQTT_ERR_NO_CONN
    assert [e[:4] for e in c._pending.values()] == [("t/state", "ready", qos, True)]
    assert c.mqttc._out_messages == {}


def test_real_paho_keeps_nothing_at_qos0():
    c = _make()
    c.publish("t/state", "ready", qos=0, retain=True)
    assert c.mqttc._out_messages == {}


def _pretend_connected(c):
    """paho's connected state with no socket: what it reports after its network
    loop has closed the socket on a dropped link and before on_disconnect runs."""
    state = getattr(mqtt, "_ConnectionState", None)
    c.mqttc._state = state.MQTT_CS_CONNECTED if state else mqtt.mqtt_cs_connected
    c._link_ready = True


@pytest.mark.parametrize("qos", [1, 2])
def test_a_publish_refused_in_the_drop_window_is_stored_by_paho_not_held(qos):
    # paho stores a QoS 1/2 publish it refuses, so the wrapper does not hold it
    # as well; the next connect takes it back (GH #21).
    c = _make()
    _pretend_connected(c)
    info = c.publish("t/state", "ready", qos=qos, retain=True)
    assert info.rc == mqtt.MQTT_ERR_NO_CONN
    assert len(c.mqttc._out_messages) == 1
    assert not c._pending


@pytest.mark.parametrize("qos", [1, 2])
def test_what_paho_stored_can_be_taken_back_into_the_hold(qos):
    # Canary for the layout _reclaim_leftovers relies on when paho has no
    # public drop_out_messages() (GH #21).
    c = _make()
    _pretend_connected(c)
    info = c.publish("t/state", "ready", qos=qos, retain=True)
    with c._pending_lock:
        c._reclaim_leftovers()
    assert c.mqttc._out_messages == {}
    assert list(c._pending.values()) == [("t/state", b"ready", qos, True)]
    assert info.rc == mqtt.MQTT_ERR_NO_CONN
