"""QoS 0 retained publishes issued before the link is up are held, not dropped.

`__init__` uses `connect_async`, so CONNACK does not arrive until the network
loop started by `start()` gets it, and paho refuses every publish issued in
between with `MQTT_ERR_NO_CONN`. Those publishes used to be logged as failures,
and the QoS 0 ones were lost outright, which for a caller that announces
retained state at startup is the whole announcement, on every start.

Which QoS is load-bearing here, so it is stated explicitly in every test rather
than left to the `publish()` default.
"""

import logging
import threading

import paho.mqtt.client as mqtt
import pytest

from ebus_mqtt_client import MqttClient

# How long the flush-vs-publish race test hands to the racing thread. Only the
# passing path waits it out; a client missing the mutual exclusion finishes the
# racer immediately and the test fails on ordering rather than on this timeout.
RACE_WINDOW = 0.25


class FakePaho:
    """Stands in for paho, refusing publishes until `connected` is set.

    Models the part that decides this feature's shape: what paho does with a
    publish it refuses. At QoS 0 it calls `_send_publish` directly and keeps
    nothing, so the message is gone. At QoS 1 and 2 it has already stored the
    message in `_out_messages` when it returns `MQTT_ERR_NO_CONN`, and re-sends
    it from `_handle_connack` once CONNACK arrives, after `on_connect` returns.
    A fake that discarded at every QoS would let these tests certify a guarantee
    the library does not have, and would hide a double publish at QoS 1 and 2.
    """

    def __init__(self):
        self.connected = False
        self.published: list[tuple[str, str, int, bool]] = []  # in wire order
        self.queued: list[tuple[str, str, int, bool]] = []  # paho's own out-queue
        self.subscribed: list[tuple[str, int]] = []

    # -- the surface MqttClient touches at construction and shutdown --------
    def will_set(self, **kw):
        pass

    def reconnect_delay_set(self, **kw):
        pass

    def user_data_set(self, _):
        pass

    def username_pw_set(self, *a):
        pass

    def connect_async(self, *a, **kw):
        pass

    def disconnect(self):
        self.connected = False

    def loop_start(self):
        pass

    def loop_stop(self):
        pass

    def is_connected(self):
        return self.connected

    def subscribe(self, sub, qos):
        self.subscribed.append((sub, qos))
        return (mqtt.MQTT_ERR_SUCCESS, 1)

    # -- the bit under test -------------------------------------------------
    def publish(self, topic, data, qos, retain):
        info = type("Info", (), {})()
        if not self.connected:
            info.rc = mqtt.MQTT_ERR_NO_CONN
            if qos > 0:
                self.queued.append((topic, data, qos, retain))
            return info
        self.published.append((topic, data, qos, retain))
        info.rc = mqtt.MQTT_ERR_SUCCESS
        return info

    def drain_queue(self):
        """What `_handle_connack` does once `on_connect` has returned."""
        self.published.extend(self.queued)
        self.queued.clear()


@pytest.fixture
def client(monkeypatch):
    fake = FakePaho()
    monkeypatch.setattr("ebus_mqtt_client.client.mqtt.Client", lambda *a, **kw: fake)
    c = MqttClient(client_id="test", endpoint="127.0.0.1", port=1883)
    c._fake = fake
    return c


def connect(client):
    """Simulate CONNACK arriving once the network loop starts."""
    client._fake.connected = True
    client._on_connect(client._fake, None, 0, 0)  # paho dispatches on_connect,
    client._fake.drain_queue()  # then re-sends its own out-queue


def topics(client):
    return [t for t, *_ in client._fake.published]


def payloads(client):
    return [d for _, d, *_ in client._fake.published]


class TestHeldUntilConnected:
    def test_a_retained_publish_before_connect_still_lands(self, client):
        client.publish("a/dev/state", "ready", qos=0, retain=True)
        assert client._fake.published == [], "published while disconnected"
        connect(client)
        assert client._fake.published == [("a/dev/state", "ready", 0, True)]

    def test_the_whole_announcement_lands_in_order(self, client):
        client.publish("a/state", "init", qos=0, retain=True)
        client.publish("a/config", "{}", qos=0, retain=True)
        client.publish("a/info/name", "x", qos=0, retain=True)
        connect(client)
        assert topics(client) == ["a/state", "a/config", "a/info/name"]

    def test_the_held_message_is_republished_verbatim(self, client):
        client.publish("a/b", "v", qos=0, retain=True)
        connect(client)
        assert client._fake.published == [("a/b", "v", 0, True)]

    def test_nothing_is_held_once_connected(self, client):
        connect(client)
        client.publish("a/b", "1", qos=0, retain=True)
        assert client._fake.published == [("a/b", "1", 0, True)]
        assert not client._pending

    def test_the_flush_precedes_subscriptions_and_the_connect_callback(self, client):
        # What was published while disconnected describes state that already
        # exists, so it belongs on the broker before anything reacts to being
        # connected. Anything the callback publishes is newer and lands after.
        fake = client._fake
        order = []
        client.on_connect_callback = lambda: order.append("callback")
        client.subscribe("a/sub", param=None)
        client.publish("a/state", "ready", qos=0, retain=True)

        real_publish, real_subscribe = fake.publish, fake.subscribe

        def note_publish(*a, **kw):
            order.append("flush")
            return real_publish(*a, **kw)

        def note_subscribe(*a, **kw):
            order.append("subscribe")
            return real_subscribe(*a, **kw)

        fake.publish, fake.subscribe = note_publish, note_subscribe
        connect(client)

        assert order == ["flush", "subscribe", "callback"]

    def test_publish_returns_pahos_message_info_even_when_held(self, client):
        info = client.publish("a/b", "v", qos=0, retain=True)
        assert info is not None
        assert info.rc == mqtt.MQTT_ERR_NO_CONN
        assert "a/b" in client._pending

    def test_no_client_still_returns_none_and_holds_nothing(self, client):
        del client.mqttc
        assert client.publish("a/b", "v", qos=0, retain=True) is None
        assert not client._pending


class TestPahoQueuesTheHigherQoSItself:
    """Why the hold is scoped to QoS 0 rather than to every retained publish.

    paho stores a QoS 1 or 2 message in `_out_messages` before it returns
    `MQTT_ERR_NO_CONN`, and re-sends it itself once CONNACK arrives. Holding one
    here as well would put a second, identical copy of every message on the wire
    on every connect: at `PENDING_LIMIT` topics that is 1024 retained PUBLISHes
    per connect instead of 512, on exactly the constrained targets this exists
    to be quiet on.
    """

    @pytest.mark.parametrize("qos", [1, 2])
    def test_a_higher_qos_retained_publish_is_not_held(self, client, qos):
        client.publish("a/state", "ready", qos=qos, retain=True)
        assert not client._pending
        assert client._fake.queued == [("a/state", "ready", qos, True)]

    @pytest.mark.parametrize("qos", [1, 2])
    def test_it_reaches_the_broker_exactly_once(self, client, qos):
        client.publish("a/state", "ready", qos=qos, retain=True)
        connect(client)
        assert client._fake.published == [("a/state", "ready", qos, True)]

    def test_the_default_qos_is_one_so_the_default_path_is_pahos(self, client):
        # Guards the scoping against a change of default: if publish() ever
        # defaulted to QoS 0, every existing caller would silently move onto
        # the hold, and this test is where that shows up.
        client.publish("a/state", "ready", retain=True)
        assert not client._pending

    def test_a_mixed_announcement_lands_once_per_topic(self, client):
        client.publish("a/state", "ready", qos=0, retain=True)
        client.publish("a/config", "{}", qos=1, retain=True)
        connect(client)
        assert sorted(topics(client)) == ["a/config", "a/state"]


class TestStaleValuesCannotWin:
    """The reason the hold is keyed by topic rather than being a plain queue."""

    def test_only_the_newest_value_per_topic_is_kept(self, client):
        client.publish("a/state", "init", qos=0, retain=True)
        client.publish("a/state", "ready", qos=0, retain=True)
        connect(client)
        assert client._fake.published == [("a/state", "ready", 0, True)]

    def test_a_held_value_cannot_overwrite_a_later_live_one(self, client):
        # A replay-everything queue would flush "init" after "ready" was already
        # on the broker, leaving the device permanently announcing a state it
        # had left. Retained state is last-value-wins, so the newest attempt is
        # the only one worth keeping.
        client.publish("a/state", "init", qos=0, retain=True)
        connect(client)
        client.publish("a/state", "ready", qos=0, retain=True)
        assert payloads(client) == ["init", "ready"]

    def test_a_publish_racing_the_flush_cannot_be_overwritten(self, client):
        # Same hazard, arriving by the other route: the flush runs on paho's
        # network thread, so without the lock a caller thread could publish a
        # newer value for a held topic after the flush drained the hold but
        # before it published, and the older value would land last.
        fake = client._fake
        client.publish("a/state", "init", qos=0, retain=True)

        racer_published = threading.Event()

        def publish_newer():
            client.publish("a/state", "ready", qos=0, retain=True)
            racer_published.set()

        racer = threading.Thread(target=publish_newer)
        flush_publish = fake.publish

        def stall_the_flush(topic, data, qos, retain):
            if data == "init":
                racer.start()
                # Hand the racer the whole window and wait for it to finish. It
                # can only finish ahead of us if publish() and the flush are not
                # mutually excluded, in which case the stale "init" below lands
                # last and the assertion catches it.
                racer_published.wait(RACE_WINDOW)
            return flush_publish(topic, data, qos, retain)

        fake.publish = stall_the_flush
        connect(client)
        racer.join(5.0)

        assert not racer.is_alive(), "publish() blocked past the flush"
        assert payloads(client) == ["init", "ready"]


class TestEventsAreNotReplayed:
    def test_a_non_retained_publish_is_dropped_rather_than_held(self, client):
        # Not retained means an event: delivering it after an arbitrary delay
        # announces something that was true once, which is worse than silence.
        client.publish("a/event", "happened", qos=0, retain=False)
        connect(client)
        assert client._fake.published == []
        assert not client._pending

    @pytest.mark.parametrize(
        ("qos", "retain", "disposition"),
        [
            (0, True, "held"),
            (0, False, "dropped"),
            (1, True, "queuedByPaho"),
            (1, False, "queuedByPaho"),
        ],
    )
    def test_publishing_before_connect_does_not_warn(
        self, client, caplog, qos, retain, disposition
    ):
        # The reported cost: warnings once per process start, on every device
        # and every restart, crowding a bounded journald budget and reading as
        # a broker fault to anyone debugging one.
        with caplog.at_level(logging.DEBUG):
            client.publish("a/topic", "v", qos=qos, retain=retain)
        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
        assert "reason=mqttPublishNotConnected" in caplog.text
        assert f"disposition={disposition}" in caplog.text


class TestBounded:
    def test_the_hold_is_bounded_and_drops_oldest(self, client):
        client.pending_limit = 3
        for i in range(5):
            client.publish(f"a/{i}", str(i), qos=0, retain=True)
        connect(client)
        # Oldest two evicted: a client that never connects must not grow forever.
        assert topics(client) == ["a/2", "a/3", "a/4"]

    def test_a_repeat_topic_does_not_count_against_the_bound(self, client):
        # Replacing a held topic's value is not growth, so it must not evict.
        # It does re-order: the hold is ordered by most recent write, so the
        # updated topic flushes last, which is also the order a caller that
        # revises a value while announcing would want it in.
        client.pending_limit = 2
        client.publish("a/0", "0", qos=0, retain=True)
        client.publish("a/1", "1", qos=0, retain=True)
        client.publish("a/0", "0-newer", qos=0, retain=True)
        connect(client)
        assert client._fake.published == [
            ("a/1", "1", 0, True),
            ("a/0", "0-newer", 0, True),
        ]

    def test_overflow_is_reported_once_then_sampled(self, client, caplog):
        client.pending_limit = 1
        with caplog.at_level(logging.WARNING):
            for i in range(20):
                client.publish(f"a/{i}", str(i), qos=0, retain=True)
        overflow = [r for r in caplog.records if "mqttPendingOverflow" in r.message]
        assert len(overflow) == 1, "one line per drop is the noise we are avoiding"
        assert "limit=1" in overflow[0].message

    def test_reconnect_flushes_again(self, client):
        connect(client)
        client._fake.connected = False
        client.publish("a/state", "lost", qos=0, retain=True)
        assert client._fake.published == []
        connect(client)
        assert client._fake.published == [("a/state", "lost", 0, True)]

    def test_a_link_lost_mid_flush_re_holds_rather_than_losing(self, client):
        client.publish("a/0", "0", qos=0, retain=True)
        client.publish("a/1", "1", qos=0, retain=True)
        fake = client._fake
        flush_publish = fake.publish

        def drop_the_link_after_the_first(topic, data, qos, retain):
            info = flush_publish(topic, data, qos, retain)
            fake.connected = False
            return info

        fake.publish = drop_the_link_after_the_first
        connect(client)

        assert topics(client) == ["a/0"]
        assert "a/1" in client._pending, "a refused flush must survive to the next connect"
        fake.publish = flush_publish
        connect(client)
        assert topics(client) == ["a/0", "a/1"]

    def test_stop_drops_the_hold(self, client):
        client.publish("a/state", "ready", qos=0, retain=True)
        client.stop(timeout=0.1)
        assert not client._pending
        connect(client)
        assert client._fake.published == []


class TestFailuresStillReported:
    def test_a_real_failure_while_connected_still_warns_with_its_rc(self, client, caplog):
        connect(client)
        client._fake.publish = lambda *a, **kw: type("Info", (), {"rc": mqtt.MQTT_ERR_QUEUE_SIZE})()
        with caplog.at_level(logging.WARNING):
            client.publish("a/b", "1", qos=0, retain=True)
        assert "mqttPublishFail" in caplog.text
        assert f"rc={mqtt.MQTT_ERR_QUEUE_SIZE}" in caplog.text
        assert not client._pending, "a real failure must not be silently held"
