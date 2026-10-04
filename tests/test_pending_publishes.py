"""Publishes issued while the link is down are held by the wrapper, not dropped
and not left to paho.

`__init__` uses `connect_async`, so CONNACK does not arrive until the network
loop started by `start()` gets it, and paho refuses every publish issued in
between with `MQTT_ERR_NO_CONN`. Those publishes used to be logged as failures,
and the QoS 0 ones were lost outright, which for a caller that announces
retained state at startup is the whole announcement, on every start. The QoS 1
and 2 ones were left to paho, which replays them on CONNACK in an uncapped burst
after `on_connect`, overwriting newer values and, at QoS 2, exceeding the
broker's receive quota (GH #20).

Which QoS is load-bearing here, so it is stated explicitly in every test rather
than left to the `publish()` default.
"""

import logging
import threading
from collections import OrderedDict
from types import SimpleNamespace

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
    The wrapper never hands paho a publish while the link is not ready, but one
    racing a link drop still lands in that queue; `refuse_sends` models that
    window. A fake that discarded at every QoS would hide it.
    """

    def __init__(self):
        self.connected = False
        self.published: list[tuple[str, str, int, bool]] = []  # in wire order
        self._out_messages: OrderedDict[int, SimpleNamespace] = OrderedDict()
        self._mid = 0
        # Report connected but refuse to send: the window after paho has noticed
        # the drop (socket closed) and before on_disconnect reaches the wrapper.
        self.refuse_sends = False
        self.subscribed: list[tuple[str, int]] = []

    @property
    def queued(self):
        return [(m.topic, m.payload, m.qos, m.retain) for m in self._out_messages.values()]

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
        self._mid += 1
        info = mqtt.MQTTMessageInfo(self._mid)
        if not self.connected or self.refuse_sends:
            info.rc = mqtt.MQTT_ERR_NO_CONN
            if qos > 0:
                self._out_messages[self._mid] = SimpleNamespace(
                    topic=topic, payload=data, qos=qos, retain=retain
                )
            return info
        self.published.append((topic, data, qos, retain))
        info.rc = mqtt.MQTT_ERR_SUCCESS
        return info

    def drain_queue(self):
        """What `_handle_connack` does once `on_connect` has returned."""
        self.published.extend(self.queued)
        self._out_messages.clear()


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
    client._fake.refuse_sends = False
    client._on_connect(client._fake, None, {"session present": 0}, 0)  # paho dispatches on_connect,
    client._fake.drain_queue()  # then re-sends its own out-queue


def disconnect_while_another_thread_holds_the_publish_lock(client):
    """Run on_disconnect on a worker while this thread holds _pending_lock, as a
    thread inside paho's publish would. True if it finished rather than
    deadlocking on that lock."""
    worker = threading.Thread(target=client._on_disconnect, args=(client._fake, None, 1))
    with client._pending_lock:
        worker.start()
        worker.join(2.0)
        finished = not worker.is_alive()
    worker.join(5.0)
    return finished


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
        # connected, so it is published before the callback runs.
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

    def test_a_held_publish_returns_a_message_info_with_no_conn(self, client):
        info = client.publish("a/b", "v", qos=0, retain=True)
        assert info is not None
        assert info.rc == mqtt.MQTT_ERR_NO_CONN
        assert "a/b" in client._pending
        assert client._fake.queued == [] and client._fake.published == [], "paho not called"

    def test_no_client_still_returns_none_and_holds_nothing(self, client):
        del client.mqttc
        assert client.publish("a/b", "v", qos=0, retain=True) is None
        assert not client._pending


class TestHigherQoSIsHeldNotLeftToPaho:
    """Why the hold covers QoS 1 and 2 rather than leaving them to paho.

    paho stores a QoS 1 or 2 message in `_out_messages` before it returns
    `MQTT_ERR_NO_CONN`, then on CONNACK replays the whole queue in one burst that
    ignores `max_inflight_messages`, after `on_connect` has published the current
    state, so an older value can overwrite it. At QoS 2 the burst can also
    exceed the broker's receive quota, and mosquitto acknowledges the excess
    from an MQTT 3.1.1 client and discards it (GH #20).
    """

    @pytest.mark.parametrize("qos", [1, 2])
    def test_a_higher_qos_retained_publish_is_held_not_queued_by_paho(self, client, qos):
        client.publish("a/state", "ready", qos=qos, retain=True)
        assert "a/state" in client._pending
        assert client._fake.queued == []

    @pytest.mark.parametrize("qos", [1, 2])
    def test_it_reaches_the_broker_exactly_once(self, client, qos):
        client.publish("a/state", "ready", qos=qos, retain=True)
        connect(client)
        assert client._fake.published == [("a/state", "ready", qos, True)]

    def test_the_default_qos_is_held_too(self, client):
        client.publish("a/state", "ready", retain=True)
        assert "a/state" in client._pending

    def test_a_revised_value_lands_once_newest_first_in_line(self, client):
        client.publish("a/state", "init", qos=2, retain=True)
        client.publish("a/state", "ready", qos=2, retain=True)
        connect(client)
        assert client._fake.published == [("a/state", "ready", 2, True)]

    def test_held_state_lands_before_what_the_connect_callback_publishes(self, client):
        # paho's own replay runs after on_connect, so a stale queued copy would
        # land on top of the fresh value the callback republished.
        client.publish("a/state", "init", qos=2, retain=True)
        client.on_connect_callback = lambda: client.publish("a/state", "ready", qos=2, retain=True)
        connect(client)
        assert payloads(client) == ["init", "ready"]

    @pytest.mark.parametrize("qos", [1, 2])
    def test_higher_qos_events_are_held_in_order_and_not_merged(self, client, qos):
        client.publish("a/event", "1", qos=qos)
        client.publish("a/other", "x", qos=qos)
        client.publish("a/event", "2", qos=qos)
        connect(client)
        assert client._fake.published == [
            ("a/event", "1", qos, False),
            ("a/other", "x", qos, False),
            ("a/event", "2", qos, False),
        ]

    def test_a_mixed_announcement_lands_once_per_topic(self, client):
        client.publish("a/state", "ready", qos=0, retain=True)
        client.publish("a/config", "{}", qos=1, retain=True)
        connect(client)
        assert topics(client) == ["a/state", "a/config"]


class TestPublishesBetweenConnackAndTheFlush:
    """paho reports connected at CONNACK, before on_connect runs the flush."""

    def connack_then(self, client, publish_in_gap):
        fake = client._fake
        fake.connected = True  # paho's state flips here ...
        publish_in_gap()  # ... an app thread publishes ...
        client._on_connect(fake, None, {"session present": 0}, 0)  # ... then the flush
        fake.drain_queue()

    def test_a_newer_value_is_not_overwritten_by_the_older_held_one(self, client):
        client.publish("a/state", "old", qos=1, retain=True)
        self.connack_then(client, lambda: client.publish("a/state", "new", qos=1, retain=True))
        assert payloads(client) == ["new"]

    def test_an_event_is_delivered_once_in_order(self, client):
        client.publish("a/event", "1", qos=1)
        self.connack_then(client, lambda: client.publish("a/event", "2", qos=1))
        assert payloads(client) == ["1", "2"]

    def test_publish_and_flush_refuses_rather_than_overtaking_the_hold(self, client):
        client.publish("a/state", "ready", qos=1, retain=True)
        result = []
        self.connack_then(
            client,
            lambda: result.append(client.publish_and_flush("a/state", "disconnected", qos=1)),
        )
        assert result == [False]
        assert payloads(client) == ["ready"]

    def test_publish_and_flush_during_the_flush_lands_after_it(self, client):
        # The flush marks the link ready before sending the held entries, so a
        # publish_and_flush arriving mid-flush must wait for the flush rather
        # than reach paho ahead of the older held value.
        client.publish("a/state", "old", qos=1, retain=True)
        client.publish("a/other", "x", qos=1, retain=True)
        fake = client._fake
        real = fake.publish
        racer = []

        def first_send_starts_a_racer(*a):
            if not racer:
                t = threading.Thread(
                    target=lambda: client.publish_and_flush(
                        "a/state", "new", qos=1, retain=True, timeout=0.1
                    )
                )
                racer.append(t)
                t.start()
                t.join(RACE_WINDOW)
            return real(*a)

        fake.publish = first_send_starts_a_racer
        connect(client)
        racer[0].join(5)
        assert payloads(client) == ["old", "x", "new"]


class TestDisconnectCallback:
    def test_publishing_from_on_disconnect_callback_holds_without_paho(self, client):
        # on_disconnect can run while paho holds its own out-queue mutex, so a
        # publish from it must not need _pending_lock or reach paho.
        connect(client)
        client.on_disconnect_callback = lambda rc: client.publish(
            "a/state", "lost", qos=1, retain=True
        )
        client._fake.connected = False
        assert disconnect_while_another_thread_holds_the_publish_lock(client)
        assert "a/state" in client._pending
        connect(client)
        assert payloads(client) == ["lost"]


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
            (1, True, "held"),
            (1, False, "held"),
            (2, True, "held"),
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


class TestInvalidPublishesRaiseToTheCaller:
    """A held publish reaches paho only in the flush, on paho's network thread,
    where an exception ends the loop. paho validates before it checks the
    connection, so these raised to the caller when they went to paho first."""

    @pytest.mark.parametrize(
        ("topic", "data", "qos", "exc"),
        [
            ("a/+/b", "v", 1, ValueError),
            ("a/#", "v", 1, ValueError),
            ("", "v", 1, ValueError),
            ("a/b", "v", 3, ValueError),
            ("a/b", {"not": "encodable"}, 1, TypeError),
        ],
    )
    def test_raised_while_the_link_is_down(self, client, topic, data, qos, exc):
        with pytest.raises(exc):
            client.publish(topic, data, qos=qos, retain=True)
        assert not client._pending

    def test_one_bad_entry_cannot_kill_the_flush(self, client):
        # Defense in depth: paho rejecting something validation let through
        # must not end the loop or drop the rest of the hold.
        client.publish("a/bad", "v", qos=1, retain=True)
        client.publish("a/good", "v", qos=1, retain=True)
        fake = client._fake
        real = fake.publish

        def reject_bad(topic, *a):
            if topic == "a/bad":
                raise ValueError("rejected")
            return real(topic, *a)

        fake.publish = reject_bad
        connect(client)
        assert topics(client) == ["a/good"]
        assert client._link_ready


class TestPublishAndFlushFromTheDisconnectCallback:
    def test_returns_false_without_needing_the_publish_lock(self, client):
        connect(client)
        result = []
        client.on_disconnect_callback = lambda rc: result.append(
            client.publish_and_flush("a/state", "lost", qos=1, retain=True, timeout=0.1)
        )
        client._fake.connected = False
        assert disconnect_while_another_thread_holds_the_publish_lock(client)
        assert result == [False]

    def test_mqttv5_disconnect_with_properties_marks_the_link_down(self, client):
        # paho passes a v5 on_disconnect a trailing properties argument; a
        # TypeError there would leave the link marked ready after a drop.
        connect(client)
        client._fake.connected = False
        client._on_disconnect(client._fake, None, 1, None)
        assert not client._link_ready
        client.publish("a/state", "v", qos=1, retain=True)
        assert "a/state" in client._pending


class TestStopped:
    def test_nothing_is_held_after_stop(self, client):
        client.stop(timeout=0.1)
        info = client.publish("a/state", "ready", qos=1, retain=True)
        assert info.rc == mqtt.MQTT_ERR_NO_CONN
        assert not client._pending
        with pytest.raises(RuntimeError):
            info.wait_for_publish(1)

    def test_start_holds_again(self, client):
        client.stop(timeout=0.1)
        client.start()
        client.publish("a/state", "ready", qos=1, retain=True)
        assert "a/state" in client._pending


class TestValidationMatchesPaho:
    def test_a_bytes_topic_raises_as_paho_does(self, client):
        with pytest.raises(AttributeError):
            client.publish(b"a/b", "v", qos=1, retain=True)

    def test_an_empty_topic_is_allowed_under_v5_only(self, monkeypatch):
        fake = FakePaho()
        monkeypatch.setattr("ebus_mqtt_client.client.mqtt.Client", lambda *a, **kw: fake)
        v5 = MqttClient(client_id="v5", endpoint="127.0.0.1", port=1883, v5=True)
        v5.publish("", "v", qos=1, retain=True)
        v3 = MqttClient(client_id="v3", endpoint="127.0.0.1", port=1883)
        with pytest.raises(ValueError):
            v3.publish("", "v", qos=1, retain=True)


class TestPahosOwnQueue:
    def test_a_qos12_publish_refused_in_the_drop_window_is_left_to_paho(self, client):
        # Between the link dropping and on_disconnect, paho refuses a publish
        # but stores it at QoS 1 and 2 and resends it itself; holding it too
        # would send it twice.
        connect(client)
        client._fake.refuse_sends = True
        info = client.publish("a/state", "v", qos=1, retain=True)
        assert info.rc == mqtt.MQTT_ERR_NO_CONN
        assert not client._pending
        assert client._fake.queued == [("a/state", "v", 1, True)]
        client._fake.connected = False
        client._on_disconnect(client._fake, None, 1)
        connect(client)
        assert payloads(client) == ["v"]

    def test_a_qos0_retained_publish_refused_in_the_drop_window_is_held(self, client):
        connect(client)
        client._fake.refuse_sends = True
        client.publish("a/state", "v", qos=0, retain=True)
        assert "a/state" in client._pending
