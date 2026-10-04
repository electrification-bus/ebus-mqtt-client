"""End-to-end against a real mosquitto, for what only a broker can show (GH #20).

paho replays publishes it queued while disconnected in one burst on CONNACK,
ignoring ``max_inflight_messages`` and landing after ``on_connect``. At QoS 2,
mosquitto (``max_inflight_messages``, default 20) acknowledges what exceeds its
receive quota from an MQTT 3.1.1 client and discards it, so retained state ended
on whichever value survived. A fake paho cannot show that, so these run a broker.

The mTLS tests (deferred since 0.1.5 for want of a broker fixture) run real
handshakes against a broker that requires client certificates, which the mocked
``load_cert_chain`` tests in ``test_client.py`` cannot: certificate and key from
files and from memory, decrypting a password-protected key, the broker refusing
a client without a certificate, and the client refusing a server certificate
from a CA it does not trust.

Skipped when no ``mosquitto`` binary is found, and the mTLS tests also when no
``openssl`` binary is found. Each test starts its own broker on a free port bound
to 127.0.0.1 and stops it afterwards.
"""

import contextlib
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time

import paho.mqtt.client as mqtt
import pytest

from ebus_mqtt_client import MqttClient

pytestmark = pytest.mark.filterwarnings("ignore:Callback API version 1 is deprecated")

MOSQUITTO = next(
    (
        p
        for p in (
            shutil.which("mosquitto"),
            "/usr/sbin/mosquitto",
            "/opt/homebrew/sbin/mosquitto",
            "/usr/local/sbin/mosquitto",
        )
        if p and os.access(p, os.X_OK)
    ),
    None,
)

if MOSQUITTO is None:
    pytest.skip("mosquitto not installed", allow_module_level=True)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@contextlib.contextmanager
def _mosquitto(extra_conf: str = ""):
    """Run mosquitto on a free 127.0.0.1 port for the duration; yield the port."""
    port = _free_port()
    with tempfile.NamedTemporaryFile("w", suffix=".conf", delete=False) as f:
        f.write(f"listener {port} 127.0.0.1\nallow_anonymous true\n{extra_conf}")
        conf = f.name
    proc = subprocess.Popen(
        [MOSQUITTO, "-c", conf], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                break
        time.sleep(0.05)
    else:
        proc.kill()
        pytest.fail("mosquitto did not start")
    try:
        yield port
    finally:
        proc.terminate()
        proc.wait(5)
        os.unlink(conf)


@pytest.fixture
def broker():
    with _mosquitto() as port:
        yield port


def _client(port, **kw) -> MqttClient:
    return MqttClient(client_id=f"t-{time.monotonic_ns()}", endpoint="127.0.0.1", port=port, **kw)


def _wait(cond, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.02)
    return False


def _retained(port, topic_filter, settle=1.0) -> dict[str, str]:
    """Every retained value under ``topic_filter``, read by a fresh subscriber."""
    got: dict[str, str] = {}
    reader = mqtt.Client(client_id=f"reader-{time.monotonic_ns()}")
    reader.on_connect = lambda c, *_: c.subscribe(topic_filter, 1)
    reader.on_message = lambda c, u, m: got.__setitem__(m.topic, m.payload.decode())
    reader.connect("127.0.0.1", port)
    reader.loop_start()
    time.sleep(settle)
    reader.loop_stop()
    reader.disconnect()
    return got


def test_a_pre_connect_burst_past_the_broker_quota_ends_on_the_last_value(broker):
    # 40 QoS 2 publishes before connect: twice mosquitto's default quota of 20.
    # paho's replay sent them all at once and the broker discarded the excess,
    # including the final value of t/state.
    c = _client(broker)
    for i in range(1, 31):
        c.publish("t/state", str(i), qos=2, retain=True)
    for i in range(10):
        c.publish(f"t/other/{i}", str(i), qos=2, retain=True)
    c.start()
    try:
        assert _wait(c.is_connected)
        got = _retained(broker, "t/#")
    finally:
        c.stop()
    assert got["t/state"] == "30"
    assert {k: v for k, v in got.items() if k.startswith("t/other/")} == {
        f"t/other/{i}": str(i) for i in range(10)
    }


def test_what_on_connect_publishes_is_not_overwritten_by_an_older_value(broker):
    # paho calls on_connect before it replays its queue, so the stale value it
    # queued while disconnected used to land on top of the fresh republish.
    c = _client(broker)
    c.on_connect_callback = lambda: c.publish("t/state", "fresh", qos=2, retain=True)
    c.publish("t/state", "stale", qos=2, retain=True)
    c.start()
    try:
        assert _wait(c.is_connected)
        got = _retained(broker, "t/state")
    finally:
        c.stop()
    assert got == {"t/state": "fresh"}


def test_a_large_on_connect_republish_lands_completely(broker):
    # The shape of a device tree republishing on connect: well past the quota,
    # on a live link, where paho's inflight cap applies.
    n = 120
    done = threading.Event()

    def republish():
        for i in range(n):
            c.publish(f"t/prop/{i}", str(i), qos=2, retain=True)
        done.set()

    c = _client(broker, on_connect_callback=republish)
    c.start()
    try:
        assert _wait(c.is_connected) and done.wait(5)
        got = _retained(broker, "t/prop/#", settle=2.0)
    finally:
        c.stop()
    assert got == {f"t/prop/{i}": str(i) for i in range(n)}


def test_a_publish_between_connack_and_the_flush_does_not_lose_to_the_hold(broker):
    # paho reports connected at CONNACK, before on_connect runs the flush. A
    # publish in that gap must join the hold, not overtake it, or the older held
    # value lands on top of it and a held event arrives after a live one.
    c = _client(broker)
    c.publish("t/state", "old", qos=1, retain=True)
    c.publish("t/event", "1", qos=1)
    flush = c._flush_pending

    def app_thread_publishes_first(*a, **kw):
        t = threading.Thread(
            target=lambda: (
                c.publish("t/state", "new", qos=1, retain=True),
                c.publish("t/event", "2", qos=1),
            )
        )
        t.start()
        t.join(5)
        return flush(*a, **kw)

    c._flush_pending = app_thread_publishes_first
    events: list[str] = []
    watcher = mqtt.Client(client_id=f"watch-{time.monotonic_ns()}")
    watcher.on_connect = lambda w, *_: w.subscribe("t/event", 1)
    watcher.on_message = lambda w, u, m: events.append(m.payload.decode())
    watcher.connect("127.0.0.1", broker)
    watcher.loop_start()
    time.sleep(0.3)
    c.start()
    try:
        assert _wait(c.is_connected)
        got = _retained(broker, "t/state")
    finally:
        c.stop()
        watcher.loop_stop()
        watcher.disconnect()
    assert got == {"t/state": "new"}
    assert events == ["1", "2"]


def test_mqtt_v5_connects_and_flushes_the_hold(broker):
    # paho passes v5 callbacks a trailing properties argument; with the v3
    # signature, CONNACK raised TypeError and the hold was never flushed.
    called = threading.Event()
    c = _client(broker, v5=True, on_connect_callback=called.set)
    c.publish("t/v5", "held", qos=1, retain=True)
    c.start()
    try:
        assert _wait(c.is_connected) and called.wait(5)
        got = _retained(broker, "t/v5")
    finally:
        c.stop()
    assert got == {"t/v5": "held"}


def test_after_the_asyncio_driver_stops_nothing_is_held_or_resurrected(broker):
    import asyncio

    c = _client(broker)

    async def run():
        driver = c.asyncio_driver()
        await driver.start()
        assert await _await(c.is_connected) and await _await(
            lambda: getattr(c, "_link_ready", True)
        )
        await driver.stop()
        info = c.publish("t/after-stop", "v", qos=1, retain=True)
        assert info.rc == mqtt.MQTT_ERR_NO_CONN
        assert not getattr(c, "_pending", None)
        with pytest.raises(RuntimeError):
            info.wait_for_publish(1)
        restarted = c.asyncio_driver()
        await restarted.start()
        assert await _await(lambda: getattr(c, "_link_ready", True))
        await asyncio.sleep(0.3)
        await restarted.stop()

    asyncio.run(run())
    assert _retained(broker, "t/after-stop") == {}


async def _await(cond, timeout=5.0):
    import asyncio

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        await asyncio.sleep(0.02)
    return False


OPENSSL = shutil.which("openssl")
KEY_PASSWORD = "test-passphrase"


def _openssl(*args, cwd):
    subprocess.run([OPENSSL, *args], cwd=cwd, check=True, capture_output=True)


@pytest.fixture(scope="module")
def pki(tmp_path_factory):
    """A throwaway CA, a server cert for 127.0.0.1, and a client cert whose key
    exists both in the clear and encrypted with ``KEY_PASSWORD``."""
    if OPENSSL is None:
        pytest.skip("openssl not installed")
    d = tmp_path_factory.mktemp("pki")
    _openssl(
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-days",
        "1",
        "-subj",
        "/CN=test-ca",
        "-keyout",
        "ca.key",
        "-out",
        "ca.crt",
        cwd=d,
    )
    (d / "san.ext").write_text("subjectAltName=IP:127.0.0.1\n")
    for name, cn in (("server", "127.0.0.1"), ("client", "test-client")):
        _openssl(
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-subj",
            f"/CN={cn}",
            "-keyout",
            f"{name}.key",
            "-out",
            f"{name}.csr",
            cwd=d,
        )
        ext = ["-extfile", "san.ext"] if name == "server" else []
        _openssl(
            "x509",
            "-req",
            "-in",
            f"{name}.csr",
            "-CA",
            "ca.crt",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-days",
            "1",
            "-out",
            f"{name}.crt",
            *ext,
            cwd=d,
        )
    _openssl(
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-days",
        "1",
        "-subj",
        "/CN=other-ca",
        "-keyout",
        "other-ca.key",
        "-out",
        "other-ca.crt",
        cwd=d,
    )
    _openssl(
        "pkey",
        "-in",
        "client.key",
        "-aes256",
        "-passout",
        f"pass:{KEY_PASSWORD}",
        "-out",
        "client-enc.key",
        cwd=d,
    )
    return d


@pytest.fixture
def mtls_broker(pki):
    conf = (
        f"cafile {pki / 'ca.crt'}\ncertfile {pki / 'server.crt'}\n"
        f"keyfile {pki / 'server.key'}\nrequire_certificate true\n"
    )
    with _mosquitto(conf) as port:
        yield port


def _mtls_connects(port, **tls) -> bool:
    c = _client(port, use_tls=True, tls_insecure=False, **tls)
    c.start()
    try:
        return _wait(c.is_connected, timeout=3.0) and c.publish_and_flush("t/mtls", "ok", qos=1)
    finally:
        c.stop()


def test_mtls_with_certificate_and_key_paths(mtls_broker, pki):
    assert _mtls_connects(
        mtls_broker,
        tls_ca_cert=str(pki / "ca.crt"),
        tls_client_cert=str(pki / "client.crt"),
        tls_client_key=str(pki / "client.key"),
    )


def test_mtls_with_in_memory_certificate_and_key(mtls_broker, pki):
    assert _mtls_connects(
        mtls_broker,
        tls_ca_data=(pki / "ca.crt").read_text(),
        tls_client_cert_data=(pki / "client.crt").read_text(),
        tls_client_key_data=(pki / "client.key").read_text(),
    )


def test_mtls_with_a_password_protected_key(mtls_broker, pki):
    assert _mtls_connects(
        mtls_broker,
        tls_ca_cert=str(pki / "ca.crt"),
        tls_client_cert=str(pki / "client.crt"),
        tls_client_key=str(pki / "client-enc.key"),
        tls_client_key_password=KEY_PASSWORD,
    )


# paho 1.x raises the handshake failure out of its network thread, which pytest
# reports as an unhandled thread exception. The client never connects.
@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_mtls_broker_refuses_a_client_without_a_certificate(mtls_broker, pki):
    # Without this, the three tests above would also pass against a broker
    # that never asked for a client certificate.
    assert not _mtls_connects(mtls_broker, tls_ca_cert=str(pki / "ca.crt"))


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_mtls_client_refuses_a_server_certificate_from_an_untrusted_ca(mtls_broker, pki):
    assert not _mtls_connects(
        mtls_broker,
        tls_ca_cert=str(pki / "other-ca.crt"),
        tls_client_cert=str(pki / "client.crt"),
        tls_client_key=str(pki / "client.key"),
    )
