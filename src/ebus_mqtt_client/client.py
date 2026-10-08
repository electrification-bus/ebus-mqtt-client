import contextlib
import logging
import os
import ssl
import tempfile
import threading
from collections import OrderedDict
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import paho.mqtt.client as mqtt
import paho.mqtt.matcher as matcher

if TYPE_CHECKING:
    import asyncio
    import concurrent.futures

    from ebus_mqtt_client.asyncio_driver import AsyncioMqttDriver

# Default broker configuration
MQTT_DEFAULT_HOST = "127.0.0.1"
MQTT_DEFAULT_PORT = 1883

# Authentication types
AUTH_TYPE_USER_PASS = "USER_PASS"

# How many not-yet-connected publishes to hold (MqttClient.publish): one per
# retained topic, one per non-retained QoS 1/2 event.
# Sized to cover a whole device tree, which is the realistic worst case, while
# still being a bound: a client that never connects must not grow forever.
PENDING_LIMIT = 512


def _validate_publish(topic: Any, data: Any, qos: Any, v5: bool) -> None:
    """Raise what paho's publish() raises for arguments it would reject.

    A publish held while the link is down never reaches paho until the flush,
    which runs on paho's network thread, where an exception would end the loop.
    Checking here keeps the error with the caller, as paho itself does. Mirrors
    paho 2.1.0 ``Client.publish`` and ``_encode_payload`` (1.6.1 is the same).
    """
    if not v5 and (topic is None or len(topic) == 0):
        raise ValueError("Invalid topic.")
    topic_bytes = topic.encode("utf-8")
    if b"+" in topic_bytes or b"#" in topic_bytes:
        raise ValueError("Publish topic cannot contain wildcards.")
    if len(topic_bytes) > 65535:
        raise ValueError("Publish topic is too long.")
    if qos < 0 or qos > 2:
        raise ValueError("Invalid QoS level.")
    if isinstance(data, str):
        size = len(data.encode("utf-8"))
    elif isinstance(data, (int, float)):
        size = len(str(data))
    elif data is None:
        size = 0
    elif isinstance(data, (bytes, bytearray)):
        size = len(data)
    else:
        raise TypeError("payload must be a string, bytearray, int, float or None.")
    if size > 268435455:
        raise ValueError("Payload too large.")


def _filters_overlap(a: str, b: str) -> bool:
    """True when some topic matches both filters ``a`` and ``b``.

    Follows MQTT matching: ``+`` matches one level, ``#`` matches the parent
    level and everything below it, and a wildcard in the first level does not
    match a topic that starts with ``$``.
    """
    la, lb = a.split("/"), b.split("/")
    for i, (x, y) in enumerate(zip(la, lb, strict=False)):
        if x in ("#", "+") or y in ("#", "+"):
            if i == 0 and (x.startswith("$") or y.startswith("$")):
                return False
            if x == "#" or y == "#":
                return True
            continue
        if x != y:
            return False
    if len(la) == len(lb):
        return True
    longer = la if len(la) > len(lb) else lb
    return len(longer) == min(len(la), len(lb)) + 1 and longer[-1] == "#"


class MqttClient:
    """MQTT client wrapper around paho-mqtt.

    Provides TLS support (secure, insecure, and none), automatic reconnection
    with backoff, subscription recovery on reconnect, topic pattern matching
    via paho's MQTTMatcher, and Last Will and Testament (LWT) support.

    Construction is decoupled from broker availability: ``__init__`` registers
    the connection target with ``connect_async`` but does not open a socket, so a
    down or briefly-unavailable broker never makes construction block or raise.
    The actual connect happens on the network thread started by :meth:`start`,
    which keeps retrying (with the reconnect backoff) until the broker appears.
    Use :meth:`is_connected` to observe when the link is up.

    Because of that deferral, a retained publish issued while the link is down is
    held (newest value per topic, bounded) and flushed on connect, ahead of
    ``on_connect_callback``, rather than dropped or left to paho's replay. A
    non-retained one is held in order at QoS 1 and 2 and dropped at QoS 0; see
    :meth:`publish`.
    """

    def __init__(
        self,
        client_id: str,
        endpoint: str,
        port: int,
        callback: Callable[[bytes | bytearray, Any], None] | None = None,
        username=None,
        password=None,
        use_tls: bool | None = False,
        tls_ca_cert: str | None = None,
        tls_ca_data: str | bytes | None = None,
        tls_insecure: bool | None = True,
        tls_client_cert: str | None = None,
        tls_client_cert_data: str | bytes | None = None,
        tls_client_key: str | None = None,
        tls_client_key_data: str | bytes | None = None,
        tls_client_key_password: str | None = None,
        v5: bool | None = False,
        lwt: dict | None = None,
        on_connect_callback: Callable | None = None,
        on_disconnect_callback: Callable | None = None,
    ):
        self.client_id = client_id
        self._v5 = bool(v5)
        try:
            if v5:
                self.mqttc = mqtt.Client(client_id=self.client_id, protocol=mqtt.MQTTv5)
            else:
                self.mqttc = mqtt.Client(client_id=self.client_id)
        except Exception:
            logging.exception("reason=mqttClientInstantiationException")

        # Last Will and Testament
        if lwt:
            self.lwt_topic = lwt.get("topic", None)
            self.lwt_payload = lwt.get("payload", None)
            self.lwt_retain = lwt.get("retain", True)
            self.lwt_qos = lwt.get("qos", 0)
            if self.lwt_topic and self.lwt_payload:
                self.mqttc.will_set(
                    topic=self.lwt_topic,
                    payload=self.lwt_payload,
                    retain=self.lwt_retain,
                    qos=self.lwt_qos,
                )

        self.mqttc.reconnect_delay_set(min_delay=1, max_delay=30)
        self.mqttc.on_connect = self._on_connect
        self.mqttc.on_disconnect = self._on_disconnect
        self.mqttc.on_message = self._on_message
        self.mqttc.user_data_set(callback)
        self.sub_callbacks: dict[str, tuple[Any, int]] = {}
        # What _on_message delivers per filter: (param, with_retain). One
        # assignment writes both and one lookup reads both, so paho's network
        # thread never pairs a resubscribe's new param with the old flag.
        self._sub_delivery: dict[str, tuple[Any, bool]] = {}
        self.sub_matcher = matcher.MQTTMatcher()
        self.on_connect_callback = on_connect_callback
        self.on_disconnect_callback = on_disconnect_callback

        self.is_running = False
        # Publishes issued while the link is down. A retained publish is keyed by
        # its topic (newest value wins); a non-retained one by a unique
        # ("event", n) key, so events keep their order and are never merged.
        # Values are (topic, data, qos, retain). See publish().
        self._pending: OrderedDict[str | tuple[str, int], tuple[str, Any, int, bool]] = (
            OrderedDict()
        )
        self._pending_seq = 0
        self._pending_dropped = 0
        # How far entries put back in front of the hold by an interrupted flush
        # widen pending_limit until the next flush.
        self._pending_extra = 0
        # Set by stop(), cleared by start(): nothing is held while stopped.
        self._stopped = False
        # Whether publishes may go to paho: set when the flush on connect takes
        # the hold (under _pending_lock, so later publishes queue behind it),
        # cleared on disconnect. Distinct from paho's is_connected(), which turns
        # true at CONNACK, before the flush.
        self._link_ready = False
        # Lock order, never reversed: _pending_lock, then _hold_lock.
        # _pending_lock serialises every publish that reaches paho against the
        # flush. _hold_lock guards the hold and _link_ready and is never held
        # across a call into paho, so the not-ready path can take it from inside
        # a paho callback, which paho may run while holding its own locks.
        self._pending_lock = threading.Lock()
        self._hold_lock = threading.Lock()
        # Per-instance so a consumer can tune the bound without subclassing.
        self.pending_limit = PENDING_LIMIT
        if username and password:
            self.mqttc.username_pw_set(username, password)
        if use_tls:
            if (tls_ca_cert or tls_ca_data) and not tls_insecure:
                # Verify server certificate against provided CA cert
                if tls_ca_data:
                    logging.info("reason=mqttClientTlsSecure,ca_data=provided")
                    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                    context.load_verify_locations(cadata=tls_ca_data)
                else:
                    logging.info(f"reason=mqttClientTlsSecure,ca_cert={tls_ca_cert}")
                    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                    context.load_verify_locations(cafile=tls_ca_cert)
                self._load_client_cert_chain(
                    context,
                    tls_client_cert,
                    tls_client_cert_data,
                    tls_client_key,
                    tls_client_key_data,
                    tls_client_key_password,
                )
                self.mqttc.tls_set_context(context)
                self.mqttc.tls_insecure_set(False)
            else:
                # Insecure mode - skip certificate verification
                logging.info("reason=mqttClientTlsInsecure")
                context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                self._load_client_cert_chain(
                    context,
                    tls_client_cert,
                    tls_client_cert_data,
                    tls_client_key,
                    tls_client_key_data,
                    tls_client_key_password,
                )
                self.mqttc.tls_set_context(context)
                self.mqttc.tls_insecure_set(True)
        # Resilient connect: use connect_async (which never blocks and never
        # raises on a down or unreachable broker) instead of the synchronous
        # connect(). The real TCP/MQTT connect runs on the network thread started
        # by start() -> loop_start(), which retries the first connection using the
        # reconnect backoff set above until the broker becomes reachable. This
        # decouples construction from broker availability: a broker that is briefly
        # unavailable at construction time (startup, restart, network blip) no
        # longer yields a silent, never-connecting zombie publisher. Guard the call
        # so construction stays exception-free even on bad connection parameters.
        try:
            self.mqttc.connect_async(endpoint, port, keepalive=60)
        except Exception:
            logging.exception(f"reason=mqttClientConnectAsyncException,client={self.client_id}")

    @staticmethod
    def _load_client_cert_chain(
        context: ssl.SSLContext,
        client_cert: str | None,
        client_cert_data: str | bytes | None,
        client_key: str | None,
        client_key_data: str | bytes | None,
        client_key_password: str | None,
    ) -> None:
        """Load a client certificate (and optional key) into the SSL context for mTLS.

        Prefers the in-memory ``*_data`` form over the file-path form when both are
        supplied (parallel to the CA cert precedence). ``ssl.SSLContext.load_cert_chain``
        only accepts file paths, so in-memory data is materialised to a 0600 temp file
        for the duration of the load and then unlinked.
        """
        if not any((client_cert, client_cert_data, client_key, client_key_data)):
            return

        if client_cert and client_cert_data:
            logging.warning("reason=mqttClientTlsClientCertConflict,using=data,ignored=path")
        if client_key and client_key_data:
            logging.warning("reason=mqttClientTlsClientKeyConflict,using=data,ignored=path")

        cert_data = client_cert_data if client_cert_data is not None else None
        key_data = client_key_data if client_key_data is not None else None
        cert_path = client_cert if cert_data is None else None
        key_path = client_key if key_data is None else None

        tmp_paths = []
        try:
            if cert_data is not None:
                cert_path = MqttClient._materialise_pem(cert_data, suffix=".crt")
                tmp_paths.append(cert_path)
            if key_data is not None:
                key_path = MqttClient._materialise_pem(key_data, suffix=".key")
                tmp_paths.append(key_path)

            if cert_path is None:
                # A client key with no client cert cannot form a certificate
                # chain (load_cert_chain requires a certfile), so there is
                # nothing to load. Log and skip rather than call load_cert_chain
                # with a missing certfile. The client cert is not loaded, so mTLS
                # is effectively not configured; if the broker requires a client
                # cert the TLS handshake will fail later. The finally block still
                # unlinks any key temp file materialised above.
                logging.warning(
                    "reason=mqttClientTlsClientKeyWithoutCert,effect=clientCertNotLoaded,mtls=disabled"
                )
                return

            logging.info(
                "reason=mqttClientTlsClientCertLoaded,cert=%s,key=%s",
                "data" if cert_data is not None else cert_path,
                "data" if key_data is not None else (key_path or "inline-with-cert"),
            )
            context.load_cert_chain(
                certfile=cert_path,
                keyfile=key_path,
                password=client_key_password,
            )
        finally:
            for path in tmp_paths:
                with contextlib.suppress(OSError):
                    os.unlink(path)

    @staticmethod
    def _materialise_pem(data: str | bytes, suffix: str) -> str:
        """Write PEM/DER bytes or string to a 0600 temp file and return its path."""
        payload = data.encode("utf-8") if isinstance(data, str) else data
        fd, path = tempfile.mkstemp(suffix=suffix, prefix="ebus-mqtt-")
        try:
            os.write(fd, payload)
        finally:
            os.close(fd)
        return path

    @classmethod
    def from_config(
        cls,
        mqtt_cfg: dict,
        client_id: str,
        callback: Callable[[bytes | bytearray, Any], None] | None = None,
        lwt: dict | None = None,
        on_connect_callback: Callable | None = None,
        on_disconnect_callback: Callable | None = None,
    ) -> "MqttClient":
        """Create an MqttClient from a configuration dictionary.

        Args:
            mqtt_cfg: Configuration dictionary with keys:
                - host: Broker hostname/IP (default: '127.0.0.1')
                - port: Broker port (default: 1883)
                - use_tls: Enable TLS (default: False)
                - tls_ca_cert: Path to CA certificate file (optional)
                - tls_ca_data: CA certificate content as PEM string or DER bytes (optional)
                - tls_insecure: Skip certificate verification (default: True)
                - tls_client_cert: Path to client certificate PEM (optional, mTLS)
                - tls_client_cert_data: Client cert as PEM string or DER bytes (optional, mTLS)
                - tls_client_key: Path to client private key PEM (optional, mTLS)
                - tls_client_key_data: Client key as PEM string or DER bytes (optional, mTLS)
                - tls_client_key_password: Passphrase for an encrypted client key (optional)
                - authentication: Dict with 'type', 'username', 'password' (optional)
            client_id: MQTT client identifier
            callback: Message callback function (optional)
            lwt: Last Will and Testament dict (optional)
            on_connect_callback: Callback invoked on successful connection (optional)
            on_disconnect_callback: Callback invoked on disconnect, receiving the
                paho reason code as its single argument (optional)

        Returns:
            Configured MqttClient instance
        """
        endpoint = mqtt_cfg.get("host", MQTT_DEFAULT_HOST)
        port = mqtt_cfg.get("port", MQTT_DEFAULT_PORT)
        use_tls = mqtt_cfg.get("use_tls", False)
        tls_ca_cert = mqtt_cfg.get("tls_ca_cert")
        tls_ca_data = mqtt_cfg.get("tls_ca_data")
        tls_insecure = mqtt_cfg.get("tls_insecure", True)
        tls_client_cert = mqtt_cfg.get("tls_client_cert")
        tls_client_cert_data = mqtt_cfg.get("tls_client_cert_data")
        tls_client_key = mqtt_cfg.get("tls_client_key")
        tls_client_key_data = mqtt_cfg.get("tls_client_key_data")
        tls_client_key_password = mqtt_cfg.get("tls_client_key_password")

        # Extract authentication credentials
        username = None
        password = None
        auth = mqtt_cfg.get("authentication", {})
        if auth.get("type") == AUTH_TYPE_USER_PASS:
            username = auth.get("username")
            password = auth.get("password")

        logging.info(
            f"reason=mqttClientFromConfig,host={endpoint},port={port},useTls={use_tls},clientID={client_id}"
        )

        return cls(
            client_id=client_id,
            endpoint=endpoint,
            port=port,
            callback=callback,
            username=username,
            password=password,
            use_tls=use_tls,
            tls_ca_cert=tls_ca_cert,
            tls_ca_data=tls_ca_data,
            tls_insecure=tls_insecure,
            tls_client_cert=tls_client_cert,
            tls_client_cert_data=tls_client_cert_data,
            tls_client_key=tls_client_key,
            tls_client_key_data=tls_client_key_data,
            tls_client_key_password=tls_client_key_password,
            lwt=lwt or {},
            on_connect_callback=on_connect_callback,
            on_disconnect_callback=on_disconnect_callback,
        )

    def is_connected(self):
        """Check if MQTT client is connected."""
        return self.mqttc.is_connected() if hasattr(self, "mqttc") else False

    def start(self, blocking=False):
        self.is_running = True
        self._resume_hold()
        if blocking:
            self.mqttc.loop_forever()
        else:
            self.mqttc.loop_start()

    def asyncio_driver(
        self,
        loop: "asyncio.AbstractEventLoop | None" = None,
        executor: "concurrent.futures.Executor | None" = None,
    ) -> "AsyncioMqttDriver":
        """Return an :class:`AsyncioMqttDriver` bound to this client.

        Optional loop-native alternative to :meth:`start`: drives paho's network
        loop on an asyncio event loop instead of a background thread. Mutually
        exclusive with :meth:`start` per instance. The driver module is imported
        lazily here, so a thread-mode consumer never loads it.

        Call this from within a running event loop, or pass ``loop=`` explicitly;
        with ``loop=None`` the driver resolves the loop via
        ``asyncio.get_running_loop()`` and raises off-loop.
        """
        from ebus_mqtt_client.asyncio_driver import AsyncioMqttDriver

        return AsyncioMqttDriver(self, loop, executor)

    def stop(self, timeout: float = 2.0):
        """Stop the client within a small, broker-independent time bound.

        Shutdown must not depend on the broker being reachable. On a coordinated
        gateway reboot the broker can stop before its consumers, so stop() may run
        against a dead broker; a stop() that blocks then eats the service's
        SIGTERM budget and stalls the reboot.

        paho's ``loop_stop()`` joins the network thread with no timeout, which can
        stall the caller if that thread is wedged in a socket op or a reconnect
        backoff sleep. The paho 2.x network thread is a daemon, so it never blocks
        interpreter exit; we therefore run the potentially-blocking disconnect and
        loop_stop() in a helper thread and bound our wait on it with ``timeout``.
        If shutdown does not finish in time we return anyway and rely on the daemon
        thread plus the LWT (set at construction) to signal that we are gone. We do
        a best-effort clean DISCONNECT (to suppress the LWT when the broker is
        alive) but never depend on the broker ACKing it.
        """
        self.is_running = False
        if hasattr(self, "mqttc"):
            # Shorten any future reconnect backoff so the network thread is not
            # parked in a long sleep we would otherwise have to wait out.
            with contextlib.suppress(Exception):
                self.mqttc.reconnect_delay_set(min_delay=1, max_delay=1)
            shutdown = threading.Thread(
                target=self._shutdown_mqttc,
                name=f"mqtt-stop-{self.client_id}",
                daemon=True,
            )
            shutdown.start()
            shutdown.join(timeout)
            if shutdown.is_alive():
                logging.warning(f"reason=mqttStopTimeout,client={self.client_id},timeout={timeout}")
        # Drop anything still held: a stopped client has no connect left to
        # flush it on, and replaying it if the caller starts the client again
        # would resurrect state from before the stop.
        self._discard_hold()
        # Release subscription callbacks and matcher to free memory
        self.sub_callbacks.clear()
        self._sub_delivery.clear()
        self.sub_matcher = matcher.MQTTMatcher()
        self.on_connect_callback = None
        self.on_disconnect_callback = None

    def _discard_hold(self) -> None:
        """Drop everything held and hold nothing until :meth:`_resume_hold`.

        Called by both stop paths (this client's and the asyncio driver's): a
        stopped client has no connect left to flush on, and replaying held
        state if it is started again would resurrect state from before the stop.
        """
        with self._hold_lock:
            self._link_ready = False
            self._stopped = True
            self._pending.clear()
            self._pending_dropped = 0
            self._pending_extra = 0

    def _resume_hold(self) -> None:
        """Hold publishes again while the link is down (both start paths)."""
        with self._hold_lock:
            self._stopped = False

    def _shutdown_mqttc(self):
        """Best-effort disconnect + loop_stop; runs in a bounded helper thread."""
        with contextlib.suppress(Exception):
            self.mqttc.disconnect()
        try:
            self.mqttc.loop_stop()
        except Exception:
            logging.warning(f"reason=mqttLoopStopException,client={self.client_id}", exc_info=True)

    def publish(
        self, topic: str, data: str, qos: int = 1, retain: bool = False
    ) -> mqtt.MQTTMessageInfo | None:
        """Publish a message and return an MQTTMessageInfo (or None if no client).

        Returning the message info lets a caller optionally wait for the message
        to be flushed to the broker via ``msg_info.wait_for_publish(timeout)``.
        See :meth:`publish_and_flush` for a bounded convenience wrapper.

        Publishing while the link is down is an expected condition here, not a
        fault: ``__init__`` deliberately uses ``connect_async``, so CONNACK does
        not arrive until the network loop started by :meth:`start` gets it. The
        link counts as down from the disconnect until the flush on the next
        connect takes the hold, so a publish issued after CONNACK but before the
        flush joins the hold behind the older values rather than overtaking
        them. Such a publish is never handed to paho and never logged as a
        failure. It is held here (see :meth:`_hold`) and flushed on connect,
        before the subscription recovery and before ``on_connect_callback``:

        * a **retained** publish, at any QoS, is held newest-value-per-topic.
          Retained messages are state, and state that was true before the link
          came up is still true after it, but only the latest value of it.
        * a **non-retained** publish at **QoS 1 or 2** is held in order, one
          entry per publish, so every event is delivered once.
        * a **non-retained** publish at **QoS 0** is dropped: delivering a
          fire-and-forget event after an arbitrary delay announces something
          that was true once, which is worse than not delivering it.

        paho is kept out of this because of what it does with a QoS 1 or 2
        publish it refuses: it stores it and, on CONNACK, replays its whole queue
        in one burst that ignores ``max_inflight_messages`` and lands after
        anything ``on_connect`` published, so a stale value can overwrite a fresh
        one. At QoS 2 the burst can also exceed the broker's receive quota
        (mosquitto's ``max_inflight_messages``, default 20); mosquitto
        acknowledges the excess from an MQTT 3.1.1 client and discards it
        (GH #20). paho still replays what it accepted on a live link and had not
        finished delivering when the link dropped, and a QoS 1 or 2 publish it
        refuses in the moment between the drop and ``on_disconnect``.

        Arguments paho would reject (a non-``str`` or wildcard topic, an empty
        topic under MQTT 3.1.1, a QoS outside 0-2, a payload paho cannot encode)
        raise here, with the exception paho itself raises, whether or not the
        publish is held. Every other failure still warns, and reports which
        result code. A held publish returns an ``MQTTMessageInfo`` with
        ``rc == MQTT_ERR_NO_CONN``, as paho reports a refused publish; the
        message is published afresh when the hold is flushed, so that info never
        completes.
        """
        if not hasattr(self, "mqttc"):
            logging.error(f"reason=mqttPublishNoClient,client={self.client_id},topic={topic}")
            return None
        _validate_publish(topic, data, qos, self._v5)
        # While the link is down, hold without touching paho or _pending_lock.
        # This is the path a publish from on_disconnect_callback takes, which
        # can run while paho holds its own out-queue mutex; taking _pending_lock
        # there could deadlock against a thread already inside paho's publish.
        with self._hold_lock:
            if not self._link_ready:
                return self._hold_or_drop(topic, data, qos, retain)
        # Held under the same lock as the flush so a publish issued while a
        # flush is in flight cannot be overtaken by the older value being
        # flushed for that topic; see _flush_pending.
        with self._pending_lock:
            msg_info = self._send(topic, data, qos, retain)
            if msg_info is None or (msg_info.rc == mqtt.MQTT_ERR_NO_CONN and qos == 0):
                with self._hold_lock:
                    return self._hold_or_drop(topic, data, qos, retain)
        if msg_info.rc == mqtt.MQTT_ERR_NO_CONN:
            # The link dropped and paho noticed before on_disconnect told us. At
            # QoS 1 and 2 paho has stored the message and will resend it itself.
            logging.debug(
                f"reason=mqttPublishNotConnected,client={self.client_id},"
                f"topic={topic},qos={qos},disposition=queuedByPaho"
            )
        elif msg_info.rc != mqtt.MQTT_ERR_SUCCESS:
            logging.warning(
                f"reason=mqttPublishFail,client={self.client_id},topic={topic},rc={msg_info.rc}"
            )
        return msg_info

    def _send(self, topic: str, data: Any, qos: int, retain: bool) -> mqtt.MQTTMessageInfo | None:
        """Hand one publish to paho if the link is ready; never holds anything.

        Returns None when the link is not ready. Caller holds _pending_lock.
        """
        with self._hold_lock:
            if not self._link_ready or not self.mqttc.is_connected():
                return None
        return self.mqttc.publish(topic, data, qos, retain)

    def _hold_or_drop(self, topic: str, data: Any, qos: int, retain: bool) -> Any:
        """Hold a publish refused for want of a link, or drop it. Caller holds _hold_lock."""
        if (retain or qos > 0) and not self._stopped:
            self._hold(topic, data, qos, retain)
            disposition = "held"
        else:
            disposition = "dropped"
        logging.debug(
            f"reason=mqttPublishNotConnected,client={self.client_id},"
            f"topic={topic},qos={qos},disposition={disposition}"
        )
        msg_info = mqtt.MQTTMessageInfo(0)
        msg_info.rc = mqtt.MQTT_ERR_NO_CONN
        return msg_info

    def _hold(self, topic: str, data: Any, qos: int, retain: bool) -> None:
        """Keep a publish until the link is up. Caller holds _hold_lock.

        A retained publish is keyed by topic, keeping the newest value, which is
        not an optimisation: retained state is last-value-wins, so a queue that
        replayed every attempt in order could write a stale value on top of a
        newer one, leaving a device permanently announcing a state it had
        already left. A non-retained publish gets its own key, so events keep
        their order and none is merged away.

        Bounded by ``pending_limit``, evicting the oldest entry, so a client
        that never connects cannot grow forever. An overflow is reported once
        and then sampled, rather than once per drop.
        """
        key: str | tuple[str, int]
        if retain:
            key = topic
        else:
            key = ("event", self._pending_seq)
            self._pending_seq += 1
        limit = self.pending_limit + self._pending_extra
        if key not in self._pending and len(self._pending) >= limit:
            self._pending.popitem(last=False)
            self._note_pending_dropped()
        self._pending[key] = (topic, data, qos, retain)
        self._pending.move_to_end(key)

    def _note_pending_dropped(self) -> None:
        self._pending_dropped += 1
        if self._pending_dropped == 1 or self._pending_dropped % 100 == 0:
            logging.warning(
                f"reason=mqttPendingOverflow,client={self.client_id},"
                f"limit={self.pending_limit},dropped={self._pending_dropped}"
            )

    def _prepend(self, older: list[tuple[str, Any, int, bool]]) -> None:
        """Put entries an interrupted flush did not send back in front of the
        hold. Caller holds _hold_lock.

        A retained topic held in both keeps the newer (already held) value. The
        older entries were already accepted into the hold, so they widen
        ``pending_limit`` until the next flush rather than being evicted by
        what was held since. After :meth:`stop` they are discarded instead.
        """
        if self._stopped:
            return
        self._pending_extra += len(older)
        newer, self._pending = self._pending, OrderedDict()
        for entry in older:
            self._hold(*entry)
        for entry in newer.values():
            self._hold(*entry)

    def _flush_pending(self) -> None:
        """Publish what was held while disconnected, oldest first, then mark the
        link ready so later publishes go straight to paho.

        Runs on paho's network thread from :meth:`_on_connect`, under the same
        lock :meth:`publish` takes for a live link, so a caller publishing
        concurrently either lands in the hold (the link is not ready yet) or
        after the flush. Without that, a newer value could reach the broker
        ahead of an older held one for the same topic and be overwritten by it.

        The flush publishes on a live link, so paho applies
        ``max_inflight_messages`` to it and queues the excess, rather than
        bursting past the broker's receive quota.

        If the link drops mid-flush, what was not sent goes back in front of the
        hold rather than being lost, so it survives to the next connect.
        """
        with self._pending_lock:
            with self._hold_lock:
                if self._stopped:
                    return
                held, self._pending = self._pending, OrderedDict()
                self._pending_extra = 0
                self._link_ready = True
            if not held:
                return
            unsent: list[tuple[str, Any, int, bool]] = []
            failed = 0
            for entry in held.values():
                topic, data, qos, retain = entry
                try:
                    info = self._send(topic, data, qos, retain)
                except Exception:
                    # publish() validates before holding, so this is paho
                    # rejecting something unforeseen. Raising here would end
                    # paho's network loop and lose the rest of the hold.
                    logging.warning(
                        f"reason=mqttPublishInvalid,client={self.client_id},topic={topic}",
                        exc_info=True,
                    )
                    failed += 1
                    continue
                if info is None or (info.rc == mqtt.MQTT_ERR_NO_CONN and qos == 0):
                    unsent.append(entry)
                elif info.rc not in (mqtt.MQTT_ERR_SUCCESS, mqtt.MQTT_ERR_NO_CONN):
                    # NO_CONN at QoS 1 or 2: paho stored it and resends it itself.
                    failed += 1
                    logging.warning(
                        f"reason=mqttPublishFail,client={self.client_id},topic={topic},rc={info.rc}"
                    )
            if unsent:
                # The link dropped mid-flush. What was not sent predates anything
                # held since the drop, so it goes back in front of it.
                with self._hold_lock:
                    self._prepend(unsent)
            logging.info(
                f"reason=mqttPendingFlushed,client={self.client_id},"
                f"count={len(held)},unsent={len(unsent)},failed={failed}"
            )

    def publish_and_flush(
        self,
        topic: str,
        data: str,
        qos: int = 1,
        retain: bool = False,
        timeout: float = 1.0,
    ) -> bool:
        """Publish a message and wait, bounded, until it is flushed to the broker.

        Publishes ``data`` to ``topic`` and then blocks for at most ``timeout``
        seconds waiting for the message to be sent (``wait_for_publish``). Useful
        for landing a final retained message (e.g. a graceful state update) right
        before a clean disconnect, without resorting to a fixed sleep.

        Always bounded and safe: never blocks indefinitely, and never raises for
        the common failure modes. Returns True once the message is published;
        returns False immediately if there is no client or the link is not up
        (including after CONNACK, until the flush begins; a call made during
        the flush waits for it, then publishes), if the publish call itself
        fails, or if the flush does not complete within ``timeout``. It never
        holds the message.
        """
        if not hasattr(self, "mqttc"):
            logging.error(f"reason=mqttPublishFlushNoClient,client={self.client_id},topic={topic}")
            return False
        # Checked before _pending_lock, as in publish(), so a call from
        # on_disconnect_callback cannot deadlock (on_disconnect clears the flag
        # before invoking it).
        with self._hold_lock:
            ready = self._link_ready
        msg_info = None
        if ready:
            # Under _pending_lock, like publish(), so it cannot land ahead of an
            # older held value that the flush is about to send for the same topic.
            with self._pending_lock:
                msg_info = self._send(topic, data, qos, retain)
        if msg_info is None:
            logging.warning(
                f"reason=mqttPublishFlushNotConnected,client={self.client_id},topic={topic}"
            )
            return False
        if msg_info.rc != mqtt.MQTT_ERR_SUCCESS:
            logging.warning(f"reason=mqttPublishFail,client={self.client_id},topic={topic}")
            return False
        try:
            msg_info.wait_for_publish(timeout)
            # paho 1.6.1's wait returns, rather than raises, on a failure set
            # while waiting; is_published() raises on it in both majors.
            return bool(msg_info.is_published())
        except (RuntimeError, ValueError) as e:
            logging.warning(
                f"reason=mqttPublishFlushTimeout,client={self.client_id},topic={topic},err={e}"
            )
            return False

    def subscribe(self, sub: str, param: Any, qos: int = 1, *, with_retain: bool = False):
        """Subscribe to the topic filter ``sub``.

        A message matching ``sub`` is delivered as ``param(topic, payload)``, or,
        when the client was constructed with a ``callback``, as
        ``callback(topic, payload, param)``.

        With ``with_retain=True`` the delivery gains a trailing ``retained`` bool
        (paho's ``msg.retain``): ``param(topic, payload, retained)`` or
        ``callback(topic, payload, param, retained)``. Under MQTT 3.1.1 a broker
        sets the flag only when it replays a stored retained message to a new
        subscription, so ``retained`` is True for that replay and False for a
        message published while the subscription was already in place.

        A delivery carries no record of which subscription caused it, and each
        message is routed to one matching filter only. When filters overlap,
        ``retained`` is therefore also True for a replay caused by subscribing
        (or resubscribing, as reconnect recovery does) any other filter that
        overlaps this one, and a message may be routed to the other filter's
        callback instead. A ``with_retain`` subscription that overlaps another
        is logged as a warning.

        The subscription, ``with_retain`` included, is restored on reconnect.
        Subscribing to the same filter again replaces its ``param``, ``qos`` and
        ``with_retain``.
        """
        if not hasattr(self, "mqttc"):
            logging.error(f"reason=mqttSubscribeNoClient,client={self.client_id},sub={sub}")
            return
        for other, (_, other_with_retain) in list(self._sub_delivery.items()):
            if other != sub and (with_retain or other_with_retain) and _filters_overlap(sub, other):
                logging.warning(
                    f"reason=mqttSubscribeWithRetainOverlap,client={self.client_id},"
                    f"sub={sub},overlaps={other}"
                )
        self.sub_callbacks[sub] = (param, qos)
        self._sub_delivery[sub] = (param, with_retain)
        self.sub_matcher[sub] = sub
        self.mqttc.subscribe(sub, qos)

    def unsubscribe(self, sub: str) -> bool:
        """Unsubscribe from a previously-subscribed topic filter.

        Removes the local callback and matcher entry so a re-publish on the
        same filter won't dispatch, then sends UNSUBSCRIBE to the broker. The
        local cleanup also ensures the filter won't be re-subscribed by the
        on-reconnect recovery path.

        Returns True if the filter was known and removed; False otherwise. A
        no-op for unknown filters (matches paho's tolerant behavior).
        """
        if not hasattr(self, "mqttc"):
            logging.error(f"reason=mqttUnsubscribeNoClient,client={self.client_id},sub={sub}")
            return False
        if sub not in self.sub_callbacks:
            logging.debug(f"reason=mqttUnsubscribeUnknownSub,client={self.client_id},sub={sub}")
            return False
        del self.sub_callbacks[sub]
        del self._sub_delivery[sub]
        with contextlib.suppress(KeyError):
            del self.sub_matcher[sub]
        self.mqttc.unsubscribe(sub)
        logging.info(f"reason=mqttUnsubscribed,client={self.client_id},sub={sub}")
        return True

    def _on_connect(
        self, mqttc: mqtt.Client, userdata: Any, flags: Any, rc: Any, properties: Any = None
    ):
        # ``properties`` is passed by paho for MQTTv5 only (VERSION1 callbacks).
        if rc != 0:
            logging.warning(f"reason=mqttBrokerConnectRefused,client={self.client_id},rc={rc}")
            return
        logging.info(f"reason=mqttBrokerConnected,client={self.client_id}")

        # Before the subscriptions and before the caller's callback: whatever
        # was published while disconnected describes state that already exists,
        # so it belongs on the broker before anything reacts to being connected.
        # The callback's publishes reach paho after the flush, but a broker may
        # apply a QoS 2 message only at PUBREL (mosquitto does), so a retained
        # value the callback republishes wins over a flushed one only at the
        # same or a higher QoS.
        self._flush_pending()

        # Re-subscribe on reconnect (iterate shallow copy in case dict changes)
        for sub, (_, qos) in list(self.sub_callbacks.items()):
            result, msg_id = self.mqttc.subscribe(sub, qos)
            if result == mqtt.MQTT_ERR_SUCCESS:
                logging.info(f"reason=mqttSubscribeSuccess,client={self.client_id},sub={sub}")
            else:
                logging.warning(f"reason=mqttSubscribeFail,client={self.client_id},sub={sub}")
        # Invoke supplied on_connect_callback if provided
        if self.on_connect_callback:
            self.on_connect_callback()

    def _on_disconnect(self, mqttc: mqtt.Client, userdata: Any, rc: Any, properties: Any = None):
        # Before anything else, so a publish from here on (including from
        # on_disconnect_callback) is held rather than handed to paho.
        with self._hold_lock:
            self._link_ready = False
        if self.is_running and rc != mqtt.MQTT_ERR_SUCCESS:
            logging.warning(f"reason=mqttBrokerConnectionLost,rc={rc},client={self.client_id}")
        else:
            logging.info(f"reason=mqttBrokerDisconnected,client={self.client_id}")
        # Invoke supplied on_disconnect_callback if provided, passing the paho
        # reason code so the caller can distinguish a clean disconnect from a
        # dropped link. Best-effort: this runs on paho's network thread, so a
        # raising consumer callback must not kill the loop.
        if self.on_disconnect_callback:
            try:
                self.on_disconnect_callback(rc)
            except Exception:
                logging.warning(
                    f"reason=onDisconnectCallbackException,client={self.client_id}",
                    exc_info=True,
                )

    def _find_matching_sub(self, topic):
        try:
            return next(self.sub_matcher.iter_match(topic))
        except StopIteration:
            return None

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage):
        try:
            sub = self._find_matching_sub(msg.topic)
        except Exception:
            logging.warning(
                f"reason=onMessageFindMatchingSubException,topic={msg.topic}",
                exc_info=True,
            )
            return

        if sub is None:
            logging.warning(f"reason=onMessageNoMatchingSubscription,topic={msg.topic}")
            return

        try:
            param, with_retain = self._sub_delivery[sub]
            extra = (bool(msg.retain),) if with_retain else ()
            if userdata:
                userdata(msg.topic, msg.payload, param, *extra)
            else:
                param(msg.topic, msg.payload, *extra)
        except Exception:
            logging.warning(
                f"reason=onMessageClientCallbackException,topic={msg.topic}",
                exc_info=True,
            )
