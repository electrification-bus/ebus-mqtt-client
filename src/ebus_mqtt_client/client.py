import contextlib
import logging
import os
import ssl
import tempfile
import threading
from collections.abc import Callable
from typing import Any

import paho.mqtt.client as mqtt
import paho.mqtt.matcher as matcher

# Default broker configuration
MQTT_DEFAULT_HOST = "127.0.0.1"
MQTT_DEFAULT_PORT = 1883

# Authentication types
AUTH_TYPE_USER_PASS = "USER_PASS"


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
    ):
        self.client_id = client_id
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
        self.sub_matcher = matcher.MQTTMatcher()
        self.on_connect_callback = on_connect_callback

        self.is_running = False
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
        )

    def is_connected(self):
        """Check if MQTT client is connected."""
        return self.mqttc.is_connected() if hasattr(self, "mqttc") else False

    def start(self, blocking=False):
        self.is_running = True
        if blocking:
            self.mqttc.loop_forever()
        else:
            self.mqttc.loop_start()

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
        # Release subscription callbacks and matcher to free memory
        self.sub_callbacks.clear()
        self.sub_matcher = matcher.MQTTMatcher()
        self.on_connect_callback = None

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
        """Publish a message and return paho's MQTTMessageInfo (or None if no client).

        Returning the message info lets a caller optionally wait for the message
        to be flushed to the broker via ``msg_info.wait_for_publish(timeout)``.
        See :meth:`publish_and_flush` for a bounded convenience wrapper.
        """
        if not hasattr(self, "mqttc"):
            logging.error(f"reason=mqttPublishNoClient,client={self.client_id},topic={topic}")
            return None
        msg_info = self.mqttc.publish(topic, data, qos, retain)

        if msg_info.rc != mqtt.MQTT_ERR_SUCCESS:
            logging.warning(f"reason=mqttPublishFail,client={self.client_id},topic={topic}")
        return msg_info

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
        returns False immediately if there is no client or the client is not
        connected, if the publish call itself fails, or if the flush does not
        complete within ``timeout``.
        """
        if not hasattr(self, "mqttc"):
            logging.error(f"reason=mqttPublishFlushNoClient,client={self.client_id},topic={topic}")
            return False
        if not self.mqttc.is_connected():
            logging.warning(
                f"reason=mqttPublishFlushNotConnected,client={self.client_id},topic={topic}"
            )
            return False
        msg_info = self.mqttc.publish(topic, data, qos, retain)
        if msg_info.rc != mqtt.MQTT_ERR_SUCCESS:
            logging.warning(f"reason=mqttPublishFail,client={self.client_id},topic={topic}")
            return False
        try:
            msg_info.wait_for_publish(timeout)
        except (RuntimeError, ValueError) as e:
            logging.warning(
                f"reason=mqttPublishFlushTimeout,client={self.client_id},topic={topic},err={e}"
            )
            return False
        return bool(msg_info.is_published())

    def subscribe(self, sub: str, param: Any, qos: int = 1):
        if not hasattr(self, "mqttc"):
            logging.error(f"reason=mqttSubscribeNoClient,client={self.client_id},sub={sub}")
            return
        self.sub_callbacks[sub] = (param, qos)
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
        with contextlib.suppress(KeyError):
            del self.sub_matcher[sub]
        self.mqttc.unsubscribe(sub)
        logging.info(f"reason=mqttUnsubscribed,client={self.client_id},sub={sub}")
        return True

    def _on_connect(self, mqttc: mqtt.Client, userdata: Any, flags: int, rc: int):
        logging.info(f"reason=mqttBrokerConnected,client={self.client_id}")

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

    def _on_disconnect(self, mqttc: mqtt.Client, userdata: Any, rc: int):
        if self.is_running and rc != mqtt.MQTT_ERR_SUCCESS:
            logging.warning(f"reason=mqttBrokerConnectionLost,rc={rc},client={self.client_id}")
        else:
            logging.info(f"reason=mqttBrokerDisconnected,client={self.client_id}")

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
            if userdata:
                userdata(msg.topic, msg.payload, self.sub_callbacks[sub][0])
            else:
                self.sub_callbacks[sub][0](msg.topic, msg.payload)
        except Exception:
            logging.warning(
                f"reason=onMessageClientCallbackException,topic={msg.topic}",
                exc_info=True,
            )
