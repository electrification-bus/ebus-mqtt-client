import logging
import os
import ssl
import tempfile
import paho.mqtt.client as mqtt
import paho.mqtt.matcher as matcher
from typing import Any, Callable, Optional, Union

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
    """

    def __init__(
        self,
        client_id: str,
        endpoint: str,
        port: int,
        callback: Callable[[Union[bytes, bytearray], Any], None] = None,
        username=None,
        password=None,
        use_tls: Optional[bool] = False,
        tls_ca_cert: Optional[str] = None,
        tls_ca_data: Optional[Union[str, bytes]] = None,
        tls_insecure: Optional[bool] = True,
        tls_client_cert: Optional[str] = None,
        tls_client_cert_data: Optional[Union[str, bytes]] = None,
        tls_client_key: Optional[str] = None,
        tls_client_key_data: Optional[Union[str, bytes]] = None,
        tls_client_key_password: Optional[str] = None,
        v5: Optional[bool] = False,
        lwt: Optional[dict] = {},
        on_connect_callback: Optional[Callable] = None,
    ):
        self.client_id = client_id
        try:
            if v5:
                self.mqttc = mqtt.Client(
                    client_id=self.client_id, protocol=mqtt.MQTTv5
                )
            else:
                self.mqttc = mqtt.Client(client_id=self.client_id)
        except Exception as e:
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
        self.sub_callbacks = {}
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
        self.mqttc.connect(endpoint, port, keepalive=60)

    @staticmethod
    def _load_client_cert_chain(
        context: ssl.SSLContext,
        client_cert: Optional[str],
        client_cert_data: Optional[Union[str, bytes]],
        client_key: Optional[str],
        client_key_data: Optional[Union[str, bytes]],
        client_key_password: Optional[str],
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
            logging.warning(
                "reason=mqttClientTlsClientCertConflict,using=data,ignored=path"
            )
        if client_key and client_key_data:
            logging.warning(
                "reason=mqttClientTlsClientKeyConflict,using=data,ignored=path"
            )

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
                try:
                    os.unlink(path)
                except OSError:
                    pass

    @staticmethod
    def _materialise_pem(data: Union[str, bytes], suffix: str) -> str:
        """Write PEM/DER bytes or string to a 0600 temp file and return its path."""
        if isinstance(data, str):
            payload = data.encode("utf-8")
        else:
            payload = data
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
        callback: Callable[[Union[bytes, bytearray], Any], None] = None,
        lwt: Optional[dict] = None,
        on_connect_callback: Optional[Callable] = None,
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

    def stop(self):
        self.is_running = False
        self.mqttc.disconnect()
        self.mqttc.loop_stop()
        # Release subscription callbacks and matcher to free memory
        self.sub_callbacks.clear()
        self.sub_matcher = matcher.MQTTMatcher()
        self.on_connect_callback = None

    def publish(self, topic: str, data: str, qos: int = 1, retain: bool = False):
        if not hasattr(self, "mqttc"):
            logging.error(
                f"reason=mqttPublishNoClient,client={self.client_id},topic={topic}"
            )
            return
        msg_info = self.mqttc.publish(topic, data, qos, retain)

        if msg_info.rc != mqtt.MQTT_ERR_SUCCESS:
            logging.warning(
                f"reason=mqttPublishFail,client={self.client_id},topic={topic}"
            )

    def subscribe(self, sub: str, param: Any, qos: int = 1):
        if not hasattr(self, "mqttc"):
            logging.error(
                f"reason=mqttSubscribeNoClient,client={self.client_id},sub={sub}"
            )
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
            logging.error(
                f"reason=mqttUnsubscribeNoClient,client={self.client_id},sub={sub}"
            )
            return False
        if sub not in self.sub_callbacks:
            logging.debug(
                f"reason=mqttUnsubscribeUnknownSub,client={self.client_id},sub={sub}"
            )
            return False
        del self.sub_callbacks[sub]
        try:
            del self.sub_matcher[sub]
        except KeyError:
            pass
        self.mqttc.unsubscribe(sub)
        logging.info(f"reason=mqttUnsubscribed,client={self.client_id},sub={sub}")
        return True

    def _on_connect(self, mqttc: mqtt.Client, userdata: Any, flags: int, rc: int):
        logging.info(f"reason=mqttBrokerConnected,client={self.client_id}")

        # Re-subscribe on reconnect (iterate shallow copy in case dict changes)
        for sub, (_, qos) in list(self.sub_callbacks.items()):
            result, msg_id = self.mqttc.subscribe(sub, qos)
            if result == mqtt.MQTT_ERR_SUCCESS:
                logging.info(
                    f"reason=mqttSubscribeSuccess,client={self.client_id},sub={sub}"
                )
            else:
                logging.warning(
                    f"reason=mqttSubscribeFail,client={self.client_id},sub={sub}"
                )
        # Invoke supplied on_connect_callback if provided
        if self.on_connect_callback:
            self.on_connect_callback()

    def _on_disconnect(self, mqttc: mqtt.Client, userdata: Any, rc: int):
        if self.is_running and rc != mqtt.MQTT_ERR_SUCCESS:
            logging.warning(
                f"reason=mqttBrokerConnectionLost,rc={rc},client={self.client_id}"
            )
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
        except:
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
        except:
            logging.warning(
                f"reason=onMessageClientCallbackException,topic={msg.topic}",
                exc_info=True,
            )
