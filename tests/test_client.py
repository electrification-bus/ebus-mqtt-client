import os
import ssl
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from ebus_mqtt_client import MqttClient
from ebus_mqtt_client.client import MQTT_DEFAULT_HOST, MQTT_DEFAULT_PORT


# Patch paho.mqtt.client.Client so no real broker connection is attempted
@pytest.fixture(autouse=True)
def mock_paho(monkeypatch):
    mock_client_cls = MagicMock()
    mock_instance = MagicMock()
    mock_client_cls.return_value = mock_instance
    mock_instance.is_connected.return_value = False
    mock_instance.subscribe.return_value = (0, 1)  # MQTT_ERR_SUCCESS, msg_id
    mock_instance.publish.return_value = MagicMock(rc=0)

    monkeypatch.setattr("ebus_mqtt_client.client.mqtt.Client", mock_client_cls)
    return {"cls": mock_client_cls, "instance": mock_instance}


class TestDefaults:
    def test_default_host(self):
        assert MQTT_DEFAULT_HOST == "127.0.0.1"

    def test_default_port(self):
        assert MQTT_DEFAULT_PORT == 1883

    def test_no_env_var_lookup(self):
        """Defaults should be plain constants, not os.environ lookups."""
        import inspect

        import ebus_mqtt_client.client as mod

        source = inspect.getsource(mod)
        assert "EBUS_MQTT_HOST" not in source
        assert "EBUS_MQTT_PORT" not in source


class TestMqttClientInit:
    def test_basic_init(self, mock_paho):
        MqttClient(client_id="test", endpoint="localhost", port=1883)
        mock_paho["cls"].assert_called_once_with(client_id="test")
        mock_paho["instance"].connect.assert_called_once_with("localhost", 1883, keepalive=60)

    def test_v5_protocol(self, mock_paho):
        MqttClient(client_id="test-v5", endpoint="localhost", port=1883, v5=True)
        call_kwargs = mock_paho["cls"].call_args
        assert call_kwargs[1]["protocol"] is not None  # MQTTv5 passed

    def test_username_password(self, mock_paho):
        MqttClient(
            client_id="test",
            endpoint="localhost",
            port=1883,
            username="user",
            password="pass",
        )
        mock_paho["instance"].username_pw_set.assert_called_once_with("user", "pass")

    def test_no_auth_without_credentials(self, mock_paho):
        MqttClient(client_id="test", endpoint="localhost", port=1883)
        mock_paho["instance"].username_pw_set.assert_not_called()

    def test_lwt_set(self, mock_paho):
        lwt = {"topic": "status/offline", "payload": "gone", "retain": True, "qos": 1}
        MqttClient(client_id="test", endpoint="localhost", port=1883, lwt=lwt)
        mock_paho["instance"].will_set.assert_called_once_with(
            topic="status/offline", payload="gone", retain=True, qos=1
        )

    def test_lwt_not_set_when_empty(self, mock_paho):
        MqttClient(client_id="test", endpoint="localhost", port=1883, lwt={})
        mock_paho["instance"].will_set.assert_not_called()

    def test_reconnect_delay_set(self, mock_paho):
        MqttClient(client_id="test", endpoint="localhost", port=1883)
        mock_paho["instance"].reconnect_delay_set.assert_called_once_with(min_delay=1, max_delay=30)


class TestTLS:
    def test_tls_insecure(self, mock_paho):
        with patch("ebus_mqtt_client.client.ssl.SSLContext") as mock_ctx_cls:
            mock_ctx = MagicMock()
            mock_ctx_cls.return_value = mock_ctx
            MqttClient(
                client_id="test",
                endpoint="localhost",
                port=8883,
                use_tls=True,
                tls_insecure=True,
            )
            mock_ctx_cls.assert_called_with(ssl.PROTOCOL_TLS_CLIENT)
            assert mock_ctx.check_hostname is False
            assert mock_ctx.verify_mode == ssl.CERT_NONE
            mock_paho["instance"].tls_insecure_set.assert_called_with(True)

    def test_tls_secure_with_ca_cert(self, mock_paho):
        with patch("ebus_mqtt_client.client.ssl.SSLContext") as mock_ctx_cls:
            mock_ctx = MagicMock()
            mock_ctx_cls.return_value = mock_ctx
            MqttClient(
                client_id="test",
                endpoint="localhost",
                port=8883,
                use_tls=True,
                tls_ca_cert="/path/to/ca.pem",
                tls_insecure=False,
            )
            mock_ctx.load_verify_locations.assert_called_once_with(cafile="/path/to/ca.pem")
            mock_paho["instance"].tls_insecure_set.assert_called_with(False)

    def test_tls_secure_with_ca_data(self, mock_paho):
        with patch("ebus_mqtt_client.client.ssl.SSLContext") as mock_ctx_cls:
            mock_ctx = MagicMock()
            mock_ctx_cls.return_value = mock_ctx
            MqttClient(
                client_id="test",
                endpoint="localhost",
                port=8883,
                use_tls=True,
                tls_ca_data="PEM-DATA-HERE",
                tls_insecure=False,
            )
            mock_ctx.load_verify_locations.assert_called_once_with(cadata="PEM-DATA-HERE")

    def test_no_tls_by_default(self, mock_paho):
        with patch("ebus_mqtt_client.client.ssl.SSLContext") as mock_ctx_cls:
            MqttClient(client_id="test", endpoint="localhost", port=1883)
            mock_ctx_cls.assert_not_called()


class TestMtls:
    def test_client_cert_path_with_ca(self, mock_paho):
        """Client cert + key paths are passed straight to load_cert_chain."""
        with patch("ebus_mqtt_client.client.ssl.SSLContext") as mock_ctx_cls:
            mock_ctx = MagicMock()
            mock_ctx_cls.return_value = mock_ctx
            MqttClient(
                client_id="test",
                endpoint="localhost",
                port=8883,
                use_tls=True,
                tls_ca_cert="/path/to/ca.pem",
                tls_insecure=False,
                tls_client_cert="/path/to/client.crt",
                tls_client_key="/path/to/client.key",
            )
            mock_ctx.load_cert_chain.assert_called_once_with(
                certfile="/path/to/client.crt",
                keyfile="/path/to/client.key",
                password=None,
            )

    def test_client_cert_path_in_insecure_mode(self, mock_paho):
        """mTLS still works when the server cert is not verified (self-signed broker)."""
        with patch("ebus_mqtt_client.client.ssl.SSLContext") as mock_ctx_cls:
            mock_ctx = MagicMock()
            mock_ctx_cls.return_value = mock_ctx
            MqttClient(
                client_id="test",
                endpoint="localhost",
                port=8883,
                use_tls=True,
                tls_insecure=True,
                tls_client_cert="/path/to/client.crt",
                tls_client_key="/path/to/client.key",
            )
            mock_ctx.load_cert_chain.assert_called_once_with(
                certfile="/path/to/client.crt",
                keyfile="/path/to/client.key",
                password=None,
            )

    def test_client_cert_data_materialised_to_tempfile(self, mock_paho):
        """In-memory PEM data is written to a temp file load_cert_chain can read."""
        cert_pem = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
        key_pem = "-----BEGIN PRIVATE KEY-----\nMIIE\n-----END PRIVATE KEY-----\n"
        seen = {}

        def fake_load(certfile, keyfile, password):
            # Capture the temp paths' contents before they're unlinked.
            with open(certfile, "rb") as f:
                seen["cert"] = f.read()
            with open(keyfile, "rb") as f:
                seen["key"] = f.read()
            seen["cert_path"] = certfile
            seen["key_path"] = keyfile

        with patch("ebus_mqtt_client.client.ssl.SSLContext") as mock_ctx_cls:
            mock_ctx = MagicMock()
            mock_ctx.load_cert_chain.side_effect = fake_load
            mock_ctx_cls.return_value = mock_ctx
            MqttClient(
                client_id="test",
                endpoint="localhost",
                port=8883,
                use_tls=True,
                tls_ca_data="CA-PEM",
                tls_insecure=False,
                tls_client_cert_data=cert_pem,
                tls_client_key_data=key_pem,
            )
            assert seen["cert"] == cert_pem.encode("utf-8")
            assert seen["key"] == key_pem.encode("utf-8")
            # Temp files cleaned up after the load returned.
            assert not os.path.exists(seen["cert_path"])
            assert not os.path.exists(seen["key_path"])

    def test_client_key_data_as_bytes(self, mock_paho):
        """DER bytes (not PEM string) flow through unchanged."""
        cert_der = b"\x30\x82\x01\x00fake-cert-der"
        key_der = b"\x30\x82\x02\x00fake-key-der"
        seen = {}

        def fake_load(certfile, keyfile, password):
            with open(certfile, "rb") as f:
                seen["cert"] = f.read()
            with open(keyfile, "rb") as f:
                seen["key"] = f.read()

        with patch("ebus_mqtt_client.client.ssl.SSLContext") as mock_ctx_cls:
            mock_ctx = MagicMock()
            mock_ctx.load_cert_chain.side_effect = fake_load
            mock_ctx_cls.return_value = mock_ctx
            MqttClient(
                client_id="test",
                endpoint="localhost",
                port=8883,
                use_tls=True,
                tls_insecure=True,
                tls_client_cert_data=cert_der,
                tls_client_key_data=key_der,
            )
            assert seen["cert"] == cert_der
            assert seen["key"] == key_der

    def test_client_key_password_passed_through(self, mock_paho):
        with patch("ebus_mqtt_client.client.ssl.SSLContext") as mock_ctx_cls:
            mock_ctx = MagicMock()
            mock_ctx_cls.return_value = mock_ctx
            MqttClient(
                client_id="test",
                endpoint="localhost",
                port=8883,
                use_tls=True,
                tls_insecure=True,
                tls_client_cert="/path/to/client.crt",
                tls_client_key="/path/to/client.key",
                tls_client_key_password="hunter2",
            )
            _, kwargs = mock_ctx.load_cert_chain.call_args
            assert kwargs["password"] == "hunter2"

    def test_combined_cert_and_key_in_one_pem(self, mock_paho):
        """Cert PEM may contain the key; keyfile=None is the documented signal."""
        with patch("ebus_mqtt_client.client.ssl.SSLContext") as mock_ctx_cls:
            mock_ctx = MagicMock()
            mock_ctx_cls.return_value = mock_ctx
            MqttClient(
                client_id="test",
                endpoint="localhost",
                port=8883,
                use_tls=True,
                tls_insecure=True,
                tls_client_cert="/path/to/combined.pem",
            )
            mock_ctx.load_cert_chain.assert_called_once_with(
                certfile="/path/to/combined.pem",
                keyfile=None,
                password=None,
            )

    def test_data_form_wins_over_path_with_warning(self, mock_paho, caplog):
        """When both *_data and the path are given, prefer *_data and warn."""
        with patch("ebus_mqtt_client.client.ssl.SSLContext") as mock_ctx_cls:
            mock_ctx = MagicMock()
            mock_ctx_cls.return_value = mock_ctx
            with caplog.at_level("WARNING"):
                MqttClient(
                    client_id="test",
                    endpoint="localhost",
                    port=8883,
                    use_tls=True,
                    tls_insecure=True,
                    tls_client_cert="/path/to/ignored.crt",
                    tls_client_cert_data="PEM-DATA",
                    tls_client_key="/path/to/ignored.key",
                    tls_client_key_data="KEY-DATA",
                )
            kwargs = mock_ctx.load_cert_chain.call_args.kwargs
            assert kwargs["certfile"] != "/path/to/ignored.crt"
            assert kwargs["keyfile"] != "/path/to/ignored.key"
            warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
            assert any("ClientCertConflict" in m for m in warnings)
            assert any("ClientKeyConflict" in m for m in warnings)

    def test_no_mtls_when_no_client_cert_supplied(self, mock_paho):
        """Server-only TLS still works — load_cert_chain is NOT called."""
        with patch("ebus_mqtt_client.client.ssl.SSLContext") as mock_ctx_cls:
            mock_ctx = MagicMock()
            mock_ctx_cls.return_value = mock_ctx
            MqttClient(
                client_id="test",
                endpoint="localhost",
                port=8883,
                use_tls=True,
                tls_ca_cert="/path/to/ca.pem",
                tls_insecure=False,
            )
            mock_ctx.load_cert_chain.assert_not_called()


class TestFromConfig:
    def test_defaults(self, mock_paho):
        MqttClient.from_config({}, client_id="cfg-test")
        mock_paho["instance"].connect.assert_called_once_with("127.0.0.1", 1883, keepalive=60)

    def test_custom_host_port(self, mock_paho):
        cfg = {"host": "broker.example.com", "port": 8883}
        MqttClient.from_config(cfg, client_id="cfg-test")
        mock_paho["instance"].connect.assert_called_once_with(
            "broker.example.com", 8883, keepalive=60
        )

    def test_auth_user_pass(self, mock_paho):
        cfg = {
            "authentication": {
                "type": "USER_PASS",
                "username": "admin",
                "password": "secret",
            }
        }
        MqttClient.from_config(cfg, client_id="cfg-test")
        mock_paho["instance"].username_pw_set.assert_called_once_with("admin", "secret")

    def test_auth_unknown_type_ignored(self, mock_paho):
        cfg = {
            "authentication": {
                "type": "CERTIFICATE",
                "cert": "/path/to/cert",
            }
        }
        MqttClient.from_config(cfg, client_id="cfg-test")
        mock_paho["instance"].username_pw_set.assert_not_called()

    def test_tls_config(self, mock_paho):
        with patch("ebus_mqtt_client.client.ssl.SSLContext"):
            cfg = {"use_tls": True, "tls_insecure": True}
            MqttClient.from_config(cfg, client_id="cfg-test")
            mock_paho["instance"].tls_insecure_set.assert_called_with(True)

    def test_lwt_passthrough(self, mock_paho):
        lwt = {"topic": "status", "payload": "offline"}
        MqttClient.from_config({}, client_id="cfg-test", lwt=lwt)
        mock_paho["instance"].will_set.assert_called_once()

    def test_mtls_config_path_form(self, mock_paho):
        with patch("ebus_mqtt_client.client.ssl.SSLContext") as mock_ctx_cls:
            mock_ctx = MagicMock()
            mock_ctx_cls.return_value = mock_ctx
            cfg = {
                "use_tls": True,
                "tls_insecure": False,
                "tls_ca_cert": "/ca.pem",
                "tls_client_cert": "/client.crt",
                "tls_client_key": "/client.key",
                "tls_client_key_password": "pw",
            }
            MqttClient.from_config(cfg, client_id="cfg-test")
            mock_ctx.load_cert_chain.assert_called_once_with(
                certfile="/client.crt", keyfile="/client.key", password="pw"
            )

    def test_mtls_config_data_form(self, mock_paho):
        with patch("ebus_mqtt_client.client.ssl.SSLContext") as mock_ctx_cls:
            mock_ctx = MagicMock()
            mock_ctx_cls.return_value = mock_ctx
            cfg = {
                "use_tls": True,
                "tls_insecure": True,
                "tls_client_cert_data": "CERT-PEM",
                "tls_client_key_data": "KEY-PEM",
            }
            MqttClient.from_config(cfg, client_id="cfg-test")
            mock_ctx.load_cert_chain.assert_called_once()


class TestSubscriptions:
    def test_subscribe_stores_callback(self, mock_paho):
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        handler = MagicMock()
        client.subscribe("sensors/#", handler, qos=1)

        assert "sensors/#" in client.sub_callbacks
        assert client.sub_callbacks["sensors/#"] == (handler, 1)
        mock_paho["instance"].subscribe.assert_called_with("sensors/#", 1)

    def test_subscription_recovery_on_connect(self, mock_paho):
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        handler = MagicMock()
        client.subscribe("sensors/#", handler, qos=1)

        # Reset subscribe call count, then simulate reconnect
        mock_paho["instance"].subscribe.reset_mock()
        client._on_connect(mock_paho["instance"], None, {}, 0)

        mock_paho["instance"].subscribe.assert_called_once_with("sensors/#", 1)

    def test_on_connect_callback_invoked(self, mock_paho):
        on_connect = MagicMock()
        client = MqttClient(
            client_id="test",
            endpoint="localhost",
            port=1883,
            on_connect_callback=on_connect,
        )
        client._on_connect(mock_paho["instance"], None, {}, 0)
        on_connect.assert_called_once()


class TestUnsubscribe:
    def test_unsubscribe_removes_callback_and_calls_paho(self, mock_paho):
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        handler = MagicMock()
        client.subscribe("sensors/#", handler, qos=1)

        result = client.unsubscribe("sensors/#")

        assert result is True
        assert "sensors/#" not in client.sub_callbacks
        mock_paho["instance"].unsubscribe.assert_called_once_with("sensors/#")

    def test_unsubscribe_stops_message_dispatch(self, mock_paho):
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        handler = MagicMock()
        client.subscribe("sensors/+", handler, qos=1)
        client.unsubscribe("sensors/+")

        msg = MagicMock()
        msg.topic = "sensors/a"
        msg.payload = b"x"
        client._on_message(mock_paho["instance"], None, msg)

        handler.assert_not_called()

    def test_unsubscribe_unknown_returns_false_and_no_paho_call(self, mock_paho):
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        result = client.unsubscribe("never/subscribed")

        assert result is False
        mock_paho["instance"].unsubscribe.assert_not_called()

    def test_unsubscribed_not_restored_on_reconnect(self, mock_paho):
        """After unsubscribe, on-reconnect recovery must not resurrect the filter."""
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        handler = MagicMock()
        client.subscribe("a/#", handler, qos=1)
        client.subscribe("b/#", handler, qos=1)
        client.unsubscribe("a/#")

        mock_paho["instance"].subscribe.reset_mock()
        client._on_connect(mock_paho["instance"], None, {}, 0)

        # Only "b/#" should be re-subscribed; "a/#" was unsubscribed
        subscribed_filters = [c[0][0] for c in mock_paho["instance"].subscribe.call_args_list]
        assert subscribed_filters == ["b/#"]


class TestPublish:
    def test_publish_calls_paho(self, mock_paho):
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        client.publish("topic/a", "data", qos=0, retain=True)
        mock_paho["instance"].publish.assert_called_once_with("topic/a", "data", 0, True)

    def test_publish_returns_msg_info(self, mock_paho):
        msg_info = MagicMock(rc=0)
        mock_paho["instance"].publish.return_value = msg_info
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        assert client.publish("topic/a", "data") is msg_info

    def test_publish_no_client_returns_none(self, mock_paho):
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        del client.mqttc
        assert client.publish("topic/a", "data") is None


class TestPublishAndFlush:
    def test_flush_success(self, mock_paho):
        msg_info = MagicMock(rc=0)
        msg_info.is_published.return_value = True
        mock_paho["instance"].is_connected.return_value = True
        mock_paho["instance"].publish.return_value = msg_info
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)

        assert client.publish_and_flush("topic/a", "data", timeout=0.5) is True
        msg_info.wait_for_publish.assert_called_once_with(0.5)

    def test_flush_not_connected_returns_false_without_publish(self, mock_paho):
        mock_paho["instance"].is_connected.return_value = False
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)

        assert client.publish_and_flush("topic/a", "data") is False
        mock_paho["instance"].publish.assert_not_called()

    def test_flush_no_client_returns_false(self, mock_paho):
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        del client.mqttc
        assert client.publish_and_flush("topic/a", "data") is False

    def test_flush_publish_rc_failure_returns_false(self, mock_paho):
        msg_info = MagicMock(rc=1)  # non-zero rc == failure
        mock_paho["instance"].is_connected.return_value = True
        mock_paho["instance"].publish.return_value = msg_info
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)

        assert client.publish_and_flush("topic/a", "data") is False
        msg_info.wait_for_publish.assert_not_called()

    def test_flush_timeout_returns_false(self, mock_paho):
        msg_info = MagicMock(rc=0)
        msg_info.wait_for_publish.side_effect = RuntimeError("not published yet")
        mock_paho["instance"].is_connected.return_value = True
        mock_paho["instance"].publish.return_value = msg_info
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)

        assert client.publish_and_flush("topic/a", "data", timeout=0.1) is False


class TestStartStop:
    def test_start_nonblocking(self, mock_paho):
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        client.start(blocking=False)
        assert client.is_running is True
        mock_paho["instance"].loop_start.assert_called_once()

    def test_stop(self, mock_paho):
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        handler = MagicMock()
        client.subscribe("test/#", handler)
        client.start()
        client.stop()

        assert client.is_running is False
        mock_paho["instance"].disconnect.assert_called_once()
        mock_paho["instance"].loop_stop.assert_called_once()
        assert len(client.sub_callbacks) == 0

    def test_stop_accepts_timeout(self, mock_paho):
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        client.start()
        client.stop(timeout=0.5)
        mock_paho["instance"].loop_stop.assert_called_once()

    def test_stop_shortens_reconnect_backoff(self, mock_paho):
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        client.stop()
        # The last reconnect_delay_set call (during stop) caps the backoff at 1s
        # so the network thread is not parked in a long sleep.
        mock_paho["instance"].reconnect_delay_set.assert_called_with(min_delay=1, max_delay=1)

    def test_stop_bounded_when_loop_stop_hangs(self, mock_paho):
        # Simulate a wedged network thread: loop_stop() blocks. stop() must still
        # return within its timeout rather than joining the hung thread.
        release = threading.Event()
        mock_paho["instance"].loop_stop.side_effect = lambda: release.wait(5)
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        handler = MagicMock()
        client.subscribe("test/#", handler)
        client.start()

        start = time.monotonic()
        client.stop(timeout=0.2)
        elapsed = time.monotonic() - start
        release.set()  # let the daemon helper unwind

        assert elapsed < 1.0
        assert client.is_running is False
        assert len(client.sub_callbacks) == 0

    def test_stop_no_client_does_not_raise(self, mock_paho):
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        del client.mqttc
        client.stop()  # must not raise even with no underlying client
        assert client.is_running is False


class TestIsConnected:
    def test_connected(self, mock_paho):
        mock_paho["instance"].is_connected.return_value = True
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        assert client.is_connected() is True

    def test_not_connected(self, mock_paho):
        mock_paho["instance"].is_connected.return_value = False
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        assert client.is_connected() is False


class TestOnMessage:
    def test_dispatches_to_subscriber(self, mock_paho):
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        handler = MagicMock()
        client.subscribe("ebus/5/+/$state", handler, qos=2)

        msg = MagicMock()
        msg.topic = "ebus/5/my-device/$state"
        msg.payload = b"ready"

        # userdata is None (no global callback) — handler is invoked directly
        client._on_message(mock_paho["instance"], None, msg)
        handler.assert_called_once_with("ebus/5/my-device/$state", b"ready")

    def test_no_matching_sub_does_not_raise(self, mock_paho):
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        msg = MagicMock()
        msg.topic = "unmatched/topic"
        msg.payload = b"data"

        # Should be a no-op (logs a warning), not raise
        client._on_message(mock_paho["instance"], None, msg)
