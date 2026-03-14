import ssl
from unittest.mock import patch, MagicMock, PropertyMock
import pytest

from ebus_mqtt_client import MqttClient, AUTH_TYPE_USER_PASS
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
        import ebus_mqtt_client.client as mod
        import inspect

        source = inspect.getsource(mod)
        assert "EBUS_MQTT_HOST" not in source
        assert "EBUS_MQTT_PORT" not in source


class TestMqttClientInit:
    def test_basic_init(self, mock_paho):
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        mock_paho["cls"].assert_called_once_with(client_id="test")
        mock_paho["instance"].connect.assert_called_once_with(
            "localhost", 1883, keepalive=60
        )

    def test_v5_protocol(self, mock_paho):
        client = MqttClient(
            client_id="test-v5", endpoint="localhost", port=1883, v5=True
        )
        call_kwargs = mock_paho["cls"].call_args
        assert call_kwargs[1]["protocol"] is not None  # MQTTv5 passed

    def test_username_password(self, mock_paho):
        client = MqttClient(
            client_id="test",
            endpoint="localhost",
            port=1883,
            username="user",
            password="pass",
        )
        mock_paho["instance"].username_pw_set.assert_called_once_with("user", "pass")

    def test_no_auth_without_credentials(self, mock_paho):
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        mock_paho["instance"].username_pw_set.assert_not_called()

    def test_lwt_set(self, mock_paho):
        lwt = {"topic": "status/offline", "payload": "gone", "retain": True, "qos": 1}
        client = MqttClient(
            client_id="test", endpoint="localhost", port=1883, lwt=lwt
        )
        mock_paho["instance"].will_set.assert_called_once_with(
            topic="status/offline", payload="gone", retain=True, qos=1
        )

    def test_lwt_not_set_when_empty(self, mock_paho):
        client = MqttClient(
            client_id="test", endpoint="localhost", port=1883, lwt={}
        )
        mock_paho["instance"].will_set.assert_not_called()

    def test_reconnect_delay_set(self, mock_paho):
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        mock_paho["instance"].reconnect_delay_set.assert_called_once_with(
            min_delay=1, max_delay=30
        )


class TestTLS:
    def test_tls_insecure(self, mock_paho):
        with patch("ebus_mqtt_client.client.ssl.SSLContext") as mock_ctx_cls:
            mock_ctx = MagicMock()
            mock_ctx_cls.return_value = mock_ctx
            client = MqttClient(
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
            client = MqttClient(
                client_id="test",
                endpoint="localhost",
                port=8883,
                use_tls=True,
                tls_ca_cert="/path/to/ca.pem",
                tls_insecure=False,
            )
            mock_ctx.load_verify_locations.assert_called_once_with(
                cafile="/path/to/ca.pem"
            )
            mock_paho["instance"].tls_insecure_set.assert_called_with(False)

    def test_tls_secure_with_ca_data(self, mock_paho):
        with patch("ebus_mqtt_client.client.ssl.SSLContext") as mock_ctx_cls:
            mock_ctx = MagicMock()
            mock_ctx_cls.return_value = mock_ctx
            client = MqttClient(
                client_id="test",
                endpoint="localhost",
                port=8883,
                use_tls=True,
                tls_ca_data="PEM-DATA-HERE",
                tls_insecure=False,
            )
            mock_ctx.load_verify_locations.assert_called_once_with(
                cadata="PEM-DATA-HERE"
            )

    def test_no_tls_by_default(self, mock_paho):
        with patch("ebus_mqtt_client.client.ssl.SSLContext") as mock_ctx_cls:
            client = MqttClient(
                client_id="test", endpoint="localhost", port=1883
            )
            mock_ctx_cls.assert_not_called()


class TestFromConfig:
    def test_defaults(self, mock_paho):
        client = MqttClient.from_config({}, client_id="cfg-test")
        mock_paho["instance"].connect.assert_called_once_with(
            "127.0.0.1", 1883, keepalive=60
        )

    def test_custom_host_port(self, mock_paho):
        cfg = {"host": "broker.example.com", "port": 8883}
        client = MqttClient.from_config(cfg, client_id="cfg-test")
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
        client = MqttClient.from_config(cfg, client_id="cfg-test")
        mock_paho["instance"].username_pw_set.assert_called_once_with(
            "admin", "secret"
        )

    def test_auth_unknown_type_ignored(self, mock_paho):
        cfg = {
            "authentication": {
                "type": "CERTIFICATE",
                "cert": "/path/to/cert",
            }
        }
        client = MqttClient.from_config(cfg, client_id="cfg-test")
        mock_paho["instance"].username_pw_set.assert_not_called()

    def test_tls_config(self, mock_paho):
        with patch("ebus_mqtt_client.client.ssl.SSLContext"):
            cfg = {"use_tls": True, "tls_insecure": True}
            client = MqttClient.from_config(cfg, client_id="cfg-test")
            mock_paho["instance"].tls_insecure_set.assert_called_with(True)

    def test_lwt_passthrough(self, mock_paho):
        lwt = {"topic": "status", "payload": "offline"}
        client = MqttClient.from_config(
            {}, client_id="cfg-test", lwt=lwt
        )
        mock_paho["instance"].will_set.assert_called_once()


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


class TestPublish:
    def test_publish_calls_paho(self, mock_paho):
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        client.publish("topic/a", "data", qos=0, retain=True)
        mock_paho["instance"].publish.assert_called_once_with(
            "topic/a", "data", 0, True
        )


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


class TestIsConnected:
    def test_connected(self, mock_paho):
        mock_paho["instance"].is_connected.return_value = True
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        assert client.is_connected() is True

    def test_not_connected(self, mock_paho):
        mock_paho["instance"].is_connected.return_value = False
        client = MqttClient(client_id="test", endpoint="localhost", port=1883)
        assert client.is_connected() is False
