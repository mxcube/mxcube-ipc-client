import json
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("paho.mqtt.client")

from mxcube_ipc_client.client import _NanoMQClientTransport  # noqa: E402


def test_nanomq_transport_sends_client_id_and_host():
    with patch("paho.mqtt.client.Client") as client_cls:
        mock_mqtt_client = MagicMock()
        client_cls.return_value = mock_mqtt_client

        frames = []
        transport = _NanoMQClientTransport(
            "localhost",
            1883,
            "mxcube/ipc",
            client_id="test-client",
            on_frame=frames.append,
        )
        transport.connect()

        transport.send({"jsonrpc": "2.0", "id": 1, "method": "ipc_gateway.ping"})

        topic, payload = mock_mqtt_client.publish.call_args[0]
        assert topic == "mxcube/ipc/request"
        sent = json.loads(payload)
        assert sent["_client_id"] == "test-client"
        assert "_client_host" in sent

        mock_mqtt_client.will_set.assert_called_once()
        will_topic, will_payload = mock_mqtt_client.will_set.call_args[0]
        assert will_topic == "mxcube/ipc/disconnect"
        assert json.loads(will_payload) == {"_client_id": "test-client"}


def test_nanomq_close_announces_disconnect():
    with patch("paho.mqtt.client.Client") as client_cls:
        mock_mqtt_client = MagicMock()
        client_cls.return_value = mock_mqtt_client

        transport = _NanoMQClientTransport(
            "localhost", 1883, "mxcube/ipc", client_id="test-client", on_frame=print
        )
        transport.connect()
        transport.close()

        topic, payload = mock_mqtt_client.publish.call_args[0]
        assert topic == "mxcube/ipc/disconnect"
        assert json.loads(payload) == {"_client_id": "test-client"}
        mock_mqtt_client.loop_stop.assert_called_once()
        mock_mqtt_client.disconnect.assert_called_once()
