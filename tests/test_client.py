import threading

import pytest
from conftest import AUTH_TOKEN
from fake_server import FakeIPCServer

from mxcube_ipc_client import ErrorCode, MXCuBEIPCClient, MXCuBEIPCError


def test_construction_authenticates_then_describes(server, client):
    assert [request["method"] for request in server.requests[:2]] == [
        "_auth",
        "_describe",
    ]
    assert server.requests[0]["params"] == {"token": AUTH_TOKEN}


def test_construction_discovers_whitelisted_methods(client):
    assert callable(client.ipc_gateway.ping)


def test_construction_discovers_forwarded_events(client):
    assert client.events == {
        "procedure.test_signal": {
            "role": "procedure",
            "signal": "test_signal",
            "args": None,
            "args_schema": None,
        }
    }


def test_role_colliding_with_client_attribute_is_skipped():
    server = FakeIPCServer(AUTH_TOKEN, methods={"close.now": lambda params: None})
    try:
        host, port = server.address
        with MXCuBEIPCClient(AUTH_TOKEN, host=host, port=port) as client:
            assert not hasattr(client.close, "now")
    finally:
        server.close()


def test_call_with_kwargs(client):
    assert client.ipc_gateway.ping(message="hello") == "pong: hello"


def test_call_with_dict(client):
    assert client.ipc_gateway.ping({"message": "hello"}) == "pong: hello"


def test_call_with_pydantic_like_model(server, client):
    class Request:
        def model_dump(self, mode):
            assert mode == "json"
            return {"message": "from model"}

    assert client.ipc_gateway.ping(Request()) == "pong: from model"


def test_nested_role_is_one_flat_attribute(client):
    # Current behaviour: the proxy is keyed by everything before the last
    # dot, so a nested role is reachable only via getattr(), not as
    # client.diffractometer.omega.
    assert not hasattr(client, "diffractometer")
    assert getattr(client, "diffractometer.omega").get_value() == 12.3


def test_call_rejects_both_args_and_kwargs(client):
    with pytest.raises(TypeError):
        client.ipc_gateway.ping({"message": "hello"}, message="hello")


def test_call_rejects_more_than_one_positional_argument(client):
    with pytest.raises(TypeError):
        client.ipc_gateway.ping({"message": "a"}, {"message": "b"})


def test_server_error_raises_ipc_error_with_code(client):
    with pytest.raises(MXCuBEIPCError) as exc_info:
        client.ipc_gateway.ping(message={"not": "a string"})
    assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


def test_wrong_token_raises(server):
    host, port = server.address
    with pytest.raises(MXCuBEIPCError) as exc_info:
        MXCuBEIPCClient("wrong-token", host=host, port=port)
    assert exc_info.value.code == ErrorCode.NOT_AUTHENTICATED


def test_unknown_transport_raises(server):
    host, port = server.address
    with pytest.raises(ValueError, match="Unknown IPC transport"):
        MXCuBEIPCClient(AUTH_TOKEN, transport="carrier-pigeon", host=host, port=port)


def test_call_times_out(client):
    with pytest.raises(TimeoutError):
        client.ipc_gateway.slow(seconds=3)


def test_concurrent_calls_get_their_own_responses(client):
    results = {}

    def _call(index):
        results[index] = client.ipc_gateway.ping(message=str(index))

    threads = [threading.Thread(target=_call, args=(index,)) for index in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert results == {index: f"pong: {index}" for index in range(20)}


def test_debug_calls_send_the_reserved_methods(client):
    assert client.debug_list_roles("diffractometer") == {
        "method": "_debug_list_roles",
        "params": {"role": "diffractometer"},
    }
    assert client.debug_describe_role("diffractometer.omega") == {
        "method": "_debug_describe_role",
        "params": {"role": "diffractometer.omega"},
    }
    assert client.debug_call(
        "diffractometer.omega", "set_value", kwargs={"value": 1}
    ) == {
        "method": "_debug_call",
        "params": {
            "role": "diffractometer.omega",
            "method": "set_value",
            "args": [],
            "kwargs": {"value": 1},
        },
    }


def test_debug_call_disabled_raises():
    server = FakeIPCServer(AUTH_TOKEN, allow_debug_calls=False)
    try:
        host, port = server.address
        with MXCuBEIPCClient(AUTH_TOKEN, host=host, port=port) as client:
            with pytest.raises(MXCuBEIPCError) as exc_info:
                client.debug_list_roles()
        assert exc_info.value.code == ErrorCode.DEBUG_CALLS_DISABLED
    finally:
        server.close()


def test_event_handler_is_called(server, client):
    received = threading.Event()
    params_seen = {}

    def handler(params):
        params_seen.update(params)
        received.set()

    client.connect("procedure.test_signal", handler)
    server.push_event("procedure.test_signal", "some_value")

    assert received.wait(timeout=5)
    assert params_seen == {"args": ["some_value"]}


def test_disconnect_stops_handler(server, client):
    calls = []
    client.connect("procedure.test_signal", calls.append)
    client.disconnect("procedure.test_signal", calls.append)

    server.push_event("procedure.test_signal", "some_value")
    # A round trip after the event guarantees the reader has processed it.
    client.ipc_gateway.ping()

    assert calls == []


def test_raising_event_handler_does_not_stop_the_client(server, client):
    received = threading.Event()

    def bad_handler(params):
        raise RuntimeError("boom")

    client.connect("procedure.test_signal", bad_handler)
    client.connect("procedure.test_signal", lambda params: received.set())
    server.push_event("procedure.test_signal", 1)

    assert received.wait(timeout=5)
    assert client.ipc_gateway.ping(message="still alive") == "pong: still alive"


def test_second_concurrent_client_is_rejected(server, client):
    host, port = server.address
    with pytest.raises(MXCuBEIPCError) as exc_info:
        MXCuBEIPCClient(AUTH_TOKEN, host=host, port=port, timeout=2)
    assert exc_info.value.code == ErrorCode.CLIENT_ALREADY_CONNECTED


def test_close_frees_the_slot_for_the_next_client(server):
    host, port = server.address
    MXCuBEIPCClient(AUTH_TOKEN, host=host, port=port).close()
    assert server.wait_for_disconnect()

    with MXCuBEIPCClient(AUTH_TOKEN, host=host, port=port) as client:
        assert client.ipc_gateway.ping(message="hi") == "pong: hi"


def test_id_less_error_frame_unblocks_oldest_pending_call():
    """A rejection with no id (e.g. "Client already connected", which has
    no specific request to blame) shouldn't be silently dropped - it should
    unblock whichever call is currently waiting with the real error,
    instead of leaving it to time out with no explanation.
    """
    client = object.__new__(MXCuBEIPCClient)
    client._lock = threading.Lock()
    client._pending = {}
    client._responses = {}
    client._event_handlers = {}
    client._unmatched_error = None

    event = threading.Event()
    client._pending[0] = event

    client._on_frame(
        {
            "jsonrpc": "2.0",
            "id": None,
            "error": {"code": -32003, "message": "Client already connected"},
        }
    )

    assert event.is_set()
    assert client._responses[0]["error"]["message"] == "Client already connected"


def test_id_less_error_before_any_call_is_raised_by_the_next_call():
    client = object.__new__(MXCuBEIPCClient)
    client._lock = threading.Lock()
    client._id_counter = iter(range(1, 100))
    client._pending = {}
    client._responses = {}
    client._event_handlers = {}
    client._unmatched_error = None
    client._timeout = 5

    client._on_frame(
        {
            "jsonrpc": "2.0",
            "id": None,
            "error": {"code": -32003, "message": "Client already connected"},
        }
    )

    with pytest.raises(MXCuBEIPCError) as exc_info:
        client._call("_auth", {"token": AUTH_TOKEN})
    assert exc_info.value.code == ErrorCode.CLIENT_ALREADY_CONNECTED
    assert client._pending == {}
