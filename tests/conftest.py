import time

import pytest
from fake_server import FakeIPCServer

from mxcube_ipc_client import MXCuBEIPCClient

AUTH_TOKEN = "s3cr3t"  # noqa: S105


def _ping(params: dict) -> str:
    message = params.get("message", "")
    if not isinstance(message, str):
        raise ValueError("message: Input should be a valid string")
    return f"pong: {message}"


def _slow(params: dict) -> None:
    time.sleep(params.get("seconds", 1))


@pytest.fixture
def server():
    fake = FakeIPCServer(
        AUTH_TOKEN,
        methods={
            "ipc_gateway.ping": _ping,
            "ipc_gateway.slow": _slow,
            "diffractometer.omega.get_value": lambda params: 12.3,
        },
        events={
            "procedure.test_signal": {
                "role": "procedure",
                "signal": "test_signal",
                "args": None,
                "args_schema": None,
            }
        },
        allow_debug_calls=True,
    )
    try:
        yield fake
    finally:
        fake.close()


@pytest.fixture
def client(server):
    host, port = server.address
    c = MXCuBEIPCClient(AUTH_TOKEN, host=host, port=port, timeout=2)
    try:
        yield c
    finally:
        c.close()
