# mxcube-ipc-client

Python client for mxcubecore's IPC server (`mxcubecore.ipc.IPCServer`), over
either of its transports: JSON-RPC over TCP, or NanoMQ (MQTT).

It has no required dependencies and doesn't need mxcubecore installed.

## Install

Install from Git checkout checkout:

```console
pip install -e .            # JSON-RPC/TCP transport only
pip install -e ".[nanomq]"  # also the NanoMQ (MQTT) transport, via paho-mqtt
```

## Usage

```python
from mxcube_ipc_client import ErrorCode, MXCuBEIPCClient, MXCuBEIPCError

with MXCuBEIPCClient("change-me", host="127.0.0.1", port=9999) as client:
    client.ipc_gateway.ping(message="hello")    # keyword arguments,
    client.ipc_gateway.ping({"message": "hello"})  # or one dict / pydantic model

    def on_state(params):
        print(params["args"])

    client.connect("diffractometer.stateChanged", on_state)
```

NanoMQ instead of JSON-RPC:

```python
MXCuBEIPCClient(
    "change-me",
    transport="nanomq",
    broker_host="localhost",
    broker_port=1883,
    topic_prefix="mxcube/ipc",
)
```

The constructor connects, authenticates with the token (`_auth`) and fetches
the server's whitelist (`_describe`), so the client is ready as soon as it's
constructed:

## Concurency

The client uses plain sockets and threads, It should be safe to call from 
several threads at once.

Don't use it in a process where gevent has patched `socket` but not
`threading`, such as `monkey.patch_all(thread=False)`. This is wahts
done in mxcubecore. This library is mean to be used outside of
mxcubecore.
