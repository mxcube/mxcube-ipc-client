# encoding: utf-8
#
#  Project name: MXCuBE
#  https://github.com/mxcube
#
#  This file is part of MXCuBE software.
#
#  MXCuBE is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  MXCuBE is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with MXCuBE. If not, see <http://www.gnu.org/licenses/>.

"""MXCuBEIPCClient: a client for mxcubecore's IPC server (`mxcubecore.ipc`),
over either transports: JSON-RPC/TCP or NanoMQ (MQTT).

    client = MXCuBEIPCClient(token, transport="jsonrpc", host=..., port=...)
    client.ipc_gateway.ping(message="hello")      # or ping({"message": "hello"})
    client.connect("procedure.test_signal", handler)
    client.disconnect("procedure.test_signal", handler)
    client.close()  # or: with MXCuBEIPCClient(...) as client: ...
"""

import itertools
import json
import socket
import threading
from collections import defaultdict
from types import SimpleNamespace
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Union,
)

from mxcube_ipc_client.constants import (
    AUTH_METHOD,
    DEBUG_CALL_METHOD,
    DEBUG_DESCRIBE_ROLE_METHOD,
    DEBUG_LIST_ROLES_METHOD,
    DESCRIBE_METHOD,
    logger,
)


class MXCuBEIPCError(Exception):
    """Raised when the server responds with an IPCError - see `.code` for
    the mxcube_ipc_client.ErrorCode value.
    """

    def __init__(self, code: int, message: str) -> None:
        super().__init__(f"[{code}] {message}")
        self.code = code
        self.message = message


def _local_hostname_and_ip() -> str:
    hostname = socket.gethostname()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.connect(("8.8.8.8", 80))
            ip = probe.getsockname()[0]
    except OSError:
        return hostname
    return f"{hostname} ({ip})"


class _JSONRPCClientTransport:
    def __init__(self, host: str, port: int, on_frame: Callable[[dict], None]) -> None:
        self._host = host
        self._port = port
        self._on_frame = on_frame
        self._sock: Optional[socket.socket] = None
        self._sock_file = None
        self._reader_thread: Optional[threading.Thread] = None
        # Calls may be made from several threads at once - keep each frame's
        # write+flush together so frames can never interleave on the wire.
        self._write_lock = threading.Lock()

    def connect(self, timeout: float = 10) -> None:
        self._sock = socket.create_connection((self._host, self._port), timeout)
        # The timeout above is for connect() only - clear it so the reader
        # thread's blocking reads wait indefinitely for the next frame
        # instead of raising after `timeout` seconds of plain silence.
        self._sock.settimeout(None)
        self._sock_file = self._sock.makefile(mode="rwb")
        self._reader_thread = threading.Thread(target=self._read_loop, daemon=True)
        self._reader_thread.start()

    def _read_loop(self) -> None:
        try:
            for line in self._sock_file:
                line = line.strip()
                if not line:
                    continue
                try:
                    frame = json.loads(line.decode("utf-8"))
                except (ValueError, UnicodeDecodeError):
                    logger.warning("IPC client: dropping malformed frame")
                    continue
                self._on_frame(frame)
        except OSError:
            pass  # socket closed from close()

    def send(self, envelope: Dict[str, Any]) -> None:
        frame = (json.dumps(envelope) + "\n").encode("utf-8")
        with self._write_lock:
            self._sock_file.write(frame)
            self._sock_file.flush()

    def close(self) -> None:
        if self._sock is None:
            return
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._sock.close()
        self._sock = None
        if self._reader_thread is not None:
            self._reader_thread.join(timeout=2)


class _NanoMQClientTransport:
    def __init__(
        self,
        broker_host: str,
        broker_port: int,
        topic_prefix: str,
        client_id: Optional[str],
        on_frame: Callable[[dict], None],
    ) -> None:
        import uuid

        self._broker_host = broker_host
        self._broker_port = broker_port
        self._request_topic = f"{topic_prefix}/request"
        self._response_topic = f"{topic_prefix}/response"
        self._event_topic = f"{topic_prefix}/event"
        self._disconnect_topic = f"{topic_prefix}/disconnect"
        self._client_id = client_id or str(uuid.uuid4())
        self._client_host = _local_hostname_and_ip()
        self._on_frame = on_frame
        self._client = None

    def connect(self, timeout: float = 10) -> None:
        import paho.mqtt.client as mqtt

        def _on_connect(client, userdata, flags, reason_code, properties=None) -> None:
            client.subscribe(self._response_topic)
            client.subscribe(self._event_topic)

        def _on_message(client, userdata, msg) -> None:
            try:
                frame = json.loads(msg.payload.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                logger.warning("IPC client: dropping malformed frame")
                return
            self._on_frame(frame)

        self._client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2, client_id=f"ipc-client-{self._client_id}"
        )
        self._client.on_connect = _on_connect
        self._client.on_message = _on_message
        self._client.will_set(
            self._disconnect_topic, json.dumps({"_client_id": self._client_id})
        )
        self._client.connect(self._broker_host, self._broker_port)
        self._client.loop_start()

    def send(self, envelope: Dict[str, Any]) -> None:
        envelope = dict(
            envelope, _client_id=self._client_id, _client_host=self._client_host
        )
        self._client.publish(self._request_topic, json.dumps(envelope))

    def close(self) -> None:
        if self._client is None:
            return
        info = self._client.publish(
            self._disconnect_topic, json.dumps({"_client_id": self._client_id})
        )
        info.wait_for_publish(timeout=2)
        self._client.loop_stop()
        self._client.disconnect()
        self._client = None


class MXCuBEIPCClient:
    """See module docstring."""

    def __init__(
        self,
        token: str,
        transport: str = "jsonrpc",
        *,
        host: str = "127.0.0.1",
        port: int = 9999,
        broker_host: str = "localhost",
        broker_port: int = 1883,
        topic_prefix: str = "mxcube/ipc",
        client_id: Optional[str] = None,
        timeout: float = 5,
    ) -> None:
        self._timeout = timeout
        self._lock = threading.Lock()
        self._id_counter = itertools.count(1)
        self._pending: Dict[Union[str, int], threading.Event] = {}
        self._responses: Dict[Union[str, int], dict] = {}
        self._unmatched_error: Optional[dict] = None
        self._event_handlers: Dict[str, List[Callable[[dict], None]]] = defaultdict(
            list
        )

        if transport == "jsonrpc":
            self._transport = _JSONRPCClientTransport(
                host, port, on_frame=self._on_frame
            )
        elif transport == "nanomq":
            self._transport = _NanoMQClientTransport(
                broker_host,
                broker_port,
                topic_prefix,
                client_id=client_id,
                on_frame=self._on_frame,
            )
        else:
            raise ValueError(f"Unknown IPC transport: {transport!r}")

        self._transport.connect()

        self._call(AUTH_METHOD, {"token": token})
        description = self._call(DESCRIBE_METHOD, {})
        # Forwarded events, by name: {"role", "signal", "args", "args_schema"}
        # each - see IPC_FORMAT.md section 5. Set before _build_proxies()
        # so a role that happens to be named "events" is skipped, not
        # silently overwriting this.
        self.events: Dict[str, Dict[str, Any]] = description.get("events", {})
        self._build_proxies(description["methods"])

    # -- calling whitelisted methods -----------------------------------

    def _next_id(self) -> int:
        with self._lock:
            return next(self._id_counter)

    def _call(self, method: str, params: Dict[str, Any]) -> Any:
        request_id = self._next_id()
        event = threading.Event()
        with self._lock:
            if self._unmatched_error is not None:
                error = self._unmatched_error["error"]
                raise MXCuBEIPCError(error["code"], error["message"])
            self._pending[request_id] = event

        self._transport.send(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
        )

        if not event.wait(timeout=self._timeout):
            with self._lock:
                self._pending.pop(request_id, None)
            raise TimeoutError(f"no response to {method!r} within {self._timeout}s")

        with self._lock:
            response = self._responses.pop(request_id)

        error = response.get("error")
        if error:
            raise MXCuBEIPCError(error["code"], error["message"])
        return response.get("result")

    def _build_proxies(self, methods: Dict[str, Any]) -> None:
        roles: Dict[str, SimpleNamespace] = {}
        for method_path in methods:
            role, method_name = method_path.rsplit(".", 1)
            role_proxy = roles.setdefault(role, SimpleNamespace())
            setattr(role_proxy, method_name, self._make_bound_method(method_path))

        for role, role_proxy in roles.items():
            if hasattr(type(self), role) or role in self.__dict__:
                logger.warning(
                    "IPC client: role %r collides with a client attribute, skipping",
                    role,
                )
                continue
            setattr(self, role, role_proxy)

    def _make_bound_method(self, method_path: str) -> Callable[..., Any]:
        def bound_method(*args: Any, **kwargs: Any) -> Any:
            if args and kwargs:
                raise TypeError(
                    f"{method_path}: pass either one dict/pydantic model argument "
                    "or keyword arguments, not both"
                )
            if args:
                if len(args) != 1:
                    raise TypeError(
                        f"{method_path}: expected at most one positional argument"
                    )
                (params,) = args
                if hasattr(params, "model_dump"):
                    # A pydantic model - duck-typed, so pydantic itself
                    # isn't a dependency of this package.
                    params = params.model_dump(mode="json")
            else:
                params = kwargs
            return self._call(method_path, params)

        bound_method.__name__ = method_path.rsplit(".", 1)[-1]
        return bound_method

    def debug_list_roles(self, role: str = "") -> Any:
        """The direct sub-roles of `role` (or of the beamline itself if
        `role` is omitted).
        """
        return self._call(DEBUG_LIST_ROLES_METHOD, {"role": role})

    def debug_describe_role(self, role: str = "") -> Any:
        """One role's class, its own sub-roles, and its public callable
        attributes with their signature (best-effort).
        """
        return self._call(DEBUG_DESCRIBE_ROLE_METHOD, {"role": role})

    def debug_call(
        self,
        role: str,
        method: str,
        args: Optional[list] = None,
        kwargs: Optional[dict] = None,
    ) -> Any:
        """Call ANY method on ANY resolvable role, with raw arguments."""
        return self._call(
            DEBUG_CALL_METHOD,
            {
                "role": role,
                "method": method,
                "args": args or [],
                "kwargs": kwargs or {},
            },
        )

    def connect(self, event: str, handler: Callable[[dict], None]) -> None:
        """Register `handler(params)` to be called whenever an IPCEvent
        named `event` (e.g. "procedure.test_signal") arrives. Only events
        listed in `self.events` (the server's `events:` config) are ever
        sent.
        """
        with self._lock:
            self._event_handlers[event].append(handler)

    def disconnect(self, event: str, handler: Callable[[dict], None]) -> None:
        with self._lock:
            handlers = self._event_handlers.get(event)
            if handlers and handler in handlers:
                handlers.remove(handler)

    def _on_frame(self, frame: dict) -> None:
        if "method" in frame:
            # IPCEvent - no "id", unlike IPCResponse.
            method = frame["method"]
            params = frame.get("params", {})
            with self._lock:
                handlers = list(self._event_handlers.get(method, ()))
            for handler in handlers:
                try:
                    handler(params)
                except Exception:
                    logger.exception("IPC event handler for %r raised", method)
            return

        request_id = frame.get("id")
        with self._lock:
            event = self._pending.get(request_id)
            if event is None and frame.get("error"):
                # A rejection with no id ("Client already connected", which
                # has no request to blame it on) can't be matched to a
                # specific call - unblock the oldest pending one so it fails
                # with the real reason instead of just timing out. The
                # JSON-RPC/TCP server sends it straight after accepting the
                # connection, so it can also arrive before the first call
                # is pending - keep it for that call to raise.
                if self._pending:
                    request_id, event = next(iter(self._pending.items()))
                else:
                    self._unmatched_error = frame
            if event is not None:
                self._responses[request_id] = frame
        if event is not None:
            event.set()

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> "MXCuBEIPCClient":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()
