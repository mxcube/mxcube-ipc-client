"""A minimal stand-in for mxcubecore's IPCServer on its JSON-RPC/TCP
transport, speaking the same wire format (mxcubecore/ipc/IPC_FORMAT.md) -
so the client can be tested without mxcubecore, a beamline, or gevent.

Plain threads: one accept loop, one handler thread per connection. Like
the real transport, only one client may be connected at a time; any
further connection gets an id-less CLIENT_ALREADY_CONNECTED error frame
and is closed.
"""

import json
import socket
import threading
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
)

from mxcube_ipc_client import ErrorCode


class FakeIPCServer:
    def __init__(
        self,
        token: str,
        methods: Optional[Dict[str, Callable[[dict], Any]]] = None,
        events: Optional[Dict[str, Dict[str, Any]]] = None,
        allow_debug_calls: bool = False,
    ) -> None:
        self.token = token
        self.methods = methods or {}
        self.events = events or {}
        self.allow_debug_calls = allow_debug_calls
        #: Every request frame received, in order.
        self.requests: List[dict] = []

        self._listener = socket.create_server(("127.0.0.1", 0))
        self.address = self._listener.getsockname()
        self._lock = threading.Lock()
        self._active: Optional[socket.socket] = None
        self._active_file = None
        self._disconnected = threading.Event()
        self._closed = False
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def push_event(self, method: str, *args: Any) -> None:
        self._send({"jsonrpc": "2.0", "method": method, "params": {"args": list(args)}})

    def wait_for_disconnect(self, timeout: float = 5) -> bool:
        return self._disconnected.wait(timeout)

    def close(self) -> None:
        self._closed = True
        self._listener.close()
        with self._lock:
            if self._active is not None:
                self._active.close()

    def _accept_loop(self) -> None:
        while not self._closed:
            try:
                sock, _ = self._listener.accept()
            except OSError:
                return
            with self._lock:
                busy = self._active is not None
                if not busy:
                    self._active = sock
                    self._active_file = sock.makefile(mode="rwb")
                    self._disconnected.clear()
            if busy:
                error = {
                    "code": ErrorCode.CLIENT_ALREADY_CONNECTED,
                    "message": "Client already connected",
                }
                sock.sendall(
                    (
                        json.dumps({"jsonrpc": "2.0", "id": None, "error": error})
                        + "\n"
                    ).encode()
                )
                sock.close()
                continue
            threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self) -> None:
        authenticated = False
        try:
            for line in self._active_file:
                if not line.strip():
                    continue
                request = json.loads(line)
                with self._lock:
                    self.requests.append(request)
                authenticated = self._handle(request, authenticated)
        except (OSError, ValueError):
            pass
        finally:
            with self._lock:
                self._active = None
                self._active_file = None
            self._disconnected.set()

    def _handle(self, request: dict, authenticated: bool) -> bool:
        request_id, method = request.get("id"), request["method"]
        params = request.get("params", {})

        if method == "_auth":
            if params.get("token") != self.token:
                self._error(request_id, ErrorCode.NOT_AUTHENTICATED, "Invalid token")
                return False
            self._result(request_id, {"authenticated": True})
            return True
        if not authenticated:
            self._error(request_id, ErrorCode.NOT_AUTHENTICATED, "Not authenticated")
        elif method == "_describe":
            methods = {
                name: {"request_schema": {}, "response_schema": {}}
                for name in self.methods
            }
            self._result(request_id, {"methods": methods, "events": self.events})
        elif method.startswith("_debug_"):
            if not self.allow_debug_calls:
                self._error(
                    request_id,
                    ErrorCode.DEBUG_CALLS_DISABLED,
                    "Debug calls are disabled",
                )
            else:
                # Echo back what was asked, so tests can check the envelope.
                self._result(request_id, {"method": method, "params": params})
        elif method not in self.methods:
            self._error(
                request_id,
                ErrorCode.METHOD_NOT_WHITELISTED,
                f"Method not whitelisted: {method}",
            )
        else:
            try:
                result = self.methods[method](params)
            except Exception as exc:
                self._error(request_id, ErrorCode.VALIDATION_ERROR, str(exc))
            else:
                self._result(request_id, result)
        return authenticated

    def _result(self, request_id: Any, result: Any) -> None:
        self._send({"jsonrpc": "2.0", "id": request_id, "result": result})

    def _error(self, request_id: Any, code: ErrorCode, message: str) -> None:
        self._send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": int(code), "message": message},
            }
        )

    def _send(self, frame: dict) -> None:
        with self._lock:
            if self._active_file is None:
                return
            self._active_file.write((json.dumps(frame) + "\n").encode("utf-8"))
            self._active_file.flush()
