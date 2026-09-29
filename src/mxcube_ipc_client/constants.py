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

"""Wire-protocol constants, mirrored from mxcubecore.ipc.constants.

These are part of the IPC JSON RPC format (mxcubecore/ipc/IPC_FORMAT.md).
Yhey're duplicated here to avoid a depdency on mxcubecore. Any change should
incrase the IPC_FORMAT_VERSION.
"""

import logging
from enum import IntEnum

#: Version of the IPC envolope format (mxcubecore/ipc/IPC_FORMAT.md) this
#: client implements.
IPC_FORMAT_VERSION = "0.0.1"

#: Method name reserved for the authentication handshake.
AUTH_METHOD = "_auth"

DESCRIBE_METHOD = "_describe"

#: Method names reserved for the debug bypass
#: Only handled at all if the server's `allow_debug_calls` config is true.
DEBUG_LIST_ROLES_METHOD = "_debug_list_roles"
DEBUG_DESCRIBE_ROLE_METHOD = "_debug_describe_role"
DEBUG_CALL_METHOD = "_debug_call"

logger = logging.getLogger("mxcube_ipc_client")


class ErrorCode(IntEnum):
    """Error codes carried in a server error response - see
    MXCuBEIPCError.code.

    The negative range below -32000 mirrors the JSON-RPC 2.0 reserved codes
    so a plain JSON-RPC client can interpret them, as defined by the
    JSON-RPC 2.0 spec.
    """

    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603

    NOT_AUTHENTICATED = -32000
    METHOD_NOT_WHITELISTED = -32001
    VALIDATION_ERROR = -32002
    CLIENT_ALREADY_CONNECTED = -32003
    DEBUG_CALLS_DISABLED = -32004
