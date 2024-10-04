#  Copyright 2016-2024. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    Callable,
                    Dict,
                    Optional)

from couchbase_columnar.protocol.core import PyCapsuleType
from couchbase_columnar.protocol.core.result import CoreQueryIterator
from couchbase_columnar.protocol.pycbcc_core import (_test_create_connection,
                                                     close_connection,
                                                     columnar_query,
                                                     create_connection)

if TYPE_CHECKING:
    from couchbase_columnar.protocol.core.request import (CloseConnectionRequest,
                                                          ConnectRequest,
                                                          QueryRequest)


class _CoreClient:
    """
    **INTERNAL**
    """

    def __init__(self) -> None:
        self._connection: Optional[PyCapsuleType] = None

    @property
    def has_connection(self) -> bool:
        """
        bool: Indicator on if the cluster has been connected or not.
        """
        return hasattr(self, "_connection") and self._connection is not None

    @property
    def connection(self) -> Optional[PyCapsuleType]:
        """
        **INTERNAL**
        """
        return self._connection

    @connection.setter
    def connection(self, conn: PyCapsuleType) -> None:
        """
        **INTERNAL**
        """
        self._connection = conn

    def close_connection(self, req: CloseConnectionRequest) -> bool:
        """
        **INTERNAL**
        """
        return close_connection(self.connection, **req.to_req_dict())

    def connect(self, req: ConnectRequest) -> PyCapsuleType:
        """
        **INTERNAL**
        """
        final_kwargs = req.to_req_dict()
        conn_str = final_kwargs.pop('connection_str')
        return create_connection(conn_str, **final_kwargs)

    def columnar_query_op(self,
                          req: QueryRequest,
                          callback: Optional[Callable[..., None]] = None,
                          row_callback: Optional[Callable[..., None]] = None,
                          run_in_background: Optional[bool] = None) -> CoreQueryIterator:
        """
        **INTERNAL**
        """
        final_kwargs = req.to_req_dict()
        final_kwargs['conn'] = self.connection
        if callback is not None:
            final_kwargs['callback'] = callback
        if row_callback is not None:
            final_kwargs['row_callback'] = row_callback
        if run_in_background is not None:
            final_kwargs['run_in_background'] = run_in_background
        return columnar_query(**final_kwargs)

    def _test_connect(self, req: ConnectRequest) -> Dict[str, Any]:
        """
        **INTERNAL**
        """
        final_kwargs = req.to_req_dict()
        conn_str = final_kwargs.pop('connection_str')
        return _test_create_connection(conn_str, **final_kwargs)
