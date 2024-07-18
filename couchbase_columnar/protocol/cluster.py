#  Copyright 2016-2024. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
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

from typing import TYPE_CHECKING, Optional

from couchbase_columnar.common.result import BlockingQueryResult
from couchbase_columnar.protocol.core.client_adapter import _ClientAdapter
from couchbase_columnar.protocol.core.request import ClusterRequestBuilder
from couchbase_columnar.protocol.query import _QueryStreamingExecutor

if TYPE_CHECKING:
    from couchbase_columnar.common.credential import Credential
    from couchbase_columnar.options import ClusterOptions


class Cluster:

    def __init__(self,
                 connstr: str,
                 credential: Credential,
                 options: Optional[ClusterOptions],
                 **kwargs: object) -> None:
        self._client_adapter = _ClientAdapter(connstr, credential, options, **kwargs)
        self._request_builder = ClusterRequestBuilder(self._client_adapter)
        self._connect()

    @property
    def client_adapter(self) -> _ClientAdapter:
        """
            **INTERNAL**
        """
        return self._client_adapter

    @property
    def has_connection(self) -> bool:
        """
            bool: Indicator on if the cluster has been connected or not.
        """
        return self._client_adapter.has_connection

    def _close(self) -> None:
        """
            **INTERNAL**
        """
        req = self._request_builder.build_close_connection_request()
        self._client_adapter.close_connection(req)
        self._client_adapter.reset_client()

    def _connect(self) -> None:
        """
            **INTERNAL**
        """
        req = self._request_builder.build_connection_request()
        self._client_adapter.connect(req)

    def close(self) -> None:
        """Shuts down this cluster instance. Cleaning up all resources associated with it.

        .. warning::
            Use of this method is almost *always* unnecessary.  Cluster resources should be cleaned
            up once the cluster instance falls out of scope.  However, in some applications tuning resources
            is necessary and in those types of applications, this method might be beneficial.

        """
        if self.has_connection:
            self._close()
        else:
            # TODO: log warning and/or exception?
            print('Cluster does not have a connection.  Ignoring')

    def execute_query(self, statement: str, *args: object, **kwargs: object) -> BlockingQueryResult:
        executor = _QueryStreamingExecutor(self.client_adapter.client,
                                           self._request_builder.build_query_request(statement,
                                                                                     *args,
                                                                                     **kwargs))
        return BlockingQueryResult(executor)

    @classmethod
    def create_instance(cls,
                        connstr: str,
                        credential: Credential,
                        options: Optional[ClusterOptions],
                        **kwargs: object) -> Cluster:
        return cls(connstr, credential, options, **kwargs)
