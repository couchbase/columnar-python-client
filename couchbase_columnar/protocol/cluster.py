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

import atexit
from concurrent.futures import Future, ThreadPoolExecutor
from typing import (TYPE_CHECKING,
                    Optional,
                    Union)

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
                 options: Optional[ClusterOptions] = None,
                 **kwargs: object) -> None:
        self._client_adapter = _ClientAdapter(connstr, credential, options, **kwargs)
        self._request_builder = ClusterRequestBuilder(self._client_adapter)
        self._connect()
        # Allow the default max_workers which is (as of Python 3.8): min(32, os.cpu_count() + 4).
        # We can add an option later if we see a need
        self._tp_executor = ThreadPoolExecutor()
        self._tp_executor_shutdown_called = False
        atexit.register(self._shutdown_executor)

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

    @property
    def threadpool_executor(self) -> ThreadPoolExecutor:
        """
            **INTERNAL**
        """
        return self._tp_executor

    def _shutdown(self) -> None:
        """
            **INTERNAL**
        """
        req = self._request_builder.build_close_connection_request()
        self._client_adapter.close_connection(req)
        self._client_adapter.reset_client()
        if self._tp_executor_shutdown_called is False:
            self._tp_executor.shutdown()

    def _connect(self) -> None:
        """
            **INTERNAL**
        """
        req = self._request_builder.build_connection_request()
        self._client_adapter.connect(req)

    def _shutdown_executor(self) -> None:
        if self._tp_executor_shutdown_called is False:
            self._tp_executor.shutdown()
        self._tp_executor_shutdown_called = True

    def shutdown(self) -> None:
        """Shuts down this cluster instance. Cleaning up all resources associated with it.

        .. warning::
            Use of this method is almost *always* unnecessary.  Cluster resources should be cleaned
            up once the cluster instance falls out of scope.  However, in some applications tuning resources
            is necessary and in those types of applications, this method might be beneficial.

        """
        if self.has_connection:
            self._shutdown()
        else:
            # TODO: log warning and/or exception?
            print('Cluster does not have a connection.  Ignoring')

    def _execute_query_in_background(self, executor: _QueryStreamingExecutor) -> BlockingQueryResult:
        """
            **INTERNAL**
        """
        executor.submit_query_in_background()
        return BlockingQueryResult(executor)

    def execute_query(self,
                      statement: str,
                      *args: object,
                      **kwargs: object) -> Union[BlockingQueryResult, Future[BlockingQueryResult]]:
        req, cancel_token = self._request_builder.build_query_request(statement, *args, **kwargs)
        lazy_execute = req.options.pop('lazy_execute', None)
        executor = _QueryStreamingExecutor(self.client_adapter.client,
                                           req,
                                           cancel_token=cancel_token,
                                           lazy_execute=lazy_execute)
        if executor.cancel_token is not None:
            executor.set_threadpool_executor(self.threadpool_executor)
            if lazy_execute is True:
                raise RuntimeError(('Cannot cancel, via cancel token, a query that is executed lazily.'
                                    ' Queries executed lazily can be cancelled only after iteration begins.'))
            ft = self.threadpool_executor.submit(self._execute_query_in_background, executor)
            return ft
        else:
            if executor.lazy_execute is not True:
                executor.submit_query()
            return BlockingQueryResult(executor)

    @classmethod
    def create_instance(cls,
                        connstr: str,
                        credential: Credential,
                        options: Optional[ClusterOptions],
                        **kwargs: object) -> Cluster:
        return cls(connstr, credential, options, **kwargs)
