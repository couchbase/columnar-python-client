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

from concurrent.futures import Future, ThreadPoolExecutor
from typing import TYPE_CHECKING, Union

from couchbase_columnar.common.result import BlockingQueryResult
from couchbase_columnar.protocol.core.client_adapter import _ClientAdapter
from couchbase_columnar.protocol.core.request import ScopeRequestBuilder
from couchbase_columnar.protocol.query import _QueryStreamingExecutor

if TYPE_CHECKING:
    from couchbase_columnar.protocol.database import Database


class Scope:

    def __init__(self, database: Database, scope_name: str) -> None:
        self._database = database
        self._scope_name = scope_name
        self._request_builder = ScopeRequestBuilder(self.client_adapter, self._database.name, self.name)

    @property
    def client_adapter(self) -> _ClientAdapter:
        """
            **INTERNAL**
        """
        return self._database.client_adapter

    @property
    def name(self) -> str:
        """
            str: The name of this :class:`~couchbase_columnar.protocol.scope.Scope` instance.
        """
        return self._scope_name

    @property
    def threadpool_executor(self) -> ThreadPoolExecutor:
        """
            **INTERNAL**
        """
        return self._database.threadpool_executor

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
