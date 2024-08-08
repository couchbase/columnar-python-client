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

from concurrent.futures import Future, ThreadPoolExecutor
from threading import Event
from typing import (TYPE_CHECKING,
                    Optional,
                    Union)

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
        # Allow the default max_workers which is (as of Python 3.8): min(32, os.cpu_count() + 4).
        # We can add an option later if we see a need
        self._tp_executor = ThreadPoolExecutor()

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
        cancel_token: Optional[Event] = kwargs.pop('cancel_token', None)
        cancel_poll_interval: float = kwargs.pop('cancel_poll_interval', 0.25)
        req = self._request_builder.build_query_request(statement, *args, **kwargs)
        lazy_execute = req.options.pop('lazy_execute', None)
        executor = _QueryStreamingExecutor(self.client_adapter.client,
                                           req,
                                           cancel_token=cancel_token,
                                           cancel_poll_interval=cancel_poll_interval,
                                           lazy_execute=lazy_execute)
        if executor.cancel_token is not None:
            # TODO:  warning or exception?  lazy is only available for the non-cancellable path
            # if lazy_execute is not None:
            #     raise Exception()
            ft = self._tp_executor.submit(self._execute_query_in_background, executor)
            return ft
        else:
            if executor.lazy_execute is not True:
                executor.submit_query()
            return BlockingQueryResult(executor)
