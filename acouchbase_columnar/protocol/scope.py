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

import sys
from asyncio import Future
from functools import partial
from typing import TYPE_CHECKING

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

from acouchbase_columnar.protocol.core.client_adapter import _ClientAdapter
from acouchbase_columnar.protocol.query import _AsyncQueryStreamingExecutor
from couchbase_columnar.common.result import AsyncQueryResult
from couchbase_columnar.protocol.core.request import ScopeRequestBuilder

if TYPE_CHECKING:
    from acouchbase_columnar.protocol.database import AsyncDatabase


class AsyncScope:

    def __init__(self, database: AsyncDatabase, scope_name: str) -> None:
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
            str: The name of this :class:`~acouchbase_columnar.protocol.scope.Scope` instance.
        """
        return self._scope_name

    def _query_done_callback(self, executor: _AsyncQueryStreamingExecutor, ft: Future) -> None:
        if ft.cancelled():
            executor.cancel()

    def execute_query(self, statement: str, *args: object, **kwargs: object) -> Future[AsyncQueryResult]:
        req, _ = self._request_builder.build_query_request(statement, *args, **kwargs)
        executor = _AsyncQueryStreamingExecutor(self.client_adapter.client,
                                                self.client_adapter.loop,
                                                req)
        ft = executor.submit_query()
        ft.add_done_callback(partial(self._query_done_callback, executor))
        return ft


Scope: TypeAlias = AsyncScope
