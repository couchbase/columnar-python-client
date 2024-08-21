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
from typing import TYPE_CHECKING

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

from couchbase_columnar.result import AsyncQueryResult

if TYPE_CHECKING:
    from acouchbase_columnar.protocol.database import AsyncDatabase


class AsyncScope:
    def __init__(self, database: AsyncDatabase, scope_name: str) -> None:
        from acouchbase_columnar.protocol.scope import AsyncScope as _AsyncScope
        self._impl = _AsyncScope(database, scope_name)

    @property
    def name(self) -> str:
        """
            str: The name of this :class:`~acouchbase_columnar.scope.AsyncScope` instance.
        """
        return self._impl.name

    def execute_query(self, statement: str, *args: object, **kwargs: object) -> Future[AsyncQueryResult]:
        return self._impl.execute_query(statement, *args, **kwargs)


Scope: TypeAlias = AsyncScope
