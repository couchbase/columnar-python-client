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
from typing import TYPE_CHECKING, Optional

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

from acouchbase_columnar.database import AsyncDatabase
from couchbase_columnar.result import AsyncQueryResult

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop

    from couchbase_columnar.credential import Credential
    from couchbase_columnar.options import ClusterOptions


class AsyncCluster:
    def __init__(self,
                 connstr: str,
                 credential: Credential,
                 options: Optional[ClusterOptions] = None,
                 loop: Optional[AbstractEventLoop] = None,
                 **kwargs: object) -> None:
        from acouchbase_columnar.protocol.cluster import AsyncCluster as _AsyncCluster
        self._impl = _AsyncCluster(connstr, credential, options, loop, **kwargs)

    def database(self, name: str) -> AsyncDatabase:
        """Creates a database instance.

        .. seealso::
            :class:`.database.AsyncDatabase`

        Args:
            name (str): Name of the database

        Returns:
            :class:`~acouchbase_columnar.database.AsyncDatabase`: A database instance

        """
        return AsyncDatabase(self._impl, name)

    def execute_query(self, statement: str, *args: object, **kwargs: object) -> Future[AsyncQueryResult]:
        return self._impl.execute_query(statement, *args, **kwargs)

    def close(self) -> None:
        return self._impl.close()

    @classmethod
    def create_instance(cls,
                        connstr: str,
                        credential: Credential,
                        options: Optional[ClusterOptions] = None,
                        loop: Optional[AbstractEventLoop] = None,
                        **kwargs: object) -> AsyncCluster:
        return cls(connstr, credential, options, loop=loop, **kwargs)


Cluster: TypeAlias = AsyncCluster
