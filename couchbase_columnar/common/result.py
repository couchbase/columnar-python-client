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

from typing import (Any,
                    Iterable,
                    List,
                    Optional)

from couchbase_columnar.common.core.result import QueryResult as QueryResult
from couchbase_columnar.common.query import QueryMetadata
from couchbase_columnar.common.streaming import (AsyncIterator,
                                                 BlockingIterator,
                                                 StreamingExecutor)


class BlockingQueryResult(QueryResult):
    def __init__(self, executor: StreamingExecutor, lazy_execute: Optional[bool] = None) -> None:
        self._executor = executor
        self._lazy_execute = lazy_execute

    def cancel(self) -> None:
        self._executor.cancel()

    def get_all_rows(self) -> List[Any]:
        """Convenience method to execute the query.

        Returns:
            List[Any]:  A list of query results.

        Example:
            q_rows = cluster.query('SELECT * FROM `travel-sample` WHERE country LIKE 'United%' LIMIT 2;').all_rows()

        """
        return BlockingIterator(self._executor).get_all_rows()

    def metadata(self) -> QueryMetadata:
        """The meta-data which has been returned by the query.

        Returns:
            :class:`~couchbase_columnar.query.QueryMetadata`: An instance of :class:`~couchbase_columnar.query.QueryMetadata`.

        Raises:
            :class:`RuntimeError`: When the metadata is not available. Metadata is only available once all rows have been iterated.
        """  # noqa: E501
        return self._executor.get_metadata()

    def rows(self) -> Iterable[Any]:
        """The rows which have been returned by the query.

        .. note::
            If using the *acouchbase* API be sure to use ``async for`` when looping over rows.

        Returns:
            Iterable: Either an iterable or async iterable.
        """
        return BlockingIterator(self._executor)

    def __iter__(self) -> BlockingIterator:
        return iter(BlockingIterator(self._executor))

    def __repr__(self) -> str:
        return "QueryResult()"


class AsyncQueryResult(QueryResult):
    def __init__(self, executor: StreamingExecutor) -> None:
        self._executor = executor

    def cancel(self) -> None:
        self._executor.cancel()

    async def get_all_rows(self) -> List[Any]:
        """Convenience method to execute the query.

        Returns:
            List[Any]:  A list of query results.

        Example:
            q_rows = cluster.query('SELECT * FROM `travel-sample` WHERE country LIKE 'United%' LIMIT 2;').execute()

        """
        return await AsyncIterator(self._executor).get_all_rows()

    def metadata(self) -> QueryMetadata:
        """The meta-data which has been returned by the query.

        Returns:
            :class:`~couchbase_columnar.query.QueryMetadata`: An instance of :class:`~couchbase_columnar.query.QueryMetadata`.

        Raises:
            :class:`RuntimeError`: When the metadata is not available. Metadata is only available once all rows have been iterated.
        """  # noqa: E501
        return self._executor.get_metadata()

    def rows(self) -> AsyncIterator:
        """The rows which have been returned by the query.

        .. note::
            If using the *acouchbase* API be sure to use ``async for`` when looping over rows.

        Returns:
            Iterable: Either an iterable or async iterable.
        """
        return AsyncIterator(self._executor)

    def __aiter__(self) -> AsyncIterator:
        return AsyncIterator(self._executor).__aiter__()

    def __repr__(self) -> str:
        return "AsyncQueryResult()"
