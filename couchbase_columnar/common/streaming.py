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

import sys
from abc import ABC, abstractmethod
from typing import (Any,
                    Coroutine,
                    List,
                    NoReturn,
                    Union)

if sys.version_info < (3, 9):
    from typing import AsyncIterator as PyAsyncIterator
    from typing import Iterator
else:
    from collections.abc import AsyncIterator as PyAsyncIterator
    from collections.abc import Iterator

from couchbase_columnar.common.exceptions import AlreadyQueriedException, ColumnarException
from couchbase_columnar.common.query import QueryMetadata


class StreamingExecutor(ABC):

    @property
    @abstractmethod
    def done_streaming(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def started_streaming(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def cancel(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_metadata(self) -> QueryMetadata:
        raise NotImplementedError

    @abstractmethod
    def handle_exception(self, ex: Exception) -> NoReturn:
        raise NotImplementedError

    @abstractmethod
    def set_metadata(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def submit_query(self) -> Union[Coroutine[Any, Any, None], None]:
        raise NotImplementedError

    @abstractmethod
    def get_next_row(self) -> Union[Coroutine[Any, Any, Any], Any]:
        raise NotImplementedError


class BlockingIterator(Iterator[Any]):
    def __init__(self, executor: StreamingExecutor) -> None:
        self._executor = executor

    def get_all_rows(self) -> List[Any]:
        return [r for r in list(self)]

    def __iter__(self) -> BlockingIterator:
        if self._executor.done_streaming:
            raise AlreadyQueriedException()

        if not self._executor.started_streaming:
            self._executor.submit_query()

        return self

    def __next__(self) -> Any:
        try:
            return self._executor.get_next_row()
        except StopIteration:
            # TODO:  get metadata automatically?
            # self._executor.set_metadata()
            raise
        except ColumnarException as ex:
            raise ex
        except Exception as ex:
            self._executor.handle_exception(ex)


class AsyncIterator(PyAsyncIterator[Any]):
    def __init__(self, executor: StreamingExecutor) -> None:
        self._executor = executor

    async def get_all_rows(self) -> List[Any]:
        return [r async for r in self]

    def __aiter__(self) -> AsyncIterator:
        if self._executor.done_streaming:
            raise AlreadyQueriedException()

        # if not self._executor.started_streaming:
        #     self._executor.submit_query()

        return self

    async def __anext__(self) -> Any:
        try:
            return await self._executor.get_next_row()
        except StopAsyncIteration:
            self._executor.set_metadata()
            raise
        except ColumnarException as ex:
            raise ex
        except Exception as ex:
            self._executor.handle_exception(ex)
