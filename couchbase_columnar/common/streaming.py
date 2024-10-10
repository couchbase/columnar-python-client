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
from abc import ABC, abstractmethod
from asyncio import Future
from enum import IntEnum
from threading import Event
from typing import (Any,
                    Coroutine,
                    List,
                    Optional,
                    Union)

if sys.version_info < (3, 9):
    from typing import AsyncIterator as PyAsyncIterator
    from typing import Iterator
else:
    from collections.abc import AsyncIterator as PyAsyncIterator
    from collections.abc import Iterator

from couchbase_columnar.common.errors import ColumnarError, InternalSDKError
from couchbase_columnar.common.query import QueryMetadata


class StreamingState(IntEnum):
    """
    **INTERNAL
    """
    NotStarted = 0
    Started = 1
    Cancelled = 2
    Completed = 3

    @staticmethod
    def okay_to_stream(state: StreamingState) -> bool:
        """
        **INTERNAL
        """
        return state == StreamingState.NotStarted

    @staticmethod
    def okay_to_iterate(state: StreamingState) -> bool:
        """
        **INTERNAL
        """
        return state == StreamingState.Started


class StreamingExecutor(ABC):
    """
    **INTERNAL
    """

    @property
    @abstractmethod
    def cancel_token(self) -> Optional[Event]:
        raise NotImplementedError

    @property
    @abstractmethod
    def cancel_poll_interval(self) -> Optional[float]:
        raise NotImplementedError

    @property
    @abstractmethod
    def lazy_execute(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def streaming_state(self) -> StreamingState:
        raise NotImplementedError

    @abstractmethod
    def cancel(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_metadata(self) -> QueryMetadata:
        raise NotImplementedError

    @abstractmethod
    def set_metadata(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def submit_query(self) -> Union[Future[Any], None]:
        raise NotImplementedError

    @abstractmethod
    def get_next_row(self) -> Union[Coroutine[Any, Any, Any], Any]:
        raise NotImplementedError


class BlockingIterator(Iterator[Any]):
    """
    **INTERNAL
    """

    def __init__(self, executor: StreamingExecutor) -> None:
        self._executor = executor

    def get_all_rows(self) -> List[Any]:
        """
        **INTERNAL
        """
        return [r for r in list(self)]

    def __iter__(self) -> BlockingIterator:
        """
        **INTERNAL
        """
        if self._executor.lazy_execute is True:
            self._executor.submit_query()

        return self

    def __next__(self) -> Any:
        """
        **INTERNAL
        """
        try:
            return self._executor.get_next_row()
        except StopIteration:
            # TODO:  get metadata automatically?
            # self._executor.set_metadata()
            raise
        except ColumnarError as err:
            raise err
        except Exception as ex:
            raise InternalSDKError(str(ex))


class AsyncIterator(PyAsyncIterator[Any]):
    """
    **INTERNAL
    """

    def __init__(self, executor: StreamingExecutor) -> None:
        self._executor = executor

    async def get_all_rows(self) -> List[Any]:
        """
        **INTERNAL
        """
        return [r async for r in self]

    def __aiter__(self) -> AsyncIterator:
        """
        **INTERNAL
        """
        return self

    async def __anext__(self) -> Any:
        """
        **INTERNAL
        """
        try:
            return await self._executor.get_next_row()
        except StopAsyncIteration:
            # TODO:  get metadata automatically?
            # self._executor.set_metadata()
            raise
        except ColumnarError as err:
            raise err
        except Exception as ex:
            raise InternalSDKError(str(ex))
