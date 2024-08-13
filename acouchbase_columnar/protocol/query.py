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

from asyncio import Future
from threading import Event
from typing import (TYPE_CHECKING,
                    Any,
                    NoReturn,
                    Optional,
                    Union)

from couchbase_columnar.common.exceptions import ColumnarException
from couchbase_columnar.common.query import QueryMetadata
from couchbase_columnar.common.result import AsyncQueryResult
from couchbase_columnar.common.streaming import StreamingExecutor, StreamingState
from couchbase_columnar.protocol.core.result import CoreQueryIterator
from couchbase_columnar.protocol.exceptions import (PYCBCC_ERROR_MAP,
                                                    CoreColumnarException,
                                                    ErrorMapper,
                                                    ExceptionMap)

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop

    from couchbase_columnar.protocol.core.client import _CoreClient
    from couchbase_columnar.protocol.core.request import QueryRequest


class _AsyncQueryStreamingExecutor(StreamingExecutor):
    """
        **INTERNAL**
    """

    def __init__(self,
                 client: _CoreClient,
                 loop: AbstractEventLoop,
                 request: QueryRequest) -> None:
        self._client = client
        self._loop = loop
        self._request = request
        self._query_iter: CoreQueryIterator
        self._deserializer = request.deserializer
        self._metadata: Optional[QueryMetadata] = None
        self._streaming_state = StreamingState.NotStarted
        self._row_ft: Future[Any]

    @property
    def cancel_token(self) -> Optional[Event]:
        """
            **INTERNAL**
        """
        return None

    @property
    def cancel_poll_interval(self) -> Optional[float]:
        """
            **INTERNAL**
        """
        return None

    @property
    def lazy_execute(self) -> bool:
        """
            **INTERNAL**
        """
        return False

    @property
    def streaming_state(self) -> StreamingState:
        """
            **INTERNAL**
        """
        return self._streaming_state

    def cancel(self) -> None:
        if self._query_iter is None:
            return
        self._query_iter.cancel()
        print('cancelled request')
        self._streaming_state = StreamingState.Cancelled

    def get_metadata(self) -> QueryMetadata:
        # TODO:  Maybe not needed if we get metadata automatically?
        if self._metadata is None:
            self.set_metadata()
            if self._metadata is None:
                raise RuntimeError('Query metadata is only available after all rows have been iterated.')
        return self._metadata

    def handle_exception(self, ex: Exception) -> NoReturn:
        exc_cls = PYCBCC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, ColumnarException)
        excptn = exc_cls(message=str(ex))
        raise excptn

    def set_metadata(self) -> None:
        if self._query_iter is None:
            return

        try:
            query_metadata = self._query_iter.metadata()
        except ColumnarException as ex:
            raise ex
        except Exception as ex:
            exc_cls = PYCBCC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, ColumnarException)
            excptn = exc_cls(message=str(ex))
            raise excptn

        if isinstance(query_metadata, CoreColumnarException):
            raise ErrorMapper.build_exception(query_metadata)
        if query_metadata is None:
            return
        self._metadata = QueryMetadata(query_metadata)

    def submit_query(self) -> Future[AsyncQueryResult]:
        if not StreamingState.okay_to_stream(self._streaming_state):
            raise StreamingState.get_streaming_exception(self._streaming_state)

        self._streaming_state = StreamingState.Started
        query_iter = self._client.columnar_query_op(self._request,
                                                    callback=self._set_query_core_result,
                                                    row_callback=self._row_callback)

        if not isinstance(query_iter, CoreQueryIterator):
            # TODO:  better exception
            raise ValueError('Columnar query op unsuccessful.')

        self._query_iter = query_iter

        self._iter_ft: Future[AsyncQueryResult] = self._loop.create_future()
        return self._iter_ft

    async def get_next_row(self) -> Any:
        return await self._get_next_row()

    def _set_query_core_result(self, res:  Union[bool, ColumnarException]) -> None:
        if self._iter_ft.cancelled():
            return

        if isinstance(res, CoreColumnarException):
            exc = ErrorMapper.build_exception(res)
            self._loop.call_soon_threadsafe(self._iter_ft.set_exception, exc)
        else:
            self._loop.call_soon_threadsafe(self._iter_ft.set_result, AsyncQueryResult(self))

    def _row_callback(self, row: Any) -> None:
        if isinstance(row, CoreColumnarException):
            exc = ErrorMapper.build_exception(row)
            self._loop.call_soon_threadsafe(self._row_ft.set_exception, exc)
        else:
            self._loop.call_soon_threadsafe(self._row_ft.set_result, row)

    async def _get_next_row(self) -> Any:
        if self._query_iter is None or not StreamingState.okay_to_iterate(self._streaming_state):
            raise StopAsyncIteration

        self._row_ft = self._loop.create_future()
        next(self._query_iter)
        row = await self._row_ft
        if row is None:
            self._done_streaming = True
            raise StopAsyncIteration

        return self._deserializer.deserialize(row)
