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
                    Any,
                    NoReturn,
                    Optional,
                    Union)

from couchbase_columnar.common.exceptions import ColumnarException, QueryOperationCanceledException
from couchbase_columnar.common.query import QueryMetadata
from couchbase_columnar.common.streaming import StreamingExecutor, StreamingState
from couchbase_columnar.protocol.core.result import CoreQueryIterator
from couchbase_columnar.protocol.exceptions import (PYCBCC_ERROR_MAP,
                                                    CoreColumnarException,
                                                    ErrorMapper,
                                                    ExceptionMap)

if TYPE_CHECKING:
    from couchbase_columnar.protocol.core.client import _CoreClient
    from couchbase_columnar.protocol.core.request import QueryRequest


class _QueryStreamingExecutor(StreamingExecutor):
    """
        **INTERNAL**
    """

    def __init__(self,
                 client: _CoreClient,
                 request: QueryRequest,
                 cancel_token: Optional[Event] = None,
                 cancel_poll_interval: Optional[float] = None,
                 lazy_execute: Optional[bool] = None) -> None:
        self._client = client
        self._request = request
        self._deserializer = request.deserializer
        if lazy_execute is not None:
            self._lazy_execute = lazy_execute
        else:
            self._lazy_execute = False
        self._streaming_state = StreamingState.NotStarted
        self._metadata: Optional[QueryMetadata] = None
        self._cancel_token: Optional[Event] = cancel_token
        self._cancel_poll_interval: Optional[float] = cancel_poll_interval
        self._query_iter: CoreQueryIterator
        self._tp_executor = ThreadPoolExecutor(max_workers=2)
        self._query_res_ft: Future[Union[bool, ColumnarException]]

    @property
    def cancel_token(self) -> Optional[Event]:
        """
            **INTERNAL**
        """
        return self._cancel_token

    @property
    def cancel_poll_interval(self) -> Optional[float]:
        """
            **INTERNAL**
        """
        return self._cancel_poll_interval

    @property
    def lazy_execute(self) -> bool:
        """
            **INTERNAL**
        """
        return self._lazy_execute

    @property
    def streaming_state(self) -> StreamingState:
        """
            **INTERNAL**
        """
        return self._streaming_state

    def cancel(self) -> None:
        """
            **INTERNAL**
        """
        if self._query_iter is None:
            return
        self._query_iter.cancel()
        # this shouldn't be possible, but check if the cancel_token should be set just in case
        if self._cancel_token is not None and not self._cancel_token.is_set():
            self._cancel_token.set()
        self._streaming_state = StreamingState.Cancelled

    def get_metadata(self) -> QueryMetadata:
        """
            **INTERNAL**
        """
        # TODO:  Maybe not needed if we get metadata automatically?
        if self._metadata is None:
            self.set_metadata()
            if self._metadata is None:
                raise RuntimeError('Query metadata is only available after all rows have been iterated.')
        return self._metadata

    def handle_exception(self, ex: Exception) -> NoReturn:
        """
            **INTERNAL**
        """
        exc_cls = PYCBCC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, ColumnarException)
        excptn = exc_cls(message=str(ex))
        raise excptn

    def set_metadata(self) -> None:
        """
            **INTERNAL**
        """
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

    def _get_core_query_result(self) -> Union[bool, ColumnarException]:
        """
            **INTERNAL**
        """
        res = self._query_iter.wait_for_core_query_result()
        if isinstance(res, CoreColumnarException):
            return ErrorMapper.build_exception(res)
        return res

    def submit_query(self) -> None:
        """
            **INTERNAL**
        """
        if not StreamingState.okay_to_stream(self._streaming_state):
            raise StreamingState.get_streaming_exception(self._streaming_state)

        self._streaming_state = StreamingState.Started
        query_iter = self._client.columnar_query_op(self._request)
        if isinstance(query_iter, CoreColumnarException):
            raise ErrorMapper.build_exception(query_iter)

        if not isinstance(query_iter, CoreQueryIterator):
            raise ValueError('Columnar query op unsuccessful.')
        self._query_iter = query_iter
        res = self._query_iter.wait_for_core_query_result()
        if isinstance(res, CoreColumnarException):
            raise ErrorMapper.build_exception(res)

    def _wait_for_result(self) -> None:
        """
            **INTERNAL**
        """
        if self._cancel_token is None:
            raise ValueError('Cannot wait in background if cancel token not provided.')

        if self._cancel_poll_interval is None:
            self._cancel_poll_interval = 0.25

        while not self._query_res_ft.done() and self._streaming_state != StreamingState.Cancelled:
            if self._cancel_token.wait(self._cancel_poll_interval):
                # this means we want to cancel
                self.cancel()

        res = self._query_res_ft.result()
        if isinstance(res, QueryOperationCanceledException):
            pass
        elif isinstance(res, Exception):
            raise res

    def submit_query_in_background(self) -> None:
        """
            **INTERNAL**
        """
        if not StreamingState.okay_to_stream(self._streaming_state):
            raise StreamingState.get_streaming_exception(self._streaming_state)
        self._streaming_state = StreamingState.Started
        res = self._client.columnar_query_op(self._request)
        if isinstance(res, CoreColumnarException):
            raise ErrorMapper.build_exception(res)

        if not isinstance(res, CoreQueryIterator):
            raise ValueError('Columnar query op unsuccessful.')
        self._query_iter = res
        self._query_res_ft = self._tp_executor.submit(self._get_core_query_result)
        self._wait_for_result()

    def get_next_row(self) -> Any:
        """
            **INTERNAL**
        """
        if self._query_iter is None or not StreamingState.okay_to_iterate(self._streaming_state):
            raise StopIteration

        if self._cancel_token is not None and self._cancel_token.is_set():
            self.cancel()
            raise StopIteration

        row = next(self._query_iter)
        if isinstance(row, CoreColumnarException):
            raise ErrorMapper.build_exception(row)
        # should only be None once query request is complete and _no_ errors found
        if row is None:
            self._streaming_state = StreamingState.Completed
            raise StopIteration

        return self._deserializer.deserialize(row)
