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
from threading import Event
from typing import (TYPE_CHECKING,
                    Any,
                    Optional,
                    Union)

from couchbase_columnar.common.errors import (ColumnarError,
                                              InternalSDKError,
                                              QueryOperationCanceledError)
from couchbase_columnar.common.query import CancelToken, QueryMetadata
from couchbase_columnar.common.streaming import StreamingExecutor, StreamingState
from couchbase_columnar.protocol.core.result import CoreQueryIterator
from couchbase_columnar.protocol.errors import (ClientError,
                                                CoreColumnarError,
                                                ErrorMapper)

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
                 cancel_token: Optional[CancelToken] = None,
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
        self._cancel_token: Optional[CancelToken] = cancel_token
        self._query_iter: CoreQueryIterator
        self._tp_executor: ThreadPoolExecutor
        self._query_res_ft: Future[Union[bool, Union[ColumnarError, ClientError]]]

    @property
    def cancel_token(self) -> Optional[Event]:
        """
            **INTERNAL**
        """
        if self._cancel_token is not None:
            return self._cancel_token.token
        return None

    @property
    def cancel_poll_interval(self) -> Optional[float]:
        """
            **INTERNAL**
        """
        if self._cancel_token is not None:
            return self._cancel_token.poll_interval
        return None

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
        if self._cancel_token is not None and not self._cancel_token.token.is_set():
            self._cancel_token.token.set()
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

    def set_metadata(self) -> None:
        """
            **INTERNAL**
        """
        if self._query_iter is None:
            return

        try:
            query_metadata = self._query_iter.metadata()
        except ColumnarError as err:
            raise err
        except Exception as ex:
            raise InternalSDKError(str(ex))

        if isinstance(query_metadata, CoreColumnarError):
            raise ErrorMapper.build_error(query_metadata)
        if query_metadata is None:
            return
        self._metadata = QueryMetadata(query_metadata)

    def set_threadpool_executor(self, tp_executor: ThreadPoolExecutor) -> None:
        """
            **INTERNAL**
        """
        self._tp_executor = tp_executor

    def _get_core_query_result(self) -> Union[bool, Union[ColumnarError, ClientError]]:
        """
            **INTERNAL**
        """
        res = self._query_iter.wait_for_core_query_result()
        if isinstance(res, CoreColumnarError):
            return ErrorMapper.build_error(res)
        return res

    def submit_query(self) -> None:
        """
            **INTERNAL**
        """
        if not StreamingState.okay_to_stream(self._streaming_state):
            raise RuntimeError('Query has been canceled or previously executed.')

        self._streaming_state = StreamingState.Started
        try:
            self._query_iter = self._client.columnar_query_op(self._request)
        except Exception as ex:
            # suppress context, we know we have raised an error from the bindings
            if isinstance(ex, CoreColumnarError):
                raise ErrorMapper.build_error(ex) from None
            raise InternalSDKError(str(ex)) from None

        res = self._query_iter.wait_for_core_query_result()
        if isinstance(res, CoreColumnarError):
            raise ErrorMapper.build_error(res)

    def _wait_for_result(self) -> None:
        """
            **INTERNAL**
        """
        if self._cancel_token is None:
            raise ValueError('Cannot wait in background if cancel token not provided.')

        while not self._query_res_ft.done() and self._streaming_state != StreamingState.Cancelled:
            if self._cancel_token.token.wait(self._cancel_token.poll_interval):
                # this means we want to cancel
                self.cancel()

        res = self._query_res_ft.result()
        if isinstance(res, QueryOperationCanceledError):
            pass
        elif isinstance(res, Exception):
            raise res

    def submit_query_in_background(self) -> None:
        """
            **INTERNAL**
        """
        if not StreamingState.okay_to_stream(self._streaming_state):
            raise RuntimeError('Query has been canceled or previously executed.')

        self._streaming_state = StreamingState.Started
        try:
            self._query_iter = self._client.columnar_query_op(self._request)
        except Exception as ex:
            # suppress context, we know we have raised an error from the bindings
            if isinstance(ex, CoreColumnarError):
                raise ErrorMapper.build_error(ex) from None
            raise InternalSDKError(str(ex)) from None

        self._query_res_ft = self._tp_executor.submit(self._get_core_query_result)
        self._wait_for_result()

    def get_next_row(self) -> Any:
        """
            **INTERNAL**
        """
        if self._query_iter is None or not StreamingState.okay_to_iterate(self._streaming_state):
            raise StopIteration

        if self._cancel_token is not None and self._cancel_token.token.is_set():
            self.cancel()
            raise StopIteration

        row = next(self._query_iter)
        if isinstance(row, CoreColumnarError):
            raise ErrorMapper.build_error(row)
        # should only be None once query request is complete and _no_ errors found
        if row is None:
            self._streaming_state = StreamingState.Completed
            raise StopIteration

        return self._deserializer.deserialize(row)
