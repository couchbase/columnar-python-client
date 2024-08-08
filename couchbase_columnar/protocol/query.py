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

from typing import (TYPE_CHECKING,
                    Any,
                    NoReturn,
                    Optional)

from couchbase_columnar.common.exceptions import ColumnarException
from couchbase_columnar.common.query import QueryMetadata
from couchbase_columnar.common.streaming import StreamingExecutor
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
                 request: QueryRequest) -> None:
        self._client = client
        self._request = request
        self._query_iter: Optional[CoreQueryIterator] = None
        self._started_streaming = False
        self._deserializer = request.deserializer
        self._done_streaming = False
        self._metadata: Optional[QueryMetadata] = None
        # self.submit_query()

    @property
    def done_streaming(self) -> bool:
        return self._done_streaming

    @property
    def started_streaming(self) -> bool:
        return self._started_streaming

    def cancel(self) -> None:
        if self._query_iter is None:
            return
        self._query_iter.cancel()

    def get_metadata(self) -> Optional[QueryMetadata]:
        # TODO:  Maybe not needed if we get metadata automatically?
        if self._metadata is None:
            self.set_metadata()
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
            # TODO:  better exception
            raise ColumnarException.from_message('Metadata unavailable.')
        self._metadata = QueryMetadata(query_metadata)

    def submit_query(self) -> None:
        if self._done_streaming:
            return
        self._started_streaming = True
        res = self._client.columnar_query_op(self._request)
        if isinstance(res, CoreColumnarException):
            raise ErrorMapper.build_exception(res)

        if not isinstance(res, CoreQueryIterator):
            raise ValueError('Columnar query op unsuccessful.')
        self._query_iter = res

    def get_next_row(self) -> Any:
        if self._done_streaming is True or self._query_iter is None:
            return

        row = next(self._query_iter)
        if isinstance(row, CoreColumnarException):
            raise ErrorMapper.build_exception(row)
        # should only be None once query request is complete and _no_ errors found
        if row is None:
            self._done_streaming = True
            raise StopIteration

        return self._deserializer.deserialize(row)
