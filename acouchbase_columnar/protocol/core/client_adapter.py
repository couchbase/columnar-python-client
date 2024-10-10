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
from asyncio import AbstractEventLoop
from functools import wraps
from typing import (Any,
                    Callable,
                    Dict,
                    Optional,
                    TypeVar,
                    Union)

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

from acouchbase_columnar import get_event_loop
from couchbase_columnar.common.credential import Credential
from couchbase_columnar.common.deserializer import Deserializer
from couchbase_columnar.common.errors import ColumnarError, InternalSDKError
from couchbase_columnar.protocol.connection import _ConnectionDetails
from couchbase_columnar.protocol.core.client import _CoreClient
from couchbase_columnar.protocol.core.request import CloseConnectionRequest, ConnectRequest
from couchbase_columnar.protocol.core.result import CoreResult
from couchbase_columnar.protocol.errors import CoreColumnarError, ErrorMapper
from couchbase_columnar.protocol.options import OptionsBuilder

ReqT = TypeVar('ReqT', ConnectRequest, CloseConnectionRequest)


class AsyncWrapper:
    """
        **INTERNAL**
    """

    @classmethod
    def blocking_call(cls) -> Callable[[InputWrappedFn[ReqT]], OutputWrappedBlockingFn[ReqT]]:
        """
            **INTERNAL**
        """

        def decorator(fn: InputWrappedFn[ReqT]) -> OutputWrappedBlockingFn[ReqT]:  # noqa: C901:
            @wraps(fn)
            def wrapped_fn(self: _ClientAdapter, req: ReqT) -> Optional[CoreResult]:
                try:
                    ret = fn(self, req)
                    if isinstance(ret, CoreColumnarError):
                        raise ErrorMapper.build_error(ret)
                    return ret
                except ColumnarError as err:
                    raise err
                except CoreColumnarError as err:
                    raise ErrorMapper.build_error(err) from None
                except Exception as ex:
                    raise InternalSDKError(str(ex))

            return wrapped_fn
        return decorator


class _ClientAdapter:
    """
        **INTERNAL**
    """

    def __init__(self,
                 connstr: str,
                 credential: Credential,
                 options: Optional[object] = None,
                 loop: Optional[AbstractEventLoop] = None,
                 **kwargs: object) -> None:

        self._loop = self._get_loop(loop)
        self._cluster_info = None
        self._server_version = None
        self._opts_builder = OptionsBuilder()
        self._conn_details = _ConnectionDetails.create(self._opts_builder,
                                                       connstr,
                                                       credential,
                                                       options,
                                                       **kwargs)

    @property
    def client(self) -> _CoreClient:
        """
            **INTERNAL**
        """
        return self._client

    @property
    def has_connection(self) -> bool:
        """
            **INTERNAL**
        """
        if not hasattr(self, '_client'):
            return False
        return self._client.has_connection

    @property
    def connection_details(self) -> _ConnectionDetails:
        """
            **INTERNAL**
        """
        return self._conn_details

    @property
    def default_deserializer(self) -> Deserializer:
        """
            **INTERNAL**
        """
        return self._conn_details.default_deserializer

    @property
    def loop(self) -> AbstractEventLoop:
        """
            **INTERNAL**
        """
        return self._loop

    @property
    def options_builder(self) -> OptionsBuilder:
        """
            **INTERNAL**
        """
        return self._opts_builder

    @AsyncWrapper.blocking_call()
    def connect(self, req: ConnectRequest) -> None:
        """
            **INTERNAL**
        """
        if not hasattr(self, '_client'):
            self._client = _CoreClient()

        ret = self.client.connect(req)
        if isinstance(ret, CoreColumnarError):
            raise ErrorMapper.build_error(ret)
        self._client.connection = ret

    def close_connection(self, req: CloseConnectionRequest) -> bool:
        """
            **INTERNAL**
        """
        return self._client.close_connection(req)

    def reset_client(self) -> None:
        """
            **INTERNAL**
        """
        if hasattr(self, '_client'):
            del self._client

    def _get_loop(self, loop: Optional[AbstractEventLoop] = None) -> AbstractEventLoop:
        """
            **INTERNAL**
        """
        if loop is None:
            loop = get_event_loop()

        return loop

    def _test_connect(self, req: ConnectRequest) -> Dict[str, Any]:
        """
            **INTERNAL**
        """
        if not hasattr(self, '_client'):
            self._client = _CoreClient()

        try:
            return self.client._test_connect(req)
        except CoreColumnarError as err:
            raise ErrorMapper.build_error(err)


InputWrappedFn: TypeAlias = Callable[[_ClientAdapter, ReqT],
                                     Optional[Union[CoreColumnarError, CoreResult]]]
OutputWrappedBlockingFn: TypeAlias = Callable[[_ClientAdapter, ReqT], Optional[CoreResult]]
