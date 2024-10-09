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

import sys
from concurrent.futures import Future, ThreadPoolExecutor
from typing import overload

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from couchbase_columnar import JSONType
from couchbase_columnar.common.credential import Credential
from couchbase_columnar.common.query import CancelToken
from couchbase_columnar.common.result import BlockingQueryResult
from couchbase_columnar.options import (ClusterOptions,
                                        ClusterOptionsKwargs,
                                        QueryOptions,
                                        QueryOptionsKwargs)
from couchbase_columnar.protocol.core.client_adapter import _ClientAdapter

class Cluster:
    @overload
    def __init__(self, connstr: str, credential: Credential) -> None: ...

    @overload
    def __init__(self,
                 connstr: str,
                 credential: Credential,
                 options: ClusterOptions) -> None: ...

    @overload
    def __init__(self,
                 connstr: str,
                 credential: Credential,
                 **kwargs: Unpack[ClusterOptionsKwargs]) -> None: ...

    @overload
    def __init__(self,
                 connstr: str,
                 credential: Credential,
                 options: ClusterOptions,
                 **kwargs: Unpack[ClusterOptionsKwargs]) -> None: ...

    @property
    def client_adapter(self) -> _ClientAdapter: ...

    @property
    def connected(self) -> bool: ...

    @property
    def threadpool_executor(self) -> ThreadPoolExecutor: ...

    def shutdown(self) -> None: ...

    @overload
    def execute_query(self, statement: str) -> BlockingQueryResult: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions) -> BlockingQueryResult: ...

    @overload
    def execute_query(self,
                      statement: str,
                      **kwargs: Unpack[QueryOptionsKwargs]) -> BlockingQueryResult: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      **kwargs: Unpack[QueryOptionsKwargs]
                      ) -> BlockingQueryResult: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      *args: JSONType,
                      **kwargs: Unpack[QueryOptionsKwargs]) -> BlockingQueryResult: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      *args: JSONType,
                      **kwargs: str) -> BlockingQueryResult: ...

    @overload
    def execute_query(self,
                      statement: str,
                      *args: JSONType,
                      **kwargs: str) -> BlockingQueryResult: ...

    @overload
    def execute_query(self,
                      statement: str,
                      cancel_token: CancelToken) -> Future[BlockingQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      cancel_token: CancelToken,
                      *args: JSONType) -> Future[BlockingQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      cancel_token: CancelToken) -> Future[BlockingQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      cancel_token: CancelToken,
                      **kwargs: Unpack[QueryOptionsKwargs]) -> Future[BlockingQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      cancel_token: CancelToken,
                      **kwargs: Unpack[QueryOptionsKwargs]) -> Future[BlockingQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      cancel_token: CancelToken,
                      *args: JSONType,
                      **kwargs: Unpack[QueryOptionsKwargs]) -> Future[BlockingQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      *args: JSONType,
                      cancel_token: CancelToken,
                      **kwargs: Unpack[QueryOptionsKwargs]) -> Future[BlockingQueryResult]: ...


    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      cancel_token: CancelToken,
                      *args: JSONType,
                      **kwargs: str) -> Future[BlockingQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      *args: JSONType,
                      cancel_token: CancelToken,
                      **kwargs: str) -> Future[BlockingQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      cancel_token: CancelToken,
                      *args: JSONType,
                      **kwargs: str) -> Future[BlockingQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      *args: JSONType,
                      cancel_token: CancelToken,
                      **kwargs: str) -> Future[BlockingQueryResult]: ...

    @overload
    @classmethod
    def create_instance(cls, connstr: str, credential: Credential) -> Cluster: ...

    @overload
    @classmethod
    def create_instance(cls,
                        connstr: str,
                        credential: Credential,
                        options: ClusterOptions) -> Cluster: ...

    @overload
    @classmethod
    def create_instance(cls,
                        connstr: str,
                        credential: Credential,
                        **kwargs: Unpack[ClusterOptionsKwargs]) -> Cluster: ...

    @overload
    @classmethod
    def create_instance(cls,
                        connstr: str,
                        credential: Credential,
                        options: ClusterOptions,
                        **kwargs: Unpack[ClusterOptionsKwargs]) -> Cluster: ...
