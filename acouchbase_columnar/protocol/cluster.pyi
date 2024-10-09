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
from asyncio import AbstractEventLoop, Future
from typing import overload

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from acouchbase_columnar.protocol.core.client_adapter import _ClientAdapter
from acouchbase_columnar.protocol.database import AsyncDatabase
from couchbase_columnar.common.credential import Credential
from couchbase_columnar.common.result import AsyncQueryResult
from couchbase_columnar.options import (ClusterOptions,
                                        ClusterOptionsKwargs,
                                        QueryOptions,
                                        QueryOptionsKwargs)

class AsyncCluster:
    @overload
    def __init__(self, connstr: str, credential: Credential) -> None: ...

    @overload
    def __init__(self,
                 connstr: str,
                 credential: Credential,
                 loop: AbstractEventLoop) -> None: ...

    @overload
    def __init__(self,
                 connstr: str,
                 credential: Credential,
                 options: ClusterOptions) -> None: ...

    @overload
    def __init__(self,
                 connstr: str,
                 credential: Credential,
                 options: ClusterOptions,
                 loop: AbstractEventLoop) -> None: ...

    @overload
    def __init__(self,
                 connstr: str,
                 credential: Credential,
                 **kwargs: Unpack[ClusterOptionsKwargs]) -> None: ...

    @overload
    def __init__(self,
                 connstr: str,
                 credential: Credential,
                 loop: AbstractEventLoop,
                 **kwargs: Unpack[ClusterOptionsKwargs]) -> None: ...

    @overload
    def __init__(self,
                 connstr: str,
                 credential: Credential,
                 options: ClusterOptions,
                 **kwargs: Unpack[ClusterOptionsKwargs]) -> None: ...

    @overload
    def __init__(self,
                 connstr: str,
                 credential: Credential,
                 options: ClusterOptions,
                 loop: AbstractEventLoop,
                 **kwargs: Unpack[ClusterOptionsKwargs]) -> None: ...

    @property
    def client_adapter(self) -> _ClientAdapter: ...

    @property
    def connected(self) -> bool: ...

    def shutdown(self) -> None: ...

    def database(self, name: str) -> AsyncDatabase: ...

    @overload
    def execute_query(self, statement: str) -> Future[AsyncQueryResult]: ...

    @overload
    def execute_query(self, statement: str, options: QueryOptions) -> Future[AsyncQueryResult]: ...

    @overload
    def execute_query(self, statement: str, **kwargs: Unpack[QueryOptionsKwargs]) -> Future[AsyncQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      **kwargs: Unpack[QueryOptionsKwargs]) -> Future[AsyncQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      *args: str,
                      **kwargs: Unpack[QueryOptionsKwargs]) -> Future[AsyncQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      *args: str,
                      **kwargs: str) -> Future[AsyncQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      *args: str,
                      **kwargs: str) -> Future[AsyncQueryResult]: ...

    @overload
    @classmethod
    def create_instance(cls, connstr: str, credential: Credential) -> AsyncCluster: ...

    @overload
    @classmethod
    def create_instance(cls,
                        connstr: str,
                        credential: Credential,
                        loop: AbstractEventLoop) -> AsyncCluster: ...

    @overload
    @classmethod
    def create_instance(cls,
                        connstr: str,
                        credential: Credential,
                        options: ClusterOptions) -> AsyncCluster: ...

    @overload
    @classmethod
    def create_instance(cls,
                        connstr: str,
                        credential: Credential,
                        options: ClusterOptions,
                        loop: AbstractEventLoop) -> AsyncCluster: ...

    @overload
    @classmethod
    def create_instance(cls,
                        connstr: str,
                        credential: Credential,
                        **kwargs: Unpack[ClusterOptionsKwargs]) -> AsyncCluster: ...

    @overload
    @classmethod
    def create_instance(cls,
                        connstr: str,
                        credential: Credential,
                        loop: AbstractEventLoop,
                        **kwargs: Unpack[ClusterOptionsKwargs]) -> AsyncCluster: ...

    @overload
    @classmethod
    def create_instance(cls,
                        connstr: str,
                        credential: Credential,
                        options: ClusterOptions,
                        **kwargs: Unpack[ClusterOptionsKwargs]) -> AsyncCluster: ...

    @overload
    @classmethod
    def create_instance(cls,
                        connstr: str,
                        credential: Credential,
                        options: ClusterOptions,
                        loop: AbstractEventLoop,
                        **kwargs: Unpack[ClusterOptionsKwargs]) -> AsyncCluster: ...
