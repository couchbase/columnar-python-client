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

from typing import TYPE_CHECKING, Optional

from couchbase_columnar.result import BlockingQueryResult

if TYPE_CHECKING:
    from couchbase_columnar.credential import Credential
    from couchbase_columnar.options import ClusterOptions


class Cluster:
    def __init__(self,
                 connstr: str,
                 credential: Credential,
                 options: Optional[ClusterOptions],
                 **kwargs: object) -> None:
        from couchbase_columnar.protocol.cluster import Cluster as _Cluster
        self._impl = _Cluster(connstr, credential, options, **kwargs)

    def execute_query(self, statement: str, *args: object, **kwargs: object) -> BlockingQueryResult:
        return self._impl.execute_query(statement, *args, **kwargs)

    def close(self) -> None:
        return self._impl.close()

    @classmethod
    def create_instance(cls,
                        connstr: str,
                        credential: Credential,
                        options: Optional[ClusterOptions],
                        **kwargs: object) -> Cluster:
        return cls(connstr, credential, options, **kwargs)
