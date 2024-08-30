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

from typing import TYPE_CHECKING

from couchbase_columnar.scope import Scope

if TYPE_CHECKING:
    from couchbase_columnar.protocol.cluster import Cluster


class Database:
    """Create a Database instance.

    The database instance exposes the operations which are available to be performed against a Columnar database.

    Args:
        cluster (:class:`~couchbase_columnar.cluster.Cluster`): A :class:`~couchbase_columnar.cluster.Cluster` instance.
        database_name (str): The database name.

    """  # noqa: E501

    def __init__(self, cluster: Cluster, database_name: str) -> None:
        from couchbase_columnar.protocol.database import Database as _Database
        self._impl = _Database(cluster, database_name)

    @property
    def name(self) -> str:
        """
            str: The name of this :class:`~couchbase_columnar.database.Database` instance.
        """
        return self._impl.name

    def scope(self, scope_name: str) -> Scope:
        """Creates a :class:`~couchbase_columnar.scope.Scope` instance.

        Args:
            scope_name (str): Name of the scope.

        Returns:
            :class:`~couchbase_columnar.scope.Scope`

        """
        return Scope(self._impl, scope_name)
