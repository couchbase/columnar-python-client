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

from concurrent.futures import Future
from typing import (TYPE_CHECKING,
                    Optional,
                    Union)

from couchbase_columnar.database import Database
from couchbase_columnar.result import BlockingQueryResult

if TYPE_CHECKING:
    from couchbase_columnar.credential import Credential
    from couchbase_columnar.options import ClusterOptions


class Cluster:
    """Create a Cluster instance.

    The cluster instance exposes the operations which are available to be performed against a Columnar cluster.

    .. important::
        Use the static :meth:`.Cluster.create_instance` method to create a Cluster.

    Args:
        connstr:
            The connection string to use for connecting to the cluster.
            The format of the connection string is the *scheme* (``couchbases`` as TLS enabled connections are _required_), followed a hostname
        credential: User credentials.
        options: Global options to set for the cluster.
            Some operations allow the global options to be overriden by passing in options to the operation.
        **kwargs: keyword arguments that can be used in place or to overrride provided :class:`~couchbase_columnar.options.ClusterOptions`

    Raises:
        ValueError: If incorrect connstr is provided.
        ValueError: If incorrect options are provided.

    """  # noqa: E501

    def __init__(self,
                 connstr: str,
                 credential: Credential,
                 options: Optional[ClusterOptions] = None,
                 **kwargs: object) -> None:
        from couchbase_columnar.protocol.cluster import Cluster as _Cluster
        self._impl = _Cluster(connstr, credential, options, **kwargs)

    def database(self, name: str) -> Database:
        """Creates a database instance.

        .. seealso::
            :class:`~couchbase_columnar.database.Database`

        Args:
            name: Name of the database

        Returns:
            A Database instance.

        """
        return Database(self._impl, name)

    def execute_query(self,
                      statement: str,
                      *args: object,
                      **kwargs: object) -> Union[Future[BlockingQueryResult], BlockingQueryResult]:
        """Executes a query against a Capella Columnar cluster.

        .. note::
            A departure from the operational SDK, the query is *NOT* executed lazily.

        .. seealso::
            :meth:`couchbase_columnar.Scope.execute_query`: For how to execute scope-level queries.

        Args:
            statement: The SQL++ statement to execute.
            options (:class:`~couchbase_columnar.options.QueryOptions`): Optional parameters for the query operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to override provided :class:`~couchbase_columnar.options.QueryOptions`

        Returns:
            :class:`~couchbase_columnar.result.BlockingQueryResult`: An instance of a :class:`~couchbase_columnar.result.BlockingQueryResult` which
            provides access to iterate over the query results and access metadata and metrics about the query.

        Examples:
            Simple query::

                q_str = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country LIKE 'United%' LIMIT 2;'
                q_res = cluster.execute_query(q_str)
                for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple query with positional parameters::

                from couchbase_columnar.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country LIKE $1 LIMIT $2;'
                q_res = cluster.execute_query(q_str, QueryOptions(positional_parameters=['United%', 5]))
                for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple query with named parameters::

                from couchbase_columnar.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country LIKE $country LIMIT $lim;'
                q_res = cluster.execute_query(q_str, QueryOptions(named_parameters={'country': 'United%', 'lim':2}))
                for row in q_res.rows():
                    print(f'Found row: {row}')

            Retrieve metadata and/or metrics from query::

                from couchbase_columnar.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $country LIMIT $lim;'
                q_res = cluster.execute_query(q_str, QueryOptions(named_parameters={'country': 'United%', 'lim':2}))
                for row in q_res.rows():
                    print(f'Found row: {row}')

                print(f'Query metadata: {q_res.metadata()}')
                print(f'Query metrics: {q_res.metadata().metrics()}')

        """  # noqa: E501
        return self._impl.execute_query(statement, *args, **kwargs)

    def shutdown(self) -> None:
        """Shuts down this cluster instance. Cleaning up all resources associated with it.

        .. warning::
            Use of this method is almost *always* unnecessary.  Cluster resources should be cleaned
            up once the cluster instance falls out of scope.  However, in some applications tuning resources
            is necessary and in those types of applications, this method might be beneficial.

        """
        return self._impl.shutdown()

    @classmethod
    def create_instance(cls,
                        connstr: str,
                        credential: Credential,
                        options: Optional[ClusterOptions] = None,
                        **kwargs: object) -> Cluster:
        """Create a Cluster instance

        Args:
            connstr:
                The connection string to use for connecting to the cluster.
                The format of the connection string is the *scheme* (``couchbases`` as TLS enabled connections are _required_), followed a hostname
            credential: User credentials.
            options: Global options to set for the cluster.
                Some operations allow the global options to be overriden by passing in options to the operation.
            **kwargs: Keyword arguments that can be used in place or to overrride provided :class:`~couchbase_columnar.options.ClusterOptions`


        Returns:
            A Capella Columnar Cluster instance.

        Raises:
            ValueError: If incorrect connstr is provided.
            ValueError: If incorrect options are provided.


        Examples:
            Initialize cluster using default options::

                from couchbase_columnar.cluster import Cluster
                from couchbase_columnar.credential import Credential

                cred = Credential.from_username_and_password('username', 'password')
                cluster = Cluster.create_instance('couchbases://hostname', cred)


            Initialize cluster using with global timeout options::

                from datetime import timedelta

                from couchbase_columnar.cluster import Cluster
                from couchbase_columnar.credential import Credential
                from couchbase_columnar.options import ClusterOptions, ClusterTimeoutOptions

                cred = Credential.from_username_and_password('username', 'password')
                opts = ClusterOptions(timeout_options=ClusterTimeoutOptions(query_timeout=timedelta(seconds=120)))
                cluster = Cluster.create_instance('couchbases://hostname', cred, opts)

        """  # noqa: E501
        return cls(connstr, credential, options, **kwargs)
