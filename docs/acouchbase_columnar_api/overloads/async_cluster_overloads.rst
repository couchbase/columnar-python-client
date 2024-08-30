=======================
AsyncCluster Overloads
=======================

.. _async-cluster-overloads-ref:

AsyncCluster
==============

.. module:: acouchbase_columnar.cluster
    :no-index:

.. important::
    Not all class methods are listed.  Only methods that allow overloads.

.. py:class:: AsyncCluster
    :no-index:

    .. py:method:: execute_query(statement: str) -> Future[AsyncQueryResult]
                   execute_query(statement: str, options: QueryOptions) -> Future[AsyncQueryResult]
                   execute_query(statement: str, **kwargs: QueryOptionsKwargs) -> Future[AsyncQueryResult]
                   execute_query(statement: str, options: QueryOptions, **kwargs: QueryOptionsKwargs) -> BlockingQueryResult
                   execute_query(statement: str, options: QueryOptions, *args: JSONType, **kwargs: QueryOptionsKwargs) -> Future[AsyncQueryResult]
                   execute_query(statement: str, options: QueryOptions, *args: JSONType, **kwargs: str) -> Future[AsyncQueryResult]
                   execute_query(statement: str, *args: JSONType, **kwargs: str) -> Future[AsyncQueryResult]
        :no-index:

        Executes a query against a Capella Columnar cluster.

        .. important::
            The cancel API is **VOLATILE** and is subject to change at any time.

        :param statement: The SQL++ statement to execute.
        :type statement: str
        :param options: Options to set for the query.
        :type options: Optional[:class:`~acouchbase_columnar.options.QueryOptions`]
        :param \*args: Can be used to pass in positional query placeholders.
        :type \*args: Optional[:py:type:`~acouchbase_columnar.JSONType`]
        :param \*\*kwargs: Keyword arguments that can be used in place or to overrride provided :class:`~acouchbase_columnar.options.ClusterOptions`.
            Can also be used to pass in named query placeholders.
        :type \*\*kwargs: Optional[Union[:class:`~acouchbase_columnar.options.QueryOptionsKwargs`, str]]

        :returns: A :class:`~asyncio.Future` is returned.  Once the :class:`~asyncio.Future` completes, an instance of a :class:`~acouchbase_columnar.result.AsyncQueryResult` will be available.
        :rtype: Future[:class:`~acouchbase_columnar.result.AsyncQueryResult`]

    .. py:method:: create_instance(connstr: str, credential: Credential) -> AsyncCluster
                   create_instance(connstr: str, credential: Credential, loop: AbstractEventLoop) -> AsyncCluster
                   create_instance(connstr: str, credential: Credential, options: ClusterOptions) -> AsyncCluster
                   create_instance(connstr: str, credential: Credential, options: ClusterOptions, loop: AbstractEventLoop) -> AsyncCluster
                   create_instance(connstr: str, credential: Credential, **kwargs: ClusterOptionsKwargs) -> AsyncCluster
                   create_instance(connstr: str, credential: Credential, loop: AbstractEventLoop, **kwargs: ClusterOptionsKwargs) -> AsyncCluster
                   create_instance(connstr: str, credential: Credential, options: ClusterOptions, **kwargs: ClusterOptionsKwargs) -> AsyncCluster
                   create_instance(connstr: str, credential: Credential, options: ClusterOptions, loop: AbstractEventLoop, **kwargs: ClusterOptionsKwargs) -> AsyncCluster
        :classmethod:
        :no-index:

        Create a Cluster instance

        :param connstr: The connection string to use for connecting to the cluster.
                        The format of the connection string is the *scheme* (``couchbases`` as TLS enabled connections are _required_), followed a hostname
        :type connstr: str
        :param credential: The user credentials.
        :type credential: :class:`~acouchbase_columnar.credential.Credential`
        :param loop: The asyncio event loop.
        :type loop: AbstractEventLoop
        :param options: Global options to set for the cluster.
                        Some operations allow the global options to be overriden by passing in options to the operation.
        :type options: Optional[:class:`~acouchbase_columnar.options.ClusterOptions`]
        :param \*\*kwargs: Keyword arguments that can be used in place or to overrride provided :class:`~acouchbase_columnar.options.ClusterOptions`
        :type \*\*kwargs: Optional[:class:`~acouchbase_columnar.options.ClusterOptionsKwargs`]

        :returns: A Capella Columnar AsyncCluster instance.
        :rtype: :class:`.AsyncCluster`

        :raises ValueError: If incorrect connstr is provided.
        :raises ValueError: If incorrect options are provided.
