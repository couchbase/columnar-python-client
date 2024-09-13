=================
Cluster Overloads
=================

.. _cluster-overloads-ref:

Cluster
==============

.. module:: couchbase_columnar.cluster
    :no-index:

.. important::
    Not all class methods are listed.  Only methods that allow overloads.

.. py:class:: Cluster
    :no-index:

    .. py:method:: execute_query(statement: str) -> BlockingQueryResult
                   execute_query(statement: str, options: QueryOptions) -> BlockingQueryResult
                   execute_query(statement: str, **kwargs: QueryOptionsKwargs) -> BlockingQueryResult
                   execute_query(statement: str, options: QueryOptions, **kwargs: QueryOptionsKwargs) -> BlockingQueryResult
                   execute_query(statement: str, options: QueryOptions, *args: JSONType, **kwargs: QueryOptionsKwargs) -> BlockingQueryResult
                   execute_query(statement: str, options: QueryOptions, *args: JSONType, **kwargs: str) -> BlockingQueryResult
                   execute_query(statement: str, *args: JSONType, **kwargs: str) -> BlockingQueryResult
                   execute_query(statement: str, cancel_token: CancelToken) -> Future[BlockingQueryResult]
                   execute_query(statement: str, cancel_token: CancelToken, *args: JSONType) -> Future[BlockingQueryResult]
                   execute_query(statement: str, options: QueryOptions, cancel_token: CancelToken) -> Future[BlockingQueryResult]
                   execute_query(statement: str, cancel_token: CancelToken, **kwargs: QueryOptionsKwargs) -> Future[BlockingQueryResult]
                   execute_query(statement: str, options: QueryOptions, cancel_token: CancelToken, **kwargs: QueryOptionsKwargs) -> Future[BlockingQueryResult]
                   execute_query(statement: str, options: QueryOptions, cancel_token: CancelToken, *args: JSONType, **kwargs: QueryOptionsKwargs) -> Future[BlockingQueryResult]
                   execute_query(statement: str, options: QueryOptions, *args: JSONType, cancel_token: CancelToken, **kwargs: QueryOptionsKwargs) -> Future[BlockingQueryResult]
                   execute_query(statement: str, options: QueryOptions, cancel_token: CancelToken, *args: JSONType, **kwargs: str) -> Future[BlockingQueryResult]
                   execute_query(statement: str, options: QueryOptions, *args: JSONType, cancel_token: CancelToken, **kwargs: str) -> Future[BlockingQueryResult]
                   execute_query(statement: str, cancel_token: CancelToken, *args: JSONType, **kwargs: str) -> Future[BlockingQueryResult]
                   execute_query(statement: str, *args: JSONType, cancel_token: CancelToken, **kwargs: str) -> Future[BlockingQueryResult]
        :no-index:

        Executes a query against a Capella Columnar cluster.

        .. important::
            The cancel API is **VOLATILE** and is subject to change at any time.

        :param statement: The SQL++ statement to execute.
        :type statement: str
        :param options: Options to set for the query.
        :type options: Optional[:class:`~couchbase_columnar.options.QueryOptions`]
        :param cancel_token: A cancel token used to cancel the results stream.
        :type cancel_token: Optional[:class:`~couchbase_columnar.query.CancelToken`]
        :param \*args: Can be used to pass in positional query placeholders.
        :type \*args: Optional[:py:type:`~couchbase_columnar.JSONType`]
        :param \*\*kwargs: Keyword arguments that can be used in place or to overrride provided :class:`~couchbase_columnar.options.ClusterOptions`.
            Can also be used to pass in named query placeholders.
        :type \*\*kwargs: Optional[Union[:class:`~couchbase_columnar.options.QueryOptionsKwargs`, str]]

        :returns: An instance of :class:`~couchbase_columnar.result.BlockingQueryResult`. When a cancel token is provided
            a :class:`~concurrent.futures.Future` is returned.  Once the :class:`~concurrent.futures.Future` completes, an instance of a :class:`~couchbase_columnar.result.BlockingQueryResult` will be available.
        :rtype: Union[Future[:class:`~couchbase_columnar.result.BlockingQueryResult`], :class:`~couchbase_columnar.result.BlockingQueryResult`]

    .. py:method:: create_instance(connstr: str, credential: Credential) -> Cluster
                   create_instance(connstr: str, credential: Credential, options: ClusterOptions) -> Cluster
                   create_instance(connstr: str, credential: Credential, **kwargs: ClusterOptionsKwargs) -> Cluster
                   create_instance(connstr: str, credential: Credential, options: ClusterOptions, **kwargs: ClusterOptionsKwargs) -> Cluster
        :classmethod:
        :no-index:

        Create a Cluster instance

        :param connstr: The connection string to use for connecting to the cluster.
                        The format of the connection string is the *scheme* (``couchbases`` as TLS enabled connections are _required_), followed a hostname
        :type connstr: str
        :param credential: The user credentials.
        :type credential: :class:`~couchbase_columnar.credential.Credential`
        :param options: Global options to set for the cluster.
                        Some operations allow the global options to be overriden by passing in options to the operation.
        :type options: Optional[:class:`~couchbase_columnar.options.ClusterOptions`]
        :param \*\*kwargs: Keyword arguments that can be used in place or to overrride provided :class:`~couchbase_columnar.options.ClusterOptions`
        :type \*\*kwargs: Optional[:class:`~couchbase_columnar.options.ClusterOptionsKwargs`]

        :returns: A Capella Columnar Cluster instance.
        :rtype: :class:`.Cluster`

        :raises ValueError: If incorrect connstr is provided.
        :raises ValueError: If incorrect options are provided.
