=====================
AsyncScope Overloads
=====================

.. _async-scope-overloads-ref:

AsyncScope
==============

.. module:: acouchbase_columnar.scope
    :no-index:

.. important::
    Not all class methods are listed.  Only methods that allow overloads.

.. py:class:: AysncScope
    :no-index:

    .. py:method:: execute_query(statement: str) -> Future[AsyncQueryResult]
                    execute_query(statement: str, options: QueryOptions) -> Future[AsyncQueryResult]
                    execute_query(statement: str, **kwargs: QueryOptionsKwargs) -> Future[AsyncQueryResult]
                    execute_query(statement: str, options: QueryOptions, **kwargs: QueryOptionsKwargs) -> BlockingQueryResult
                    execute_query(statement: str, options: QueryOptions, *args: JSONType, **kwargs: QueryOptionsKwargs) -> Future[AsyncQueryResult]
                    execute_query(statement: str, options: QueryOptions, *args: JSONType, **kwargs: str) -> Future[AsyncQueryResult]
                    execute_query(statement: str, *args: JSONType, **kwargs: str) -> Future[AsyncQueryResult]
        :no-index:

        Executes a query against a Capella Columnar scope.

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
