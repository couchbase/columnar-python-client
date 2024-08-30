======================================
Core API
======================================


.. contents::
    :local:

AsyncCluster
==============

.. module:: acouchbase_columnar.cluster
.. autoclass:: AsyncCluster

    .. important::
        See :ref:`AsyncCluster Overloads<async-cluster-overloads-ref>` for details on overloaded methods.

    .. automethod:: create_instance
    .. automethod:: database

    .. important::
        See :ref:`AsyncCluster Overloads<async-cluster-overloads-ref>` for details on overloaded methods.

    .. automethod:: execute_query
    .. automethod:: close


AsyncDatabase
==============

.. module:: acouchbase_columnar.database
.. autoclass:: AsyncDatabase

    .. autoproperty:: name
    .. automethod:: scope

AsyncScope
==============

.. module:: acouchbase_columnar.scope
.. autoclass:: AsyncScope

    .. autoproperty:: name

    .. important::
        See :ref:`AsyncScope Overloads<async-scope-overloads-ref>` for details on overloaded methods.

    .. automethod:: execute_query
