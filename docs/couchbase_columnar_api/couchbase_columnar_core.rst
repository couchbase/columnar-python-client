===========================
Core API
===========================

.. contents::
    :local:

Cluster
==============

.. module:: couchbase_columnar.cluster
.. autoclass:: Cluster

    .. important::
        See :ref:`Cluster Overloads<cluster-overloads-ref>` for details on overloaded methods.

    .. automethod:: create_instance
    .. automethod:: database

    .. important::
        See :ref:`Cluster Overloads<cluster-overloads-ref>` for details on overloaded methods.

    .. automethod:: execute_query
    .. automethod:: close


Database
==============

.. module:: couchbase_columnar.database
.. autoclass:: Database

    .. autoproperty:: name
    .. automethod:: scope

Scope
==============

.. module:: couchbase_columnar.scope
.. autoclass:: Scope

    .. autoproperty:: name

    .. important::
        See :ref:`Scope Overloads<scope-overloads-ref>` for details on overloaded methods.

    .. automethod:: execute_query
