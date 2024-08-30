==============
Query (SQL++)
==============

.. contents::
    :local:
    :depth: 2


Enumerations
===============

.. module:: couchbase_columnar.query
.. autoenum:: QueryScanConsistency


Options
===============

.. module:: couchbase_columnar.options
    :no-index:

.. autoclass:: QueryOptions
    :no-index:


Results
===============

BlockingQueryResult
+++++++++++++++++++
.. module:: couchbase_columnar.result
    :no-index:

.. py:class:: BlockingQueryResult
    :no-index:

    .. automethod:: cancel
        :no-index:
    .. automethod:: rows
        :no-index:
    .. automethod:: get_all_rows
        :no-index:
    .. automethod:: metadata
        :no-index:

.. module:: couchbase_columnar.query
    :no-index:

QueryMetadata
+++++++++++++++++++
.. py:class:: QueryMetadata

    .. automethod:: request_id
    .. automethod:: warnings
    .. automethod:: metrics

QueryMetrics
+++++++++++++++++++
.. py:class:: QueryMetrics

    .. automethod:: elapsed_time
    .. automethod:: execution_time
    .. automethod:: result_count
    .. automethod:: result_size
    .. automethod:: processed_objects

QueryWarning
+++++++++++++++++++
.. py:class:: QueryWarning

    .. automethod:: code
    .. automethod:: message


Cancellation
===============

.. module:: couchbase_columnar.query
    :no-index:

.. warning::
    **VOLATILE** This API is subject to change at any time.

CancelToken
+++++++++++++++++++
.. autoclass:: CancelToken
    :members:
    :undoc-members:
