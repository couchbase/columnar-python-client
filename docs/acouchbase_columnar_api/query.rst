==============
Query (SQL++)
==============

.. contents::
    :local:
    :depth: 2


Enumerations
===============

.. module:: acouchbase_columnar.query
.. autoenum:: QueryScanConsistency
    :no-index:


Options
===============

.. module:: acouchbase_columnar.options
    :no-index:

.. autoclass:: QueryOptions
    :no-index:


Results
===============

AsyncQueryResult
+++++++++++++++++++
.. module:: acouchbase_columnar.result
    :no-index:

.. py:class:: AsyncQueryResult
    :no-index:

    .. automethod:: cancel
        :no-index:
    .. automethod:: rows
        :no-index:
    .. automethod:: get_all_rows
        :no-index:
    .. automethod:: metadata
        :no-index:

.. module:: acouchbase_columnar.query
    :no-index:

QueryMetadata
+++++++++++++++++++
.. py:class:: QueryMetadata
    :no-index:

    .. automethod:: request_id
    .. automethod:: warnings
    .. automethod:: metrics

QueryMetrics
+++++++++++++++++++
.. py:class:: QueryMetrics
    :no-index:

    .. automethod:: elapsed_time
    .. automethod:: execution_time
    .. automethod:: result_count
    .. automethod:: result_size
    .. automethod:: processed_objects

QueryWarning
+++++++++++++++++++
.. py:class:: QueryWarning
    :no-index:

    .. automethod:: code
    .. automethod:: message
