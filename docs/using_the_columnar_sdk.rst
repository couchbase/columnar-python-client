=============================
Using the Python Columnar SDK
=============================

The Columnar Python SDK library allows you to connect to a Capella Columnar cluster from Python.

Useful Links
=======================

* :columnar_sdk_github:`Source <>`
* :columnar_sdk_jira:`Bug Tracker <>`
* :columnar_sdk_docs:`Python docs on the Couchbase website <>`
* :columnar_sdk_release_notes:`Release Notes <>`
* :columnar_sdk_compatibility:`Compatibility Guide <>`
* :couchbase_dev_portal:`Couchbase Developer Portal <>`

How to Engage
=======================

* :couchbase_discord:`Join Discord and contribute <>`.
    The Couchbase Discord server is a place where you can collaborate about all things Couchbase.
    Connect with others from the community, learn tips and tricks, and ask questions.
* Ask and/or answer questions on the :columnar_sdk_forums:`Python SDK Forums <>`.


Installing the SDK
=======================

.. note::
    Best practice is to use a Python virtual environment such as venv or pyenv.
    Checkout:

        * Linux/MacOS: `pyenv <https://github.com/pyenv/>`_
        * Windows: `pyenv-win <https://github.com/pyenv-win/pyenv-win>`_


.. note::
    The Columnar Python SDK provides wheels for Windows, MacOS and Linux platforms (via manylinux) for supported versions of Python.
    See :columnar_sdk_version_compat:`Columnar Python Version Compatibility <>` docs for details.

Prereqs
++++++++++

If not on platform that has a binary wheel availble, the following is needed:

* A supported Python version (see :columnar_sdk_version_compat:`Columnar Python Version Compatibility <>` for details)
* A C++ compiler supporting C++ 17
* CMake (version >= 3.18)
* Git (if not on a platform that offers wheels)
* OpenSSL (optional)

After the above have been installed, pip install ``setuptools`` and ``wheel`` (see command below).

.. code-block:: console

    $ python3 -m pip install --upgrade pip setuptools wheel

Install
++++++++++

.. code-block:: console

    $ python3 -m pip install couchbase-columnar

Introduction
=======================

Connecting to a Capella Columnar cluster is as simple as creating a new ``Cluster`` instance to represent the ``Cluster``
you are using. You are able to execute most operations immediately, and they will be queued until the connection is successfully established.

Here is a simple example of creating a ``Cluster`` instance and issuing a query.

.. code-block:: python

    from couchbase_columnar.cluster import Cluster
    from couchbase_columnar.credential import Credential
    from couchbase_columnar.options import (ClusterOptions,
                                            QueryOptions,
                                            SecurityOptions)


    # Update this to your cluster
    connstr = 'couchbases://--your-instance--'
    username = 'username'
    pw = 'Password!123'
    # User Input ends here.

    cred = Credential.from_username_and_password(username, pw)
    cluster = Cluster.create_instance(connstr, cred)

    # Execute a query and process rows as they arrive from server.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country="United States" LIMIT 10;'
    res = cluster.execute_query(statement)
    for row in res.rows():
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

Source Control
=======================

The source control is available  on :columnar_sdk_github:`Github <>`.
Once you have cloned the repository, you may contribute changes through Github.
For more details see :columnar_sdk_contribute:`CONTRIBUTING.md <>`.

License
=======================

The Columnar Python SDK is licensed under the Apache License 2.0.

See :columnar_sdk_license:`LICENSE <>` for further details.
