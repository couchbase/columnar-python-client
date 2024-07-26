# Couchbase Python Columnar Client
Python client for [Couchbase](https://couchbase.com) Columnar

>**IMPORTANT:** This client is currently under active development and the API is subject to change.

Currently Python 3.8 - Python 3.11 is supported.  Python 3.12 support is coming soon.

The Columnar SDK supports static typing.  Currently only [mypy](https://github.com/python/mypy) is supported.  You mileage may vary (YMMV) with the use of other static type checkers (e.g. [pyright](https://github.com/microsoft/pyright)).

# Installing the SDK<a id="installing-the-sdk"></a>

Eventually the SDK will be published to PyPI, but in the interim a select set of wheels are available on the [Releases page](https://github.com/couchbaselabs/columnar-python-client/releases).  If a wheel is not available for your specific Python version and/or platform, see the [BUILDING page](https://github.com/couchbaselabs/columnar-python-client/blob/main/BUILDING.md) for details on how to build the SDK's binary.

To install the SDK from a wheel on the [Releases page](https://github.com/couchbaselabs/columnar-python-client/releases):
1. Download the appropriate wheel
2. Unzip the downloaded file
3. Install via pip: `python3 -m pip install <path to unzipped wheel>`
4. Install the `typing-extensions` dependency: `python3 -m pip install typing-extensions`

If a compatible wheel is not available, the SDK's binary will need to be built from source:
1. Follow the steps on the [BUILDING page](https://github.com/couchbaselabs/columnar-python-client/blob/main/BUILDING.md)
2. After the build succeeds, the SDK can be used by running Python scripts from within the cloned repository or the SDK can be installed via pip: `python3 -m pip install <path to cloned repository>`
4. Install the `typing-extensions` dependency: `python3 -m pip install typing-extensions`

# Using the SDK<a id="using-the-sdk"></a>

**Connecting and executing a query**
```python
from couchbase_columnar.cluster import Cluster
from couchbase_columnar.credential import Credential
from couchbase_columnar.options import (ClusterOptions,
                                        QueryOptions,
                                        SecurityOptions)


def main() -> None:
    # Update this to your cluster
    connstr = 'couchbases://--your-instance--'
    username = 'username'
    pw = 'Password!123'
    # User Input ends here.

    cred = Credential.from_username_and_password(username, pw)
    # Configure a secure connection to a Couchbase internal pre-production cluster.
    # (Omit this when connecting to a production cluster!)
    from couchbase_columnar.common.core._certificates import _Certificates
    sec_opts = SecurityOptions.trust_only_certificates(_Certificates.get_nonprod_certificates())
    opts = ClusterOptions(security_options=sec_opts)
    cluster = Cluster.create_instance(connstr, cred, opts)

    # Execute a query and buffer all result rows in client memory.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline LIMIT 10;'
    res = cluster.execute_query(statement)
    all_rows = res.get_all_rows()
    for row in all_rows:
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a query and process rows as they arrive from server.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country="United States" LIMIT 10;'
    res = cluster.execute_query(statement)
    for row in res.rows():
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a streaming query with positional arguments.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country=$1 LIMIT $2;'
    res = cluster.execute_query(statement, QueryOptions(positional_parameters=['United States', 10]))
    for row in res:
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a streaming query with named arguments.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country=$country LIMIT $limit;'
    res = cluster.execute_query(statement, QueryOptions(named_parameters={'country': 'United States',
                                                                          'limit': 10}))
    for row in res.rows():
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')


if __name__ == '__main__':
    main()

```

## Using the async API
```python
from acouchbase_columnar import get_event_loop
from acouchbase_columnar.cluster import AsyncCluster
from couchbase_columnar.credential import Credential
from couchbase_columnar.options import (ClusterOptions,
                                        QueryOptions,
                                        SecurityOptions)


async def main() -> None:
    # Update this to your cluster
    connstr = 'couchbases://--your-instance--'
    username = 'username'
    pw = 'Password!123'
    # User Input ends here.

    cred = Credential.from_username_and_password(username, pw)
    # Configure a secure connection to a Couchbase internal pre-production cluster.
    # (Omit this when connecting to a production cluster!)
    from couchbase_columnar.common.core._certificates import _Certificates
    sec_opts = SecurityOptions.trust_only_certificates(_Certificates.get_nonprod_certificates())
    opts = ClusterOptions(security_options=sec_opts)
    cluster = AsyncCluster.create_instance(connstr, cred, opts)

    # Execute a query and buffer all result rows in client memory.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline LIMIT 10;'
    res = await cluster.execute_query(statement)
    all_rows = await res.get_all_rows()
    # NOTE: all_rows is a list, _do not_ use `async for`
    for row in all_rows:
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a query and process rows as they arrive from server.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country="United States" LIMIT 10;'
    res = await cluster.execute_query(statement)
    async for row in res.rows():
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a streaming query with positional arguments.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country=$1 LIMIT $2;'
    res = await cluster.execute_query(statement, QueryOptions(positional_parameters=['United States', 10]))
    async for row in res:
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a streaming query with named arguments.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country=$country LIMIT $limit;'
    res = await cluster.execute_query(statement, QueryOptions(named_parameters={'country': 'United States',
                                                                                'limit': 10}))
    async for row in res.rows():
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

if __name__ == '__main__':
    loop = get_event_loop()
    loop.run_until_complete(main())

```
