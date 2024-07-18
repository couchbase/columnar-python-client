from acouchbase_columnar import get_event_loop
from acouchbase_columnar.cluster import AsyncCluster
from couchbase_columnar.credential import Credential
from couchbase_columnar.options import (ClusterOptions,
                                        QueryOptions,
                                        SecurityOptions)


async def main() -> None:
    connstr = 'couchbases://cb.mqc-5fcvbwo9jxi.customsubdomain.nonprod-project-avengers.com'
    username = 'testuser'
    pw = 'P@ssw0rd_12345!'

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
