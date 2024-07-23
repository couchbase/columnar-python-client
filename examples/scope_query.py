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
    scope = Cluster.create_instance(connstr, cred, opts).database('travel-sample').scope('inventory')

    # Execute a scope-level query and buffer all result rows in client memory.
    statement = 'SELECT * FROM airline LIMIT 10;'
    res = scope.execute_query(statement)
    all_rows = res.get_all_rows()
    for row in all_rows:
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a scope-level query and process rows as they arrive from server.
    statement = 'SELECT * FROM airline WHERE country="United States" LIMIT 10;'
    res = scope.execute_query(statement)
    for row in res.rows():
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a streaming scope-level query with positional arguments.
    statement = 'SELECT * FROM airline WHERE country=$1 LIMIT $2;'
    res = scope.execute_query(statement, QueryOptions(positional_parameters=['United States', 10]))
    for row in res:
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a streaming scope-level query with named arguments.
    statement = 'SELECT * FROM airline WHERE country=$country LIMIT $limit;'
    res = scope.execute_query(statement, QueryOptions(named_parameters={'country': 'United States',
                                                                        'limit': 10}))
    for row in res.rows():
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')


if __name__ == '__main__':
    main()
