#  Copyright 2016-2024. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

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
