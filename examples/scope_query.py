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

from datetime import timedelta

from couchbase_columnar.cluster import Cluster
from couchbase_columnar.credential import Credential
from couchbase_columnar.options import (ClusterOptions,
                                        QueryOptions,
                                        TimeoutOptions)


def main() -> None:
    # Update this to your cluster
    connstr = 'couchbases://--your-instance--'
    username = 'username'
    pw = 'Password!123'
    # User Input ends here.

    cred = Credential.from_username_and_password(username, pw)
    # NOTE:  Only an example on how to use options.  Not a recommendation.
    timeout_opts = TimeoutOptions(query_timeout=timedelta(seconds=30))
    opts = ClusterOptions(timeout_options=timeout_opts)
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
