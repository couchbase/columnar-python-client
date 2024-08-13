#  Copyright 2016-2024. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
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

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from couchbase_columnar.options import QueryOptions
from tests import YieldFixture

if TYPE_CHECKING:
    from tests.environments.base_environment import BlockingTestEnvironment


class QueryTestSuite:
    TEST_MANIFEST = [
        'test_query_named_parameters',
        'test_query_named_parameters_no_options',
        'test_query_named_parameters_override',
        'test_query_positional_params',
        'test_query_positional_params_no_option',
        'test_query_positional_params_override',
        'test_query_raw_options',
        'test_simple_query',
        'test_query_metadata',
        'test_query_metadata_not_available',
    ]

    def test_query_named_parameters(self, test_env: BlockingTestEnvironment) -> None:
        statement = f'SELECT * FROM {test_env.fqdn} WHERE country = $country LIMIT 2;'
        result = test_env.cluster.execute_query(statement,
                                                QueryOptions(named_parameters={'country': 'United States'}))
        test_env.assert_rows(result, 2)

    def test_query_named_parameters_no_options(self, test_env: BlockingTestEnvironment) -> None:
        statement = f'SELECT * FROM {test_env.fqdn} WHERE country = $country LIMIT 2;'
        result = test_env.cluster.execute_query(statement, country='United States')
        test_env.assert_rows(result, 2)

    def test_query_named_parameters_override(self, test_env: BlockingTestEnvironment) -> None:
        statement = f'SELECT * FROM {test_env.fqdn} WHERE country = $country LIMIT 2;'
        result = test_env.cluster.execute_query(statement,
                                                QueryOptions(named_parameters={'country': 'abcdefg'}),
                                                country='United States')
        test_env.assert_rows(result, 2)

    def test_query_positional_params(self, test_env: BlockingTestEnvironment) -> None:
        statement = f'SELECT * FROM {test_env.fqdn} WHERE country = $1 LIMIT 2;'
        result = test_env.cluster.execute_query(statement,
                                                QueryOptions(positional_parameters=['United States']))
        test_env.assert_rows(result, 2)

    def test_query_positional_params_no_option(self, test_env: BlockingTestEnvironment) -> None:
        statement = f'SELECT * FROM {test_env.fqdn} WHERE country = $1 LIMIT 2;'
        result = test_env.cluster.execute_query(statement, 'United States')
        test_env.assert_rows(result, 2)

    def test_query_positional_params_override(self, test_env: BlockingTestEnvironment) -> None:
        statement = f'SELECT * FROM {test_env.fqdn} WHERE country = $1 LIMIT 2;'
        result = test_env.cluster.execute_query(statement,
                                                QueryOptions(positional_parameters=['abcdefg']),
                                                'United States')
        test_env.assert_rows(result, 2)

    def test_query_raw_options(self, test_env: BlockingTestEnvironment) -> None:
        # via raw, we should be able to pass any option
        # if using named params, need to match full name param in query
        # which is different for when we pass in name_parameters via their specific
        # query option (i.e. include the $ when using raw)
        statement = f'SELECT * FROM {test_env.fqdn} WHERE country = $country LIMIT $1;'
        result = test_env.cluster.execute_query(statement, QueryOptions(raw={'$country': 'United States',
                                                                             'args': [2]}))
        test_env.assert_rows(result, 2)

        statement = f'SELECT * FROM {test_env.fqdn} WHERE country = $1 LIMIT 2;'
        result = test_env.cluster.execute_query(statement, QueryOptions(raw={'args': ['United States']}))
        test_env.assert_rows(result, 2)

    def test_simple_query(self, test_env: BlockingTestEnvironment) -> None:
        statement = f'SELECT * FROM {test_env.fqdn} LIMIT 2;'
        result = test_env.cluster.execute_query(statement)
        test_env.assert_rows(result, 2)

    def test_query_metadata(self, test_env: BlockingTestEnvironment) -> None:
        statement = f'SELECT * FROM {test_env.fqdn} LIMIT 2;'
        result = test_env.cluster.execute_query(statement)
        test_env.assert_rows(result, 2)

        metadata = result.metadata()

        assert len(metadata.warnings()) == 0
        assert len(metadata.request_id()) > 0

        metrics = metadata.metrics()

        assert metrics.result_size() > 0
        assert metrics.result_count() == 2
        assert metrics.processed_objects() > 0
        assert metrics.elapsed_time() > timedelta(0)
        assert metrics.execution_time() > timedelta(0)

    def test_query_metadata_not_available(self, test_env: BlockingTestEnvironment) -> None:
        statement = f'SELECT * FROM {test_env.fqdn} LIMIT 5;'
        result = test_env.cluster.execute_query(statement)

        with pytest.raises(RuntimeError):
            result.metadata()

        # Read one row
        next(iter(result.rows()))

        with pytest.raises(RuntimeError):
            result.metadata()

        # Iterate the rest of the rows
        rows = list(result.rows())
        assert len(rows) == 4

        metadata = result.metadata()
        assert len(metadata.warnings()) == 0
        assert len(metadata.request_id()) > 0


class QueryTests(QueryTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(QueryTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')

        method_list = [meth for meth in dir(QueryTests) if valid_test_method(meth)]
        test_list = set(QueryTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='test_env')
    def couchbase_test_environment(self,
                                   sync_test_env: BlockingTestEnvironment) -> YieldFixture[BlockingTestEnvironment]:
        yield sync_test_env
