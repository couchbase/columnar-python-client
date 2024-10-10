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

from __future__ import annotations

import json
from concurrent.futures import Future
from datetime import timedelta
from threading import Event
from typing import TYPE_CHECKING

import pytest

from couchbase_columnar.common.streaming import StreamingState
from couchbase_columnar.deserializer import PassthroughDeserializer
from couchbase_columnar.errors import QueryError
from couchbase_columnar.options import QueryOptions
from couchbase_columnar.query import CancelToken, QueryScanConsistency
from couchbase_columnar.result import BlockingQueryResult
from tests import YieldFixture

if TYPE_CHECKING:
    from tests.environments.base_environment import BlockingTestEnvironment


class QueryTestSuite:
    TEST_MANIFEST = [
        'test_cancel_positional_params_override',
        'test_cancel_positional_params_override_token_in_kwargs',
        'test_cancel_prior_iterating',
        'test_cancel_prior_iterating_positional_params',
        'test_cancel_prior_iterating_with_kwargs',
        'test_cancel_prior_iterating_with_options',
        'test_cancel_prior_iterating_with_opts_and_kwargs',
        'test_cancel_while_iterating',
        'test_query_metadata',
        'test_query_metadata_not_available',
        'test_query_named_parameters',
        'test_query_named_parameters_no_options',
        'test_query_named_parameters_override',
        'test_query_positional_params',
        'test_query_positional_params_no_option',
        'test_query_positional_params_override',
        'test_query_raises_exception_prior_to_iterating',
        'test_query_raw_options',
        'test_simple_query',
        'test_query_with_unused_cancel_token',
        'test_query_with_unused_cancel_token_raises_exception',
        'test_query_with_lazy_execution',
        'test_query_with_lazy_execution_raises_exception',
        'test_query_passthrough_deserializer',
    ]

    @pytest.fixture(scope='class')
    def query_statement_limit2(self, test_env: BlockingTestEnvironment) -> str:
        if test_env.use_scope:
            return f'SELECT * FROM {test_env.collection_name} LIMIT 2;'
        else:
            return f'SELECT * FROM {test_env.fqdn} LIMIT 2;'

    @pytest.fixture(scope='class')
    def query_statement_pos_params_limit2(self, test_env: BlockingTestEnvironment) -> str:
        if test_env.use_scope:
            return f'SELECT * FROM {test_env.collection_name} WHERE country = $1 LIMIT 2;'
        else:
            return f'SELECT * FROM {test_env.fqdn} WHERE country = $1 LIMIT 2;'

    @pytest.fixture(scope='class')
    def query_statement_named_params_limit2(self, test_env: BlockingTestEnvironment) -> str:
        if test_env.use_scope:
            return f'SELECT * FROM {test_env.collection_name} WHERE country = $country LIMIT 2;'
        else:
            return f'SELECT * FROM {test_env.fqdn} WHERE country = $country LIMIT 2;'

    @pytest.fixture(scope='class')
    def query_statement_limit5(self, test_env: BlockingTestEnvironment) -> str:
        if test_env.use_scope:
            return f'SELECT * FROM {test_env.collection_name} LIMIT 5;'
        else:
            return f'SELECT * FROM {test_env.fqdn} LIMIT 5;'

    @pytest.mark.parametrize('cancel_via_token', [False, True])
    def test_cancel_positional_params_override(self,
                                               test_env: BlockingTestEnvironment,
                                               query_statement_pos_params_limit2: str,
                                               cancel_via_token: bool) -> None:
        cancel_token = CancelToken(Event())
        ft = test_env.cluster_or_scope.execute_query(query_statement_pos_params_limit2,
                                                     QueryOptions(positional_parameters=['abcdefg']),
                                                     cancel_token,
                                                     'United States')
        assert isinstance(ft, Future)
        if cancel_via_token:
            cancel_token.cancel()
            res = ft.result()
        else:
            res = ft.result()
            res.cancel()
        assert isinstance(res, BlockingQueryResult)
        assert res._executor.streaming_state == StreamingState.Cancelled

    @pytest.mark.parametrize('cancel_via_token', [False, True])
    def test_cancel_positional_params_override_token_in_kwargs(self,
                                                               test_env: BlockingTestEnvironment,
                                                               query_statement_pos_params_limit2: str,
                                                               cancel_via_token: bool) -> None:
        cancel_token = CancelToken(Event())
        ft = test_env.cluster_or_scope.execute_query(query_statement_pos_params_limit2,
                                                     QueryOptions(positional_parameters=['abcdefg']),
                                                     'United States',
                                                     cancel_token=cancel_token)
        assert isinstance(ft, Future)
        if cancel_via_token:
            cancel_token.cancel()
            res = ft.result()
        else:
            res = ft.result()
            res.cancel()
        assert isinstance(res, BlockingQueryResult)
        assert res._executor.streaming_state == StreamingState.Cancelled

    @pytest.mark.parametrize('cancel_via_token', [False, True])
    def test_cancel_prior_iterating(self, test_env: BlockingTestEnvironment, cancel_via_token: bool) -> None:
        statement = 'FROM range(0, 100000) AS r SELECT *'
        cancel_token = CancelToken(Event())
        ft = test_env.cluster_or_scope.execute_query(statement, cancel_token=cancel_token)
        assert isinstance(ft, Future)
        if cancel_via_token:
            cancel_token.cancel()
            res = ft.result()
        else:
            res = ft.result()
            res.cancel()
        assert isinstance(res, BlockingQueryResult)
        assert res._executor.streaming_state == StreamingState.Cancelled

    @pytest.mark.parametrize('cancel_via_token', [False, True])
    def test_cancel_prior_iterating_positional_params(self,
                                                      test_env: BlockingTestEnvironment,
                                                      query_statement_pos_params_limit2: str,
                                                      cancel_via_token: bool) -> None:
        cancel_token = CancelToken(Event())
        ft = test_env.cluster_or_scope.execute_query(query_statement_pos_params_limit2,
                                                     cancel_token,
                                                     'United States')
        assert isinstance(ft, Future)
        if cancel_via_token:
            cancel_token.cancel()
            res = ft.result()
        else:
            res = ft.result()
            res.cancel()
        assert isinstance(res, BlockingQueryResult)
        assert res._executor.streaming_state == StreamingState.Cancelled

    @pytest.mark.parametrize('cancel_via_token', [False, True])
    def test_cancel_prior_iterating_with_kwargs(self,
                                                test_env: BlockingTestEnvironment,
                                                cancel_via_token: bool) -> None:
        statement = 'FROM range(0, 100000) AS r SELECT *'
        cancel_token = CancelToken(Event())
        ft = test_env.cluster_or_scope.execute_query(statement,
                                                     timeout=timedelta(seconds=10),
                                                     cancel_token=cancel_token)
        assert isinstance(ft, Future)
        if cancel_via_token:
            cancel_token.cancel()
            res = ft.result()
        else:
            res = ft.result()
            res.cancel()
        assert isinstance(res, BlockingQueryResult)
        assert res._executor.streaming_state == StreamingState.Cancelled

    @pytest.mark.parametrize('cancel_via_token', [False, True])
    def test_cancel_prior_iterating_with_options(self,
                                                 test_env: BlockingTestEnvironment,
                                                 cancel_via_token: bool) -> None:
        statement = 'FROM range(0, 100000) AS r SELECT *'
        cancel_token = CancelToken(Event())
        ft = test_env.cluster_or_scope.execute_query(statement,
                                                     QueryOptions(timeout=timedelta(seconds=10)),
                                                     cancel_token)
        assert isinstance(ft, Future)
        if cancel_via_token:
            cancel_token.cancel()
            res = ft.result()
        else:
            res = ft.result()
            res.cancel()
        assert isinstance(res, BlockingQueryResult)
        assert res._executor.streaming_state == StreamingState.Cancelled

    @pytest.mark.parametrize('cancel_via_token', [False, True])
    def test_cancel_prior_iterating_with_opts_and_kwargs(self,
                                                         test_env: BlockingTestEnvironment,
                                                         cancel_via_token: bool) -> None:
        statement = 'FROM range(0, 100000) AS r SELECT *'
        cancel_token = CancelToken(Event())
        ft = test_env.cluster_or_scope.execute_query(statement,
                                                     QueryOptions(scan_consistency=QueryScanConsistency.NOT_BOUNDED),
                                                     timeout=timedelta(seconds=10),
                                                     cancel_token=cancel_token)
        assert isinstance(ft, Future)
        if cancel_via_token:
            cancel_token.cancel()
            res = ft.result()
        else:
            res = ft.result()
            res.cancel()
        assert isinstance(res, BlockingQueryResult)
        assert res._executor.streaming_state == StreamingState.Cancelled

    @pytest.mark.parametrize('cancel_via_token', [False, True])
    def test_cancel_while_iterating(self,
                                    test_env: BlockingTestEnvironment,
                                    query_statement_limit5: str,
                                    cancel_via_token: bool) -> None:
        cancel_token = CancelToken(Event())
        ft = test_env.cluster_or_scope.execute_query(query_statement_limit5,
                                                     cancel_token=cancel_token)
        assert isinstance(ft, Future)
        res = ft.result()
        assert isinstance(res, BlockingQueryResult)
        expected_state = StreamingState.Started
        assert res._executor.streaming_state == expected_state
        rows = []
        count = 0
        for row in res.rows():
            if count == 2:
                if cancel_via_token:
                    cancel_token.cancel()
                else:
                    res.cancel()
            assert row is not None
            rows.append(row)
            count += 1

        assert len(rows) == count
        expected_state = StreamingState.Cancelled
        assert res._executor.streaming_state == expected_state

    def test_query_metadata(self,
                            test_env: BlockingTestEnvironment,
                            query_statement_limit5: str) -> None:
        result = test_env.cluster_or_scope.execute_query(query_statement_limit5)
        expected_count = 5
        test_env.assert_rows(result, expected_count)

        metadata = result.metadata()

        assert len(metadata.warnings()) == 0
        assert len(metadata.request_id()) > 0

        metrics = metadata.metrics()

        assert metrics.result_size() > 0
        assert metrics.result_count() == expected_count
        assert metrics.processed_objects() > 0
        assert metrics.elapsed_time() > timedelta(0)
        assert metrics.execution_time() > timedelta(0)

    def test_query_metadata_not_available(self,
                                          test_env: BlockingTestEnvironment,
                                          query_statement_limit5: str) -> None:
        result = test_env.cluster_or_scope.execute_query(query_statement_limit5)

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

    def test_query_named_parameters(self,
                                    test_env: BlockingTestEnvironment,
                                    query_statement_named_params_limit2: str) -> None:
        result = test_env.cluster_or_scope.execute_query(query_statement_named_params_limit2,
                                                         QueryOptions(named_parameters={'country': 'United States'}))
        test_env.assert_rows(result, 2)

    def test_query_named_parameters_no_options(self,
                                               test_env: BlockingTestEnvironment,
                                               query_statement_named_params_limit2: str) -> None:
        result = test_env.cluster_or_scope.execute_query(query_statement_named_params_limit2, country='United States')
        test_env.assert_rows(result, 2)

    def test_query_named_parameters_override(self,
                                             test_env: BlockingTestEnvironment,
                                             query_statement_named_params_limit2: str) -> None:
        result = test_env.cluster_or_scope.execute_query(query_statement_named_params_limit2,
                                                         QueryOptions(named_parameters={'country': 'abcdefg'}),
                                                         country='United States')
        test_env.assert_rows(result, 2)

    def test_query_positional_params(self,
                                     test_env: BlockingTestEnvironment,
                                     query_statement_pos_params_limit2: str) -> None:
        result = test_env.cluster_or_scope.execute_query(query_statement_pos_params_limit2,
                                                         QueryOptions(positional_parameters=['United States']))
        test_env.assert_rows(result, 2)

    def test_query_positional_params_no_option(self,
                                               test_env: BlockingTestEnvironment,
                                               query_statement_pos_params_limit2: str) -> None:
        result = test_env.cluster_or_scope.execute_query(query_statement_pos_params_limit2, 'United States')
        test_env.assert_rows(result, 2)

    def test_query_positional_params_override(self,
                                              test_env: BlockingTestEnvironment,
                                              query_statement_pos_params_limit2: str) -> None:
        result = test_env.cluster_or_scope.execute_query(query_statement_pos_params_limit2,
                                                         QueryOptions(positional_parameters=['abcdefg']),
                                                         'United States')
        test_env.assert_rows(result, 2)

    def test_query_raises_exception_prior_to_iterating(self, test_env: BlockingTestEnvironment) -> None:
        statement = "I'm not N1QL!"
        with pytest.raises(QueryError):
            test_env.cluster_or_scope.execute_query(statement)

    def test_query_raw_options(self,
                               test_env: BlockingTestEnvironment,
                               query_statement_pos_params_limit2: str) -> None:
        # via raw, we should be able to pass any option
        # if using named params, need to match full name param in query
        # which is different for when we pass in name_parameters via their specific
        # query option (i.e. include the $ when using raw)
        if test_env.use_scope:
            statement = f'SELECT * FROM {test_env.collection_name} WHERE country = $country LIMIT $1;'
        else:
            statement = f'SELECT * FROM {test_env.fqdn} WHERE country = $country LIMIT $1;'
        result = test_env.cluster_or_scope.execute_query(statement,
                                                         QueryOptions(raw={'$country': 'United States',
                                                                           'args': [2]}))
        test_env.assert_rows(result, 2)

        result = test_env.cluster_or_scope.execute_query(query_statement_pos_params_limit2,
                                                         QueryOptions(raw={'args': ['United States']}))
        test_env.assert_rows(result, 2)

    def test_simple_query(self,
                          test_env: BlockingTestEnvironment,
                          query_statement_limit2: str) -> None:
        result = test_env.cluster_or_scope.execute_query(query_statement_limit2)
        test_env.assert_rows(result, 2)

    def test_query_with_unused_cancel_token(self,
                                            test_env: BlockingTestEnvironment,
                                            query_statement_limit2: str) -> None:
        cancel_token = CancelToken(Event())
        ft = test_env.cluster_or_scope.execute_query(query_statement_limit2,
                                                     cancel_token=cancel_token)
        assert isinstance(ft, Future)
        result = ft.result()
        assert isinstance(result, BlockingQueryResult)
        expected_state = StreamingState.Started
        assert result._executor.streaming_state == expected_state
        test_env.assert_rows(result, 2)

    def test_query_with_unused_cancel_token_raises_exception(self, test_env: BlockingTestEnvironment) -> None:
        statement = "I'm not N1QL!"
        cancel_token = CancelToken(Event())
        ft = test_env.cluster_or_scope.execute_query(statement, cancel_token=cancel_token)
        assert isinstance(ft, Future)
        with pytest.raises(QueryError):
            ft.result()

    def test_query_with_lazy_execution(self,
                                       test_env: BlockingTestEnvironment,
                                       query_statement_limit2: str) -> None:
        result = test_env.cluster_or_scope.execute_query(query_statement_limit2,
                                                         QueryOptions(lazy_execute=True))
        expected_state = StreamingState.NotStarted
        assert result._executor.streaming_state == expected_state
        expected_state = StreamingState.Started
        count = 0
        for row in result.rows():
            assert result._executor.streaming_state == expected_state
            assert row is not None
            count += 1
        assert count == 2

    def test_query_with_lazy_execution_raises_exception(self, test_env: BlockingTestEnvironment) -> None:
        statement = "I'm not N1QL!"
        result = test_env.cluster_or_scope.execute_query(statement, QueryOptions(lazy_execute=True))
        expected_state = StreamingState.NotStarted
        assert result._executor.streaming_state == expected_state
        with pytest.raises(QueryError):
            [r for r in result.rows()]

    def test_query_passthrough_deserializer(self, test_env: BlockingTestEnvironment) -> None:
        statement = 'FROM range(0, 10) AS num SELECT *'
        result = test_env.cluster_or_scope.execute_query(statement,
                                                         QueryOptions(deserializer=PassthroughDeserializer()))
        for idx, row in enumerate(result.rows()):
            assert isinstance(row, bytes)
            assert json.loads(row) == {'num': idx}


class ClusterQueryTests(QueryTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(ClusterQueryTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClusterQueryTests) if valid_test_method(meth)]
        test_list = set(QueryTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='test_env')
    def couchbase_test_environment(self,
                                   sync_test_env: BlockingTestEnvironment) -> YieldFixture[BlockingTestEnvironment]:
        sync_test_env.setup()
        yield sync_test_env
        sync_test_env.teardown()


class ScopeQueryTests(QueryTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(ScopeQueryTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ScopeQueryTests) if valid_test_method(meth)]
        test_list = set(QueryTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='test_env')
    def couchbase_test_environment(self,
                                   sync_test_env: BlockingTestEnvironment) -> YieldFixture[BlockingTestEnvironment]:
        sync_test_env.setup()
        test_env = sync_test_env.enable_scope()
        yield test_env
        test_env.disable_scope()
        test_env.teardown()
