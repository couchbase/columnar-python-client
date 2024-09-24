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

from dataclasses import dataclass
from datetime import timedelta
from typing import (Dict,
                    List,
                    Optional,
                    Union)

import pytest

from acouchbase_columnar import JSONType
from acouchbase_columnar.credential import Credential
from acouchbase_columnar.options import QueryOptions
from acouchbase_columnar.protocol.core.client_adapter import _ClientAdapter
from couchbase_columnar.protocol.core.request import ClusterRequestBuilder, ScopeRequestBuilder


@dataclass
class QueryContext:
    database_name: Optional[str] = None
    scope_name: Optional[str] = None


class QueryOptionsTestSuite:
    TEST_MANIFEST = [
        'test_options_deserializer',
        'test_options_deserializer_kwargs',
        'test_options_named_parameters',
        'test_options_named_parameters_kwargs',
        'test_options_positional_parameters',
        'test_options_positional_parameters_kwargs',
        'test_options_priority',
        'test_options_priority_kwargs',
        'test_options_raw',
        'test_options_raw_kwargs',
        'test_options_readonly',
        'test_options_readonly_kwargs',
        'test_options_scan_consistency',
        'test_options_scan_consistency_kwargs',
        'test_options_timeout',
        'test_options_timeout_kwargs',
        'test_options_timeout_must_be_positive',
        'test_options_timeout_must_be_positive_kwargs'
    ]

    @pytest.fixture(scope='class')
    def query_statment(self) -> str:
        return 'SELECT * FROM default'

    def test_options_deserializer(self,
                                  query_statment: str,
                                  request_builder: Union[ClusterRequestBuilder, ScopeRequestBuilder],
                                  query_ctx: QueryContext) -> None:
        from acouchbase_columnar.deserializer import DefaultJsonDeserializer
        deserializer = DefaultJsonDeserializer()
        q_opts = QueryOptions(deserializer=deserializer)
        req, cancel_token = request_builder.build_query_request(query_statment, q_opts)
        exp_opts: Dict[str, object] = {}
        assert cancel_token is None
        assert req.options == exp_opts
        assert req.deserializer == deserializer
        assert req.database_name == query_ctx.database_name
        assert req.scope_name == query_ctx.scope_name

    def test_options_deserializer_kwargs(self,
                                         query_statment: str,
                                         request_builder: Union[ClusterRequestBuilder, ScopeRequestBuilder],
                                         query_ctx: QueryContext) -> None:
        from acouchbase_columnar.deserializer import DefaultJsonDeserializer
        deserializer = DefaultJsonDeserializer()
        kwargs = {'deserializer': deserializer}
        req, cancel_token = request_builder.build_query_request(query_statment, **kwargs)
        exp_opts: Dict[str, object] = {}
        assert cancel_token is None
        assert req.options == exp_opts
        assert req.deserializer == deserializer
        assert req.database_name == query_ctx.database_name
        assert req.scope_name == query_ctx.scope_name

    def test_options_named_parameters(self,
                                      query_statment: str,
                                      request_builder: Union[ClusterRequestBuilder, ScopeRequestBuilder],
                                      query_ctx: QueryContext) -> None:
        params: Dict[str, JSONType] = {'foo': 'bar', 'baz': 1, 'quz': False}
        q_opts = QueryOptions(named_parameters=params)
        req, cancel_token = request_builder.build_query_request(query_statment, q_opts)
        exp_opts = {'named_parameters': params}
        assert cancel_token is None
        assert req.options == exp_opts
        assert req.database_name == query_ctx.database_name
        assert req.scope_name == query_ctx.scope_name

    def test_options_named_parameters_kwargs(self,
                                             query_statment: str,
                                             request_builder: Union[ClusterRequestBuilder, ScopeRequestBuilder],
                                             query_ctx: QueryContext) -> None:
        params = {'foo': 'bar', 'baz': 1, 'quz': False}
        kwargs = {'named_parameters': params}
        req, cancel_token = request_builder.build_query_request(query_statment, **kwargs)
        exp_opts = {'named_parameters': params}
        assert cancel_token is None
        assert req.options == exp_opts
        assert req.database_name == query_ctx.database_name
        assert req.scope_name == query_ctx.scope_name

    def test_options_positional_parameters(self,
                                           query_statment: str,
                                           request_builder: Union[ClusterRequestBuilder, ScopeRequestBuilder],
                                           query_ctx: QueryContext) -> None:
        params: List[JSONType] = ['foo', 'bar', 1, False]
        q_opts = QueryOptions(positional_parameters=params)
        req, cancel_token = request_builder.build_query_request(query_statment, q_opts)
        exp_opts = {'positional_parameters': params}
        assert cancel_token is None
        assert req.options == exp_opts
        assert req.database_name == query_ctx.database_name
        assert req.scope_name == query_ctx.scope_name

    def test_options_positional_parameters_kwargs(self,
                                                  query_statment: str,
                                                  request_builder: Union[ClusterRequestBuilder, ScopeRequestBuilder],
                                                  query_ctx: QueryContext) -> None:
        params = ['foo', 'bar', 1, False]
        kwargs = {'positional_parameters': params}
        req, cancel_token = request_builder.build_query_request(query_statment, **kwargs)
        exp_opts = {'positional_parameters': params}
        assert cancel_token is None
        assert req.options == exp_opts
        assert req.database_name == query_ctx.database_name
        assert req.scope_name == query_ctx.scope_name

    def test_options_priority(self,
                              query_statment: str,
                              request_builder: Union[ClusterRequestBuilder, ScopeRequestBuilder],
                              query_ctx: QueryContext) -> None:
        q_opts = QueryOptions(priority=True)
        req, cancel_token = request_builder.build_query_request(query_statment, q_opts)
        exp_opts = {'priority': True}
        assert cancel_token is None
        assert req.options == exp_opts
        assert req.database_name == query_ctx.database_name
        assert req.scope_name == query_ctx.scope_name

    def test_options_priority_kwargs(self,
                                     query_statment: str,
                                     request_builder: Union[ClusterRequestBuilder, ScopeRequestBuilder],
                                     query_ctx: QueryContext) -> None:
        kwargs = {'priority': True}
        req, cancel_token = request_builder.build_query_request(query_statment, **kwargs)
        exp_opts = {'priority': True}
        assert cancel_token is None
        assert req.options == exp_opts
        assert req.database_name == query_ctx.database_name
        assert req.scope_name == query_ctx.scope_name

    def test_options_raw(self,
                         query_statment: str,
                         request_builder: Union[ClusterRequestBuilder, ScopeRequestBuilder],
                         query_ctx: QueryContext) -> None:
        pos_params = ['foo', 'bar', 1, False]
        params = {'readonly': True, 'positional_params': pos_params}
        q_opts = QueryOptions(raw=params)
        req, cancel_token = request_builder.build_query_request(query_statment, q_opts)
        exp_opts = {'raw': params}
        assert cancel_token is None
        assert req.options == exp_opts
        assert req.database_name == query_ctx.database_name
        assert req.scope_name == query_ctx.scope_name

    def test_options_raw_kwargs(self,
                                query_statment: str,
                                request_builder: Union[ClusterRequestBuilder, ScopeRequestBuilder],
                                query_ctx: QueryContext) -> None:
        pos_params = ['foo', 'bar', 1, False]
        kwargs = {'raw': {'readonly': True, 'positional_params': pos_params}}
        req, cancel_token = request_builder.build_query_request(query_statment, **kwargs)
        assert cancel_token is None
        assert req.options == kwargs
        assert req.database_name == query_ctx.database_name
        assert req.scope_name == query_ctx.scope_name

    def test_options_readonly(self,
                              query_statment: str,
                              request_builder: Union[ClusterRequestBuilder, ScopeRequestBuilder],
                              query_ctx: QueryContext) -> None:
        q_opts = QueryOptions(read_only=True)
        req, cancel_token = request_builder.build_query_request(query_statment, q_opts)
        exp_opts = {'readonly': True}
        assert cancel_token is None
        assert req.options == exp_opts
        assert req.database_name == query_ctx.database_name
        assert req.scope_name == query_ctx.scope_name

    def test_options_readonly_kwargs(self,
                                     query_statment: str,
                                     request_builder: Union[ClusterRequestBuilder, ScopeRequestBuilder],
                                     query_ctx: QueryContext) -> None:
        kwargs = {'read_only': True}
        req, cancel_token = request_builder.build_query_request(query_statment, **kwargs)
        exp_opts = {'readonly': True}
        assert cancel_token is None
        assert req.options == exp_opts
        assert req.database_name == query_ctx.database_name
        assert req.scope_name == query_ctx.scope_name

    def test_options_scan_consistency(self,
                                      query_statment: str,
                                      request_builder: Union[ClusterRequestBuilder, ScopeRequestBuilder],
                                      query_ctx: QueryContext) -> None:
        from acouchbase_columnar.query import QueryScanConsistency
        q_opts = QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS)
        req, cancel_token = request_builder.build_query_request(query_statment, q_opts)
        exp_opts = {
            'scan_consistency': QueryScanConsistency.REQUEST_PLUS.value
        }
        assert cancel_token is None
        assert req.options == exp_opts
        assert req.scope_name == query_ctx.scope_name
        assert req.database_name == query_ctx.database_name
        assert req.scope_name == query_ctx.scope_name

    def test_options_scan_consistency_kwargs(self,
                                             query_statment: str,
                                             request_builder: Union[ClusterRequestBuilder, ScopeRequestBuilder],
                                             query_ctx: QueryContext) -> None:
        from acouchbase_columnar.query import QueryScanConsistency
        kwargs = {'scan_consistency': QueryScanConsistency.REQUEST_PLUS}
        req, cancel_token = request_builder.build_query_request(query_statment, **kwargs)
        exp_opts = {
            'scan_consistency': QueryScanConsistency.REQUEST_PLUS.value
        }
        assert cancel_token is None
        assert req.options == exp_opts
        assert req.scope_name == query_ctx.scope_name
        assert req.database_name == query_ctx.database_name
        assert req.scope_name == query_ctx.scope_name

    def test_options_timeout(self,
                             query_statment: str,
                             request_builder: Union[ClusterRequestBuilder, ScopeRequestBuilder],
                             query_ctx: QueryContext) -> None:
        q_opts = QueryOptions(timeout=timedelta(seconds=20))
        req, cancel_token = request_builder.build_query_request(query_statment, q_opts)
        exp_opts = {
            'timeout': 20000000
        }
        assert cancel_token is None
        assert req.options == exp_opts
        assert req.database_name == query_ctx.database_name
        assert req.scope_name == query_ctx.scope_name

    def test_options_timeout_kwargs(self,
                                    query_statment: str,
                                    request_builder: Union[ClusterRequestBuilder, ScopeRequestBuilder],
                                    query_ctx: QueryContext) -> None:
        kwargs = {'timeout': timedelta(seconds=20)}
        req, cancel_token = request_builder.build_query_request(query_statment, **kwargs)
        exp_opts = {
            'timeout': 20000000
        }
        assert cancel_token is None
        assert req.options == exp_opts
        assert req.database_name == query_ctx.database_name
        assert req.scope_name == query_ctx.scope_name

    def test_options_timeout_must_be_positive(self,
                                              query_statment: str,
                                              request_builder: Union[ClusterRequestBuilder, ScopeRequestBuilder]
                                              ) -> None:
        q_opts = QueryOptions(timeout=timedelta(seconds=-1))
        with pytest.raises(ValueError):
            request_builder.build_query_request(query_statment, q_opts)

    def test_options_timeout_must_be_positive_kwargs(self,
                                                     query_statment: str,
                                                     request_builder: Union[ClusterRequestBuilder, ScopeRequestBuilder]
                                                     ) -> None:
        kwargs = {'timeout': timedelta(seconds=-1)}
        with pytest.raises(ValueError):
            request_builder.build_query_request(query_statment, **kwargs)


class ClusterQueryOptionsTests(QueryOptionsTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(ClusterQueryOptionsTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClusterQueryOptionsTests) if valid_test_method(meth)]
        test_list = set(QueryOptionsTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='query_ctx')
    def query_context(self) -> QueryContext:
        return QueryContext()

    @pytest.fixture(scope='class')
    def request_builder(self) -> ClusterRequestBuilder:
        cred = Credential.from_username_and_password('Administrator', 'password')
        return ClusterRequestBuilder(_ClientAdapter('couchbases://localhost', cred))


class ScopeQueryOptionsTests(QueryOptionsTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(ScopeQueryOptionsTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ScopeQueryOptionsTests) if valid_test_method(meth)]
        test_list = set(QueryOptionsTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='query_ctx')
    def query_context(self) -> QueryContext:
        return QueryContext('test-database', 'test-scope')

    @pytest.fixture(scope='class')
    def request_builder(self) -> ScopeRequestBuilder:
        cred = Credential.from_username_and_password('Administrator', 'password')
        return ScopeRequestBuilder(_ClientAdapter('couchbases://localhost', cred),
                                   'test-database',
                                   'test-scope')
