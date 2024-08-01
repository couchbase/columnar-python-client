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

from typing import Dict

import pytest

from acouchbase_columnar.cluster import AsyncCluster as Cluster
from acouchbase_columnar.credential import Credential
from acouchbase_columnar.protocol.core.client_adapter import _ClientAdapter


class ConnectionTestSuite:
    TEST_MANIFEST = [
        'test_connection_string_options',
        'test_invalid_connection_strings',
        'test_valid_connection_strings',
    ]

    @pytest.mark.parametrize('connstr, expected_opts',
                             [('couchbases://10.0.0.1?dns_nameserver=127.0.0.1&dump_configuration=true',
                               {'dns_nameserver': '127.0.0.1', 'dump_configuration': True}),
                              ('couchbases://10.0.0.1?an_invalid_option=10',
                               {}),
                              ])
    def test_connection_string_options(self, connstr: str, expected_opts: Dict[str, object]) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        client = _ClientAdapter(connstr, cred)

        user_agent = client.connection_details.cluster_options.pop('user_agent_extra', None)
        assert expected_opts == client.connection_details.cluster_options
        assert user_agent is not None
        assert 'pycbcc/' in user_agent
        assert 'python/' in user_agent
        expected_conn_str = connstr.split('?')[0]
        assert expected_conn_str == client.connection_details.connection_str

    @pytest.mark.parametrize('connstr', ['10.0.0.1:8091',
                                         'http://host1',
                                         'http://host2:8091',
                                         'https://host2',
                                         'https://host2:8091',
                                         'couchbase://10.0.0.1'])
    @pytest.mark.asyncio
    def test_invalid_connection_strings(self, connstr: str) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        with pytest.raises(ValueError):
            Cluster.create_instance(connstr, cred)

    @pytest.mark.parametrize('connstr', ['couchbases://10.0.0.1',
                                         'couchbases://10.0.0.1:11222,10.0.0.2,10.0.0.3:11207',
                                         'couchbases://10.0.0.1;10.0.0.2:11210;10.0.0.3',
                                         'couchbases://[3ffe:2a00:100:7031::1]',
                                         'couchbases://[::ffff:192.168.0.1]:11207,[::ffff:192.168.0.2]:11207',
                                         'couchbases://test.local:11210?key=value',
                                         'couchbases://fqdn'
                                         ])
    def test_valid_connection_strings(self, connstr: str) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        client = _ClientAdapter(connstr, cred)
        # pop user_agent as that is additive
        user_agent = client.connection_details.cluster_options.pop('user_agent_extra', None)
        # options should be empty
        assert {} == client.connection_details.cluster_options
        assert user_agent is not None
        assert 'pycbcc/' in user_agent
        assert 'python/' in user_agent
        expected_conn_str = connstr.split('?')[0]
        assert expected_conn_str == client.connection_details.connection_str


class ConnectionTests(ConnectionTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(ConnectionTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ConnectionTests) if valid_test_method(meth)]
        test_list = set(ConnectionTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')
