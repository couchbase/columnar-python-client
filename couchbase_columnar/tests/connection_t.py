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

from typing import Dict, Optional

import pytest

from couchbase_columnar.cluster import Cluster
from couchbase_columnar.common.core.utils import to_query_str
from couchbase_columnar.credential import Credential
from couchbase_columnar.protocol.core.client_adapter import _ClientAdapter
from couchbase_columnar.protocol.core.request import ClusterRequestBuilder


class ConnectionTestSuite:
    TEST_MANIFEST = [
        'test_connstr_options_general',
        'test_connstr_options_general_fail',
        # 'test_connstr_options_general_override_opts',
        'test_connstr_options_timeout',
        'test_connstr_options_timeout_fail',
        'test_connstr_options_timeout_invalid_duration',
        # 'test_connstr_options_timeout_override_opts',
        'test_connstr_options_security',
        'test_connstr_options_security_fail',
        # 'test_connstr_options_security_override_opts',
        'test_dns_srv_disabled',
        'test_invalid_connection_strings',
        'test_valid_connection_strings',
    ]

    @pytest.mark.parametrize('connstr_opts, expected_opts',
                             [({'config_poll_floor': '2500ms',
                                'config_poll_interval': '2500ms',
                                'dns_nameserver': '127.0.0.1',
                                'dns_port': '1051',
                                'dump_configuration': 'True',
                                'enable_clustermap_notification': 'True',
                                'ip_protocol': 'force_ipv4',
                                'network': 'external'
                                },
                               {'config_poll_floor': 2500,
                                 'config_poll_interval': 2500,
                                 'dump_configuration': True,
                                 'enable_clustermap_notification': True,
                                 'ip_protocol': 'force_ipv4',
                                 'network': 'external'
                                }),
                              ])
    def test_connstr_options_general(self,
                                     connstr_opts:  Dict[str, object],
                                     expected_opts: Dict[str, object]) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        connstr = f'couchbases://localhost?{to_query_str(connstr_opts)}'
        client = _ClientAdapter(connstr, cred)

        user_agent = client.connection_details.cluster_options.get('user_agent_extra', None)
        # only dns_nameserver and dns_port and user_agent_extra should be here
        assert 'dns_nameserver' in client.connection_details.cluster_options
        assert 'dns_port' in client.connection_details.cluster_options
        assert user_agent is not None
        assert 'pycbcc/' in user_agent
        assert 'python/' in user_agent

        req_builder = ClusterRequestBuilder(client)
        req = req_builder.build_connection_request()
        res = client._test_connect(req)
        # add special cases
        expected_opts['enable_dns_srv'] = True
        expected_opts['user_agent_extra'] = user_agent
        assert res['general'] == expected_opts

    @pytest.mark.parametrize('connstr_opt',
                             ['invalid_op=10',
                              'connection_timeout=2500ms',
                              'dispatch_timeout=2500ms',
                              'query_timeout=2500ms',
                              'resolve_timeout=2500ms',
                              'socket_connect_timeout=2500ms',
                              'trust_only_pem_file=/path/to/file',
                              'disable_server_certificate_verification=True'
                              ])
    def test_connstr_options_general_fail(self,
                                          connstr_opt: str) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        connstr = f'couchbases://localhost?{connstr_opt}'
        client = _ClientAdapter(connstr, cred)
        req_builder = ClusterRequestBuilder(client)
        req = req_builder.build_connection_request()
        with pytest.raises(ValueError):
            client._test_connect(req)

    @pytest.mark.parametrize('duration, expected_millis',
                             [('0', '0'),
                              ('1h', '3.6e6'),
                              ('+1h', '3.6e6'),
                              ('+1h', '3.6e6'),
                              ('1h10m', '4.2e6'),
                              ('1.h10m', '4.2e6'),
                              ('.1h10m', '9.6e5'),
                              ('0001h00010m', '4.2e6'),
                              ('2m3s4ms', '123004'),
                              (('100ns', '0')),
                              (('100us', '0')),
                              (('100μs', '0')),
                              (('1000000ns', '1')),
                              (('1000us', '1')),
                              (('1000μs', '1')),
                              ('4ms3s2m', '123004'),
                              ('4ms3s2m5s', '128004'),
                              ('2m3.125s', '123125'),
                              ])
    def test_connstr_options_timeout(self,
                                     duration: str,
                                     expected_millis: str) -> None:
        opt_keys = ['timeout.connect_timeout',
                    'timeout.dispatch_timeout',
                    'timeout.query_timeout',
                    'timeout.resolve_timeout',
                    'timeout.socket_connect_timeout']
        opts = {k: duration for k in opt_keys}
        cred = Credential.from_username_and_password('Administrator', 'password')
        connstr = f'couchbases://localhost?{to_query_str(opts)}'
        client = _ClientAdapter(connstr, cred)
        req_builder = ClusterRequestBuilder(client)
        req = req_builder.build_connection_request()
        res = client._test_connect(req)
        expected = float(expected_millis)
        returned_timeout_opts = res.get('timeout_options', None)
        assert isinstance(returned_timeout_opts, dict)
        for k in opts.keys():
            opt_key = k.split('.')[1]
            assert float(returned_timeout_opts[opt_key]) == expected

    def test_connstr_options_timeout_fail(self) -> None:
        opt_keys = ['connect_timeout',
                    'dispatch_timeout',
                    'query_timeout',
                    'resolve_timeout',
                    'socket_connect_timeout']
        opts = {k: '2500s' for k in opt_keys}
        cred = Credential.from_username_and_password('Administrator', 'password')
        connstr = f'couchbases://localhost?{to_query_str(opts)}'
        client = _ClientAdapter(connstr, cred)
        req_builder = ClusterRequestBuilder(client)
        req = req_builder.build_connection_request()
        try:
            client._test_connect(req)
        except Exception as ex:
            assert isinstance(ex, ValueError)
            errstr = str(ex)
            assert 'Invalid option(s)' in errstr
            for k in opts.keys():
                assert k in errstr

    @pytest.mark.parametrize('bad_duration',
                             ['123',
                              '00',
                              ' 1h',
                              '1h ',
                              '1h 2m'
                              '+-3h',
                              '-+3h',
                              '-',
                              '-.',
                              '.',
                              '.h',
                              '2.3.4h',
                              '3x',
                              '3',
                              '3h4x',
                              '1H',
                              '1h-2m',
                              '-1h',
                              '-1m',
                              '-1s'
                              ])
    def test_connstr_options_timeout_invalid_duration(self,
                                                      bad_duration: str) -> None:
        opt_keys = ['timeout.connect_timeout',
                    'timeout.dispatch_timeout',
                    'timeout.query_timeout',
                    'timeout.resolve_timeout',
                    'timeout.socket_connect_timeout']
        opts = {k: bad_duration for k in opt_keys}
        cred = Credential.from_username_and_password('Administrator', 'password')
        connstr = f'couchbases://localhost?{to_query_str(opts)}'
        client = _ClientAdapter(connstr, cred)
        req_builder = ClusterRequestBuilder(client)
        req = req_builder.build_connection_request()
        try:
            client._test_connect(req)
        except Exception as ex:
            assert isinstance(ex, ValueError)
            errstr = str(ex)
            assert 'Invalid option(s)' in errstr
            for k in opts.keys():
                assert k in errstr

    @pytest.mark.parametrize('connstr_opts, expected_opts',
                             [({'security.trust_only_pem_file': '/path/to/file'},
                               {'trust_certificate': '/path/to/file',
                               'tls_verify': 'peer'}),
                              ({'security.disable_server_certificate_verification': 'true'},
                               {'trust_certificate': '',
                                'tls_verify': 'none'}),
                              ])
    def test_connstr_options_security(self,
                                      connstr_opts: Dict[str, object],
                                      expected_opts: Dict[str, object]) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        connstr = f'couchbases://localhost?{to_query_str(connstr_opts)}'
        client = _ClientAdapter(connstr, cred)
        req_builder = ClusterRequestBuilder(client)
        req = req_builder.build_connection_request()
        res = client._test_connect(req)
        assert 'security_options' in res
        assert res['security_options'] == expected_opts

    def test_connstr_options_security_fail(self) -> None:
        opt_keys = ['trust_only_capella',
                    'trust_only_pem_file',
                    'trust_only_pem_str',
                    'trust_only_certificates',
                    'trust_only_platform',
                    'disable_server_certificate_verification']
        opts = {k: 'True' for k in opt_keys}
        cred = Credential.from_username_and_password('Administrator', 'password')
        connstr = f'couchbases://localhost?{to_query_str(opts)}'
        client = _ClientAdapter(connstr, cred)
        req_builder = ClusterRequestBuilder(client)
        req = req_builder.build_connection_request()
        try:
            client._test_connect(req)
        except Exception as ex:
            assert isinstance(ex, ValueError)
            errstr = str(ex)
            assert 'Invalid option(s)' in errstr
            for k in opts.keys():
                assert k in errstr

    @pytest.mark.parametrize('connstr, expected_connstr, expected_opts, enable_dns_srv',
                             [('couchbases://localhost?dns_nameserver=127.0.0.1&dump_configuration=true',
                               'couchbases://localhost?dump_configuration=true',
                               {'dns_nameserver': '127.0.0.1'},
                               None),
                              ('couchbases://localhost?srv=false&dump_configuration=true',
                               'couchbases://localhost?dump_configuration=true',
                               {},
                               False),
                              ('couchbases://localhost?an_invalid_option=10',
                               'couchbases://localhost?an_invalid_option=10',
                               {},
                               None),
                              ('couchbases://localhost?srv=False&an_invalid_option=10',
                               'couchbases://localhost?an_invalid_option=10',
                               {},
                               False),
                              ('couchbases://localhost',
                               'couchbases://localhost',
                               {},
                               None),
                              ('couchbases://localhost?srv=false',
                               'couchbases://localhost',
                               {},
                               False),
                              ])
    def test_dns_srv_disabled(self,
                              connstr: str,
                              expected_connstr: str,
                              expected_opts: Dict[str, object],
                              enable_dns_srv: Optional[bool]) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        client = _ClientAdapter(connstr, cred)

        user_agent = client.connection_details.cluster_options.pop('user_agent_extra', None)
        assert expected_opts == client.connection_details.cluster_options
        assert user_agent is not None
        assert 'pycbcc/' in user_agent
        assert 'python/' in user_agent
        assert expected_connstr == client.connection_details.connection_str
        assert client.connection_details.enable_dns_srv == enable_dns_srv

    @pytest.mark.parametrize('connstr', ['10.0.0.1:8091',
                                         'http://host1',
                                         'http://host2:8091',
                                         'https://host2',
                                         'https://host2:8091',
                                         'couchbase://10.0.0.1'])
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
        assert connstr == client.connection_details.connection_str


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
