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
from typing import (TYPE_CHECKING,
                    Dict,
                    List,
                    Optional,
                    Tuple,
                    TypedDict)
from urllib.parse import parse_qs, urlparse

from couchbase_columnar.common.core.utils import to_query_str
from couchbase_columnar.common.credential import Credential
from couchbase_columnar.common.deserializer import DefaultJsonDeserializer, Deserializer
from couchbase_columnar.common.options import ClusterOptions
from couchbase_columnar.protocol import PYCBCC_VERSION
from couchbase_columnar.protocol.options import (ClusterOptionsTransformedKwargs,
                                                 QueryStrVal,
                                                 SecurityOptionsTransformedKwargs)

if TYPE_CHECKING:
    from couchbase_columnar.protocol.options import OptionsBuilder


class StreamingTimeouts(TypedDict, total=False):
    query_timeout: Optional[int]


def parse_connection_string(connection_str: str) -> Tuple[str, Dict[str, QueryStrVal]]:
    """ **INTERNAL**

    Parse the provided connection string

    The provided connection string will be parsed to split the connection string
    and the the query options.  Query options will be split into legacy options
    and 'current' options.

    Args:
        connection_str (str): The connection string for the cluster.

    Returns:
        Tuple[str, Dict[str, Any], Dict[str, Any]]: The parsed connection string,
            current options and legacy options.
    """
    parsed_conn = urlparse(connection_str)
    if parsed_conn.scheme is None or parsed_conn.scheme != 'couchbases':
        raise ValueError(f"The connection scheme must be 'couchbases'.  Found: {parsed_conn.scheme}.")

    conn_str = f'{parsed_conn.scheme}://{parsed_conn.netloc}{parsed_conn.path}'
    query_str = parsed_conn.query

    query_str_opts = parse_query_string_options(query_str)
    return conn_str, query_str_opts


def parse_query_string_options(query_str: str) -> Dict[str, QueryStrVal]:
    """Parse the query string options

    Query options will be split into legacy options and 'current' options. The values for the
    'current' options are cast to integers or booleans where applicable

    Args:
        query_str (str): The query string.

    Returns:
        Tuple[Dict[str, QueryStrVal], Dict[str, QueryStrVal]]: The parsed current options and legacy options.
    """
    options = parse_qs(query_str)

    query_str_opts: Dict[str, QueryStrVal] = {}
    for k, v in options.items():
        query_str_opts[k] = parse_query_string_value(v)

    return query_str_opts


def parse_query_string_value(value: List[str]) -> QueryStrVal:
    """Parse a query string value

    The provided value is a list of at least one element. Returns either a list of strings or a single element
    which might be cast to an integer or a boolean if that's appropriate.

    Args:
        value (List[str]): The query string value.

    Returns:
        Union[List[str], str, bool, int]: The parsed current options and legacy options.
    """

    if len(value) > 1:
        return value
    v = value[0]
    if v.isnumeric():
        return int(v)
    elif v.lower() in ['true', 'false']:
        return v.lower() == 'true'
    return v


def parse_connstr_options(query_str_opts: Dict[str, QueryStrVal],
                          options_in_connstr: Dict[str, List[str]]) -> None:
    for k in query_str_opts.keys():
        tokens = k.split('.')
        if len(tokens) == 2:
            if 'timeout' in k:
                options_in_connstr['timeout_options'].append(k.split('.')[1])
            elif 'security' in k:
                options_in_connstr['security_options'].append(k.split('.')[1])
        else:
            options_in_connstr['general_options'].append(k)


@dataclass
class _ConnectionDetails:
    """
    **INTERNAL**
    """
    connection_str: str
    cluster_options: ClusterOptionsTransformedKwargs
    credential: Dict[str, str]
    default_deserializer: Deserializer
    options_in_connstr: Dict[str, List[str]]
    enable_dns_srv: Optional[bool] = None
    dns_srv_timeout: Optional[str] = None

    def validate_security_options(self) -> None:
        security_opts: Optional[SecurityOptionsTransformedKwargs] = self.cluster_options.get('security_options')
        if security_opts is None:
            # if we have security.trust_only_pem_file in the connstr, and nothing else is set,
            # override the default for trust_only_capella
            if ('security_options' in self.options_in_connstr
                    and 'trust_only_pem_file' in self.options_in_connstr['security_options']):
                self.cluster_options['security_options'] = {'trust_only_capella': False}
            return

        solo_security_opts = ['trust_only_pem_file',
                              'trust_only_pem_str',
                              'trust_only_certificates',
                              'trust_only_platform']
        security_opt_count = sum(map(lambda k: 1 if security_opts.get(k, None) is not None else 0, solo_security_opts))
        trust_capella = security_opts.get('trust_only_capella', None)
        if security_opt_count > 1 or (security_opt_count == 1 and trust_capella is True):
            raise ValueError(('Can only set one of the following options: '
                              f'[{", ".join(["trust_only_capella"] + solo_security_opts)}]'))

        # we need to set trust_only_capella=False to override the default other security
        # options that should not use the default
        if any(map(lambda opt: security_opts.get(opt, None) is not None, solo_security_opts)):
            if trust_capella is True or trust_capella is None:
                security_opts['trust_only_capella'] = False

            # if we have security options in the connstr, override the options
            if 'security_options' in self.options_in_connstr:
                if 'trust_only_pem_file' in self.options_in_connstr['security_options']:
                    for opt in solo_security_opts:
                        security_opts.pop(opt, None)  # type: ignore
                if ('disable_server_certificate_verification' in self.options_in_connstr['security_options']
                        and 'disable_server_certificate_verification' in security_opts):
                    security_opts.pop('disable_server_certificate_verification')

    @classmethod
    def create(cls,
               opts_builder: OptionsBuilder,
               connstr: str,
               credential: Credential,
               options: Optional[object] = None,
               **kwargs: object) -> _ConnectionDetails:
        connection_str, query_str_opts = parse_connection_string(connstr)

        # DNS 'things' are special
        srv = query_str_opts.pop('srv', None)
        enable_dns_srv: Optional[bool] = None
        if srv is False:
            enable_dns_srv = srv
        # need to keep dns_srv_timeout separate b/c it can be a golang duration format
        # and we use the C++ core to handle that
        srv_timeout = query_str_opts.pop('timeout.dns_srv_timeout', None)
        dns_srv_timeout: Optional[str] = None
        if srv_timeout is not None:
            if not isinstance(srv_timeout, str):
                raise TypeError('timeout.dns_srv_timeout connection string param must be a str.')
            dns_srv_timeout = srv_timeout

        dns_opts = {}
        dns_nameserver = query_str_opts.pop('dns_nameserver', None)
        dns_port = query_str_opts.pop('dns_port', None)
        if dns_nameserver is not None:
            dns_opts['dns_nameserver'] = dns_nameserver
        if dns_port is not None:
            dns_opts['dns_port'] = dns_port
        if dns_opts:
            kwargs.update(dns_opts)

        options_in_connstr: Dict[str, List[str]] = {
            'timeout_options': [],
            'security_options': [],
            'general_options': []
        }
        # NOTE:  we handle connstr overrides for timeouts/general options in
        # ConnectRequest._process_connstr_options() before we finalize the request dict
        # we handle security options in validate_security_options()
        parse_connstr_options(query_str_opts, options_in_connstr)

        cluster_opts = opts_builder.build_cluster_options(ClusterOptions,
                                                          ClusterOptionsTransformedKwargs,
                                                          kwargs,
                                                          options)

        parsed_connstr = connection_str
        conn_str_opts = to_query_str(query_str_opts)
        if conn_str_opts:
            parsed_connstr += f'?{conn_str_opts}'

        default_deserializer = cluster_opts.pop('deserializer', None)
        if default_deserializer is None:
            default_deserializer = DefaultJsonDeserializer()

        if 'user_agent_extra' in cluster_opts:
            cluster_opts['user_agent_extra'] = f'{PYCBCC_VERSION};{cluster_opts["user_agent_extra"]}'
        else:
            cluster_opts['user_agent_extra'] = PYCBCC_VERSION

        conn_dtls = cls(parsed_connstr,
                        cluster_opts,
                        credential.asdict(),
                        default_deserializer,
                        options_in_connstr=options_in_connstr,
                        enable_dns_srv=enable_dns_srv,
                        dns_srv_timeout=dns_srv_timeout)
        conn_dtls.validate_security_options()
        return conn_dtls
