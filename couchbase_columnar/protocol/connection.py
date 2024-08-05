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

from dataclasses import dataclass
from typing import (TYPE_CHECKING,
                    Dict,
                    List,
                    Optional,
                    Tuple,
                    TypedDict)
from urllib.parse import parse_qs, urlparse

from couchbase_columnar.common.core.utils import is_null_or_empty, to_query_str
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


def add_unknown_opts_to_connstr(connstr: str,
                                query_str_opts: Dict[str, QueryStrVal]) -> str:
    """**INTERNAL**

    If `allow_unknown_qstr_options` is set, parse the connection string's query params for unknown
    parameters and append unknown paremeters back to the connection string.  This is helpful in the
    event the underlying client allows for extended query string params the Python SDK does not parse.


    Args:
        connstr (str): Current connection string.
        query_opts (Dict[str, Any]): Initial connection string's query params.


    Returns:
        str: Final connection string.
    """
    unknown_opts = to_query_str(query_str_opts)
    if unknown_opts:
        return f'{connstr}?{unknown_opts}'
    return connstr


@dataclass
class _ConnectionDetails:
    """
    **INTERNAL**
    """
    connection_str: str
    cluster_options: ClusterOptionsTransformedKwargs
    credential: Dict[str, str]
    default_deserializer: Deserializer

    # TODO:  is this needed?  If so, need to flesh out the validation matrix
    def validate_security_options(self) -> None:
        security_opts: Optional[SecurityOptionsTransformedKwargs] = self.cluster_options.get('security_options')
        if security_opts is not None and security_opts.get('trust_only_capella', False) is True:
            if not (is_null_or_empty(security_opts.get('trust_only_pem_file', None))
                    and is_null_or_empty(security_opts.get('trust_only_pem_str', None))
                    and len(security_opts.get('trust_only_certificates') or []) == 0):
                raise ValueError('Can only trust from Capella if trust_only_capella is True.')

    @classmethod
    def create(cls,
               opts_builder: OptionsBuilder,
               connstr: str,
               credential: Credential,
               options: Optional[object] = None,
               **kwargs: object) -> _ConnectionDetails:
        connection_str, query_str_opts = parse_connection_string(connstr)
        kwargs.update(query_str_opts)

        cluster_opts = opts_builder.build_cluster_options(ClusterOptions,
                                                          ClusterOptionsTransformedKwargs,
                                                          kwargs,
                                                          options)

        # handle unknown query string options
        allow_unknown_qstr_options = cluster_opts.pop('allow_unknown_qstr_options', False)
        if allow_unknown_qstr_options:
            connection_str = add_unknown_opts_to_connstr(connection_str, query_str_opts)

        default_deserializer = cluster_opts.pop('deserializer', None)
        if default_deserializer is None:
            default_deserializer = DefaultJsonDeserializer()

        if 'user_agent_extra' in cluster_opts:
            cluster_opts['user_agent_extra'] = f'{PYCBCC_VERSION};{cluster_opts["user_agent_extra"]}'
        else:
            cluster_opts['user_agent_extra'] = PYCBCC_VERSION

        conn_dtls = cls(connection_str,
                        cluster_opts,
                        credential.asdict(),
                        default_deserializer)
        conn_dtls.validate_security_options()
        return conn_dtls
