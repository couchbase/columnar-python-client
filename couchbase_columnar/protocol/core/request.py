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
import sys
from dataclasses import asdict, dataclass
from typing import (TYPE_CHECKING,
                    Any,
                    Callable,
                    Dict,
                    List,
                    Optional,
                    Tuple,
                    Union,
                    cast)

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

from couchbase_columnar.common.deserializer import Deserializer
from couchbase_columnar.common.options import QueryOptions
from couchbase_columnar.common.query import CancelToken
from couchbase_columnar.protocol.options import ClusterOptionsTransformedKwargs, QueryOptionsTransformedKwargs

if TYPE_CHECKING:
    from acouchbase_columnar.protocol.core.client_adapter import _ClientAdapter as AsyncClientAdapter
    from couchbase_columnar.protocol.core.client_adapter import _ClientAdapter as BlockingClientAdapter


@dataclass
class CloseConnectionRequest:
    callback: Optional[Callable[..., None]] = None
    errback: Optional[Callable[..., None]] = None

    def to_req_dict(self) -> Dict[str, Any]:
        return ClusterRequestBuilder.to_req_dict(self)


@dataclass
class ConnectRequest:
    connection_str: str
    credential: Dict[str, str]
    options_in_connstr: Dict[str, List[str]]
    options: Optional[ClusterOptionsTransformedKwargs] = None
    enable_dns_srv: Optional[bool] = None
    dns_srv_timeout: Optional[str] = None

    def _process_connstr_options(self) -> None:
        if self.options is None:
            return
        # remove options that were passed in the connstr
        if 'timeout_options' in self.options_in_connstr and len(self.options_in_connstr['timeout_options']) > 0:
            opts = self.options.get('timeout_options', None)
            if opts is not None:
                for key in self.options_in_connstr['timeout_options']:
                    cast(Dict[str, object], opts).pop(key, None)

        if 'general_options' in self.options_in_connstr and len(self.options_in_connstr['general_options']) > 0:
            for key in self.options_in_connstr['general_options']:
                self.options.pop(key, None)  # type: ignore

    def to_req_dict(self) -> Dict[str, Any]:
        self._process_connstr_options()
        req_dict = asdict(self)
        if self.enable_dns_srv is False:
            if 'options' not in req_dict:
                req_dict['options'] = {}
            req_dict['options']['enable_dns_srv'] = req_dict.pop('enable_dns_srv')
        else:
            req_dict.pop('enable_dns_srv')
        # dns_srv_timeout might be both options and options.timeout_options, in the bindings
        # options value will take precedence.  This is what we want as dns_srv_timeout not in the
        # timeout_options means user specified value in connection string param
        if self.dns_srv_timeout is not None:
            if 'options' not in req_dict:
                req_dict['options'] = {}
            req_dict['options']['dns_srv_timeout'] = req_dict.pop('dns_srv_timeout')
        else:
            req_dict.pop('dns_srv_timeout')

        # we need to keep track of certain timeout options to make sure we set the in the
        # agent's timeout_config correctly
        connstr_opts = req_dict.pop('options_in_connstr')
        if 'timeout_options' in connstr_opts and len(connstr_opts['timeout_options']) > 0:
            req_dict['connstr_timeout_options'] = [k for k in connstr_opts['timeout_options'] if k in ['query_timeout']]

        return req_dict


@dataclass
class QueryRequest:
    statement: str
    deserializer: Deserializer
    options: Optional[QueryOptionsTransformedKwargs] = None
    database_name: Optional[str] = None
    scope_name: Optional[str] = None

    def to_req_dict(self) -> Dict[str, Any]:
        req_dict = {k: v for k, v in asdict(self).items() if v is not None}
        # we don't need the deserializer in the request
        req_dict.pop('deserializer', None)
        req_options = req_dict.pop('options', None)
        # core C++ wants all args JSONified,
        for opt_key, opt_val in req_options.items():
            if opt_key == 'serializer':
                continue
            elif opt_key == 'raw':
                req_dict[opt_key] = {f'{k}': json.dumps(v).encode('utf-8')
                                     for k, v in opt_val.items()}
            elif opt_key == 'positional_parameters':
                req_dict[opt_key] = [json.dumps(arg).encode('utf-8') for arg in opt_val]
            elif opt_key == 'named_parameters':
                req_dict[opt_key] = {f'${k}': json.dumps(v).encode('utf-8')
                                     for k, v in opt_val.items()}
            else:
                req_dict[opt_key] = opt_val

        final_req = {}
        final_req['query_args'] = req_dict
        return final_req


ClusterRequest: TypeAlias = Union[CloseConnectionRequest,
                                  ConnectRequest]


class ClusterRequestBuilder:

    def __init__(self,
                 client: Union[AsyncClientAdapter, BlockingClientAdapter]) -> None:
        self._conn_details = client.connection_details
        self._opts_builder = client.options_builder

    def build_connection_request(self) -> ConnectRequest:
        return ConnectRequest(self._conn_details.connection_str,
                              self._conn_details.credential,
                              self._conn_details.options_in_connstr,
                              self._conn_details.cluster_options,
                              self._conn_details.enable_dns_srv,
                              self._conn_details.dns_srv_timeout)

    def build_close_connection_request(self) -> CloseConnectionRequest:
        return CloseConnectionRequest()

    def build_query_request(self,  # noqa: C901
                            statement: str,
                            *args: object,
                            **kwargs: object) -> Tuple[QueryRequest, Optional[CancelToken]]:  # noqa: C901
        cancel_token: Optional[CancelToken] = None
        kwarg_token = kwargs.pop('cancel_token', None)
        if isinstance(kwarg_token, CancelToken):
            cancel_token = kwarg_token

        # default if no options provided
        opts = QueryOptions()
        args_list = list(args)
        parsed_args_list = []
        for arg in args_list:
            if isinstance(arg, QueryOptions):
                # we have options passed in
                opts = arg
            elif cancel_token is None and isinstance(arg, CancelToken):
                cancel_token = arg
            else:
                parsed_args_list.append(arg)

        # need to pop out named params prior to sending options to the builder
        named_param_keys = list(filter(lambda k: k not in QueryOptions.VALID_OPTION_KEYS, kwargs.keys()))
        named_params = {}
        for key in named_param_keys:
            named_params[key] = kwargs.pop(key)

        q_opts = self._opts_builder.build_options(QueryOptions,
                                                  QueryOptionsTransformedKwargs,
                                                  kwargs,
                                                  opts)
        # positional params and named params passed in outside of QueryOptions serve as overrides
        if parsed_args_list and len(parsed_args_list) > 0:
            q_opts['positional_parameters'] = parsed_args_list
        if named_params and len(named_params) > 0:
            q_opts['named_parameters'] = named_params
        # add the default serializer if one does not exist
        deserializer = q_opts.pop('deserializer', None) or self._conn_details.default_deserializer

        final_opts = {}
        for k, v in q_opts.items():
            if k != 'deserializer':
                final_opts[k] = v

        return QueryRequest(statement, deserializer, options=q_opts), cancel_token

    @staticmethod
    def to_req_dict(request: ClusterRequest) -> Dict[str, Any]:
        req_dict = asdict(request)
        # always handle callbacks
        callback = req_dict.pop('callback', None)
        errback = req_dict.pop('errback', None)

        # we don't want the callback/errback in the request if it doesn't exist
        if callback:
            req_dict['callback'] = callback
        if errback:
            req_dict['errback'] = errback

        return req_dict


class ScopeRequestBuilder:

    def __init__(self,
                 client: Union[AsyncClientAdapter, BlockingClientAdapter],
                 database_name: str,
                 scope_name: str) -> None:
        self._conn_details = client.connection_details
        self._opts_builder = client.options_builder
        self._database_name = database_name
        self._scope_name = scope_name

    def build_query_request(self,  # noqa: C901
                            statement: str,
                            *args: object,
                            **kwargs: object) -> Tuple[QueryRequest, Optional[CancelToken]]:  # noqa: C901
        cancel_token: Optional[CancelToken] = None
        kwarg_token = kwargs.pop('cancel_token', None)
        if isinstance(kwarg_token, CancelToken):
            cancel_token = kwarg_token

        # default if no options provided
        opts = QueryOptions()
        args_list = list(args)
        parsed_args_list = []
        for arg in args_list:
            if isinstance(arg, QueryOptions):
                # we have options passed in
                opts = arg
            elif cancel_token is None and isinstance(arg, CancelToken):
                cancel_token = arg
            else:
                parsed_args_list.append(arg)

        # need to pop out named params prior to sending options to the builder
        named_param_keys = list(filter(lambda k: k not in QueryOptions.VALID_OPTION_KEYS, kwargs.keys()))
        named_params = {}
        for key in named_param_keys:
            named_params[key] = kwargs.pop(key)

        q_opts = self._opts_builder.build_options(QueryOptions,
                                                  QueryOptionsTransformedKwargs,
                                                  kwargs,
                                                  opts)
        # positional params and named params passed in outside of QueryOptions serve as overrides
        if parsed_args_list and len(parsed_args_list) > 0:
            q_opts['positional_parameters'] = parsed_args_list
        if named_params and len(named_params) > 0:
            q_opts['named_parameters'] = named_params
        # add the default serializer if one does not exist
        deserializer = q_opts.pop('deserializer', None) or self._conn_details.default_deserializer

        final_opts = {}
        for k, v in q_opts.items():
            if k != 'deserializer':
                final_opts[k] = v

        return (QueryRequest(statement,
                             deserializer,
                             options=q_opts,
                             database_name=self._database_name,
                             scope_name=self._scope_name),
                cancel_token)

    @staticmethod
    def to_req_dict(request: ClusterRequest) -> Dict[str, Any]:
        req_dict = asdict(request)
        # always handle callbacks
        callback = req_dict.pop('callback', None)
        errback = req_dict.pop('errback', None)

        # we don't want the callback/errback in the request if it doesn't exist
        if callback:
            req_dict['callback'] = callback
        if errback:
            req_dict['errback'] = errback

        return req_dict
