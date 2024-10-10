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

import sys
from datetime import timedelta
from typing import (Any,
                    Dict,
                    Iterable,
                    List,
                    Literal,
                    Optional,
                    TypedDict,
                    Union,
                    overload)

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

from couchbase_columnar.common import JSONType
from couchbase_columnar.common.deserializer import Deserializer
from couchbase_columnar.common.enums import IpProtocol, QueryScanConsistency

# need to populate the TypedDict to help the static type checker
class ClusterOptionsKwargs(TypedDict, total=False):
    config_poll_floor: Optional[timedelta]
    config_poll_interval: Optional[timedelta]
    deserializer: Optional[Deserializer]
    dns_nameserver: Optional[str]
    dns_port: Optional[int]
    dump_configuration: Optional[bool]
    enable_clustermap_notification: Optional[bool]
    ip_protocol: Optional[Union[IpProtocol, str]]
    network: Optional[str]
    security_options: Optional[SecurityOptionsBase]
    timeout_options: Optional[TimeoutOptionsBase]
    user_agent_extra: Optional[str]

ClusterOptionsValidKeys: TypeAlias = Literal[
    'config_poll_floor',
    'config_poll_interval',
    'deserializer',
    'dns_nameserver',
    'dns_port',
    'dump_configuration',
    'enable_clustermap_notification',
    'ip_protocol',
    'network',
    'security_options',
    'timeout_options',
    'user_agent_extra',
]

class ClusterOptionsBase(Dict[str, Any]):
    """
        **INTERNAL**
    """

    VALID_OPTION_KEYS: List[ClusterOptionsValidKeys] = [
        'config_poll_floor',
        'config_poll_interval',
        'deserializer',
        'dns_nameserver',
        'dns_port',
        'dump_configuration',
        'enable_clustermap_notification',
        'ip_protocol',
        'network',
        'security_options',
        'timeout_options',
        'user_agent_extra',
    ]

    @overload
    def __init__(self) -> None:
        ...

    @overload
    def __init__(self,
                 *,
                 config_poll_floor: Optional[timedelta] = None,
                 config_poll_interval: Optional[timedelta] = None,
                 deserializer: Optional[Deserializer] = None,
                 dns_nameserver: Optional[str] = None,
                 dns_port: Optional[int] = None,
                 dump_configuration: Optional[bool] = None,
                 enable_clustermap_notification: Optional[bool] = None,
                 ip_protocol: Optional[Union[IpProtocol, str]] = None,
                 network: Optional[str] = None,
                 security_options: Optional[SecurityOptionsBase] = None,
                 timeout_options: Optional[TimeoutOptionsBase] = None,
                 user_agent_extra: Optional[str] = None,
                 ) -> None:
        ...

# need to populate the TypedDict to help the static type checker
class SecurityOptionsKwargs(TypedDict, total=False):
    trust_only_capella: Optional[bool]
    trust_only_pem_file: Optional[str]
    trust_only_pem_str: Optional[str]
    trust_only_certificates: Optional[List[str]]
    trust_only_platform: Optional[bool]
    disable_server_certificate_verification: Optional[bool]

SecurityOptionsValidKeys: TypeAlias = Literal[
    'trust_only_capella',
    'trust_only_pem_file',
    'trust_only_pem_str',
    'trust_only_certificates',
    'trust_only_platform',
    'disable_server_certificate_verification',
]


class SecurityOptionsBase(Dict[str, object]):
    """
        **INTERNAL**
    """

    VALID_OPTION_KEYS: List[SecurityOptionsValidKeys] = [
        'trust_only_capella',
        'trust_only_pem_file',
        'trust_only_pem_str',
        'trust_only_certificates',
        'trust_only_platform',
        'disable_server_certificate_verification',
    ]

    @overload
    def __init__(self) -> None:
        ...

    @overload
    def __init__(self,
                 *,
                 trust_only_capella: Optional[bool] = None,
                 trust_only_pem_file: Optional[str] = None,
                 trust_only_pem_str: Optional[str] = None,
                 trust_only_certificates: Optional[List[str]] = None,
                 trust_only_platform: Optional[bool] = None,
                 disable_server_certificate_verification: Optional[bool] = None,
                 ) -> None:
        ...

# need to populate the TypedDict to help the static type checker
class TimeoutOptionsKwargs(TypedDict, total=False):
    connect_timeout: Optional[timedelta]
    dispatch_timeout: Optional[timedelta]
    dns_srv_timeout: Optional[timedelta]
    management_timeout: Optional[timedelta]
    query_timeout: Optional[timedelta]
    resolve_timeout: Optional[timedelta]
    socket_connect_timeout: Optional[timedelta]

TimeoutOptionsValidKeys: TypeAlias = Literal[
    'connect_timeout',
    'dispatch_timeout',
    'dns_srv_timeout',
    'management_timeout',
    'query_timeout',
    'resolve_timeout',
    'socket_connect_timeout',
]

class TimeoutOptionsBase(Dict[str, object]):
    """
    **INTERNAL**
    """

    VALID_OPTION_KEYS: List[TimeoutOptionsValidKeys] = [
        'connect_timeout',
        'dispatch_timeout',
        'dns_srv_timeout',
        'management_timeout',
        'query_timeout',
        'resolve_timeout',
        'socket_connect_timeout',
    ]

    @overload
    def __init__(self) -> None:
        ...

    @overload
    def __init__(self,
                 *,
                 connect_timeout: Optional[timedelta] = None,
                 dispatch_timeout: Optional[timedelta] = None,
                 dns_srv_timeout: Optional[timedelta] = None,
                 query_timeout: Optional[timedelta] = None,
                 resolve_timeout: Optional[timedelta] = None,
                 socket_connect_timeout: Optional[timedelta] = None,
                 ) -> None:
        ...

# need to populate the TypedDict to help the static type checker
class QueryOptionsKwargs(TypedDict, total=False):
    deserializer: Optional[Deserializer]
    lazy_execute: Optional[bool]
    named_parameters: Optional[Dict[str, JSONType]]
    positional_parameters: Optional[List[JSONType]]
    priority: Optional[bool]
    query_context: Optional[str]
    raw: Optional[Dict[str, Any]]
    read_only: Optional[bool]
    scan_consistency: Optional[QueryScanConsistency]
    timeout: Optional[timedelta]


QueryOptionsValidKeys: TypeAlias = Literal[
    'deserializer',
    'lazy_execute',
    'named_parameters',
    'positional_parameters',
    'priority',
    'query_context',
    'raw',
    'read_only',
    'scan_consistency',
    'timeout',
]


class QueryOptionsBase(Dict[str, object]):
    """
    **INTERNAL**
    """

    VALID_OPTION_KEYS: List[QueryOptionsValidKeys] = [
        'deserializer',
        'lazy_execute',
        'named_parameters',
        'positional_parameters',
        'priority',
        'query_context',
        'raw',
        'read_only',
        'scan_consistency',
        'timeout',
    ]

    @overload
    def __init__(self) -> None:
        ...

    @overload
    def __init__(self,
                 *,
                 deserializer: Optional[Deserializer] = None,
                 lazy_execute: Optional[bool] = None,
                 named_parameters: Optional[Dict[str, JSONType]] = None,
                 positional_parameters: Optional[Iterable[JSONType]] = None,
                 priority: Optional[bool] = None,
                 query_context: Optional[str] = None,
                 raw: Optional[Dict[str, Any]] = None,
                 read_only: Optional[bool] = None,
                 scan_consistency: Optional[QueryScanConsistency] = None,
                 timeout: Optional[timedelta] = None,
                 ) -> None:
        ...
