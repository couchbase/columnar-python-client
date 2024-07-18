#  Copyright 2016-2024. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the 'License')
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an 'AS IS' BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import sys
from copy import copy
from typing import (Any,
                    Callable,
                    Dict,
                    List,
                    Literal,
                    Optional,
                    Tuple,
                    TypedDict,
                    TypeVar,
                    Union)

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

from couchbase_columnar.common.core.utils import (VALIDATE_BOOL,
                                                  VALIDATE_DESERIALIZER,
                                                  VALIDATE_INT,
                                                  VALIDATE_STR,
                                                  VALIDATE_STR_LIST,
                                                  EnumToStr,
                                                  timedelta_as_microseconds,
                                                  to_microseconds,
                                                  validate_path,
                                                  validate_raw_dict)
from couchbase_columnar.common.deserializer import Deserializer
from couchbase_columnar.common.enums import (IpProtocol,
                                             QueryScanConsistency,
                                             TLSVerifyMode)
from couchbase_columnar.common.exceptions import InvalidArgumentException
from couchbase_columnar.common.options import (ClusterOptions,
                                               OptionsClass,
                                               QueryOptions,
                                               SecurityOptions,
                                               TimeoutOptions,
                                               TracingOptions)
from couchbase_columnar.common.options_base import (ClusterOptionsValidKeys,
                                                    SecurityOptionsValidKeys,
                                                    TimeoutOptionsValidKeys,
                                                    TracingOptionsValidKeys)

QUERY_CONSISTENCY_TO_STR = EnumToStr[QueryScanConsistency]()

QueryStrVal = Union[List[str], str, bool, int]


class ClusterOptionsTransforms(TypedDict):
    allow_unknown_qstr_options: Dict[Literal['allow_unknown_qstr_options'], Callable[[Any], bool]]
    config_poll_floor: Dict[Literal['config_poll_floor'], Callable[[Any], int]]
    config_poll_interval: Dict[Literal['config_poll_interval'], Callable[[Any], int]]
    deserializer: Dict[Literal['deserializer'], Callable[[Any], Deserializer]]
    disable_mozilla_ca_certificates: Dict[Literal['disable_mozilla_ca_certificates'], Callable[[Any], bool]]
    dns_nameserver: Dict[Literal['dns_nameserver'], Callable[[Any], str]]
    dns_port: Dict[Literal['dns_port'], Callable[[Any], int]]
    dump_configuration: Dict[Literal['dump_configuration'], Callable[[Any], bool]]
    enable_clustermap_notification: Dict[Literal['enable_clustermap_notification'], Callable[[Any], bool]]
    enable_dns_srv: Dict[Literal['enable_dns_srv'], Callable[[Any], bool]]
    enable_metrics: Dict[Literal['enable_metrics'], Callable[[Any], bool]]
    enable_tracing: Dict[Literal['enable_tracing'], Callable[[Any], bool]]
    ip_protocol: Dict[Literal['use_ip_protocol'], Callable[[Any], str]]
    log_redaction: Dict[Literal['log_redaction'], Callable[[Any], bool]]
    logging_meter_emit_interval: Dict[Literal['emit_interval'], Callable[[Any], int]]
    network: Dict[Literal['network'], Callable[[Any], str]]
    security_options: Dict[Literal['security_options'], Callable[[Any], Any]]
    timeout_options: Dict[Literal['timeout_options'], Callable[[Any], Any]]
    tls_verify: Dict[Literal['tls_verify'], Callable[[Any], str]]
    tracing_options: Dict[Literal['tracing_options'], Callable[[Any], Any]]
    user_agent_extra: Dict[Literal['user_agent_extra'], Callable[[Any], str]]


CLUSTER_OPTIONS_TRANSFORMS: ClusterOptionsTransforms = {
    'allow_unknown_qstr_options': {'allow_unknown_qstr_options': VALIDATE_BOOL},
    'config_poll_floor': {'config_poll_floor': timedelta_as_microseconds},
    'config_poll_interval': {'config_poll_interval': timedelta_as_microseconds},
    'deserializer': {'deserializer': VALIDATE_DESERIALIZER},
    'disable_mozilla_ca_certificates': {'disable_mozilla_ca_certificates': VALIDATE_BOOL},
    'dns_nameserver': {'dns_nameserver': VALIDATE_STR},
    'dns_port': {'dns_port': VALIDATE_INT},
    'dump_configuration': {'dump_configuration': VALIDATE_BOOL},
    'enable_clustermap_notification': {'enable_clustermap_notification': VALIDATE_BOOL},
    'enable_dns_srv': {'enable_dns_srv': VALIDATE_BOOL},
    'enable_metrics': {'enable_metrics': VALIDATE_BOOL},
    'enable_tracing': {'enable_tracing': VALIDATE_BOOL},
    'ip_protocol': {'use_ip_protocol': EnumToStr[IpProtocol]()},
    'log_redaction': {'log_redaction': VALIDATE_BOOL},
    'logging_meter_emit_interval': {'emit_interval': timedelta_as_microseconds},
    'network': {'network': VALIDATE_STR},
    'security_options': {'security_options': lambda x: x},
    'timeout_options': {'timeout_options': lambda x: x},
    'tls_verify': {'tls_verify': EnumToStr[TLSVerifyMode]()},
    'tracing_options': {'tracing_options': lambda x: x},
    'user_agent_extra': {'user_agent_extra': VALIDATE_STR},
}


class ClusterOptionsTransformedKwargs(TypedDict, total=False):
    allow_unknown_qstr_options: Optional[bool]
    config_poll_floor: Optional[int]
    config_poll_interval: Optional[int]
    deserializer: Optional[Deserializer]
    disable_mozilla_ca_certificates: Optional[bool]
    dns_nameserver: Optional[str]
    dns_port: Optional[int]
    dump_configuration: Optional[bool]
    emit_interval: Optional[int]
    enable_clustermap_notification: Optional[bool]
    enable_dns_srv: Optional[bool]
    enable_metrics: Optional[bool]
    enable_tracing: Optional[bool]
    log_redaction: Optional[bool]
    network: Optional[str]
    security_options: Optional[SecurityOptionsTransformedKwargs]
    timeout_options: Optional[TimeoutOptionsTransformedKwargs]
    tls_verify: Optional[str]
    tracing_options: Optional[TracingOptionsTransformedKwargs]
    user_agent_extra: Optional[str]
    use_ip_protocol: Optional[str]


class SecurityOptionsTransforms(TypedDict):
    trust_only_capella: Dict[Literal['trust_only_capella'], Callable[[Any], bool]]
    trust_only_pem_file: Dict[Literal['trust_only_pem_file'], Callable[[Any], str]]
    trust_only_pem_str: Dict[Literal['trust_only_pem_str'], Callable[[Any], str]]
    trust_only_certificates: Dict[Literal['trust_only_certificates'], Callable[[Any], List[str]]]
    trust_only_platform: Dict[Literal['trust_only_platform'], Callable[[Any], bool]]
    verify_server_certificate: Dict[Literal['verify_server_certificate'], Callable[[Any], bool]]
    cipher_suites: Dict[Literal['cipher_suites'], Callable[[Any], List[str]]]


SECURITY_OPTIONS_TRANSFORMS: SecurityOptionsTransforms = {
    'trust_only_capella': {'trust_only_capella': VALIDATE_BOOL},
    'trust_only_pem_file': {'trust_only_pem_file': validate_path},
    'trust_only_pem_str': {'trust_only_pem_str': VALIDATE_STR},
    'trust_only_certificates': {'trust_only_certificates': VALIDATE_STR_LIST},
    'trust_only_platform': {'trust_only_platform': VALIDATE_BOOL},
    'verify_server_certificate': {'verify_server_certificate': VALIDATE_BOOL},
    'cipher_suites': {'cipher_suites': VALIDATE_STR_LIST},
}


class SecurityOptionsTransformedKwargs(TypedDict, total=False):
    trust_only_capella: Optional[bool]
    trust_only_pem_file: Optional[str]
    trust_only_pem_str: Optional[str]
    trust_only_certificates: Optional[List[str]]
    trust_only_platform: Optional[bool]
    verify_server_certificate: Optional[bool]
    cipher_suites: Optional[List[str]]


class TimeoutOptionsTransforms(TypedDict):
    connect_timeout: Dict[Literal['bootstrap_timeout'], Callable[[Any], int]]
    dispatch_timeout: Dict[Literal['dispatch_timeout'], Callable[[Any], int]]
    dns_srv_timeout: Dict[Literal['dns_srv_timeout'], Callable[[Any], int]]
    management_timeout: Dict[Literal['management_timeout'], Callable[[Any], int]]
    query_timeout: Dict[Literal['analytics_timeout'], Callable[[Any], int]]
    resolve_timeout: Dict[Literal['resolve_timeout'], Callable[[Any], int]]
    socket_connect_timeout: Dict[Literal['connect_timeout'], Callable[[Any], int]]


TIMEOUT_OPTIONS_TRANSFORMS: TimeoutOptionsTransforms = {
    'connect_timeout': {'bootstrap_timeout': timedelta_as_microseconds},
    'dispatch_timeout': {'dispatch_timeout': timedelta_as_microseconds},
    'dns_srv_timeout': {'dns_srv_timeout': timedelta_as_microseconds},
    'management_timeout': {'management_timeout': timedelta_as_microseconds},
    'query_timeout': {'analytics_timeout': timedelta_as_microseconds},
    'resolve_timeout': {'resolve_timeout': timedelta_as_microseconds},
    'socket_connect_timeout': {'connect_timeout': timedelta_as_microseconds},
}


class TimeoutOptionsTransformedKwargs(TypedDict, total=False):
    connect_timeout: Optional[int]
    dispatch_timeout: Optional[int]
    dns_srv_timeout: Optional[int]
    management_timeout: Optional[int]
    query_timeout: Optional[int]
    resolve_timeout: Optional[int]
    socket_connect_timeout: Optional[int]


class TracingOptionsTransforms(TypedDict):
    tracing_orphaned_queue_flush_interval: Dict[Literal['orphaned_emit_interval'], Callable[[Any], int]]
    tracing_orphaned_queue_size: Dict[Literal['orphaned_sample_size'], Callable[[Any], int]]
    tracing_threshold_analytics: Dict[Literal['analytics_threshold'], Callable[[Any], int]]
    tracing_threshold_management: Dict[Literal['management_threshold'], Callable[[Any], int]]
    tracing_threshold_queue_flush_interval: Dict[Literal['threshold_emit_interval'], Callable[[Any], int]]
    tracing_threshold_queue_size: Dict[Literal['threshold_sample_size'], Callable[[Any], int]]


TRACING_OPTIONS_TRANSFORMS: TracingOptionsTransforms = {
    'tracing_orphaned_queue_flush_interval': {'orphaned_emit_interval': timedelta_as_microseconds},
    'tracing_orphaned_queue_size': {'orphaned_sample_size': VALIDATE_INT},
    'tracing_threshold_analytics': {'analytics_threshold': timedelta_as_microseconds},
    'tracing_threshold_management': {'management_threshold': timedelta_as_microseconds},
    'tracing_threshold_queue_flush_interval': {'threshold_emit_interval': timedelta_as_microseconds},
    'tracing_threshold_queue_size': {'threshold_sample_size': VALIDATE_INT},
}


class TracingOptionsTransformedKwargs(TypedDict, total=False):
    orphaned_emit_interval: Optional[int]
    orphaned_sample_size: Optional[int]
    analytics_threshold: Optional[int]
    management_threshold: Optional[int]
    threshold_emit_interval: Optional[int]
    threshold_sample_size: Optional[int]


QueryOptionsValidKeys: TypeAlias = Literal[
    'client_context_id',
    'deserializer',
    'metrics',
    'named_parameters',
    'positional_parameters',
    'priority',
    'query_context',
    'raw',
    'read_only',
    'scan_consistency',
    'timeout',
]


class QueryOptionsTransforms(TypedDict):
    client_context_id: Dict[Literal['client_context_id'], Callable[[Any], str]]
    deserializer: Dict[Literal['deserializer'], Callable[[Any], Deserializer]]
    metrics: Dict[Literal['metrics'], Callable[[Any], bool]]
    named_parameters: Dict[Literal['named_parameters'], Callable[[Any], Any]]
    positional_parameters: Dict[Literal['positional_parameters'], Callable[[Any], Any]]
    priority: Dict[Literal['priority'], Callable[[Any], bool]]
    query_context: Dict[Literal['query_context'], Callable[[Any], str]]
    raw: Dict[Literal['raw'], Callable[[Any], Dict[str, Any]]]
    read_only: Dict[Literal['readonly'], Callable[[Any], bool]]
    scan_consistency: Dict[Literal['scan_consistency'], Callable[[Any], str]]
    timeout: Dict[Literal['timeout'], Callable[[Any], int]]


QUERY_OPTIONS_TRANSFORMS: QueryOptionsTransforms = {
    'client_context_id': {'client_context_id': VALIDATE_STR},
    'deserializer': {'deserializer': VALIDATE_DESERIALIZER},
    'metrics': {'metrics': VALIDATE_BOOL},
    'named_parameters':  {'named_parameters': lambda x: x},
    'positional_parameters':  {'positional_parameters': lambda x: x},
    'priority': {'priority': VALIDATE_BOOL},
    'query_context': {'query_context': VALIDATE_STR},
    'raw': {'raw': validate_raw_dict},
    'read_only': {'readonly': VALIDATE_BOOL},
    'scan_consistency': {'scan_consistency': QUERY_CONSISTENCY_TO_STR},
    'timeout': {'timeout': to_microseconds}
}


class QueryOptionsTransformedKwargs(TypedDict, total=False):
    client_context_id: Optional[str]
    deserializer: Optional[Deserializer]
    metrics: Optional[bool]
    named_parameters: Optional[Any]
    positional_parameters: Optional[Any]
    priority: Optional[bool]
    query_context: Optional[str]
    raw: Optional[Dict[str, Any]]
    readonly: Optional[bool]
    scan_consistency: Optional[str]
    timeout: Optional[int]


TransformedOptionKwargs = TypeVar('TransformedOptionKwargs',
                                  QueryOptionsTransformedKwargs,
                                  ClusterOptionsTransformedKwargs,
                                  SecurityOptionsTransformedKwargs,
                                  TimeoutOptionsTransformedKwargs,
                                  TracingOptionsTransformedKwargs)

TransformedClusterOptionKwargs = TypeVar('TransformedClusterOptionKwargs',
                                         ClusterOptionsTransformedKwargs,
                                         SecurityOptionsTransformedKwargs,
                                         TimeoutOptionsTransformedKwargs,
                                         TracingOptionsTransformedKwargs)

TransformDetailsPair = Union[Tuple[List[QueryOptionsValidKeys], QueryOptionsTransforms],
                             Tuple[List[ClusterOptionsValidKeys], ClusterOptionsTransforms],
                             Tuple[List[SecurityOptionsValidKeys], SecurityOptionsTransforms],
                             Tuple[List[TimeoutOptionsValidKeys], TimeoutOptionsTransforms],
                             Tuple[List[TracingOptionsValidKeys], TracingOptionsTransforms],
                             ]


class OptionsBuilder:
    """
        **INTERNAL**
    """

    def _get_options_copy(self,
                          options_class: type[OptionsClass],
                          orig_kwargs: Dict[str, object],
                          options: Optional[object] = None) -> Dict[str, object]:
        orig_kwargs = copy(orig_kwargs) if orig_kwargs else dict()
        # set our options base dict()
        temp_options: Dict[str, object] = {}
        if options and isinstance(options, (options_class, dict)):
            # mypy cannot recognize that all our options classes are dicts
            temp_options = options_class(**options)
        else:
            temp_options = dict()
        temp_options.update(orig_kwargs)

        return temp_options

    def _get_transform_details(self, option_type: str) -> TransformDetailsPair:  # noqa: C901

        if option_type == 'ClusterOptions':
            return ClusterOptions.VALID_OPTION_KEYS, CLUSTER_OPTIONS_TRANSFORMS
        elif option_type == 'SecurityOptions':
            return SecurityOptions.VALID_OPTION_KEYS, SECURITY_OPTIONS_TRANSFORMS
        elif option_type == 'TimeoutOptions':
            return TimeoutOptions.VALID_OPTION_KEYS, TIMEOUT_OPTIONS_TRANSFORMS
        elif option_type == 'TracingOptions':
            return TracingOptions.VALID_OPTION_KEYS, TRACING_OPTIONS_TRANSFORMS
        elif option_type == 'QueryOptions':
            return QueryOptions.VALID_OPTION_KEYS, QUERY_OPTIONS_TRANSFORMS
        else:
            raise InvalidArgumentException('Invalid OptionType.')

    def build_cluster_options(self,  # noqa: C901
                              option_type: type[OptionsClass],
                              output_type: type[TransformedClusterOptionKwargs],
                              orig_kwargs: Dict[str, object],
                              options: Optional[object] = None
                              ) -> TransformedClusterOptionKwargs:
        temp_options = self._get_options_copy(option_type, orig_kwargs, options)

        # we flatten all the nested options (timeout_options, tracing_options & transaction_config)
        # so that we can combine the nested options w/ potential query string options
        # when parsing the various nested options we pass in keys that are okay to be ignored as
        # we know they are included in the overall "cluster options" umbrella (mainly due to handling
        # the query string options).

        security_opts = temp_options.pop('security_options', {})
        if security_opts and isinstance(security_opts, dict):
            for k, v in security_opts.items():
                if k not in temp_options:
                    temp_options[k] = v

        timeout_opts = temp_options.pop('timeout_options', {})
        if timeout_opts and isinstance(timeout_opts, dict):
            for k, v in timeout_opts.items():
                if k not in temp_options:
                    temp_options[k] = v

        tracing_opts = temp_options.pop('tracing_options', {})
        if tracing_opts and isinstance(tracing_opts, dict):
            for k, v in tracing_opts.items():
                if k not in temp_options:
                    temp_options[k] = v

        keys_to_ignore: List[str] = [*ClusterOptions.VALID_OPTION_KEYS,
                                     *TimeoutOptions.VALID_OPTION_KEYS,
                                     *TracingOptions.VALID_OPTION_KEYS]

        # not going to be able to make mypy happy w/ keys_to_ignore :/
        transformed_security_opts = self.build_options(SecurityOptions,
                                                       SecurityOptionsTransformedKwargs,
                                                       temp_options,
                                                       keys_to_ignore=keys_to_ignore)
        if transformed_security_opts:
            temp_options['security_options'] = transformed_security_opts

        keys_to_ignore = [*ClusterOptions.VALID_OPTION_KEYS,
                          *SecurityOptions.VALID_OPTION_KEYS,
                          *TracingOptions.VALID_OPTION_KEYS]

        # not going to be able to make mypy happy w/ keys_to_ignore :/
        transformed_timeout_opts = self.build_options(TimeoutOptions,
                                                      TimeoutOptionsTransformedKwargs,
                                                      temp_options,
                                                      keys_to_ignore=keys_to_ignore)
        if transformed_timeout_opts:
            temp_options['timeout_options'] = transformed_timeout_opts

        keys_to_ignore = [*ClusterOptions.VALID_OPTION_KEYS,
                          *SecurityOptions.VALID_OPTION_KEYS,
                          *TimeoutOptions.VALID_OPTION_KEYS]

        # not going to be able to make mypy happy w/ keys_to_ignore :/
        transformed_tracing_opts = self.build_options(TracingOptions,
                                                      TracingOptionsTransformedKwargs,
                                                      temp_options,
                                                      keys_to_ignore=keys_to_ignore)

        if transformed_tracing_opts:
            temp_options['tracing_options'] = transformed_tracing_opts

        # transform final ClusterOptions
        transformed_opts = self.build_options(option_type, output_type, temp_options)

        return transformed_opts

    def build_options(self,
                      option_type: type[OptionsClass],
                      output_type: type[TransformedOptionKwargs],
                      orig_kwargs: Dict[str, object],
                      options: Optional[object] = None,
                      keys_to_ignore: Optional[List[str]] = None
                      ) -> TransformedOptionKwargs:

        temp_options = self._get_options_copy(option_type, orig_kwargs, options)
        transformed_opts: TransformedOptionKwargs = {}
        # Option 1 satisfies mypy, but we want temp_options to be the limiting
        # factor for the loop.
        # Option 2. Also makes providing warnings/exceptions for users not using static type checking easier,
        # but unfortunately we need to use soem type: ignore comments

        # 1.
        # for k in option_type.VALID_OPTION_KEYS:
        #     if k in ALLOWED_TRANSFORM_KEYS and k in temp_options:
        #         for nk, cfn in tf_dict[k].items():
        #             conv = cfn(temp_options[k])
        #             transformed_opts[nk] = conv # type: ignore

        # 2.
        allowed_keys, option_transforms = self._get_transform_details(option_type.__name__)
        for k, v in temp_options.items():
            if k in allowed_keys:
                transforms = option_transforms[k]  # type: ignore[literal-required]
                for nk, cfn in transforms.items():
                    conv = cfn(v)
                    if conv is not None:
                        transformed_opts[nk] = conv  # type: ignore[literal-required]
            elif keys_to_ignore and k not in keys_to_ignore:
                # TODO:  exception??
                print(f'Invalid key: {k}')

        return transformed_opts
