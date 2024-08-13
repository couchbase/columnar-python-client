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

import sys
from typing import List, Union

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

from couchbase_columnar.common.config_profile import CONFIG_PROFILES
from couchbase_columnar.common.enums import KnownConfigProfiles
from couchbase_columnar.common.options_base import ClusterOptionsBase
from couchbase_columnar.common.options_base import ClusterOptionsKwargs as ClusterOptionsKwargs  # noqa: F401
from couchbase_columnar.common.options_base import QueryOptionsBase
from couchbase_columnar.common.options_base import QueryOptionsKwargs as QueryOptionsKwargs  # noqa: F401
from couchbase_columnar.common.options_base import SecurityOptionsBase
from couchbase_columnar.common.options_base import SecurityOptionsKwargs as SecurityOptionsKwargs  # noqa: F401
from couchbase_columnar.common.options_base import TimeoutOptionsBase
from couchbase_columnar.common.options_base import TimeoutOptionsKwargs as TimeoutOptionsKwargs  # noqa: F401
from couchbase_columnar.common.options_base import TracingOptionsBase
from couchbase_columnar.common.options_base import TracingOptionsKwargs as TracingOptionsKwargs  # noqa: F401

"""
    Python SDK Cluster Options Classes
"""


class ClusterOptions(ClusterOptionsBase):
    """Available options to set when creating a cluster.

    Cluster options enable the configuration of various global cluster settings.
    Some options can be set globally for the cluster, but overridden for specific operations (i.e. :class:`~couchbase_columnar.options.ClusterTimeoutOptions`).
    Most options are optional, values in parenthesis indicate C++ core default that will be used.

    .. note::

        The authenticator is mandatory, all the other cluster options are optional.

    Args:
        allow_unknown_qstr_options (bool, optional): If enabled, allows unknown query string options to pass through to C++ core. Defaults to `False` (disabled).
        config_poll_floor (timedelta, optional): Set to configure polling floor interval. Defaults to `None` (50ms).
        config_poll_interval (timedelta, optional): Set to configure polling floor interval. Defaults to `None` (2.5s).
        deserializer (Deserializer, optional): Set to configure global serializer to translate JSON to Python objects. Defaults to `None` (:class:`~couchbase_columnar.deserializer.DefaultJsonDeserializer`).
        disable_mozilla_ca_certificates (bool, optional): If enabled, prevents the C++ core from loading Mozilla certificates. Defaults to `False` (disabled).
        dns_nameserver (str, optional): **VOLATILE** This API is subject to change at any time. Set to configure custom DNS nameserver. Defaults to `None`.
        dns_port (int, optional): **VOLATILE** This API is subject to change at any time. Set to configure custom DNS port. Defaults to `None`.
        dump_configuration (bool, optional): If enabled, dump received server configuration when TRACE level logging. Defaults to `False` (disabled).
        enable_clustermap_notification (bool, optional): If enabled, allows server to push configuration updates asynchronously. Defaults to `True` (enabled).
        enable_dns_srv (bool, optional): If enabled, SDK will attempt to bootstrap using DNS SRV. Defaults to `True` (enabled).
        enable_metrics (bool, optional): If enabled, use C++ core logging meter.  Otherwise use no-op meter. Defaults to `True` (enabled).
        enable_tracing (bool, optional): If enabled, use C++ core threshold-logging meter.  Otherwise use no-op meter. Defaults to `True` (enabled).
        ip_protocol (Union[IpProtocol, str], optional): Controls preference of IP protocol for name resolution. Defaults to `None` (any).
        logging_meter_emit_interval (timedelta, optional): Set to configure, logging meter emit interval.  Defaults to 10 minutes.
        log_redaction (bool, optional): **NOTE:** Currently a no-op. If enabled, allows log redaction. Defaults to `False` (disabled).
        network (str, optional): Set to configure external network. Defaults to `None` (auto).
        security_options (SecurityOptions, optional): Security options for SDK connection.
        timeout_options (TimeoutOptions, optional): Timeout options for various SDK operations. See :class:`~couchbase_columnar.options.ClusterTimeoutOptions` for details.
        tls_verify (Union[TLSVerifyMode, str], optional): Set to configure TLS verify mode. Defaults to `None` (peer).
        tracing_options (TracingOptions, optional): Tracing options for SDK tracing bevavior. Ignored if `tracer` option is set. See :class:`~couchbase_columnar.options.ClusterTracingOptions` for details.
        user_agent_extra (str, optional): Set to add further details to identification fields in server protocols. Defaults to `None` (`{Python SDK version} (python/{Python version})`).
    """  # noqa: E501

    def apply_profile(self, profile_name: Union[KnownConfigProfiles, str]) -> None:
        """
        **VOLATILE** This API is subject to change at any time.

        Apply the provided ConfigProfile options.

        Args:
            profile_name ([:class:`~couchbase_columnar.options.KnownConfigProfiles`, str]):  The name of the profile to apply
                toward ClusterOptions.

        Raises:
            :class:`~couchbase_columnar.exceptions.InvalidArgumentException`: If the specified profile is not registered.

        """  # noqa: E501
        prof_name = profile_name.value if isinstance(profile_name, KnownConfigProfiles) else profile_name
        CONFIG_PROFILES.apply_profile(prof_name, self)

    @classmethod
    def create_options_with_profile(cls, profile_name: Union[KnownConfigProfiles, str]) -> ClusterOptions:
        """
        **VOLATILE** This API is subject to change at any time.

        Create a ClusterOptions instance and apply the provided ConfigProfile options.

        Args:
            profile_name ([:class:`~couchbase_columnar.options.KnownConfigProfiles`, str]):  The name of the profile to apply toward ClusterOptions.

        Raises:
            :class:`~couchbase_columnar.exceptions.InvalidArgumentException`: If the specified profile is not registered.

        """  # noqa: E501
        opts = cls()
        opts.apply_profile(profile_name)
        return opts


class SecurityOptions(SecurityOptionsBase):
    """Available security options to set when creating a cluster.

    All options are optional and not required to be specified.  By default the SDK will trust only the Capella CA certificate(s).
    Only a single option related to which certificate(s) the SDK should trust can be used.
    The `verify_server_certificate` option can either be enabled or disabled for any of the specified trust settings.

    Args:
        trust_only_capella (bool, optional): If enabled, SDK will trust only the Capella CA certificate(s). Defaults to `True` (enabled).
        trust_only_pem_file (str, optional): If set, SDK will trust only the PEM-encoded certificate(s) at the specified file path. Defaults to `None`.
        trust_only_pem_str (str, optional): If set, SDK will trust only the PEM-encoded certificate(s) in the specified str. Defaults to `None`.
        trust_only_certificates (List[str], optional): If set, SDK will trust only the PEM-encoded certificate(s) specified. Defaults to `None`.
        trust_only_platform (bool, optional): If enabled, SDK will trust only the platform certificate(s). Defaults to `None`.
        verify_server_certificate (bool, optional): If disabled, SDK will trust any certificate regardless of validity.
            Should not be disabled in production environments. Defaults to `True` (enabled).
        cipher_suites (List[str], optional): Names of TLS cipher suites the SDK is allowed to use while negotiating TLS settings.
            An empty list indicates any cipher suite supported by the runtime environment may be used.  Defaults to `None` (empty list).
    """  # noqa: E501

    @classmethod
    def trust_only_capella(cls) -> SecurityOptions:
        """
        Convenience method that returns `SecurityOptions` instance with `trust_only_capella=True`.

        Returns:
            :class:`~couchbase_columnar.common.options.SecurityOptions`
        """
        return cls(trust_only_capella=True)

    @classmethod
    def trust_only_pem_file(cls, pem_file: str) -> SecurityOptions:
        """
        Convenience method that returns `SecurityOptions` instance with `trust_only_pem_file` set to provided certificate(s) path.

        Args:
            pem_file (str): Path to PEM-encoded certificate(s) the SDK should trust.

        Returns:
            :class:`~couchbase_columnar.common.options.SecurityOptions`
        """  # noqa: E501
        return cls(trust_only_pem_file=pem_file)

    @classmethod
    def trust_only_pem_str(cls, pem_str: str) -> SecurityOptions:
        """
        Convenience method that returns `SecurityOptions` instance with `trust_only_pem_str` set to provided certificate(s) str.

        Args:
            pem_str (str): PEM-encoded certificate(s) the SDK should trust.

        Returns:
            :class:`~couchbase_columnar.common.options.SecurityOptions`
        """  # noqa: E501
        return cls(trust_only_pem_str=pem_str)

    @classmethod
    def trust_only_certificates(cls, certificates: List[str]) -> SecurityOptions:
        """
        Convenience method that returns `SecurityOptions` instance with `trust_only_certificates` set to provided certificates.

        Args:
            trust_only_certificates (List[str]): List of PEM-encoded certificate(s) the SDK should trust.

        Returns:
            :class:`~couchbase_columnar.common.options.SecurityOptions`
        """  # noqa: E501
        return cls(trust_only_certificates=certificates)

    @classmethod
    def trust_only_platform(cls) -> SecurityOptions:
        """
        Convenience method that returns `SecurityOptions` instance with `trust_only_platform=True`.

        Returns:
            :class:`~couchbase_columnar.common.options.SecurityOptions`
        """
        return cls(trust_only_platform=True)


class TimeoutOptions(TimeoutOptionsBase):
    """Available timeout options to set when creating a cluster.

    These options set the default timeouts for operations for the cluster.  Some operations allow the timeout to be overridden on a per operation basis.
    All options are optional and default to `None`. Values in parenthesis indicate C++ core default that will be used if the option is not set.

    Args:
        connect_timeout (timedelta, optional): Set to configure the period of time allowed to complete bootstrap connection. Defaults to `None` (10s).
        dispatch_timeout (timedelta, optional): Set to configure the period of time allowed to complete HTTP connection prior to sending request. Defaults to `None` (30s).
        dns_srv_timeout (timedelta, optional): Set to configure the period of time allowed to complete DNS SRV query. Defaults to `None` (500ms).
        management_timeout (timedelta, optional): **VOLATILE** Set to configure the period of time allowed for management operations. Defaults to `None` (75s).
        query_timeout (timedelta, optional): Set to configure the period of time allowed for query operations. Defaults to `None` (10m).
        resolve_timeout (timedelta, optional): Set to configure the period of time allowed to complete resolve hostname of node to IP address. Defaults to `None` (2s).
        socket_connect_timeout (timedelta, optional): Set to configure the period of time allowed to complete creating socket connection to resolved IP. Defaults to `None` (2s).
    """  # noqa: E501


class TracingOptions(TracingOptionsBase):
    """Available tracing options to set when creating a cluster.

    These options set the default interval/size/threshold for tracing and orphaned operations.
    All options are optional and default to `None`. Values in parenthesis indicate C++ core default that will be used if the option is not set.

    Args:
        tracing_orphaned_queue_flush_interval (timedelta, optional): Set to configure the interveral to flush the orphaned operations queue. Defaults to `None` (10s).
        tracing_orphaned_queue_size (int, optional): Set to configure size of the orphaned operations queue. Defaults to `None` (64).
        tracing_threshold_analytics (timedelta, optional): Set to configure analytics operations threshold. Defaults to `None` (1s).
        tracing_threshold_management (timedelta, optional): Set to configure management operations threshold. Defaults to `None` (1s).
        tracing_threshold_queue_flush_interval (timedelta, optional): Set to configure the interveral to flush tracing operations queue. Defaults to `None` (10s).
        tracing_threshold_queue_size (int, optional): Set to configure the size of tracing operations queue. Defaults to `None` (64).
    """  # noqa: E501


class QueryOptions(QueryOptionsBase):
    """Available options for columnar query operation.

    Timeout will default to cluster setting if not set for the operation.

    Args:
        cancel_token (:class:~`threaad.Event`, optional): None
        cancel_poll_interval (float, optional): None
        deserializer (Deserializer, optional): None
        lazy_execute: (bool, optional): None
        named_parameters (Dict[str, JSONType], optional): None
        positional_parameters (Iterable[JSONType], optional): None
        priority (bool, optional): None
        query_context (str, optional): None
        raw (Dict[str, Any], optional): None
        read_only (bool, optional): None
        scan_consistency (QueryScanConsistency, optional): None
        timeout (timedelta, optional): Set to configure allowed time for operation to complete. Defaults to `None` (75s).
    """  # noqa: E501


OptionsClass: TypeAlias = Union[
    ClusterOptions,
    SecurityOptions,
    TimeoutOptions,
    TracingOptions,
    QueryOptions,
]
