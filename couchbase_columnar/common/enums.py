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

from enum import Enum
from typing import Union


class QueryScanConsistency(Enum):
    """
    Represents the various scan consistency options that are available.
    """

    NOT_BOUNDED = 'not_bounded'
    REQUEST_PLUS = 'request_plus'


# This is unfortunate, but Enum is 'special' and this is one of the least invasive manners to document the members
QueryScanConsistency.NOT_BOUNDED.__doc__ = ('Indicates that no specific consistency is required, '
                                            'this is the fastest options, but results may not include '
                                            'the most recent operations which have been performed.')
QueryScanConsistency.REQUEST_PLUS.__doc__ = ('Indicates that the results to the query should include '
                                             'all operations that have occurred up until the query was started. '
                                             'This incurs a performance penalty of waiting for the index to catch '
                                             'up to the most recent operations, but provides the highest level '
                                             'of consistency.')


class IpProtocol(Enum):
    """
    Represents the various IP protocol options that are available when resolving hostnames during the bootstrap and HTTP connection process.
    """  # noqa: E501

    Any = 'any'
    ForceIPv4 = 'force_ipv4'
    ForceIPv6 = 'force_ipv6'

    @classmethod
    def from_str(cls, value: str) -> IpProtocol:
        if isinstance(value, str):
            if value == cls.Any.value:
                return cls.Any
            elif value == cls.ForceIPv4.value:
                return cls.ForceIPv4
            elif value == cls.ForceIPv6.value:
                return cls.ForceIPv6

        raise ValueError((f"{value} is not a valid IpProtocol option. "
                          "Excepted str representation of type IpProtocol."))

    @classmethod
    def to_str(cls, value: Union[IpProtocol, str]) -> str:
        if isinstance(value, IpProtocol):
            return value.value
        if isinstance(value, str):
            if value == cls.Any.value:
                return cls.Any.value
            elif value == cls.ForceIPv4.value:
                return cls.ForceIPv4.value
            elif value == cls.ForceIPv6.value:
                return cls.ForceIPv6.value

        raise ValueError((f"{value} is not a valid IpProtocol option. "
                          "Excepted IP Protocol mode to be either of type "
                          "IpProtocol or str representation "
                          "of IpProtocol."))


IpProtocol.Any.__doc__ = 'Indicates that any IP protocol can be used.'
IpProtocol.ForceIPv4.__doc__ = 'Indicates that IPv4 must be used.'
IpProtocol.ForceIPv6.__doc__ = 'Indicates that IPv6 must be used.'
