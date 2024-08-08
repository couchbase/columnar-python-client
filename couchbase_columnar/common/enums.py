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

from enum import Enum
from typing import Union

from couchbase_columnar.common.exceptions import InvalidArgumentException


class QueryScanConsistency(Enum):
    """
    will allow cached values to be returned. This will improve performance but may not
    reflect the latest data in the server.
    """
    NOT_BOUNDED = "not_bounded"
    REQUEST_PLUS = "request_plus"


class IpProtocol(Enum):
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

        raise InvalidArgumentException(message=(f"{value} is not a valid IpProtocol option. "
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

        raise InvalidArgumentException(message=(f"{value} is not a valid IpProtocol option. "
                                                "Excepted IP Protocol mode to be either of type "
                                                "IpProtocol or str representation "
                                                "of IpProtocol."))


class KnownConfigProfiles(Enum):
    """
    **VOLATILE** This API is subject to change at any time.

    Represents the name of a specific configuration profile that is associated with predetermined cluster options.

    """
    WanDevelopment = 'wan_development'

    @classmethod
    def from_str(cls, value: str) -> KnownConfigProfiles:
        if isinstance(value, str):
            if value == cls.WanDevelopment.value:
                return cls.WanDevelopment

        raise InvalidArgumentException(message=(f"{value} is not a valid KnownConfigProfiles option. "
                                                "Excepted str representation of type KnownConfigProfiles."))

    @classmethod
    def to_str(cls, value: Union[KnownConfigProfiles, str]) -> str:
        if isinstance(value, KnownConfigProfiles):
            return value.value

        # just retun the str to allow for future customer config profiles
        if isinstance(value, str):
            return value

        raise InvalidArgumentException(message=(f"{value} is not a valid KnownConfigProfiles option. "
                                                "Excepted config profile to be either of type "
                                                "KnownConfigProfiles or str representation "
                                                "of KnownConfigProfiles."))


class TLSVerifyMode(Enum):
    NONE = 'none'
    PEER = 'peer'
    NO_VERIFY = 'no_verify'

    @classmethod
    def from_str(cls, value: str) -> TLSVerifyMode:
        if isinstance(value, str):
            if value == cls.NONE.value:
                return cls.NONE
            elif value == cls.PEER.value:
                return cls.PEER
            elif value == cls.NO_VERIFY.value:
                return cls.NONE

        raise InvalidArgumentException(message=(f"{value} is not a valid TLSVerifyMode option. "
                                                "Excepted str representation of type TLSVerifyMode."))

    @classmethod
    def to_str(cls, value: Union[TLSVerifyMode, str]) -> str:
        if isinstance(value, TLSVerifyMode):
            if value == cls.NO_VERIFY:
                return cls.NONE.value
            return value.value  # type: ignore[no-any-return]
        if isinstance(value, str):
            if value == cls.NONE.value:
                return cls.NONE.value
            elif value == cls.PEER.value:
                return cls.PEER.value
            elif value == cls.NO_VERIFY.value:
                return cls.NONE.value

        raise InvalidArgumentException(message=(f"{value} is not a valid TLSVerifyMode option. "
                                                "Excepted TLS verify mode to be either of type "
                                                "TLSVerifyMode or str representation "
                                                "of TLSVerifyMode."))
