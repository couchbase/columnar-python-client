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

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Dict,
                    Optional)

from couchbase_columnar.common.enums import KnownConfigProfiles

if TYPE_CHECKING:
    from couchbase_columnar.common.options import ClusterOptions


class ConfigProfile(ABC):
    """
    **VOLATILE** This API is subject to change at any time.

    This is an abstract base class intended to use with creating Configuration Profiles.  Any derived class
    will need to implement the :meth:`apply` method.
    """

    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def apply(self, options: ClusterOptions) -> None:
        """
        **VOLATILE** This API is subject to change at any time.

        Apply the provided options to ClusterOptions. This method will need to be implemented in derived classes.

        Args:
            options (:class:`~couchbase_columnar.options.ClusterOptions`): The options the profile will apply toward.
        """


class WanDevelopmentProfile(ConfigProfile):
    """
    **VOLATILE** This API is subject to change at any time.

    The WAN Development profile sets various timeout options that are useful when develoption in a WAN environment.
    """

    def __init__(self) -> None:
        super().__init__()

    def apply(self, options: ClusterOptions) -> None:
        options['connect_timeout'] = timedelta(seconds=60)
        options['dispatch_timeout'] = timedelta(seconds=120)
        options['dns_srv_timeout'] = timedelta(seconds=20)
        # options['management_timeout'] = timedelta(seconds=120)
        options['query_timeout'] = timedelta(minutes=15)
        options['resolve_timeout'] = timedelta(seconds=20)
        options['socket_connect_timeout'] = timedelta(seconds=20)


class ConfigProfiles():
    """
    **VOLATILE** This API is subject to change at any time.

    The `ConfigProfiles` class is responsible for keeping track of registered/known Configuration
    Profiles.
    """

    def __init__(self) -> None:
        self._profiles: Dict[str, ConfigProfile] = {}
        self.register_profile(KnownConfigProfiles.WanDevelopment.value, WanDevelopmentProfile())

    def apply_profile(self, profile_name: str, options: ClusterOptions) -> None:
        """
        **VOLATILE** This API is subject to change at any time.

        Apply the provided ConfigProfile options.

        Args:
            profile_name (str):  The name of the profile to apply.
            options (:class:`~couchbase_columnar.options.ClusterOptions`): The options to apply the ConfigProfile options
                toward. The ConfigProfile options will override any matching option(s) previously set.

        Raises:
            `ValueError`: If the specified profile is not registered.
        """  # noqa: E501
        if profile_name not in self._profiles:
            raise ValueError(f'{profile_name} is not a registered profile.')

        self._profiles[profile_name].apply(options)

    def register_profile(self, profile_name: str, profile: ConfigProfile) -> None:
        """
        **VOLATILE** This API is subject to change at any time.

        Register a :class:`~couchbase_columnar.options.ConfigProfile`.

        Args:
            profile_name (str):  The name of the :class:`~couchbase_columnar.options.ConfigProfile` to register.
            profile (:class:`~couchbase_columnar.options.ConfigProfile`): The :class:`~couchbase_columnar.options.ConfigProfile` to register.

        Raises:
            `ValueError`: If the specified profile is not derived from :class:`~couchbase_columnar.options.ConfigProfile`.

        """  # noqa: E501
        if not issubclass(profile.__class__, ConfigProfile):
            raise ValueError('A Configuration Profile must be derived from ConfigProfile')

        self._profiles[profile_name] = profile

    def unregister_profile(self, profile_name: str) -> Optional[ConfigProfile]:
        """
        **VOLATILE** This API is subject to change at any time.

        Unregister a :class:`~couchbase_columnar.options.ConfigProfile`.

        Args:
            profile_name (str):  The name of the :class:`~couchbase_columnar.options.ConfigProfile` to unregister.

        Returns
            Optional(:class:`~couchbase_columnar.options.ConfigProfile`): The unregistered :class:`~couchbase_columnar.options.ConfigProfile`
        """  # noqa: E501

        return self._profiles.pop(profile_name, None)


"""
**VOLATILE** The ConfigProfiles API is subject to change at any time.
"""
CONFIG_PROFILES = ConfigProfiles()
