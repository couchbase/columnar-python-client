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

from typing import Callable, Dict


class Credential:
    """Create a Credential instance.

    A Credential is required in order to connect to a Capalla Columnar server.

    .. important::
        Use the the provided classmethods to create a :class:`.Credential` instance.

    """

    def __init__(self, **kwargs: str) -> None:
        username = kwargs.pop('username', None)
        password = kwargs.pop('password', None)

        if username is None:
            raise ValueError('Must provide a username.')
        if not isinstance(username, str):
            raise ValueError('The username must be a str.')

        if password is None:
            raise ValueError('Must provide a password.')
        if not isinstance(password, str):
            raise ValueError('The password must be a str.')

        self._username = username
        self._password = password

    def asdict(self) -> Dict[str, str]:
        """
        **INTERNAL**
        """
        return {
            'username': self._username,
            'password': self._password
        }

    @classmethod
    def from_username_and_password(cls, username: str, password: str) -> Credential:
        """Create a :class:`.Credential` from a username and password.

        Args:
            username: The username for the Capalla Columnar cluster.
            password: The password for the Capalla Columnar cluster.

        Returns:
            A Credential instance.
        """
        return Credential(username=username, password=password)

    @classmethod
    def from_callable(cls, callback: Callable[[], Credential]) -> Credential:
        """Create a :class:`.Credential` from provided callback.

        The callback is

        Args:
            callback: Callback that returns a :class:`.Credential`.

        Returns:
            A Credential instance.

        Example:
            Retrieve credentials from environment variables::

                def _cred_from_env() -> Credential:
                    from os import getenv
                    return Credential.from_username_and_password(getenv('PYCBCC_USERNAME'),
                                                                 getenv('PYCBCC_PW'))

                cred = Credential.from_callable(_cred_from_env)

        """
        return Credential(**callback().asdict())
