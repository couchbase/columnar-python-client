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

from typing import (Dict,
                    Optional,
                    Union,
                    cast)

"""

Error Classes

"""


class ColumnarError(Exception):
    """
    Generic base error.  Columnar specific errors inherit from this base error.
    """

    def __init__(self, base: Optional[Exception] = None, message: Optional[str] = None) -> None:
        self._base = base
        self._message = message
        super().__init__(message)

    def __repr__(self) -> str:
        details: Dict[str, str] = {}
        if self._base is not None:
            details['base'] = self._base.__repr__()

        if self._message is not None and not self._message.isspace():
            details['message'] = self._message

        if details:
            return f'{type(self).__name__}({details})'
        return f'{type(self).__name__}()'

    def __str__(self) -> str:
        return self.__repr__()


class InvalidCredentialError(ColumnarError):
    """
    Indicates that an error occurred authenticating the user to the cluster.
    """

    def __init__(self, base: Optional[Exception] = None, message: Optional[str] = None) -> None:
        super().__init__(base, message)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()


class QueryError(ColumnarError):
    """
    Indicates that an query request received an error from the Columnar server.
    """

    def __init__(self, base: Optional[Exception] = None, message: Optional[str] = None) -> None:
        super().__init__(base, message)
        self._code = 0
        self._server_message = ''
        if self._base and hasattr(self._base, 'error_properties'):
            props: Optional[Dict[str, Union[int, str]]] = self._base.error_properties
            if props is not None:
                self._code = cast(int, props['code'])
                self._server_message = cast(str, props['server_message'])

    @property
    def code(self) -> int:
        """
        Returns:
            Error code from Columnar server
        """
        return self._code

    @property
    def server_message(self) -> str:
        """
        Returns:
            Error message from Columnar server
        """
        return self._server_message

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()


class TimeoutError(ColumnarError):
    """
    Indicates that a request was unable to complete prior to reaching the deadline specified for the reqest.
    """

    def __init__(self, base: Optional[Exception] = None, message: Optional[str] = None) -> None:
        super().__init__(base, message)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()


class FeatureUnavailableError(Exception):
    """
    Raised when feature that is not available with the current server version is used.
    """

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()


class InternalSDKError(Exception):
    """
    This means the SDK has done something wrong. Get support.
    (this doesn't mean *you* didn't do anything wrong, it does mean you should not be seeing this message)
    """

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()


class QueryOperationCanceledError(Exception):
    """
    **INTERNAL**
    """

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()
