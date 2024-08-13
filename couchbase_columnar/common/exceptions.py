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
from abc import ABC, abstractmethod
from typing import (Any,
                    Dict,
                    Optional,
                    Set,
                    Tuple,
                    TypedDict)

from typing_extensions import Unpack

from couchbase_columnar.common.core.exception import (ErrorContextCore,
                                                      GenericErrorContextCore,
                                                      HTTPErrorContextCore)

"""

Error Context Classes

"""


class ErrorContext(Dict[str, Any]):
    _EC_KEYS = ['cinfo',
                'error_message',
                'last_dispatched_from',
                'last_dispatched_to',
                'retry_attempts',
                'retry_reasons',
                ]

    def __init__(self,
                 **kwargs: Unpack[GenericErrorContextCore]
                 ) -> None:
        self._err_ctx: ErrorContextCore = {}
        # mypy does not understand the dict comprehension :/
        for k, v in kwargs.items():
            if k in self._EC_KEYS:
                self._err_ctx[k] = v  # type: ignore
        super().__init__(**kwargs)

    @property
    def cinfo(self) -> Optional[Tuple[str, int]]:
        return self._err_ctx.get('cinfo', None)

    @property
    def error_message(self) -> Optional[str]:
        return self._err_ctx.get('error_message', None)

    @property
    def last_dispatched_to(self) -> Optional[str]:
        return self._err_ctx.get('last_dispatched_to', None)

    @property
    def last_dispatched_from(self) -> Optional[str]:
        return self._err_ctx.get('last_dispatched_from', None)

    @property
    def retry_attempts(self) -> Optional[int]:
        return self._err_ctx.get('retry_attempts', None)

    @property
    def retry_reasons(self) -> Optional[Set[str]]:
        return self._err_ctx.get('retry_reasons', None)

    @staticmethod
    def from_dict(**kwargs: Unpack[ErrorContextCore]) -> ErrorContext:
        ctx_class = kwargs.get('context_type', None) or 'ErrorContext'
        cls = getattr(sys.modules[__name__], ctx_class)
        err_ctx: ErrorContext = cls(**kwargs)
        return err_ctx

    def _get_base(self) -> ErrorContextCore:
        """**INTERNAL**"""
        return self._err_ctx

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._err_ctx})'

    def __str__(self) -> str:
        return self.__repr__()


class HTTPErrorContext(ErrorContext):
    _HTTP_EC_KEYS = ['client_context_id', 'method', 'path', 'http_status', 'http_body']

    def __init__(self, **kwargs: Unpack[GenericErrorContextCore]) -> None:
        self._http_err_ctx: HTTPErrorContextCore = {}
        # mypy does not understand the dict comprehension :/
        for k, v in kwargs.items():
            if k in self._HTTP_EC_KEYS:
                self._http_err_ctx[k] = v  # type: ignore
        super().__init__(**kwargs)

    @property
    def method(self) -> Optional[str]:
        return self._http_err_ctx.get('method', None)

    @property
    def response_code(self) -> Optional[int]:
        return self._http_err_ctx.get('http_status', None)

    @property
    def path(self) -> Optional[str]:
        return self._http_err_ctx.get('path', None)

    @property
    def response_body(self) -> Optional[str]:
        return self._http_err_ctx.get('http_body', None)

    @property
    def client_context_id(self) -> Optional[str]:
        return self._http_err_ctx.get('client_context_id', None)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()


"""

Exception Classes

"""


class ColumnarExceptionKwargs(TypedDict, total=False):
    message: Optional[str]
    base: Optional[AbstractBaseColumnarException]


class AbstractBaseColumnarException(ABC):

    @property
    @abstractmethod
    def error_code(self) -> Optional[int]:
        raise NotImplementedError

    @property
    @abstractmethod
    def error_context(self) -> Optional[str]:
        raise NotImplementedError

    @property
    def inner_cause(self) -> Optional[Exception]:
        raise NotImplementedError

    @property
    def message(self) -> Optional[str]:
        raise NotImplementedError

    def __repr__(self) -> str:
        raise NotImplementedError

    def __str__(self) -> str:
        raise NotImplementedError


class ColumnarException(Exception):
    def __init__(self,
                 base: Optional[AbstractBaseColumnarException] = None,
                 message: Optional[str] = None
                 ):
        self._base = base
        self._context = self._base.error_context if self._base else None
        self._message = message
        if self._message is None and self._base is not None:
            self._message = self._base.message
        super().__init__(message)

    @property
    def error_code(self) -> Optional[int]:
        """
        **VOLATILE** This API is subject to change at any time.

        Returns:
            Optional[int]: Exception's error code, if it exists.
        """
        if self._base:
            return self._base.error_code

        return None

    @property
    def error_context(self) -> Optional[str]:
        return self._context

    @property
    def message(self) -> Optional[str]:
        """
        **VOLATILE** This API is subject to change at any time.

        Returns:
            Optional[str]: Exception's error message, if it exists.
        """
        if self._base:
            return self._base.message

        return None

    @property
    def inner_cause(self) -> Optional[Exception]:
        """
        **VOLATILE** This API is subject to change at any time.

        Returns:
            Optional[Exception]: Exception's inner cause, if it exists.
        """
        if self._base:
            return self._base.inner_cause

        return None

    @classmethod
    def from_message(cls, message: str) -> ColumnarException:
        return ColumnarException(None, message)

    def __repr__(self) -> str:
        if self._base:
            return self._base.__repr__()

        # TODO: Provide actual information
        return 'ColumnarException()'

    def __str__(self) -> str:
        return self.__repr__()

# TODO:  replace?


class InternalServerFailureException(ColumnarException):
    """ Raised when the server service provides error w/in specific ranges.
    """

    def __init__(self,
                 msg: Optional[str] = None,
                 **kwargs: Unpack[ColumnarExceptionKwargs]) -> None:
        if msg and isinstance(msg, str) and 'message' not in kwargs:
            kwargs['message'] = msg
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()


class TimeoutException(ColumnarException):
    def __init__(self,
                 msg: Optional[str] = None,
                 **kwargs: Unpack[ColumnarExceptionKwargs]) -> None:
        if msg and isinstance(msg, str) and 'message' not in kwargs:
            kwargs['message'] = msg
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()


class QueryException(ColumnarException):
    def __init__(self,
                 msg: Optional[str] = None,
                 **kwargs: Unpack[ColumnarExceptionKwargs]) -> None:
        if msg and isinstance(msg, str) and 'message' not in kwargs:
            kwargs['message'] = msg
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()

# TODO:  replace?


class InvalidArgumentException(ColumnarException):
    """ Raised when a provided argmument has an invalid value and/or invalid type.
    """

    def __init__(self,
                 msg: Optional[str] = None,
                 **kwargs: Unpack[ColumnarExceptionKwargs]) -> None:
        if msg and isinstance(msg, str) and 'message' not in kwargs:
            kwargs['message'] = msg
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()


class InvalidCredentialException(ColumnarException):
    """Indicates that an error occurred authenticating the user to the cluster."""

    def __init__(self,
                 msg: Optional[str] = None,
                 **kwargs: Unpack[ColumnarExceptionKwargs]) -> None:
        if msg and isinstance(msg, str) and 'message' not in kwargs:
            kwargs['message'] = msg
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()


class ServiceUnavailableException(ColumnarException):
    """ Raised if tt can be determined from the config unambiguously that a
        given service is not available.
        I.e. no query node in the config, or a memcached bucket is accessed
        and views or n1ql queries should be performed
    """

    def __init__(self,
                 msg: Optional[str] = None,
                 **kwargs: Unpack[ColumnarExceptionKwargs]) -> None:
        if msg and isinstance(msg, str) and 'message' not in kwargs:
            kwargs['message'] = msg
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()


class FeatureUnavailableException(ColumnarException):
    """Raised when feature that is not available with the current server version is used."""

    def __init__(self,
                 msg: Optional[str] = None,
                 **kwargs: Unpack[ColumnarExceptionKwargs]) -> None:
        if msg and isinstance(msg, str) and 'message' not in kwargs:
            kwargs['message'] = msg
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()

# TODO:  replace?


class InternalSDKException(ColumnarException):
    """
    This means the SDK has done something wrong. Get support.
    (this doesn't mean *you* didn't do anything wrong, it does mean you should
    not be seeing this message)
    """

    def __init__(self,
                 msg: Optional[str] = None,
                 **kwargs: Unpack[ColumnarExceptionKwargs]) -> None:
        if msg and isinstance(msg, str) and 'message' not in kwargs:
            kwargs['message'] = msg
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()

# TODO:  replace?


class AlreadyQueriedException(ColumnarException):
    """
    Raised when query (N1QL, Search, Analytics or Views) results
    have already been iterated over.
    """

    def __init__(self,
                 msg: Optional[str] = None,
                 **kwargs: Unpack[ColumnarExceptionKwargs]) -> None:
        if msg is None and 'message' not in kwargs:
            kwargs['message'] = 'Previously iterated over results.'
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()


# TODO:  replace?
class UnsuccessfulOperationException(ColumnarException):
    """Thrown when a specific pycbcc_core operation is unsuccessful."""

    def __init__(self,
                 msg: Optional[str] = None,
                 **kwargs: Unpack[ColumnarExceptionKwargs]) -> None:
        if msg and isinstance(msg, str) and 'message' not in kwargs:
            kwargs['message'] = msg
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()


class QueryOperationCanceledException(ColumnarException):
    """
    Raised when query (N1QL, Search, Analytics or Views) results
    have already been iterated over.
    """

    def __init__(self,
                 msg: Optional[str] = None,
                 **kwargs: Unpack[ColumnarExceptionKwargs]) -> None:
        if msg is None and 'message' not in kwargs:
            kwargs['message'] = 'Previously iterated over results.'
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()
