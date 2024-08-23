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
from enum import Enum
from typing import (Any,
                    Dict,
                    Optional,
                    Union,
                    cast)

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

from couchbase_columnar.common.core.utils import is_null_or_empty
from couchbase_columnar.common.exceptions import (ColumnarError,
                                                  InternalSDKError,
                                                  QueryOperationCanceledError)
from couchbase_columnar.protocol.pycbcc_core import core_error, core_errors

CoreError: TypeAlias = core_error
SdkError: TypeAlias = Union[ColumnarError,
                            InternalSDKError,
                            QueryOperationCanceledError,
                            RuntimeError,
                            ValueError]


class CoreColumnarError(Exception):
    """
        **INTERNAL**
    """

    def __init__(self, core_error: Optional[CoreError] = None) -> None:
        super().__init__()
        self._core_error = core_error

    @property
    def error_details(self) -> Optional[Dict[str, Any]]:
        """
            **INTERNAL**
        """
        details = None
        if self._core_error is not None:
            details = self._core_error.error_details()
        return details

    @property
    def error_properties(self) -> Optional[Dict[str, Union[int, str]]]:
        """
            **INTERNAL**
        """
        props = None
        if self.error_details and 'properties' in self.error_details:
            props = self.error_details['properties']
        return props

    def __repr__(self) -> str:  # noqa: C901
        if self.error_details is None:
            return f"{type(self).__name__}()"

        details: Dict[str, str] = {}
        if 'error_code' in self.error_details:
            details['error_code'] = f'{self.error_details["error_code"]}'
        if 'inner_cause' in self.error_details:
            details['inner_cause'] = f'{self.error_details["inner_cause"]}'
        if 'message' in self.error_details and not is_null_or_empty(self.error_details['message']):
            details['message'] = f'{self.error_details["message"]}'
        if 'properties' in self.error_details:
            if 'code' in self.error_details['properties']:
                details['properties.code'] = f'{self.error_details["properties"]["code"]}'
            if 'server_message' in self.error_details['properties']:
                details['properties.server_message'] = f'{self.error_details["properties"]["server_message"]}'
        if 'context' in self.error_details:
            details['context'] = f'{self.error_details["context"]}'
        if 'file' in self.error_details:
            details['bindings.file'] = f'{self.error_details["file"]}'
        if 'line' in self.error_details:
            details['bindings.line'] = f'{self.error_details["line"]}'

        return f'{details}'

    def __str__(self) -> str:
        return self.__repr__()


class ExceptionMap(Enum):
    ColumnarError = 1
    InvalidCredentialError = 2
    TimeoutError = 3
    QueryError = 4
    InternalSDKError = 5000


PYCBCC_ERROR_MAP: Dict[int, type[ColumnarError]] = {
    e.value: getattr(sys.modules['couchbase_columnar.common.exceptions'], e.name) for e in ExceptionMap
}


class ErrorMapper:
    @staticmethod  # noqa: C901
    def build_error(core_error: CoreColumnarError,
                    mapping: Optional[Dict[str, type[ColumnarError]]] = None
                    ) -> SdkError:
        err_class: Optional[type[ColumnarError]] = None
        err_details = core_error.error_details
        if err_details is not None:
            # Handle special case to handle when a query is canceled prior to iterating over rows.
            # The C++ core pending_op callback will return a errc::generic w/
            # message = "The query operation was canceled".
            if ('message' in err_details
               and 'query operation' in err_details['message']
               and 'canceled' in err_details['message']):
                return QueryOperationCanceledError(err_details['message'])
            # Handle C++ core errors
            elif 'error_code' in err_details:
                err_code = err_details['error_code']
                # Handle special case inherited C++ operational auth failure
                if err_code == 6:
                    err_code = 2
                err_class = PYCBCC_ERROR_MAP.get(cast(int, err_code))
            # Handle errors from the CPython bindings
            elif 'error_type' in err_details:
                error_type = cast(int, err_details['error_type'])
                if error_type == core_errors.VALUE.value:
                    return ValueError(err_details.get('message'))
                elif error_type == core_errors.RUNTIME.value:
                    return RuntimeError(err_details.get('message'))
                elif error_type == core_errors.INTERNAL_SDK.value:
                    return InternalSDKError(err_details.get('message'))

        if err_class is None:
            return ColumnarError(base=core_error)

        return err_class(base=core_error)
