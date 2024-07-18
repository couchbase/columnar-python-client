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

import json
import re
import sys
from enum import Enum
from typing import (Any,
                    Dict,
                    Optional,
                    Pattern)

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

from couchbase_columnar.common.core.utils import is_null_or_empty
from couchbase_columnar.common.exceptions import (AbstractBaseColumnarException,
                                                  ColumnarException,
                                                  ErrorContext,
                                                  HTTPErrorContext)
from couchbase_columnar.protocol.pycbcc_core import exception

CoreColumnarException: TypeAlias = exception


class BaseColumnarException(AbstractBaseColumnarException):
    def __init__(self,
                 base: Optional[CoreColumnarException] = None,
                 message: Optional[str] = None,
                 context: Optional[str] = None,
                 error_code: Optional[int] = None,
                 exc_info: Optional[Dict[str, Any]] = None
                 ) -> None:
        self._base = base
        self._context = context
        self._message = message
        self._error_code = error_code
        self._exc_info = exc_info

    @property
    def error_code(self) -> Optional[int]:
        if self._error_code:
            return self._error_code
        if self._base:
            return self._base.err()
        return None

    @property
    def error_context(self) -> Optional[str]:
        # TODO:  need to flesh out bindings to make sure error context is no longer a dict
        # if not self._context:
        #     if self._base:
        #         base_ec = self._base.error_context() or dict()
        #     else:
        #         base_ec = dict()
        #     self._context = ErrorContext.from_dict(**base_ec)
        return self._context

    @property
    def inner_cause(self) -> Optional[Exception]:
        inner_exc: Optional[Exception] = None
        if self._exc_info is not None:
            inner_exc = self._exc_info.get('inner_cause', None)
        return inner_exc

    @property
    def message(self) -> Optional[str]:
        if self._message:
            return self._message
        if self._base is not None:
            message = self._base.strerror()
            if message is not None:
                return message.replace('_', ' ')
        return None

    @staticmethod
    def create_columnar_exception(base: Optional[CoreColumnarException] = None,
                                  message: Optional[str] = None,
                                  context: Optional[str] = None,
                                  error_code: Optional[int] = None,
                                  exc_info: Optional[Dict[str, Any]] = None) -> ColumnarException:
        base_exc = BaseColumnarException(base=base,
                                         message=message,
                                         context=context,
                                         error_code=error_code,
                                         exc_info=exc_info)
        return ColumnarException(base=base_exc)

    def __repr__(self) -> str:
        details = []
        if self._base:
            details.append(f"ec={self._base.err()}, category={self._base.err_category()}")
            if not is_null_or_empty(self._message):
                details.append(f"message={self._message}")
            else:
                details.append(f"message={self._base.strerror()}")
        else:
            if not is_null_or_empty(self._message):
                details.append(f'message={self._message}')
        if self._context:
            details.append(f'context={self._context}')
        if self._exc_info and 'cinfo' in self._exc_info:
            details.append('C Source={0}:{1}'.format(*self._exc_info['cinfo']))
        if self._exc_info and 'inner_cause' in self._exc_info:
            details.append('Inner cause={0}'.format(self._exc_info['inner_cause']))
        return "<{}>".format(", ".join(details))

    def __str__(self) -> str:
        return self.__repr__()


class ExceptionMap(Enum):
    ColumnarException = 1
    InvalidCredentialException = 2
    TimeoutException = 3
    QueryException = 4
    InternalSDKException = 5000
    UnsuccessfulOperationException = 5002


PYCBCC_ERROR_MAP: Dict[int, type[ColumnarException]] = {
    e.value: getattr(sys.modules['couchbase_columnar.common.exceptions'], e.name) for e in ExceptionMap
}


class ErrorMapper:

    @staticmethod
    def _process_mapping(compiled_map: Dict[Pattern[str], type[ColumnarException]],
                         err_content: str
                         ) -> Optional[type[ColumnarException]]:
        matches = None
        for pattern, exc_class in compiled_map.items():
            try:
                matches = pattern.match(err_content)
            except Exception:  # nosec
                pass
            if matches:
                return exc_class

        return None

    @staticmethod  # noqa: C901
    def _parse_http_response_body(compiled_map: Dict[Pattern[str], type[ColumnarException]],  # noqa: C901
                                  response_body: str
                                  ) -> Optional[type[ColumnarException]]:

        err_text = None
        try:
            http_body = json.loads(response_body)
        except json.decoder.JSONDecodeError:
            return None

        if isinstance(http_body, str):
            exc_class = ErrorMapper._process_mapping(compiled_map, http_body)
            if exc_class is not None:
                return exc_class
        elif isinstance(http_body, dict):
            errors = http_body.get('errors', None)
            if errors is not None:
                if isinstance(errors, list):
                    for err in errors:
                        err_text = f"{err.get('code', None)} {err.get('msg', None)}"
                        if err_text:
                            exc_class = ErrorMapper._process_mapping(compiled_map, err_text)
                            if exc_class is not None:
                                return exc_class
                            err_text = None
                elif isinstance(errors, dict):
                    err_text = errors.get('name', None)
        # eventing function mgmt cases
        elif isinstance(http_body, dict) and http_body.get('name', None) is not None:
            name = http_body.get('name', None)
            if name is not None:
                exc = ErrorMapper._process_mapping(compiled_map, http_body.get('name', None))
                if exc is not None:
                    return exc

        if err_text is not None:
            exc_class = ErrorMapper._process_mapping(compiled_map, err_text)
            return exc_class

        return None

    @staticmethod
    def _parse_http_context(err_ctx: HTTPErrorContext,
                            mapping: Optional[Dict[str, type[ColumnarException]]] = None,
                            err_info: Optional[Dict[str, Any]] = None
                            ) -> Optional[type[ColumnarException]]:
        from couchbase_columnar.common.core.utils import is_null_or_empty

        compiled_map: Dict[Pattern[str], type[ColumnarException]] = {}
        if mapping:
            compiled_map = {{str: re.compile}.get(
                type(k), lambda x: x)(k): v for k, v in mapping.items()}  # type: ignore[arg-type]

        exc_msg = err_info.get('error_message', None) if err_info else None
        if not is_null_or_empty(exc_msg):
            exc_class = ErrorMapper._process_mapping(compiled_map, exc_msg)
            if exc_class is not None:
                return exc_class

        if not is_null_or_empty(err_ctx.response_body):
            err_text = err_ctx.response_body
            if err_text is not None:
                exc_class = ErrorMapper._process_mapping(compiled_map, err_text)
            if exc_class is not None:
                return exc_class

            if err_text is not None:
                exc_class = ErrorMapper._parse_http_response_body(compiled_map, err_text)
            if exc_class is not None:
                return exc_class

        return None

    @classmethod  # noqa: C901
    def build_exception(cls,  # noqa: C901
                        base_exc: CoreColumnarException,
                        mapping: Optional[Dict[str, type[ColumnarException]]] = None
                        ) -> ColumnarException:
        exc_class = None
        err_ctx = None
        ctx = base_exc.error_context()
        if ctx is None:
            exc_class = PYCBCC_ERROR_MAP.get(base_exc.err(), ColumnarException)
            err_info = base_exc.error_info()
        else:
            err_ctx = ErrorContext.from_dict(**ctx)
            err_info = base_exc.error_info()

            if isinstance(err_ctx, HTTPErrorContext):
                exc_class = ErrorMapper._parse_http_context(err_ctx, mapping, err_info=err_info)
                if exc_class is not None:
                    base = BaseColumnarException(base=base_exc, exc_info=err_info, context=str(err_ctx))
                    return exc_class(base=base)

            # if isinstance(err_ctx, QueryErrorContext):
            #     if mapping is None:
            #         exc_class = ErrorMapper._parse_http_context(err_ctx, QUERY_ERROR_MAPPING)
            #         if exc_class is not None:
            #             base = BaseColumnarException(base=base_exc, exc_info=err_info, context=err_ctx)
            #             return exc_class(base=base)

        if exc_class is None:
            exc_class = PYCBCC_ERROR_MAP.get(base_exc.err(), ColumnarException)

        base = BaseColumnarException(base=base_exc, exc_info=err_info, context=str(err_ctx))
        return exc_class(base=base)
