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

from enum import IntEnum

import pytest

from couchbase_columnar.errors import (ColumnarError,
                                       InternalSDKError,
                                       InvalidCredentialError,
                                       QueryError,
                                       TimeoutError)
from couchbase_columnar.protocol.errors import CoreColumnarError, ErrorMapper
from couchbase_columnar.protocol.pycbcc_core import _test_exception_builder, core_client_error_code


class CppCoreErrorCode(IntEnum):
    GENERIC = 1
    INVALID_CREDENTIAL = 2
    TIMEOUT = 3
    QUERY_ERROR = 4


class BindingErrorTestSuite:
    TEST_MANIFEST = [
        'test_binding_core_error',
        'test_binding_client_error',
        'test_binding_error_non_cpp_with_inner',
    ]

    @pytest.mark.parametrize('core_error_code, expected_err',
                             [(CppCoreErrorCode.GENERIC, ColumnarError),
                              (CppCoreErrorCode.INVALID_CREDENTIAL, InvalidCredentialError),
                              (CppCoreErrorCode.TIMEOUT, TimeoutError),
                              (CppCoreErrorCode.QUERY_ERROR, QueryError)])
    def test_binding_core_error(self, core_error_code: CppCoreErrorCode, expected_err: type[Exception]) -> None:
        err = _test_exception_builder(core_error_code.value, True)
        assert isinstance(err, CoreColumnarError)
        assert err.error_details is not None
        assert isinstance(err.error_details, dict)
        error_code = err.error_details.get('core_error_code', None)
        assert error_code is not None
        assert error_code == core_error_code.value
        built_err = ErrorMapper.build_error(err)
        assert isinstance(built_err, expected_err)

    @pytest.mark.parametrize('client_error_code, expected_err',
                             [(core_client_error_code.VALUE, ValueError),
                              (core_client_error_code.RUNTIME, RuntimeError),
                              (core_client_error_code.INTERNAL_SDK, InternalSDKError)])
    def test_binding_client_error(self,
                                  client_error_code: core_client_error_code,
                                  expected_err: type[Exception]) -> None:
        err = _test_exception_builder(client_error_code.value)
        assert isinstance(err, CoreColumnarError)
        assert err.error_details is not None
        assert isinstance(err.error_details, dict)
        error_code = err.error_details.get('client_error_code', None)
        assert error_code is not None
        assert error_code == client_error_code.value
        built_err = ErrorMapper.build_error(err)
        assert isinstance(built_err, expected_err)

    @pytest.mark.parametrize('client_error_code, expected_err',
                             [(core_client_error_code.VALUE, ValueError),
                              (core_client_error_code.RUNTIME, RuntimeError),
                              (core_client_error_code.INTERNAL_SDK, InternalSDKError)])
    def test_binding_error_non_cpp_with_inner(self,
                                              client_error_code: core_client_error_code,
                                              expected_err: type[Exception]) -> None:
        err = _test_exception_builder(client_error_code.value, False, True)
        assert isinstance(err, CoreColumnarError)
        assert err.error_details is not None
        assert isinstance(err.error_details, dict)
        error_code = err.error_details.get('client_error_code', None)
        assert error_code is not None
        assert error_code == client_error_code.value
        inner_cause = err.error_details.get('inner_cause', None)
        assert inner_cause is not None
        assert isinstance(inner_cause, RuntimeError)
        built_err = ErrorMapper.build_error(err)
        assert isinstance(built_err, expected_err)


class BindingErrorTests(BindingErrorTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(BindingErrorTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(BindingErrorTests) if valid_test_method(meth)]
        test_list = set(BindingErrorTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')
