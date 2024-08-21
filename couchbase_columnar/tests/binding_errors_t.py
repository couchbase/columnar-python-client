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

from couchbase_columnar.exceptions import (ColumnarError,
                                           InternalSDKError,
                                           InvalidCredentialError,
                                           QueryError,
                                           TimeoutError)
from couchbase_columnar.protocol.exceptions import CoreColumnarError, ErrorMapper
from couchbase_columnar.protocol.pycbcc_core import _test_exception_builder, core_errors


class CppCoreErrorCodes(IntEnum):
    GENERIC = 1
    INVALID_CREDENTIAL = 2
    TIMEOUT = 3
    QUERY_ERROR = 4


class BindingErrorTestSuite:
    TEST_MANIFEST = [
        'test_binding_error_cpp',
        'test_binding_error_non_cpp',
        'test_binding_error_non_cpp_with_inner',
    ]

    @pytest.mark.parametrize('error_type, expected_err',
                             [(CppCoreErrorCodes.GENERIC, ColumnarError),
                              (CppCoreErrorCodes.INVALID_CREDENTIAL, InvalidCredentialError),
                              (CppCoreErrorCodes.TIMEOUT, TimeoutError),
                              (CppCoreErrorCodes.QUERY_ERROR, QueryError)])
    def test_binding_error_cpp(self, error_type: CppCoreErrorCodes, expected_err: type[Exception]) -> None:
        err = _test_exception_builder(error_type.value, True)
        assert isinstance(err, CoreColumnarError)
        assert err.error_details is not None
        assert isinstance(err.error_details, dict)
        error_code = err.error_details.get('error_code', None)
        assert error_code is not None
        assert error_code == error_type.value
        built_err = ErrorMapper.build_error(err)
        assert isinstance(built_err, expected_err)

    @pytest.mark.parametrize('error_type, expected_err',
                             [(core_errors.VALUE, ValueError),
                              (core_errors.RUNTIME, RuntimeError),
                              (core_errors.INTERNAL_SDK, InternalSDKError)])
    def test_binding_error_non_cpp(self, error_type: core_errors, expected_err: type[Exception]) -> None:
        err = _test_exception_builder(error_type.value)
        assert isinstance(err, CoreColumnarError)
        assert err.error_details is not None
        assert isinstance(err.error_details, dict)
        err_type = err.error_details.get('error_type', None)
        assert err_type is not None
        assert err_type == error_type.value
        built_err = ErrorMapper.build_error(err)
        assert isinstance(built_err, expected_err)

    @pytest.mark.parametrize('error_type, expected_err',
                             [(core_errors.VALUE, ValueError),
                              (core_errors.RUNTIME, RuntimeError),
                              (core_errors.INTERNAL_SDK, InternalSDKError)])
    def test_binding_error_non_cpp_with_inner(self, error_type: core_errors, expected_err: type[Exception]) -> None:
        err = _test_exception_builder(error_type.value, False, True)
        assert isinstance(err, CoreColumnarError)
        assert err.error_details is not None
        assert isinstance(err.error_details, dict)
        err_type = err.error_details.get('error_type', None)
        assert err_type is not None
        assert err_type == error_type.value
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
