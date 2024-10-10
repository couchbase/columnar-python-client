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

from datetime import timedelta
from time import sleep
from typing import TYPE_CHECKING

import pytest

from couchbase_columnar.credential import Credential
from couchbase_columnar.errors import (InvalidCredentialError,
                                       QueryError,
                                       TimeoutError)
from couchbase_columnar.options import QueryOptions
from couchbase_columnar.protocol.errors import CoreColumnarError
from tests import YieldFixture

if TYPE_CHECKING:
    from tests.environments.base_environment import BlockingTestEnvironment


class ErrorTestSuite:
    TEST_MANIFEST = [
        'test_timeout_error',
        'test_query_error',
        'test_invalid_credential_error',
    ]

    def test_invalid_credential_error(self, test_env: BlockingTestEnvironment) -> None:
        statement = 'FROM range(0, 100000) AS r SELECT *'
        cluster = test_env.create_new_cluster_instance(credential=Credential.from_username_and_password('Admin', 'pw'))
        # Need to sleep so we don't seen the query op prior to receiving init errors.
        # WIP on C++ core to not have to wait.
        sleep(1)
        with pytest.raises(InvalidCredentialError):
            cluster.execute_query(statement)

    def test_timeout_error(self, test_env: BlockingTestEnvironment) -> None:
        statement = 'FROM range(0, 10000000) AS r SELECT *'
        with pytest.raises(TimeoutError):
            test_env.cluster.execute_query(statement, QueryOptions(timeout=timedelta(seconds=1)))

    def test_query_error(self, test_env: BlockingTestEnvironment) -> None:
        statement = "I'm not N1QL!"
        caught_err = None
        try:
            test_env.cluster.execute_query(statement, QueryOptions(timeout=timedelta(seconds=1)))
        except Exception as err:
            caught_err = err

        assert isinstance(caught_err, QueryError)
        assert isinstance(caught_err.code, int)
        assert caught_err.code > 0
        assert isinstance(caught_err.server_message, str)
        assert caught_err.server_message != ''
        # ** INTERNAL**
        assert caught_err._base is not None
        assert isinstance(caught_err._base, CoreColumnarError)
        err_details = caught_err._base.error_details
        assert isinstance(err_details, dict)
        assert 'core_error_code' in err_details
        assert 'message' in err_details
        assert 'properties' in err_details
        assert 'code' in err_details['properties']
        assert 'server_message' in err_details['properties']
        assert 'context' in err_details
        assert 'file' in err_details
        assert 'line' in err_details


class ErrorTests(ErrorTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(ErrorTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ErrorTests) if valid_test_method(meth)]
        test_list = set(ErrorTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='test_env')
    def couchbase_test_environment(self,
                                   sync_test_env: BlockingTestEnvironment) -> YieldFixture[BlockingTestEnvironment]:
        yield sync_test_env
