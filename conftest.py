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

from typing import List

import pytest

pytest_plugins = [
    'tests.columnar_config',
    'tests.environments.base_environment'
]

_UNIT_TESTS = [
    'couchbase_columnar/tests/connection_t.py::ConnectionTests'
]

_INTEGRATRION_TESTS = [
    'couchbase_columnar/tests/query_t.py::QueryTests'
]

# https://docs.pytest.org/en/7.4.x/reference/reference.html#pytest.hookspec.pytest_collection_modifyitems


def pytest_collection_modifyitems(session: pytest.Session,
                                  config: pytest.Config,
                                  items: List[pytest.Item]) -> None:  # noqa: C901
    for item in items:
        item_details = item.nodeid.split('::')

        item_api = item_details[0].split('/')
        if item_api[0] == 'couchbase_columnar':
            item.add_marker(pytest.mark.pycbcc_couchbase)
        elif item_api[0] == 'acouchbase_columnar':
            item.add_marker(pytest.mark.pycbcc_acouchbase)

        test_class_path = '::'.join(item_details[:-1])
        if test_class_path in _UNIT_TESTS:
            item.add_marker(pytest.mark.pycbcc_unit)
        elif test_class_path in _INTEGRATRION_TESTS:
            item.add_marker(pytest.mark.pycbcc_integration)
