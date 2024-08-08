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

import os
import pathlib
from configparser import ConfigParser
from typing import Tuple

import pytest

from tests import ColumnarTestEnvironmentException

BASEDIR = pathlib.Path(__file__).parent.parent
CONFIG_FILE = os.path.join(pathlib.Path(__file__).parent, "test_config.ini")
ENV_TRUE = ['true', '1', 'y', 'yes', 'on']


class ColumnarConfig:
    def __init__(self) -> None:
        self._scheme = 'couchbase'
        self._host = 'localhost'
        self._port = 8091
        self._username = 'Administrator'
        self._password = 'password'
        self._nonprod = False
        self._database_name = 'travel-sample'
        self._scope_name = 'inventory'
        self._collection_name = 'airline'
        self._tls_verify = True

    @property
    def database_name(self) -> str:
        return self._database_name

    @property
    def collection_name(self) -> str:
        return self._collection_name

    @property
    def fqdn(self) -> str:
        return f'`{self._database_name}`.`{self._scope_name}`.`{self._collection_name}`'

    @property
    def nonprod(self) -> bool:
        return self._nonprod

    @property
    def tls_verify(self) -> bool:
        return self._tls_verify

    @property
    def scope_name(self) -> str:
        return self._scope_name

    def get_connection_string(self) -> str:
        return f'{self._scheme}://{self._host}'

    def get_username_and_pw(self) -> Tuple[str, str]:
        return self._username, self._password

    @classmethod
    def load_config(cls) -> ColumnarConfig:
        columnar_config = cls()
        try:
            test_config = ConfigParser()
            test_config.read(CONFIG_FILE)
            test_config_columnar = test_config['columnar']
            columnar_config._scheme = os.environ.get('PYCBCC_SCHEME',
                                                     test_config_columnar.get('scheme', fallback='couchbase'))
            columnar_config._host = os.environ.get('PYCBCC_HOST',
                                                   test_config_columnar.get('host', fallback='localhost'))
            port = os.environ.get('PYCBCC_PORT', test_config_columnar.get('port', fallback='8091'))
            columnar_config._port = int(port)
            columnar_config._username = os.environ.get('PYCBCC_USERNAME',
                                                       test_config_columnar.get('username', fallback='Administrator'))
            columnar_config._password = os.environ.get('PYCBCC_PASSWORD',
                                                       test_config_columnar.get('password', fallback='password'))
            use_nonprod = os.environ.get('PYCBCC_NONPROD', test_config_columnar.get('nonprod', fallback='OFF'))
            if use_nonprod.lower() in ENV_TRUE:
                columnar_config._nonprod = True
            else:
                columnar_config._nonprod = False
            columnar_config._database_name = os.environ.get('PYCBCC_DATABASE',
                                                            test_config_columnar.get('database_name',
                                                                                     fallback='travel-sample'))
            columnar_config._scope_name = os.environ.get('PYCBCC_SCOPE',
                                                         test_config_columnar.get('scope_name', fallback='inventory'))
            columnar_config._collection_name = os.environ.get('PYCBCC_COLLECTION',
                                                              test_config_columnar.get('collection_name',
                                                                                       fallback='airline'))
            tls_verify = os.environ.get('PYCBCC_TLS_VERIFY', test_config_columnar.get('tls_verify', fallback='ON'))
            if tls_verify.lower() not in ENV_TRUE:
                columnar_config._tls_verify = False

        except Exception as ex:
            raise ColumnarTestEnvironmentException(f'Problem trying read/load test configuration:\n{ex}')

        return columnar_config


@pytest.fixture(name='columnar_config', scope='session')
def columnar_test_config() -> ColumnarConfig:
    config = ColumnarConfig.load_config()
    return config
