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

import os
import pathlib
from configparser import ConfigParser
from typing import Tuple
from uuid import uuid4

import pytest

from tests import ColumnarTestEnvironmentError

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
        self._database_name = ''
        self._scope_name = ''
        self._collection_name = ''
        self._disable_server_certificate_verification = False
        self._create_keyspace = True

    @property
    def database_name(self) -> str:
        return self._database_name

    @property
    def collection_name(self) -> str:
        return self._collection_name

    @property
    def create_keyspace(self) -> bool:
        return self._create_keyspace

    @property
    def fqdn(self) -> str:
        return f'`{self._database_name}`.`{self._scope_name}`.`{self._collection_name}`'

    @property
    def nonprod(self) -> bool:
        return self._nonprod

    @property
    def disable_server_certificate_verification(self) -> bool:
        return self._disable_server_certificate_verification

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
                                                     test_config_columnar.get('scheme', fallback='couchbases'))
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
            disable_cert_verification = os.environ.get('PYCBCC_DISABLE_SERVER_CERT_VERIFICATION',
                                                       test_config_columnar.get('disable_server_cert_verification',
                                                                                fallback='ON'))
            if disable_cert_verification.lower() in ENV_TRUE:
                columnar_config._disable_server_certificate_verification = True
            fqdn = os.environ.get('PYCBCC_FQDN', test_config_columnar.get('fqdn', fallback=None))
            if fqdn is not None:
                fqdn_tokens = fqdn.split('.')
                if len(fqdn_tokens) != 3:
                    raise ColumnarTestEnvironmentError(('Invalid FQDN provided. Expected database.scope.collection. '
                                                        f'FQDN provide={fqdn}'))

                columnar_config._database_name = f'{fqdn_tokens[0]}'
                columnar_config._scope_name = f'{fqdn_tokens[1]}'
                columnar_config._collection_name = f'{fqdn_tokens[2]}'
                columnar_config._create_keyspace = False
            else:
                # lets make the database unique (enough)
                columnar_config._database_name = f'travel-sample-{str(uuid4())[:8]}'
                columnar_config._scope_name = 'inventory'
                columnar_config._collection_name = 'airline'

        except Exception as ex:
            raise ColumnarTestEnvironmentError(f'Problem trying read/load test configuration:\n{ex}')

        return columnar_config


@pytest.fixture(name='columnar_config', scope='session')
def columnar_test_config() -> ColumnarConfig:
    config = ColumnarConfig.load_config()
    return config
