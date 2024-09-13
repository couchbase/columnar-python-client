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

import json
import pathlib
import sys
from asyncio import AbstractEventLoop
from os import path
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Optional,
                    TypedDict,
                    Union)

import pytest
import pytest_asyncio

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from acouchbase_columnar import get_event_loop
from acouchbase_columnar.cluster import AsyncCluster
from acouchbase_columnar.result import AsyncQueryResult
from acouchbase_columnar.scope import AsyncScope
from couchbase_columnar.cluster import Cluster
from couchbase_columnar.credential import Credential
from couchbase_columnar.options import ClusterOptions, SecurityOptions
from couchbase_columnar.result import BlockingQueryResult
from couchbase_columnar.scope import Scope
from tests import ColumnarTestEnvironmentError, YieldFixture

if TYPE_CHECKING:
    from tests.columnar_config import ColumnarConfig


class TestEnvironmentOptionsKwargs(TypedDict, total=False):
    async_cluster: Optional[AsyncCluster]
    cluster: Optional[Cluster]
    database_name: Optional[str]
    scope_name: Optional[str]
    collection_name: Optional[str]


TEST_AIRLINE_DATA_PATH = path.join(pathlib.Path(__file__).parent.parent,
                                   'test_data',
                                   'airline.json')


class TestEnvironment:

    def __init__(self, config: ColumnarConfig, **kwargs: Unpack[TestEnvironmentOptionsKwargs]) -> None:
        self._config = config
        self._async_cluster = kwargs.pop('async_cluster', None)
        self._cluster = kwargs.pop('cluster', None)
        self._database_name = kwargs.pop('database_name', None)
        self._scope_name = kwargs.pop('scope_name', None)
        self._collection_name = kwargs.pop('collection_name', None)
        self._async_scope: Optional[AsyncScope] = None
        self._scope: Optional[Scope] = None
        self._use_scope = False

    @property
    def config(self) -> ColumnarConfig:
        return self._config

    @property
    def fqdn(self) -> str:
        return self.config.fqdn

    @property
    def collection_name(self) -> Optional[str]:
        return self._collection_name

    @property
    def use_scope(self) -> bool:
        return self._use_scope

    def load_collection_data_from_file(self, file_path: str, limit: Optional[int] = 100) -> List[Dict[str, Any]]:
        with open(file_path, mode='+r') as json_file:
            json_data: List[Dict[str, Any]] = json.load(json_file)

        if limit is not None and len(json_data) > limit:
            return json_data[:limit]
        return json_data


class BlockingTestEnvironment(TestEnvironment):
    def __init__(self, config: ColumnarConfig, **kwargs: Unpack[TestEnvironmentOptionsKwargs]) -> None:
        super().__init__(config, **kwargs)

    @property
    def cluster(self) -> Cluster:
        if self._cluster is None:
            raise ColumnarTestEnvironmentError('No cluster available.')
        return self._cluster

    @property
    def scope(self) -> Scope:
        if self._scope is None:
            raise ColumnarTestEnvironmentError('No scope available.')
        return self._scope

    @property
    def cluster_or_scope(self) -> Union[Cluster, Scope]:
        if self._scope is not None:
            return self.scope
        return self.cluster

    def enable_scope(self,
                     database_name: Optional[str] = None,
                     scope_name: Optional[str] = None) -> BlockingTestEnvironment:

        if self._cluster is None:
            raise ColumnarTestEnvironmentError('No cluster available.')
        db_name = database_name if database_name is not None else self._database_name
        if db_name is None:
            raise ColumnarTestEnvironmentError('Cannot create scope without a database name.')
        scope_name = scope_name if scope_name is not None else self._scope_name
        if scope_name is None:
            raise ColumnarTestEnvironmentError('Cannot create scope without a scope name.')
        self._scope = self._cluster.database(db_name).scope(scope_name)
        self._use_scope = True
        return self

    def disable_scope(self) -> BlockingTestEnvironment:
        self._scope = None
        self._use_scope = False
        return self

    def assert_rows(self, result: BlockingQueryResult, expected_count: int) -> None:
        count = 0
        assert isinstance(result, (BlockingQueryResult,))
        for row in result.rows():
            assert row is not None
            count += 1
        assert count >= expected_count

    def create_new_cluster_instance(self,
                                    connstr: Optional[str] = None,
                                    credential: Optional[Credential] = None) -> Cluster:
        if connstr is None:
            connstr = self.config.get_connection_string()
        if credential is None:
            username, pw = self.config.get_username_and_pw()
            credential = Credential.from_username_and_password(username, pw)

        sec_opts = None
        if self.config.nonprod is True:
            from couchbase_columnar.common.core._certificates import _Certificates
            sec_opts = SecurityOptions.trust_only_certificates(_Certificates.get_nonprod_certificates())

        if self.config.disable_server_certificate_verification is True:
            if sec_opts is not None:
                sec_opts['disable_server_certificate_verification'] = True
            else:
                sec_opts = SecurityOptions(disable_server_certificate_verification=True)

        if sec_opts is not None:
            opts = ClusterOptions(security_options=sec_opts)
            return Cluster.create_instance(connstr, credential, opts)
        else:
            return Cluster.create_instance(connstr, credential)

    def setup(self) -> None:
        if self.config.create_keyspace is False:
            return

        setup_statements = [
            f'CREATE DATABASE `{self.config.database_name}` IF NOT EXISTS;',
            f'CREATE SCOPE `{self.config.database_name}`.`{self.config.scope_name}` IF NOT EXISTS;',
            ('CREATE COLLECTION '
             f'`{self.config.database_name}`.`{self.config.scope_name}`.`{self.config.collection_name}`'
             ' IF NOT EXISTS PRIMARY KEY (pk: UUID) AUTOGENERATED;')
        ]

        for statement in setup_statements:
            try:
                self.cluster.execute_query(statement)
            except Exception as ex:
                raise ColumnarTestEnvironmentError(f'Unable to execute statement={statement}. Error: {ex}')

        json_data = self.load_collection_data_from_file(TEST_AIRLINE_DATA_PATH)
        docs = []
        for d in json_data:
            if 'collection' in d:
                d['collection'] = self.config.collection_name
            if 'scope' in d:
                d['scope'] = self.config.scope_name
            docs.append(json.dumps(d))
        statement = (f'USE `{self.config.database_name}`.`{self.config.scope_name}`; '
                     f'UPSERT INTO `{self.config.collection_name}` ({",".join(docs)})')

        try:
            self.cluster.execute_query(statement)
        except Exception as ex:
            raise ColumnarTestEnvironmentError(f'Unable to load collection data. Error: {ex}')

    def teardown(self) -> None:
        if self.config.create_keyspace is False:
            return

        teardown_statements = [
            f'DROP DATABASE `{self.config.database_name}` IF EXISTS;',
            f'DROP SCOPE `{self.config.database_name}`.`{self.config.scope_name}` IF EXISTS;',
            ('DROP COLLECTION '
             f'`{self.config.database_name}`.`{self.config.scope_name}`.`{self.config.collection_name}`'
             ' IF EXISTS;')
        ]

        for statement in teardown_statements:
            try:
                self.cluster.execute_query(statement)
            except Exception as ex:
                raise ColumnarTestEnvironmentError(f'Unable to execute statement={statement}. Error: {ex}')

    @classmethod
    def get_environment(cls, config: ColumnarConfig) -> BlockingTestEnvironment:
        if config is None:
            raise ColumnarTestEnvironmentError('No test config provided.')

        connstr = config.get_connection_string()
        username, pw = config.get_username_and_pw()
        cred = Credential.from_username_and_password(username, pw)
        sec_opts: Optional[SecurityOptions] = None
        if config.nonprod is True:
            from couchbase_columnar.common.core._certificates import _Certificates
            sec_opts = SecurityOptions.trust_only_certificates(_Certificates.get_nonprod_certificates())

        if config.disable_server_certificate_verification is True:
            if sec_opts is not None:
                sec_opts['disable_server_certificate_verification'] = True
            else:
                sec_opts = SecurityOptions(disable_server_certificate_verification=True)

        env_opts: TestEnvironmentOptionsKwargs = {}
        if sec_opts is not None:
            opts = ClusterOptions(security_options=sec_opts)
            env_opts['cluster'] = Cluster.create_instance(connstr, cred, opts)
        else:
            env_opts['cluster'] = Cluster.create_instance(connstr, cred)

        env_opts['database_name'] = config.database_name
        env_opts['scope_name'] = config.scope_name
        env_opts['collection_name'] = config.collection_name

        return cls(config, **env_opts)


class AsyncTestEnvironment(TestEnvironment):
    def __init__(self, config: ColumnarConfig, **kwargs: Unpack[TestEnvironmentOptionsKwargs]) -> None:
        super().__init__(config, **kwargs)

    @property
    def cluster(self) -> AsyncCluster:
        if self._async_cluster is None:
            raise ColumnarTestEnvironmentError('No async cluster available.')
        return self._async_cluster

    @property
    def scope(self) -> AsyncScope:
        if self._async_scope is None:
            raise ColumnarTestEnvironmentError('No scope available.')
        return self._async_scope

    @property
    def cluster_or_scope(self) -> Union[AsyncCluster, AsyncScope]:
        if self._async_scope is not None:
            return self.scope
        return self.cluster

    def enable_scope(self,
                     database_name: Optional[str] = None,
                     scope_name: Optional[str] = None) -> AsyncTestEnvironment:

        if self._async_cluster is None:
            raise ColumnarTestEnvironmentError('No cluster available.')
        db_name = database_name if database_name is not None else self._database_name
        if db_name is None:
            raise ColumnarTestEnvironmentError('Cannot create scope without a database name.')
        scope_name = scope_name if scope_name is not None else self._scope_name
        if scope_name is None:
            raise ColumnarTestEnvironmentError('Cannot create scope without a scope name.')
        self._async_scope = self._async_cluster.database(db_name).scope(scope_name)
        self._use_scope = True
        return self

    def disable_scope(self) -> AsyncTestEnvironment:
        self._async_scope = None
        self._use_scope = False
        return self

    async def assert_rows(self, result: AsyncQueryResult, expected_count: int) -> None:
        count = 0
        assert isinstance(result, (AsyncQueryResult,))
        async for row in result.rows():
            assert row is not None
            count += 1
        assert count >= expected_count

    def create_new_cluster_instance(self,
                                    connstr: Optional[str] = None,
                                    credential: Optional[Credential] = None) -> AsyncCluster:
        if connstr is None:
            connstr = self.config.get_connection_string()
        if credential is None:
            username, pw = self.config.get_username_and_pw()
            credential = Credential.from_username_and_password(username, pw)

        sec_opts = None
        if self.config.nonprod is True:
            from couchbase_columnar.common.core._certificates import _Certificates
            sec_opts = SecurityOptions.trust_only_certificates(_Certificates.get_nonprod_certificates())

        if self.config.disable_server_certificate_verification is True:
            if sec_opts is not None:
                sec_opts['disable_server_certificate_verification'] = True
            else:
                sec_opts = SecurityOptions(disable_server_certificate_verification=True)

        if sec_opts is not None:
            opts = ClusterOptions(security_options=sec_opts)
            return AsyncCluster.create_instance(connstr, credential, opts)
        else:
            return AsyncCluster.create_instance(connstr, credential)

    async def setup(self) -> None:
        if self.config.create_keyspace is False:
            return

        setup_statements = [
            f'CREATE DATABASE `{self.config.database_name}` IF NOT EXISTS;',
            f'CREATE SCOPE `{self.config.database_name}`.`{self.config.scope_name}` IF NOT EXISTS;',
            ('CREATE COLLECTION '
             f'`{self.config.database_name}`.`{self.config.scope_name}`.`{self.config.collection_name}`'
             ' IF NOT EXISTS PRIMARY KEY (pk: UUID) AUTOGENERATED;')
        ]

        for statement in setup_statements:
            try:
                await self.cluster.execute_query(statement)
            except Exception as ex:
                raise ColumnarTestEnvironmentError(f'Unable to execute statement={statement}. Error: {ex}')

        json_data = self.load_collection_data_from_file(TEST_AIRLINE_DATA_PATH)
        docs = []
        for d in json_data:
            if 'collection' in d:
                d['collection'] = self.config.collection_name
            if 'scope' in d:
                d['scope'] = self.config.scope_name
            docs.append(json.dumps(d))
        statement = (f'USE `{self.config.database_name}`.`{self.config.scope_name}`; '
                     f'UPSERT INTO `{self.config.collection_name}` ({",".join(docs)})')

        try:
            await self.cluster.execute_query(statement)
        except Exception as ex:
            raise ColumnarTestEnvironmentError(f'Unable to load collection data. Error: {ex}')

    async def teardown(self) -> None:
        if self.config.create_keyspace is False:
            return

        teardown_statements = [
            f'DROP DATABASE `{self.config.database_name}` IF EXISTS;',
            f'DROP SCOPE `{self.config.database_name}`.`{self.config.scope_name}` IF EXISTS;',
            ('DROP COLLECTION '
             f'`{self.config.database_name}`.`{self.config.scope_name}`.`{self.config.collection_name}`'
             ' IF EXISTS;')
        ]

        for statement in teardown_statements:
            try:
                await self.cluster.execute_query(statement)
            except Exception as ex:
                raise ColumnarTestEnvironmentError(f'Unable to execute statement={statement}. Error: {ex}')

    @classmethod
    def get_environment(cls, config: ColumnarConfig) -> AsyncTestEnvironment:
        if config is None:
            raise ColumnarTestEnvironmentError('No test config provided.')

        connstr = config.get_connection_string()
        username, pw = config.get_username_and_pw()
        cred = Credential.from_username_and_password(username, pw)
        sec_opts: Optional[SecurityOptions] = None
        if config.nonprod is True:
            from couchbase_columnar.common.core._certificates import _Certificates
            sec_opts = SecurityOptions.trust_only_certificates(_Certificates.get_nonprod_certificates())

        if config.disable_server_certificate_verification is True:
            if sec_opts is not None:
                sec_opts['disable_server_certificate_verification'] = True
            else:
                sec_opts = SecurityOptions(disable_server_certificate_verification=True)

        env_opts: TestEnvironmentOptionsKwargs = {}
        if sec_opts is not None:
            opts = ClusterOptions(security_options=sec_opts)
            env_opts['async_cluster'] = AsyncCluster.create_instance(connstr, cred, opts)
        else:
            env_opts['async_cluster'] = AsyncCluster.create_instance(connstr, cred)

        env_opts['database_name'] = config.database_name
        env_opts['scope_name'] = config.scope_name
        env_opts['collection_name'] = config.collection_name
        return cls(config, **env_opts)


@pytest_asyncio.fixture(scope='session')
def event_loop() -> YieldFixture[AbstractEventLoop]:
    loop = get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope='session', name='sync_test_env')
def base_test_environment(columnar_config: ColumnarConfig) -> BlockingTestEnvironment:
    return BlockingTestEnvironment.get_environment(columnar_config)


@pytest.fixture(scope='session', name='async_test_env')
def base_async_test_environment(columnar_config: ColumnarConfig) -> AsyncTestEnvironment:
    return AsyncTestEnvironment.get_environment(columnar_config)
