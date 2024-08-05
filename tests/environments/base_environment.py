from __future__ import annotations

from asyncio import AbstractEventLoop
from typing import TYPE_CHECKING, Optional

import pytest
import pytest_asyncio

from acouchbase_columnar import get_event_loop
from acouchbase_columnar.cluster import AsyncCluster
from acouchbase_columnar.result import AsyncQueryResult
from couchbase_columnar.cluster import Cluster
from couchbase_columnar.credential import Credential
from couchbase_columnar.options import ClusterOptions, SecurityOptions
from couchbase_columnar.result import BlockingQueryResult
from tests import ColumnarTestEnvironmentException, YieldFixture

if TYPE_CHECKING:
    from tests.columnar_config import ColumnarConfig


class TestEnvironment:

    def __init__(self,
                 config: ColumnarConfig,
                 async_cluster: Optional[AsyncCluster] = None,
                 cluster: Optional[Cluster] = None) -> None:
        self._config = config
        self._async_cluster = async_cluster
        self._cluster = cluster

    @property
    def config(self) -> ColumnarConfig:
        return self._config

    @property
    def fqdn(self) -> str:
        return self.config.fqdn


class BlockingTestEnvironment(TestEnvironment):
    def __init__(self, config: ColumnarConfig, cluster: Cluster) -> None:
        super().__init__(config, cluster=cluster)

    @property
    def cluster(self) -> Cluster:
        if self._cluster is None:
            raise ColumnarTestEnvironmentException('No cluster available.')
        return self._cluster

    def assert_rows(self, result: BlockingQueryResult, expected_count: int) -> None:
        count = 0
        assert isinstance(result, (BlockingQueryResult,))
        for row in result.rows():
            assert row is not None
            count += 1
        assert count >= expected_count

    @classmethod
    def get_environment(cls, config: ColumnarConfig) -> BlockingTestEnvironment:
        if config is None:
            raise ColumnarTestEnvironmentException('No test config provided.')

        connstr = config.get_connection_string()
        username, pw = config.get_username_and_pw()
        cred = Credential.from_username_and_password(username, pw)
        sec_opts: Optional[SecurityOptions] = None
        if config.nonprod is True:
            from couchbase_columnar.common.core._certificates import _Certificates
            sec_opts = SecurityOptions.trust_only_certificates(_Certificates.get_nonprod_certificates())

        if config.tls_verify is False:
            if sec_opts is not None:
                sec_opts['verify_server_certificate'] = False
            else:
                sec_opts = SecurityOptions(verify_server_certificate=False)

        if sec_opts is not None:
            opts = ClusterOptions(security_options=sec_opts)
            cluster = Cluster.create_instance(connstr, cred, opts)
        else:
            cluster = Cluster.create_instance(connstr, cred)

        return cls(config, cluster)


class AsyncTestEnvironment(TestEnvironment):
    def __init__(self, config: ColumnarConfig, async_cluster: AsyncCluster) -> None:
        super().__init__(config, async_cluster=async_cluster)

    @property
    def cluster(self) -> AsyncCluster:
        if self._async_cluster is None:
            raise ColumnarTestEnvironmentException('No async cluster available.')
        return self._async_cluster

    async def assert_rows(self, result: AsyncQueryResult, expected_count: int) -> None:
        count = 0
        assert isinstance(result, (AsyncQueryResult,))
        async for row in result.rows():
            assert row is not None
            count += 1
        assert count >= expected_count

    @classmethod
    def get_environment(cls, config: ColumnarConfig) -> AsyncTestEnvironment:
        if config is None:
            raise ColumnarTestEnvironmentException('No test config provided.')

        connstr = config.get_connection_string()
        username, pw = config.get_username_and_pw()
        cred = Credential.from_username_and_password(username, pw)
        sec_opts: Optional[SecurityOptions] = None
        if config.nonprod is True:
            from couchbase_columnar.common.core._certificates import _Certificates
            sec_opts = SecurityOptions.trust_only_certificates(_Certificates.get_nonprod_certificates())

        if config.tls_verify is False:
            if sec_opts is not None:
                sec_opts['verify_server_certificate'] = False
            else:
                sec_opts = SecurityOptions(verify_server_certificate=False)

        if sec_opts is not None:
            opts = ClusterOptions(security_options=sec_opts)
            cluster = AsyncCluster.create_instance(connstr, cred, opts)
        else:
            cluster = AsyncCluster.create_instance(connstr, cred)

        return cls(config, cluster)


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
