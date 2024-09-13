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

from dataclasses import dataclass
from datetime import timedelta
from threading import Event
from typing import List, Optional

from couchbase_columnar.common.core.query import (QueryMetadataCore,
                                                  QueryMetricsCore,
                                                  QueryWarningCore)


@dataclass
class CancelToken:
    """Token that can be passed into a blocking query enabling streaming to be canceled.

    **VOLATILE** This API is subject to change at any time.
    """
    token: Event
    poll_interval: float = 0.25

    def cancel(self) -> None:
        """Set the token's event to trigger streaming cancellation.

        **VOLATILE** This API is subject to change at any time.
        """
        self.token.set()


class QueryWarning:
    def __init__(self, raw: QueryWarningCore) -> None:
        self._raw = raw

    def code(self) -> int:
        """
        Returns:
            The query warning code.
        """
        return self._raw['code']

    def message(self) -> str:
        """
        Returns:
            The query warning message.
        """
        return self._raw['message']

    def __repr__(self) -> str:
        return "QueryWarning:{}".format(self._raw)


class QueryMetrics:
    def __init__(self, raw: QueryMetricsCore) -> None:
        self._raw = raw

    def elapsed_time(self) -> timedelta:
        """Get the total amount of time spent running the query.

        Returns:
            The total amount of time spent running the query.
        """
        us = (self._raw.get('elapsed_time') or 0) / 1000
        return timedelta(microseconds=us)

    def execution_time(self) -> timedelta:
        """Get the total amount of time spent executing the query.

        Returns:
            The total amount of time spent executing the query.
        """
        us = (self._raw.get('execution_time') or 0) / 1000
        return timedelta(microseconds=us)

    def result_count(self) -> int:
        """Get the total number of rows which were part of the result set.

        Returns:
            The total number of rows which were part of the result set.
        """
        return self._raw.get('result_count') or 0

    def result_size(self) -> int:
        """Get the total number of bytes which were generated as part of the result set.

        Returns:
            The total number of bytes which were generated as part of the result set.
        """  # noqa: E501
        return self._raw.get('result_size') or 0

    def processed_objects(self) -> int:
        """Get the total number of objects that were processed to create the result set.

        Returns:
            The total number of objects that were processed to create the result set.
        """
        return self._raw.get('processed_objects') or 0

    def __repr__(self) -> str:
        return "QueryMetrics:{}".format(self._raw)


class QueryMetadata:
    def __init__(self, raw: Optional[QueryMetadataCore]) -> None:
        self._raw = raw if raw is not None else {}

    def request_id(self) -> str:
        """Get the request ID which is associated with the executed query.

        Returns:
            The request ID which is associated with the executed query.
        """
        return self._raw['request_id']

    def warnings(self) -> List[QueryWarning]:
        """Get warnings that occurred during the execution of the query.

        Returns:
            Any warnings that occurred during the execution of the query.
        """
        return list(map(QueryWarning, self._raw['warnings']))

    def metrics(self) -> QueryMetrics:
        """Get the various metrics which are made available by the query engine.

        Returns:
            A :class:`~couchbase_columnar.query.QueryMetrics` instance.
        """
        return QueryMetrics(self._raw['metrics'])

    def __repr__(self) -> str:
        return "QueryMetadata:{}".format(self._raw)
