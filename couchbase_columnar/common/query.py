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

from datetime import timedelta
from typing import List, Optional

from couchbase_columnar.common import JSONType
from couchbase_columnar.common.core.query import (QueryMetaDataCore,
                                                  QueryMetricsCore,
                                                  QueryProblemCore)
from couchbase_columnar.common.enums import QueryStatus


class QueryProblem:
    def __init__(self, raw: QueryProblemCore) -> None:
        self._raw = raw

    def code(self) -> Optional[int]:
        return self._raw.get('code', None)

    def message(self) -> Optional[str]:
        return self._raw.get('message', None)


class QueryWarning(QueryProblem):
    def __init__(self, raw: QueryProblemCore) -> None:
        super().__init__(raw)

    def __repr__(self) -> str:
        return "QueryWarning:{}".format(super()._raw)


class QueryError(QueryProblem):
    def __init__(self, raw: QueryProblemCore) -> None:
        super().__init__(raw)

    def __repr__(self) -> str:
        return "QueryError:{}".format(super()._raw)


class QueryMetrics:
    def __init__(self, raw: QueryMetricsCore) -> None:
        self._raw = raw

    def elapsed_time(self) -> timedelta:
        """Get the total amount of time spent running the query.

        Returns:
            timedelta: The total amount of time spent running the query.
        """
        us = (self._raw.get('elapsed_time') or 0) / 1000
        return timedelta(microseconds=us)

    def execution_time(self) -> timedelta:
        """Get the total amount of time spent executing the query.

        Returns:
            timedelta: The total amount of time spent executing the query.
        """
        us = (self._raw.get('execution_time') or 0) / 1000
        return timedelta(microseconds=us)

    def result_count(self) -> int:
        """Get the total number of rows which were part of the result set.

        Returns:
            int: The total number of rows which were part of the result set.
        """
        return self._raw.get('result_count') or 0

    def result_size(self) -> int:
        """Get the total number of bytes which were generated as part of the result set.

        Returns:
            int: The total number of bytes which were generated as part of the result set.
        """  # noqa: E501
        return self._raw.get('result_size') or 0

    def error_count(self) -> int:
        """Get the total number of errors which were encountered during the execution of the query.

        Returns:
            int: The total number of errors which were encountered during the execution of the query.
        """  # noqa: E501
        return self._raw.get('error_count') or 0

    def warning_count(self) -> int:
        """Get the total number of warnings which were encountered during the execution of the query.

        Returns:
            int: The total number of warnings which were encountered during the execution of the query.
        """  # noqa: E501
        return self._raw.get('warning_count') or 0

    def __repr__(self) -> str:
        return "QueryMetrics:{}".format(self._raw)


class QueryMetaData:
    def __init__(self, raw: Optional[QueryMetaDataCore]) -> None:
        self._raw = raw if raw is not None else {}

    def request_id(self) -> Optional[str]:
        """Get the request ID which is associated with the executed query.

        Returns:
            str: The request ID which is associated with the executed query.
        """
        return self._raw.get('request_id', None)

    def client_context_id(self) -> Optional[str]:
        """Get the client context id which is assoicated with the executed query.

        Returns:
            str: The client context id which is assoicated with the executed query.
        """
        return self._raw.get('client_context_id', None)

    def status(self) -> QueryStatus:
        """Get the status of the query at the time the query meta-data was generated.

        Returns:
            :class:`.QueryStatus`: The status of the query at the time the query meta-data was generated.
        """
        q_status = self._raw.get('status')
        return QueryStatus.from_str(q_status) if q_status is not None else QueryStatus.UNKNOWN

    def signature(self) -> Optional[JSONType]:
        """Provides the signature of the query.

        Returns:
            Optional[JSONType]:  The signature of the query.
        """
        return self._raw.get('signature', None)

    def warnings(self) -> Optional[List[QueryWarning]]:
        """Get warnings that occurred during the execution of the query.

        Returns:
            List[:class:`.QueryWarning`]: Any warnings that occurred during the execution of the query.
        """
        warnings = self._raw.get('warnings')
        if warnings is not None:
            return list(map(QueryWarning, warnings))
        return None

    def errors(self) -> Optional[List[QueryError]]:
        """Get errors that occurred during the execution of the query.

        Returns:
            List[:class:`.QueryWarning`]: Any errors that occurred during the execution of the query.
        """
        errors = self._raw.get('errors')
        if errors is not None:
            return list(map(QueryError, errors))
        return None

    def metrics(self) -> Optional[QueryMetrics]:
        """Get the various metrics which are made available by the query engine.

        Returns:
            Optional[:class:`.QueryMetrics`]: A :class:`.QueryMetrics` instance.
        """
        metrics = self._raw.get('metrics')
        if metrics is not None:
            return QueryMetrics(metrics)
        return None

    def __repr__(self) -> str:
        return "QueryMetaData:{}".format(self._raw)
