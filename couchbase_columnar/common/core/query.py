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

from typing import (List,
                    Optional,
                    TypedDict)

from couchbase_columnar.common import JSONType


class QueryMetricsCore(TypedDict, total=False):
    """
        **INTERNAL**
    """

    elapsed_time: Optional[int]
    execution_time: Optional[int]
    sort_count: Optional[int]
    result_count: Optional[int]
    result_size: Optional[int]
    mutation_count: Optional[int]
    error_count: Optional[int]
    warning_count: Optional[int]


class QueryProblemCore(TypedDict, total=False):
    """
        **INTERNAL**
    """

    code: Optional[int]
    message: Optional[str]


class QueryMetaDataCore(TypedDict, total=False):
    """
        **INTERNAL**
    """

    request_id: Optional[str]
    client_context_id: Optional[str]
    status: Optional[str]
    signature: Optional[JSONType]
    warnings: Optional[List[QueryProblemCore]]
    errors: Optional[List[QueryProblemCore]]
    metrics: Optional[QueryMetricsCore]
    profile: Optional[JSONType]
