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

from typing import (Optional,
                    Set,
                    Tuple,
                    TypedDict)


class GenericErrorContextCore(TypedDict, total=False):
    # base
    cinfo: Optional[Tuple[str, int]]
    context_type: Optional[str]
    error_message: Optional[str]
    last_dispatched_from: Optional[str]
    last_dispatched_to: Optional[str]
    retry_attempts: Optional[int]
    retry_reasons: Optional[Set[str]]
    # http
    client_context_id: Optional[str]
    context_detail_type: Optional[str]
    http_body: Optional[str]
    http_status: Optional[int]
    method: Optional[str]
    path: Optional[str]
    # mgmt
    content: Optional[str]
    # query/analytics/search
    parameters: Optional[str]
    # query/analytics
    first_error_code: Optional[int]
    first_error_message: Optional[str]
    statement: Optional[str]


class ErrorContextCore(TypedDict, total=False):
    cinfo: Optional[Tuple[str, int]]
    context_type: Optional[str]
    error_message: Optional[str]
    last_dispatched_from: Optional[str]
    last_dispatched_to: Optional[str]
    retry_attempts: Optional[int]
    retry_reasons: Optional[Set[str]]


class HTTPErrorContextCore(ErrorContextCore, total=False):
    client_context_id: Optional[str]
    context_detail_type: Optional[str]
    http_body: Optional[str]
    http_status: Optional[int]
    method: Optional[str]
    path: Optional[str]
