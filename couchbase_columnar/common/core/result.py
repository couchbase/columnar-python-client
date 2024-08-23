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

from abc import ABC, abstractmethod
from typing import (Any,
                    AsyncIterable,
                    Coroutine,
                    Iterable,
                    List,
                    Optional,
                    Union)

from couchbase_columnar.common.query import QueryMetadata


class QueryResult(ABC):
    @abstractmethod
    def cancel(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_all_rows(self) -> Union[Coroutine[Any, Any, List[Any]], List[Any]]:
        raise NotImplementedError

    @abstractmethod
    def metadata(self) -> Optional[QueryMetadata]:
        raise NotImplementedError

    @abstractmethod
    def rows(self) -> Union[AsyncIterable[Any], Iterable[Any]]:
        raise NotImplementedError
