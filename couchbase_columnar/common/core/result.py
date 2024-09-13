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

import sys
from abc import ABC, abstractmethod
from typing import (Any,
                    Coroutine,
                    List,
                    Optional,
                    Union)

if sys.version_info < (3, 9):
    from typing import AsyncIterator as PyAsyncIterator
    from typing import Iterator
else:
    from collections.abc import AsyncIterator as PyAsyncIterator
    from collections.abc import Iterator

from couchbase_columnar.common.query import QueryMetadata


class QueryResult(ABC):
    """Abstract base class for query results."""

    @abstractmethod
    def cancel(self) -> None:
        """
        Cancel streaming the query results.

        **VOLATILE** This API is subject to change at any time.
        """
        raise NotImplementedError

    @abstractmethod
    def get_all_rows(self) -> Union[Coroutine[Any, Any, List[Any]], List[Any]]:
        """Convenience method to load all query results into memory."""
        raise NotImplementedError

    @abstractmethod
    def metadata(self) -> Optional[QueryMetadata]:
        """Get the query metadata."""
        raise NotImplementedError

    @abstractmethod
    def rows(self) -> Union[PyAsyncIterator[Any], Iterator[Any]]:
        """Retrieve the rows which have been returned by the query."""
        raise NotImplementedError
