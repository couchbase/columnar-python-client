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
from abc import ABC, abstractmethod
from typing import Any


class Deserializer(ABC):
    """Interface a Custom Deserializer must implement
    """

    @abstractmethod
    def deserialize(self, value: bytes) -> Any:
        raise NotImplementedError

    @classmethod
    def __subclasshook__(cls, subclass: type) -> bool:
        return (hasattr(subclass, 'deserialize') and
                callable(subclass.deserialize))


class DefaultJsonDeserializer(Deserializer):

    def deserialize(self, value: bytes) -> Any:
        return json.loads(value.decode('utf-8'))
