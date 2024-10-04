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

from datetime import timedelta
from enum import Enum
from os import path
from typing import (Any,
                    Dict,
                    Generic,
                    List,
                    Optional,
                    TypeVar,
                    Union)
from urllib.parse import quote

from couchbase_columnar.common.deserializer import Deserializer

T = TypeVar('T')
E = TypeVar('E', bound=Enum)


def is_null_or_empty(value: Optional[str]) -> bool:
    return value is None or value.isspace()


def timedelta_as_microseconds(duration: timedelta) -> int:
    if duration and not isinstance(duration, timedelta):
        raise ValueError(f"Expected timedelta instead of {duration}")
    if duration.total_seconds() < 0:
        raise ValueError('Timeout must be non-negative.')
    return int(duration.total_seconds() * 1e6 if duration else 0)


def to_microseconds(value: Union[timedelta, float, int]) -> int:
    if value and not isinstance(value, (timedelta, float, int)):
        raise ValueError(f"Excepted value to be of type Union[timedelta, float, int] instead of {value}")
    if not value:
        total_us = 0
    elif isinstance(value, timedelta):
        if value.total_seconds() < 0:
            raise ValueError('Timeout must be non-negative.')
        total_us = int(value.total_seconds() * 1e6)
    else:
        if value < 0:
            raise ValueError('Timeout must be non-negative.')
        total_us = int(value * 1e6)

    return total_us


def to_query_str(params: Dict[str, Any]) -> str:
    encoded_params = []
    for k, v in params.items():
        if v in [True, False]:
            encoded_params.append(f'{quote(k)}={quote(str(v).lower())}')
        else:
            encoded_params.append(f'{quote(k)}={quote(str(v))}')

    return '&'.join(encoded_params)


def validate_raw_dict(value: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("Raw option must be of type Dict[str, Any].")
    if not all(map(lambda k: isinstance(k, str), value.keys())):
        raise ValueError("All keys in raw dict must be a str.")
    return value


def validate_path(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("Path option must be str.")
    if not path.exists(value):
        raise FileNotFoundError("Provided path does not exist.")

    return value


class ValidateBaseClass(Generic[T]):
    """ **INTERNAL** """

    def __call__(self, value: Any) -> T:
        expected_base_class = self.__orig_class__.__args__[0]  # type: ignore[attr-defined]
        # this will pass w/ duck-typing which is okay
        if not issubclass(value.__class__, expected_base_class):
            raise ValueError((f"Expected value to be subclass of {expected_base_class} "
                              "(or implement necessary functionality for the "
                              f"{expected_base_class} base class)."))
        return value  # type: ignore[no-any-return]


class EnumToStr(Generic[E]):
    def __call__(self, value: Any) -> str:
        expected_type = self.__orig_class__.__args__[0]  # type: ignore[attr-defined]

        if isinstance(value, str):
            if value in map(lambda x: x.value, expected_type):
                # TODO: use warning -- maybe don't want to allow str representation?
                return value
            raise ValueError(f"Invalid str representation of {expected_type}. Received '{value}'.")

        if not isinstance(value, expected_type):
            raise ValueError(f"Expected value to be of type {expected_type} instead of {type(value)}")

        return value.value  # type: ignore[no-any-return]


class ValidateType(Generic[T]):
    def __call__(self, value: Any) -> T:
        expected_type = self.__orig_class__.__args__[0]  # type: ignore[attr-defined]
        if not isinstance(value, expected_type):
            raise ValueError(f"Expected value to be of type {expected_type} instead of {type(value)}")
        return value  # type: ignore[no-any-return]


class ValidateList(Generic[T]):
    def __call__(self, value: Any) -> List[T]:
        expected_type = self.__orig_class__.__args__[0]  # type: ignore[attr-defined]
        if not isinstance(value, list):
            raise ValueError("Expected value to be a list.")
        if not all(map(lambda x: isinstance(x, expected_type), value)):
            item_types = list(map(lambda x: type(x), value))
            raise ValueError(("Expected all items in list to be of type "
                              f"{expected_type}. Provided item types {item_types}."))
        # we are returning List[T]
        return value


VALIDATE_BOOL = ValidateType[bool]()
VALIDATE_INT = ValidateType[int]()
VALIDATE_FLOAT = ValidateType[float]()
VALIDATE_STR = ValidateType[str]()
VALIDATE_DESERIALIZER = ValidateBaseClass[Deserializer]()
VALIDATE_STR_LIST = ValidateList[str]()
