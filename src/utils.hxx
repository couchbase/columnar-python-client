/*
 *  Copyright 2016-2024. Couchbase, Inc.
 *  All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#pragma once

#include "Python.h" // NOLINT
#include <chrono>
#include <core/utils/binary.hxx>
#include <core/utils/duration_parser.hxx>
#include <core/utils/join_strings.hxx>
#include <core/utils/json.hxx>
#include <stdexcept>
#include <string>
#include <tao/json/value.hpp>

constexpr std::chrono::seconds FIFTY_YEARS{ 50 * 365 * 24 * 60 * 60 };

couchbase::core::utils::binary
PyObject_to_binary(PyObject*);
PyObject*
binary_to_PyObject(couchbase::core::utils::binary value);

PyObject*
binary_to_PyObject_unicode(couchbase::core::utils::binary value);

std::string
binary_to_string(couchbase::core::utils::binary value);

std::size_t py_ssize_t_to_size_t(Py_ssize_t);
Py_ssize_t size_t_to_py_ssize_t(std::size_t);

std::chrono::milliseconds
pyObj_to_duration(PyObject* pyObj_duration);
