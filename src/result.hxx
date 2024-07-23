/*
 *   Copyright 2016-2022. Couchbase, Inc.
 *   All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#pragma once

#include "client.hxx"
#include "utils.hxx"
#include <core/columnar/query_result.hxx>
#include <core/scan_result.hxx>

struct result {
  PyObject_HEAD PyObject* dict;
  std::error_code ec;
};

int
pycbcc_result_type_init(PyObject** ptr);

PyObject*
create_result_obj();

struct columnar_query_iterator {
  PyObject_HEAD std::shared_ptr<couchbase::core::columnar::query_result> query_result_;
  PyObject* row_callback = nullptr;
};

int
pycbcc_columnar_query_iterator_type_init(PyObject** ptr);

columnar_query_iterator*
create_columnar_query_iterator_obj(couchbase::core::columnar::query_result result,
                                   PyObject* pyObj_row_callback);

PyObject*
get_columnar_query_metadata();
