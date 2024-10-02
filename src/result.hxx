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

#include "client.hxx"
#include "utils.hxx"
#include <core/columnar/query_result.hxx>
#include <core/pending_operation.hxx>
#include <core/scan_result.hxx>

struct result {
  PyObject_HEAD PyObject* dict;
};

PyObject*
create_result_obj();

struct columnar_query_iterator {
  PyObject_HEAD std::shared_ptr<couchbase::core::pending_operation> pending_op_;
  std::shared_ptr<couchbase::core::columnar::query_result> query_result_;
  std::shared_ptr<std::promise<PyObject*>> barrier_ = nullptr;
  PyObject* row_callback = nullptr;

  void set_pending_operation(std::shared_ptr<couchbase::core::pending_operation> pending_op)
  {
    pending_op_ = pending_op;
  }

  void set_query_result(couchbase::core::columnar::query_result query_result)
  {
    query_result_.reset();
    query_result_ = std::make_shared<couchbase::core::columnar::query_result>(query_result);
  }
};

PyObject*
create_columnar_query_iterator_obj(PyObject* pyObj_row_callback);

PyObject*
get_columnar_query_metadata();

PyObject*
add_result_objects(PyObject* pyObj_module);
