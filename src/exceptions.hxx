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
#include <core/columnar/error.hxx>
#include <core/columnar/error_codes.hxx>
#include <core/utils/json.hxx>

#define NULL_CONN_OBJECT "Received a null connection."

class CoreClientErrors
{
public:
  enum ErrorCode : std::uint8_t {
    VALUE = 1,
    RUNTIME,
    CANCELED,
    INTERNAL_SDK
  };

  CoreClientErrors()
    : error_code_{ INTERNAL_SDK }
  {
  }

  constexpr CoreClientErrors(ErrorCode error)
    : error_code_{ error }
  {
  }

  operator ErrorCode() const
  {
    return error_code_;
  }
  // lets prevent the implicit promotion of bool to int
  explicit operator bool() = delete;
  constexpr bool operator==(CoreClientErrors err) const
  {
    return error_code_ == err.error_code_;
  }
  constexpr bool operator!=(CoreClientErrors err) const
  {
    return error_code_ != err.error_code_;
  }

  static const char* ALL_CORE_CLIENT_ERROR_CODES(void)
  {
    const char* errors = "VALUE "
                         "RUNTIME "
                         "CANCELED "
                         "INTERNAL_SDK";

    return errors;
  }

private:
  ErrorCode error_code_;
};

struct core_error {
  PyObject_HEAD PyObject* error_details = nullptr;
};

core_error*
create_core_error_obj();

PyObject*
pycbcc_build_exception(couchbase::core::columnar::error err, const char* file, int line);

PyObject*
pycbcc_build_exception(CoreClientErrors::ErrorCode client_error_code,
                       const char* file,
                       int line,
                       const char* msg,
                       bool check_inner_cause = false);

void
pycbcc_set_python_exception(couchbase::core::columnar::error err, const char* file, int line);

void
pycbcc_set_python_exception(CoreClientErrors::ErrorCode client_error_code,
                            const char* file,
                            int line,
                            const char* msg);

PyObject*
build_exception(PyObject* self, PyObject* args);

PyObject*
add_exception_objects(PyObject* pyObj_module);
