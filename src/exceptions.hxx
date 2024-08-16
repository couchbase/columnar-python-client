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
#include <core/columnar/error.hxx>
#include <core/columnar/error_codes.hxx>
#include <core/utils/json.hxx>

#define NULL_CONN_OBJECT "Received a null connection."

class CoreErrors
{
public:
  enum ErrorType : std::uint8_t {
    VALUE = 1,
    RUNTIME,
    INTERNAL_SDK
  };

  CoreErrors()
    : error_{ INTERNAL_SDK }
  {
  }

  constexpr CoreErrors(ErrorType error)
    : error_{ error }
  {
  }

  operator ErrorType() const
  {
    return error_;
  }
  // lets prevent the implicit promotion of bool to int
  explicit operator bool() = delete;
  constexpr bool operator==(CoreErrors err) const
  {
    return error_ == err.error_;
  }
  constexpr bool operator!=(CoreErrors err) const
  {
    return error_ != err.error_;
  }

  static const char* ALL_CORE_ERRORS(void)
  {
    const char* errors = "VALUE "
                         "RUNTIME "
                         "INTERNAL_SDK";

    return errors;
  }

private:
  ErrorType error_;
};

struct core_error {
  PyObject_HEAD PyObject* error_details = nullptr;
};

int
pycbcc_core_error_type_init(PyObject** ptr);

core_error*
create_core_error_obj();

PyObject*
pycbcc_build_exception(couchbase::core::columnar::error err, const char* file, int line);

PyObject*
pycbcc_build_exception(CoreErrors::ErrorType error_type,
                       const char* file,
                       int line,
                       const char* msg,
                       bool check_inner_cause = false);

void
pycbcc_set_python_exception(couchbase::core::columnar::error err, const char* file, int line);

void
pycbcc_set_python_exception(CoreErrors::ErrorType error_type,
                            const char* file,
                            int line,
                            const char* msg);

PyObject*
build_exception(PyObject* self, PyObject* args);
