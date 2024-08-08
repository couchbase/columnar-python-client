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

#include "columnar_query.hxx"

#include <core/columnar/error.hxx>
#include <core/columnar/query_options.hxx>
#include <core/columnar/query_result.hxx>

#include "exceptions.hxx"
#include "result.hxx"

couchbase::core::columnar::query_scan_consistency
str_to_columnar_scan_consistency_type(std::string consistency)
{
  if (consistency.compare("not_bounded") == 0) {
    return couchbase::core::columnar::query_scan_consistency::not_bounded;
  }
  if (consistency.compare("request_plus") == 0) {
    return couchbase::core::columnar::query_scan_consistency::request_plus;
  }

  // TODO: better exception
  PyErr_SetString(PyExc_ValueError, "Invalid Columnar Query Scan Consistency type.");
  return {};
}

void
create_columnar_query_iterator(couchbase::core::columnar::query_result resp,
                               couchbase::core::columnar::error err,
                               PyObject* pyObj_callback,
                               PyObject* pyObj_row_callback,
                               std::shared_ptr<std::promise<PyObject*>> barrier = nullptr)
{
  auto set_exception = false;
  PyObject* pyObj_exc = nullptr;
  PyObject* pyObj_args = NULL;
  PyObject* pyObj_func = NULL;
  PyObject* pyObj_callback_res = nullptr;

  PyGILState_STATE state = PyGILState_Ensure();
  if (err.ec) {
    pyObj_exc = pycbcc_build_exception(err.ec, __FILE__, __LINE__, "Error doing query operation.");
    if (pyObj_callback == nullptr) {
      barrier->set_value(pyObj_exc);
    } else {
      pyObj_func = pyObj_callback;
      pyObj_args = PyTuple_New(1);
      PyTuple_SET_ITEM(pyObj_args, 0, pyObj_exc);
    }
    // lets clear any errors
    PyErr_Clear();
  } else {
    auto query_iter = create_columnar_query_iterator_obj(resp, pyObj_row_callback);
    if (query_iter == nullptr || PyErr_Occurred() != nullptr) {
      set_exception = true;
    } else {
      if (pyObj_callback == nullptr) {
        barrier->set_value(reinterpret_cast<PyObject*>(query_iter));
      } else {
        pyObj_func = pyObj_callback;
        pyObj_args = PyTuple_New(1);
        PyTuple_SET_ITEM(pyObj_args, 0, reinterpret_cast<PyObject*>(query_iter));
      }
    }
  }

  if (set_exception) {
    pyObj_exc = pycbcc_build_exception(
      PycbccError::UnableToBuildResult, __FILE__, __LINE__, "Columnar query operation error.");
    if (pyObj_callback == nullptr) {
      barrier->set_value(pyObj_exc);
    } else {
      pyObj_func = pyObj_callback;
      pyObj_args = PyTuple_New(1);
      PyTuple_SET_ITEM(pyObj_args, 0, pyObj_exc);
    }
  }

  if (pyObj_func != nullptr) {
    pyObj_callback_res = PyObject_CallObject(pyObj_func, pyObj_args);
    if (pyObj_callback_res) {
      Py_DECREF(pyObj_callback_res);
    } else {
      pycbcc_set_python_exception(
        PycbccError::InternalSDKError, __FILE__, __LINE__, "Columnar query callback failed.");
    }
    Py_DECREF(pyObj_args);
    Py_XDECREF(pyObj_callback);
  }
  PyGILState_Release(state);
}

couchbase::core::columnar::query_options
build_query_options(PyObject* pyObj_query_args)
{
  couchbase::core::columnar::query_options options;
  PyObject* pyObj_statement = PyDict_GetItemString(pyObj_query_args, "statement");
  if (pyObj_statement != nullptr) {
    if (PyUnicode_Check(pyObj_statement)) {
      options.statement = std::string(PyUnicode_AsUTF8(pyObj_statement));
    } else {
      PyErr_SetString(PyExc_ValueError, "Columnar query statement is not a string.");
      return {};
    }
  }

  PyObject* pyObj_database_name = PyDict_GetItemString(pyObj_query_args, "database_name");
  if (pyObj_database_name != nullptr) {
    options.database_name = std::string(PyUnicode_AsUTF8(pyObj_database_name));
  }

  PyObject* pyObj_scope_name = PyDict_GetItemString(pyObj_query_args, "scope_name");
  if (pyObj_scope_name != nullptr) {
    options.scope_name = std::string(PyUnicode_AsUTF8(pyObj_scope_name));
  }

  PyObject* pyObj_priority = PyDict_GetItemString(pyObj_query_args, "priority");
  if (pyObj_priority != nullptr) {
    options.priority = pyObj_priority == Py_True ? true : false;
  }

  PyObject* pyObj_readonly = PyDict_GetItemString(pyObj_query_args, "readonly");
  if (pyObj_readonly != nullptr) {
    options.read_only = pyObj_readonly == Py_True ? true : false;
  }

  PyObject* pyObj_scan_consistency = PyDict_GetItemString(pyObj_query_args, "scan_consistency");
  if (pyObj_scan_consistency != nullptr) {
    if (PyUnicode_Check(pyObj_scan_consistency)) {
      options.scan_consistency = str_to_columnar_scan_consistency_type(
        std::string(PyUnicode_AsUTF8(pyObj_scan_consistency)));
    } else {
      PyErr_SetString(PyExc_ValueError, "scan_consistency is not a string.");
    }
    if (PyErr_Occurred()) {
      return {};
    }
  }

  PyObject* pyObj_timeout = PyDict_GetItemString(pyObj_query_args, "timeout");
  if (nullptr != pyObj_timeout) {
    // comes in as microseconds
    options.timeout = std::chrono::milliseconds(PyLong_AsUnsignedLongLong(pyObj_timeout) / 1000ULL);
  } else {
    options.timeout = couchbase::core::timeout_defaults::analytics_timeout;
  }

  PyObject* pyObj_raw = PyDict_GetItemString(pyObj_query_args, "raw");
  std::map<std::string, couchbase::core::json_string> raw_options{};
  if (pyObj_raw && PyDict_Check(pyObj_raw)) {
    PyObject *pyObj_key, *pyObj_value;
    Py_ssize_t pos = 0;

    // PyObj_key and pyObj_value are borrowed references
    while (PyDict_Next(pyObj_raw, &pos, &pyObj_key, &pyObj_value)) {
      std::string k;
      if (PyUnicode_Check(pyObj_key)) {
        k = std::string(PyUnicode_AsUTF8(pyObj_key));
      } else {
        PyErr_SetString(
          PyExc_ValueError,
          "Raw option key is not a string.  The raw option should be a dict[str, JSONString].");
        return {};
      }
      if (k.empty()) {
        PyErr_SetString(
          PyExc_ValueError,
          "Raw option key is empty!  The raw option should be a dict[str, JSONString].");
        return {};
      }

      if (PyBytes_Check(pyObj_value)) {
        try {
          auto res = PyObject_to_binary(pyObj_value);
          // this will crash b/c txns query_options expects a std::vector<std::byte>
          // auto res = std::string(PyBytes_AsString(pyObj_value));
          raw_options.emplace(k, couchbase::core::json_string{ std::move(res) });
        } catch (const std::exception& e) {
          PyErr_SetString(
            PyExc_ValueError,
            "Unable to parse raw option value.  The raw option should be a dict[str, JSONString].");
        }
      } else {
        PyErr_SetString(
          PyExc_ValueError,
          "Raw option value not a string.  The raw option should be a dict[str, JSONString].");
        return {};
      }
    }
  }
  if (raw_options.size() > 0) {
    options.raw = raw_options;
  }

  PyObject* pyObj_positional_parameters =
    PyDict_GetItemString(pyObj_query_args, "positional_parameters");
  std::vector<couchbase::core::json_string> positional_parameters{};
  if (pyObj_positional_parameters && PyList_Check(pyObj_positional_parameters)) {
    size_t nargs = static_cast<size_t>(PyList_Size(pyObj_positional_parameters));
    size_t ii;
    for (ii = 0; ii < nargs; ++ii) {
      PyObject* pyOb_param = PyList_GetItem(pyObj_positional_parameters, ii);
      if (!pyOb_param) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse positional parameter.");
        return {};
      }
      // PyList_GetItem returns borrowed ref, inc while using, decr after done
      Py_INCREF(pyOb_param);
      if (PyBytes_Check(pyOb_param)) {
        try {
          auto res = PyObject_to_binary(pyOb_param);
          positional_parameters.push_back(couchbase::core::json_string{ std::move(res) });
        } catch (const std::exception& e) {
          PyErr_SetString(PyExc_ValueError,
                          "Unable to parse positional parameter option value. Positional parameter "
                          "options must all be json strings.");
        }
      } else {
        PyErr_SetString(PyExc_ValueError,
                        "Unable to parse positional parameter.  Positional parameter options must "
                        "all be json strings.");
        return {};
      }
      Py_DECREF(pyOb_param);
      pyOb_param = nullptr;
    }
  }
  if (positional_parameters.size() > 0) {
    options.positional_parameters = positional_parameters;
  }

  PyObject* pyObj_named_parameters = PyDict_GetItemString(pyObj_query_args, "named_parameters");
  std::map<std::string, couchbase::core::json_string> named_parameters{};
  if (pyObj_named_parameters && PyDict_Check(pyObj_named_parameters)) {
    PyObject *pyObj_key, *pyObj_value;
    Py_ssize_t pos = 0;

    // PyObj_key and pyObj_value are borrowed references
    while (PyDict_Next(pyObj_named_parameters, &pos, &pyObj_key, &pyObj_value)) {
      std::string k;
      if (PyUnicode_Check(pyObj_key)) {
        k = std::string(PyUnicode_AsUTF8(pyObj_key));
      } else {
        PyErr_SetString(PyExc_ValueError,
                        "Named parameter key is not a string.  Named parameters should be a "
                        "dict[str, JSONString].");
        return {};
      }
      if (k.empty()) {
        PyErr_SetString(
          PyExc_ValueError,
          "Named parameter key is empty. Named parameters should be a dict[str, JSONString].");
        return {};
      }
      if (PyBytes_Check(pyObj_value)) {
        try {
          auto res = PyObject_to_binary(pyObj_value);
          named_parameters.emplace(k, couchbase::core::json_string{ std::move(res) });
        } catch (const std::exception& e) {
          PyErr_SetString(PyExc_ValueError,
                          "Unable to parse named parameter option.  Named parameters should be a "
                          "dict[str, JSONString].");
        }
      } else {
        PyErr_SetString(PyExc_ValueError,
                        "Named parameter value not a string.  Named parameters should be a "
                        "dict[str, JSONString].");
        return {};
      }
    }
  }
  if (named_parameters.size() > 0) {
    options.named_parameters = named_parameters;
  }

  return options;
}

PyObject*
handle_columnar_query([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* pyObj_conn = nullptr;
  PyObject* pyObj_query_args = nullptr;
  PyObject* pyObj_callback = nullptr;
  PyObject* pyObj_row_callback = nullptr;

  static const char* kw_list[] = { "conn", "query_args", "callback", "row_callback", nullptr };
  const char* kw_format = "O!|OOO";
  int ret = PyArg_ParseTupleAndKeywords(args,
                                        kwargs,
                                        kw_format,
                                        const_cast<char**>(kw_list),
                                        &PyCapsule_Type,
                                        &pyObj_conn,
                                        &pyObj_query_args,
                                        &pyObj_callback,
                                        &pyObj_row_callback);
  if (!ret) {
    PyErr_SetString(PyExc_ValueError, "Unable to parse arguments");
    return nullptr;
  }

  connection* conn = nullptr;
  conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
  if (nullptr == conn) {
    PyErr_SetString(PyExc_ValueError, "passed null connection");
    return nullptr;
  }
  PyErr_Clear();

  auto query_options = build_query_options(pyObj_query_args);
  if (PyErr_Occurred()) {
    return nullptr;
  }

  Py_XINCREF(pyObj_callback);
  Py_XINCREF(pyObj_row_callback);

  std::shared_ptr<std::promise<PyObject*>> barrier = nullptr;
  std::future<PyObject*> fut;
  if (nullptr == pyObj_callback) {
    barrier = std::make_shared<std::promise<PyObject*>>();
    fut = barrier->get_future();
  }

  tl::expected<std::shared_ptr<couchbase::core::pending_operation>,
               couchbase::core::columnar::error>
    resp;
  {
    Py_BEGIN_ALLOW_THREADS resp = conn->agent_.execute_query(
      query_options,
      [pyObj_callback, pyObj_row_callback, barrier](couchbase::core::columnar::query_result res,
                                                    couchbase::core::columnar::error err) mutable {
        create_columnar_query_iterator(
          std::move(res), err, pyObj_callback, pyObj_row_callback, barrier);
      });
    Py_END_ALLOW_THREADS
  }

  if (!resp.has_value()) {
    CB_LOG_DEBUG(
      "{} Error: code={}, message={}", "PYCBC", resp.error().ec.value(), resp.error().message);
    pycbcc_set_python_exception(resp.error().ec, __FILE__, __LINE__, resp.error().message.c_str());
    return nullptr;
  }

  if (nullptr == pyObj_callback) {
    PyObject* ret = nullptr;
    Py_BEGIN_ALLOW_THREADS ret = fut.get();
    Py_END_ALLOW_THREADS return ret;
  }
  Py_RETURN_NONE;
}
