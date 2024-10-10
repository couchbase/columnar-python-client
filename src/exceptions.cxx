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

#include "exceptions.hxx"

static PyObject*
core_error__error_details__(core_error* self)
{
  if (self->error_details) {
    PyObject* pyObj_error_details = PyDict_Copy(self->error_details);
    return pyObj_error_details;
  }
  Py_RETURN_NONE;
}

static void
core_error_dealloc(core_error* self)
{
  if (self->error_details) {
    if (PyDict_Check(self->error_details)) {
      PyDict_Clear(self->error_details);
    }
    Py_DECREF(self->error_details);
  }
  Py_TYPE(self)->tp_free((PyObject*)self);
  CB_LOG_DEBUG("{}: core_error_dealloc completed", "PYCBCC");
}

static PyObject*
core_error__new__(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
  core_error* self = reinterpret_cast<core_error*>(type->tp_alloc(type, 0));
  // self->error_details = PyDict_New();
  return reinterpret_cast<PyObject*>(self);
}

static PyMethodDef core_error_methods[] = { { "error_details",
                                              (PyCFunction)core_error__error_details__,
                                              METH_NOARGS,
                                              PyDoc_STR("Core error details") },
                                            { nullptr, nullptr, 0, nullptr } };

static PyTypeObject
init_pycbcc_core_error_type()
{
  PyTypeObject obj = {};
  obj.ob_base = PyVarObject_HEAD_INIT(NULL, 0) obj.tp_name = "pycbcc_core.core_error";
  obj.tp_doc = PyDoc_STR("Base class for exceptions coming from pycbcc_core");
  obj.tp_basicsize = sizeof(core_error);
  obj.tp_itemsize = 0;
  obj.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  obj.tp_new = core_error__new__;
  obj.tp_dealloc = (destructor)core_error_dealloc;
  obj.tp_methods = core_error_methods;
  return obj;
}

static PyTypeObject pycbcc_core_error_type = init_pycbcc_core_error_type();

core_error*
create_core_error_obj()
{
  PyObject* err =
    PyObject_CallObject(reinterpret_cast<PyObject*>(&pycbcc_core_error_type), nullptr);
  return reinterpret_cast<core_error*>(err);
}

PyObject*
get_core_error_instance(core_error* core_err)
{
  PyObject* pyObj_protocol_exc_module = PyImport_ImportModule("couchbase_columnar.protocol.errors");
  if (pyObj_protocol_exc_module == nullptr) {
    PyErr_Clear();
    return nullptr;
  }

  PyObject* pyObj_core_err_class =
    PyObject_GetAttrString(pyObj_protocol_exc_module, "CoreColumnarError");
  if (pyObj_core_err_class == nullptr) {
    PyErr_Clear();
    Py_DECREF(pyObj_protocol_exc_module);
    return nullptr;
  }
  Py_DECREF(pyObj_protocol_exc_module);

  PyObject* pyObj_args = PyTuple_New(0);
  PyObject* pyObj_kwargs = PyDict_New();
  if (-1 ==
      PyDict_SetItemString(pyObj_kwargs, "core_error", reinterpret_cast<PyObject*>(core_err))) {
    PyErr_Clear();
    Py_XDECREF(pyObj_args);
    Py_XDECREF(pyObj_kwargs);
    Py_DECREF(pyObj_core_err_class);
    return nullptr;
  }

  PyObject* pyObj_core_err_instance = PyObject_Call(pyObj_core_err_class, pyObj_args, pyObj_kwargs);
  Py_DECREF(pyObj_args);
  Py_DECREF(pyObj_kwargs);
  Py_DECREF(pyObj_core_err_class);

  if (pyObj_core_err_instance == nullptr) {
    PyErr_Clear();
    return nullptr;
  }

  return pyObj_core_err_instance;
}

PyObject*
pycbcc_build_error_details(couchbase::core::columnar::error err, const char* file, int line)
{
  PyObject* pyObj_error_details = PyDict_New();

  if (err.ec == couchbase::core::columnar::client_errc::canceled) {
    PyObject* pyObj_err_ec = PyLong_FromLong(static_cast<long>(CoreClientErrors::CANCELED));
    if (-1 == PyDict_SetItemString(pyObj_error_details, "client_error_code", pyObj_err_ec)) {
      PyErr_Clear();
      Py_DECREF(pyObj_error_details);
      Py_DECREF(pyObj_err_ec);
      PyErr_SetString(PyExc_RuntimeError,
                      "Unable to add columnar::error::error_code to error_details.");
      return nullptr;
    }
    Py_DECREF(pyObj_err_ec);
  } else if (err.ec == couchbase::core::columnar::client_errc::cluster_closed) {
    // TODO:  should we have another internal error for easier parsing on the Python side?
    PyObject* pyObj_err_ec = PyLong_FromLong(static_cast<long>(CoreClientErrors::RUNTIME));
    if (-1 == PyDict_SetItemString(pyObj_error_details, "client_error_code", pyObj_err_ec)) {
      PyErr_Clear();
      Py_DECREF(pyObj_error_details);
      Py_DECREF(pyObj_err_ec);
      PyErr_SetString(PyExc_RuntimeError,
                      "Unable to add columnar::error::error_code to error_details.");
      return nullptr;
    }
    Py_DECREF(pyObj_err_ec);
  } else if (err.ec == couchbase::core::columnar::client_errc::invalid_argument) {
    PyObject* pyObj_err_ec = PyLong_FromLong(static_cast<long>(CoreClientErrors::VALUE));
    if (-1 == PyDict_SetItemString(pyObj_error_details, "client_error_code", pyObj_err_ec)) {
      PyErr_Clear();
      Py_DECREF(pyObj_error_details);
      Py_DECREF(pyObj_err_ec);
      PyErr_SetString(PyExc_RuntimeError,
                      "Unable to add columnar::error::error_code to error_details.");
      return nullptr;
    }
    Py_DECREF(pyObj_err_ec);
  } else {
    PyObject* pyObj_err_ec = PyLong_FromLong(err.ec.value());
    if (-1 == PyDict_SetItemString(pyObj_error_details, "core_error_code", pyObj_err_ec)) {
      PyErr_Clear();
      Py_DECREF(pyObj_error_details);
      Py_DECREF(pyObj_err_ec);
      PyErr_SetString(PyExc_RuntimeError,
                      "Unable to add columnar::error::error_code to error_details.");
      return nullptr;
    }
    Py_DECREF(pyObj_err_ec);
  }

  auto msg = err.message.empty() ? err.ec.message() : err.message;
  PyObject* pyObj_msg = PyUnicode_FromString(msg.c_str());
  if (-1 == PyDict_SetItemString(pyObj_error_details, "message", pyObj_msg)) {
    PyErr_Clear();
    Py_DECREF(pyObj_error_details);
    Py_DECREF(pyObj_msg);
    PyErr_SetString(PyExc_RuntimeError, "Unable to add columnar::error::message to error_details.");
    return nullptr;
  }
  Py_DECREF(pyObj_msg);

  if (err.ec == couchbase::core::columnar::errc::query_error &&
      std::holds_alternative<couchbase::core::columnar::query_error_properties>(err.properties)) {
    PyObject* pyObj_error_properties = PyDict_New();
    auto properties = std::get<couchbase::core::columnar::query_error_properties>(err.properties);
    PyObject* pyObj_prop = PyLong_FromLong(properties.code);
    if (-1 == PyDict_SetItemString(pyObj_error_properties, "code", pyObj_prop)) {
      PyErr_Clear();
      Py_DECREF(pyObj_error_properties);
      Py_DECREF(pyObj_error_details);
      Py_DECREF(pyObj_prop);
      PyErr_SetString(PyExc_RuntimeError,
                      "Unable to add columnar::error::properties::code to error_details.");
      return nullptr;
    }
    Py_DECREF(pyObj_prop);

    pyObj_prop = PyUnicode_FromString(properties.server_message.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_properties, "server_message", pyObj_prop)) {
      PyErr_Clear();
      Py_DECREF(pyObj_error_properties);
      Py_DECREF(pyObj_error_details);
      Py_DECREF(pyObj_prop);
      PyErr_SetString(
        PyExc_RuntimeError,
        "Unable to add columnar::error::properties::server_message to error_details.");
      return nullptr;
    }
    Py_DECREF(pyObj_prop);

    if (-1 == PyDict_SetItemString(pyObj_error_details, "properties", pyObj_error_properties)) {
      PyErr_Clear();
      Py_DECREF(pyObj_error_details);
      Py_DECREF(pyObj_error_properties);
      PyErr_SetString(PyExc_RuntimeError,
                      "Unable to add columnar::error::properties to error_details.");
      return nullptr;
    }
    Py_DECREF(pyObj_error_properties);
  }

  PyObject* pyObj_ctx =
    PyUnicode_FromString(couchbase::core::utils::json::generate(err.ctx).c_str());
  if (-1 == PyDict_SetItemString(pyObj_error_details, "context", pyObj_ctx)) {
    PyErr_Clear();
    Py_DECREF(pyObj_error_details);
    Py_DECREF(pyObj_ctx);
    PyErr_SetString(PyExc_RuntimeError, "Unable to add columnar::error::ctx to error_details.");
    return nullptr;
  }
  Py_DECREF(pyObj_ctx);

  PyObject* pyObj_file = PyUnicode_FromString(file);
  if (-1 == PyDict_SetItemString(pyObj_error_details, "file", pyObj_file)) {
    PyErr_Clear();
    Py_DECREF(pyObj_error_details);
    PyErr_SetString(PyExc_RuntimeError, "Unable to add file to error_details.");
    return nullptr;
  }
  Py_DECREF(pyObj_file);

  PyObject* pyObj_line = PyLong_FromLong(line);
  if (-1 == PyDict_SetItemString(pyObj_error_details, "line", pyObj_line)) {
    PyErr_Clear();
    Py_DECREF(pyObj_error_details);
    Py_DECREF(pyObj_line);
    PyErr_SetString(PyExc_RuntimeError, "Unable to add line to error_details.");
    return nullptr;
  }
  Py_DECREF(pyObj_line);

  return pyObj_error_details;
}

PyObject*
pycbcc_build_error_details(CoreClientErrors::ErrorCode client_error_code,
                           const char* file,
                           int line,
                           const char* msg)
{
  PyObject* pyObj_error_details = PyDict_New();
  PyObject* pyObj_client_err_code = PyLong_FromLong(client_error_code);
  if (-1 == PyDict_SetItemString(pyObj_error_details, "client_error_code", pyObj_client_err_code)) {
    PyErr_Clear();
    Py_DECREF(pyObj_error_details);
    Py_DECREF(pyObj_client_err_code);
    PyErr_SetString(PyExc_RuntimeError, "Unable to add client_error_code to error_details.");
    return nullptr;
  }
  Py_DECREF(pyObj_client_err_code);

  PyObject* pyObj_msg = PyUnicode_FromString(msg);
  if (-1 == PyDict_SetItemString(pyObj_error_details, "message", pyObj_msg)) {
    PyErr_Clear();
    Py_DECREF(pyObj_error_details);
    Py_DECREF(pyObj_msg);
    PyErr_SetString(PyExc_RuntimeError, "Unable to add message to error_details.");
    return nullptr;
  }
  Py_DECREF(pyObj_msg);

  PyObject* pyObj_file = PyUnicode_FromString(file);
  if (-1 == PyDict_SetItemString(pyObj_error_details, "file", pyObj_file)) {
    PyErr_Clear();
    Py_DECREF(pyObj_error_details);
    Py_DECREF(pyObj_file);
    PyErr_SetString(PyExc_RuntimeError, "Unable to add file to error_details.");
    return nullptr;
  }
  Py_DECREF(pyObj_file);

  PyObject* pyObj_line = PyLong_FromLong(line);
  if (-1 == PyDict_SetItemString(pyObj_error_details, "line", pyObj_line)) {
    PyErr_Clear();
    Py_DECREF(pyObj_error_details);
    Py_DECREF(pyObj_line);
    PyErr_SetString(PyExc_RuntimeError, "Unable to add line to error_details.");
    return nullptr;
  }
  Py_DECREF(pyObj_line);

  return pyObj_error_details;
}

PyObject*
pycbcc_build_exception(couchbase::core::columnar::error err, const char* file, int line)
{
  PyObject* pyObj_error_details = pycbcc_build_error_details(err, file, line);
  if (pyObj_error_details == nullptr) {
    return nullptr;
  }

  core_error* core_err = create_core_error_obj();
  core_err->error_details = pyObj_error_details;
  Py_INCREF(core_err->error_details);
  PyObject* pyObj_core_err_instance = get_core_error_instance(core_err);
  if (pyObj_core_err_instance == nullptr) {
    Py_DECREF(core_err->error_details);
    PyErr_SetString(PyExc_RuntimeError, "Unable to build CoreColumnarError from bindings.");
    return nullptr;
  }
  return pyObj_core_err_instance;
}

PyObject*
pycbcc_build_exception(CoreClientErrors::ErrorCode client_error_code,
                       const char* file,
                       int line,
                       const char* msg,
                       bool check_inner_cause)
{
  PyObject *pyObj_type = nullptr, *pyObj_value = nullptr, *pyObj_traceback = nullptr;
  if (check_inner_cause) {
    PyErr_Fetch(&pyObj_type, &pyObj_value, &pyObj_traceback);
    PyErr_Clear();
  }

  PyObject* pyObj_error_details = pycbcc_build_error_details(client_error_code, file, line, msg);
  if (pyObj_error_details == nullptr) {
    return nullptr;
  }

  if (check_inner_cause && pyObj_type != nullptr) {
    PyErr_NormalizeException(&pyObj_type, &pyObj_value, &pyObj_traceback);
    if (-1 == PyDict_SetItemString(pyObj_error_details, "inner_cause", pyObj_value)) {
      PyErr_Clear();
      Py_DECREF(pyObj_type);
      Py_XDECREF(pyObj_value);
      Py_XDECREF(pyObj_traceback);
      Py_DECREF(pyObj_error_details);
      PyErr_SetString(PyExc_RuntimeError, "Unable to add inner_cause to error_details.");
      return nullptr;
    }
    Py_XDECREF(pyObj_type);
    Py_XDECREF(pyObj_value);
  }

  core_error* core_err = create_core_error_obj();
  core_err->error_details = pyObj_error_details;
  Py_INCREF(core_err->error_details);
  PyObject* pyObj_core_err_instance = get_core_error_instance(core_err);
  if (pyObj_core_err_instance == nullptr) {
    Py_DECREF(core_err->error_details);
    Py_XDECREF(pyObj_traceback);
    PyErr_SetString(PyExc_RuntimeError, "Unable to build CoreColumnarError from bindings.");
    return nullptr;
  }
  return pyObj_core_err_instance;
}

void
pycbcc_set_python_exception(CoreClientErrors::ErrorCode client_error_code,
                            const char* file,
                            int line,
                            const char* msg)
{
  PyObject *pyObj_type = nullptr, *pyObj_value = nullptr, *pyObj_traceback = nullptr;
  PyErr_Fetch(&pyObj_type, &pyObj_value, &pyObj_traceback);
  PyErr_Clear();

  PyObject* pyObj_error_details = pycbcc_build_error_details(client_error_code, file, line, msg);
  if (pyObj_error_details == nullptr) {
    return;
  }

  if (pyObj_type != nullptr) {
    PyErr_NormalizeException(&pyObj_type, &pyObj_value, &pyObj_traceback);
    if (-1 == PyDict_SetItemString(pyObj_error_details, "inner_cause", pyObj_value)) {
      PyErr_Clear();
      Py_DECREF(pyObj_type);
      Py_XDECREF(pyObj_value);
      Py_XDECREF(pyObj_traceback);
      Py_DECREF(pyObj_error_details);
      PyErr_SetString(PyExc_RuntimeError, "Unable to add inner_cause to error_details.");
      return;
    }
    Py_XDECREF(pyObj_type);
    Py_XDECREF(pyObj_value);
  }

  core_error* core_err = create_core_error_obj();
  core_err->error_details = pyObj_error_details;
  Py_INCREF(core_err->error_details);
  PyObject* pyObj_core_err_instance = get_core_error_instance(core_err);
  if (pyObj_core_err_instance == nullptr) {
    Py_DECREF(core_err->error_details);
    Py_XDECREF(pyObj_traceback);
    PyErr_SetString(PyExc_RuntimeError, "Unable to build CoreColumnarError from bindings.");
    return;
  }
  Py_INCREF(Py_TYPE(pyObj_core_err_instance));
  PyErr_Restore(
    (PyObject*)Py_TYPE(pyObj_core_err_instance), pyObj_core_err_instance, pyObj_traceback);
}

void
pycbcc_set_python_exception(couchbase::core::columnar::error err, const char* file, int line)
{
  PyObject* pyObj_core_err_instance = pycbcc_build_exception(err, file, line);
  if (pyObj_core_err_instance == nullptr) {
    return;
  }
  Py_INCREF(Py_TYPE(pyObj_core_err_instance));
  PyErr_Restore((PyObject*)Py_TYPE(pyObj_core_err_instance), pyObj_core_err_instance, nullptr);
}

PyObject*
build_exception([[maybe_unused]] PyObject* self, PyObject* args)
{
  int error_type = 0;
  int build_cpp_core_exception = 0;
  int set_inner_cause = 0;
  const char* arg_format = "i|ii";
  int ret =
    PyArg_ParseTuple(args, arg_format, &error_type, &build_cpp_core_exception, &set_inner_cause);
  if (!ret) {
    PyErr_Print();
    PyErr_SetString(PyExc_RuntimeError,
                    "Invalid argument provided. Cannot test building exception.");
    Py_RETURN_NONE;
  }
  if (build_cpp_core_exception) {
    if (static_cast<uint8_t>(couchbase::core::columnar::errc::generic) == error_type) {
      auto error = couchbase::core::columnar::error{ couchbase::core::columnar::errc::generic,
                                                     "Test generic error code." };
      return pycbcc_build_exception(error, __FILE__, __LINE__);
    } else if (static_cast<uint8_t>(couchbase::core::columnar::errc::invalid_credential) ==
               error_type) {
      auto error =
        couchbase::core::columnar::error{ couchbase::core::columnar::errc::invalid_credential,
                                          "Test invalid credential error code." };
      return pycbcc_build_exception(error, __FILE__, __LINE__);
    } else if (static_cast<uint8_t>(couchbase::core::columnar::errc::timeout) == error_type) {
      auto error = couchbase::core::columnar::error{ couchbase::core::columnar::errc::timeout,
                                                     "Test timeout error code." };
      return pycbcc_build_exception(error, __FILE__, __LINE__);
    } else if (static_cast<uint8_t>(couchbase::core::columnar::errc::query_error) == error_type) {
      auto error = couchbase::core::columnar::error{ couchbase::core::columnar::errc::query_error,
                                                     "Test query error code." };
      return pycbcc_build_exception(error, __FILE__, __LINE__);
    }
  } else {
    if (set_inner_cause) {
      PyErr_SetString(PyExc_RuntimeError, "Test to set bindings inner exception.");
    }
    if (CoreClientErrors::VALUE == error_type) {
      return pycbcc_build_exception(
        CoreClientErrors::VALUE, __FILE__, __LINE__, "Test to raise ValueError.", set_inner_cause);
    } else if (CoreClientErrors::RUNTIME == error_type) {
      return pycbcc_build_exception(CoreClientErrors::RUNTIME,
                                    __FILE__,
                                    __LINE__,
                                    "Test to raise RuntimeError.",
                                    set_inner_cause);
    } else if (CoreClientErrors::INTERNAL_SDK == error_type) {
      return pycbcc_build_exception(CoreClientErrors::INTERNAL_SDK,
                                    __FILE__,
                                    __LINE__,
                                    "Test to raise InternalSdkError.",
                                    set_inner_cause);
    }
  }

  Py_RETURN_NONE;
}

PyObject*
add_exception_objects(PyObject* pyObj_module)
{
  if (PyType_Ready(&pycbcc_core_error_type) < 0) {
    return nullptr;
  }
  Py_INCREF(&pycbcc_core_error_type);
  if (PyModule_AddObject(
        pyObj_module, "core_error", reinterpret_cast<PyObject*>(&pycbcc_core_error_type)) < 0) {
    Py_DECREF(&pycbcc_core_error_type);
    return nullptr;
  }
  return pyObj_module;
}
