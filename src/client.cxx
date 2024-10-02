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

#include "client.hxx"
#include <cstdlib>
#include <structmember.h>

#include <core/meta/version.hxx>

#include "columnar_query.hxx"
#include "connection.hxx"
#include "exceptions.hxx"
#include "logger.hxx"
#include "result.hxx"
#include "utils.hxx"

void
add_core_enums(PyObject* pyObj_module)
{
  PyObject* pyObj_enum_module = PyImport_ImportModule("enum");
  if (!pyObj_enum_module) {
    return;
  }
  PyObject* pyObj_enum_class = PyObject_GetAttrString(pyObj_enum_module, "Enum");

  PyObject* pyObj_enum_values = PyUnicode_FromString(Operations::ALL_OPERATIONS());
  PyObject* pyObj_enum_name = PyUnicode_FromString("Operations");
  // PyTuple_Pack returns new reference, need to Py_DECREF values provided
  PyObject* pyObj_args = PyTuple_Pack(2, pyObj_enum_name, pyObj_enum_values);
  Py_DECREF(pyObj_enum_name);
  Py_DECREF(pyObj_enum_values);

  PyObject* pyObj_kwargs = PyDict_New();
  PyObject_SetItem(
    pyObj_kwargs, PyUnicode_FromString("module"), PyModule_GetNameObject(pyObj_module));
  PyObject* pyObj_operations = PyObject_Call(pyObj_enum_class, pyObj_args, pyObj_kwargs);
  Py_DECREF(pyObj_args);
  Py_DECREF(pyObj_kwargs);

  if (PyModule_AddObject(pyObj_module, "operations", pyObj_operations) < 0) {
    // only need to Py_DECREF on failure to add when using PyModule_AddObject()
    Py_XDECREF(pyObj_operations);
    return;
  }

  pyObj_enum_values = PyUnicode_FromString(CoreClientErrors::ALL_CORE_CLIENT_ERROR_CODES());
  pyObj_enum_name = PyUnicode_FromString("CoreClientErrorCode");
  // PyTuple_Pack returns new reference, need to Py_DECREF values provided
  pyObj_args = PyTuple_Pack(2, pyObj_enum_name, pyObj_enum_values);
  Py_DECREF(pyObj_enum_name);
  Py_DECREF(pyObj_enum_values);

  pyObj_kwargs = PyDict_New();
  PyObject_SetItem(
    pyObj_kwargs, PyUnicode_FromString("module"), PyModule_GetNameObject(pyObj_module));
  pyObj_operations = PyObject_Call(pyObj_enum_class, pyObj_args, pyObj_kwargs);
  Py_DECREF(pyObj_args);
  Py_DECREF(pyObj_kwargs);

  if (PyModule_AddObject(pyObj_module, "core_client_error_code", pyObj_operations) < 0) {
    // only need to Py_DECREF on failure to add when using PyModule_AddObject()
    Py_XDECREF(pyObj_operations);
    return;
  }
}

void
add_constants(PyObject* module)
{
  if (PyModule_AddIntConstant(module, "FMT_JSON", PYCBC_FMT_JSON) < 0) {
    Py_XDECREF(module);
    return;
  }
  if (PyModule_AddIntConstant(module, "FMT_BYTES", PYCBC_FMT_BYTES) < 0) {
    Py_XDECREF(module);
    return;
  }
  if (PyModule_AddIntConstant(module, "FMT_UTF8", PYCBC_FMT_UTF8) < 0) {
    Py_XDECREF(module);
    return;
  }
  if (PyModule_AddIntConstant(module, "FMT_PICKLE", PYCBC_FMT_PICKLE) < 0) {
    Py_XDECREF(module);
    return;
  }
  if (PyModule_AddIntConstant(module, "FMT_LEGACY_MASK", PYCBC_FMT_LEGACY_MASK) < 0) {
    Py_XDECREF(module);
    return;
  }
  if (PyModule_AddIntConstant(module, "FMT_COMMON_MASK", PYCBC_FMT_COMMON_MASK) < 0) {
    Py_XDECREF(module);
    return;
  }
  auto cxxcbc_metadata = couchbase::core::meta::sdk_build_info_json();
  if (PyModule_AddStringConstant(module, "CXXCBC_METADATA", cxxcbc_metadata.c_str())) {
    Py_XDECREF(module);
    return;
  }
}

static PyObject*
columnar_query(PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* res = handle_columnar_query(self, args, kwargs);
  if (res == nullptr && PyErr_Occurred() == nullptr) {
    pycbcc_set_python_exception(CoreClientErrors::INTERNAL_SDK,
                                __FILE__,
                                __LINE__,
                                "Unable to perform Columnar query operation.");
  }
  return res;
}

static PyObject*
create_connection(PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* res = handle_create_connection(self, args, kwargs);
  if (res == nullptr && PyErr_Occurred() == nullptr) {
    pycbcc_set_python_exception(
      CoreClientErrors::INTERNAL_SDK, __FILE__, __LINE__, "Unable to create connection.");
  }
  return res;
}

static PyObject*
test_create_connection(PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* res = handle_create_connection_test(self, args, kwargs);
  if (res == nullptr && PyErr_Occurred() == nullptr) {
    pycbcc_set_python_exception(
      CoreClientErrors::INTERNAL_SDK, __FILE__, __LINE__, "Unable to create connection.");
  }
  return res;
}

static PyObject*
test_exception_builder(PyObject* self, PyObject* args)
{
  return build_exception(self, args);
}

static PyObject*
close_connection(PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* res = handle_close_connection(self, args, kwargs);
  if (res == nullptr && PyErr_Occurred() != nullptr) {
    pycbcc_set_python_exception(
      CoreClientErrors::INTERNAL_SDK, __FILE__, __LINE__, "Unable to close connection.");
  }
  return res;
}

static struct PyMethodDef methods[] = { { "create_connection",
                                          (PyCFunction)create_connection,
                                          METH_VARARGS | METH_KEYWORDS,
                                          "Create connection object" },
                                        { "close_connection",
                                          (PyCFunction)close_connection,
                                          METH_VARARGS | METH_KEYWORDS,
                                          "Close a connection" },
                                        { "columnar_query",
                                          (PyCFunction)columnar_query,
                                          METH_VARARGS | METH_KEYWORDS,
                                          "Execute a streaming columnar query" },
                                        { "_test_exception_builder",
                                          (PyCFunction)test_exception_builder,
                                          METH_VARARGS,
                                          "Test method to build exceptions from bindings" },
                                        { "_test_create_connection",
                                          (PyCFunction)test_create_connection,
                                          METH_VARARGS | METH_KEYWORDS,
                                          "Test creating a connection" },
                                        { nullptr, nullptr, 0, nullptr } };

static struct PyModuleDef pycbcc_core_module = { { PyObject_HEAD_INIT(NULL) nullptr, 0, nullptr },
                                                 "pycbcc_core",
                                                 "Python interface to couchbase-cxx-client",
                                                 -1,
                                                 methods,
                                                 nullptr,
                                                 nullptr,
                                                 nullptr,
                                                 nullptr };

PyMODINIT_FUNC
PyInit_pycbcc_core(void)
{
  Py_Initialize();
  PyObject* m = PyModule_Create(&pycbcc_core_module);
  if (m == nullptr) {
    return nullptr;
  }

  if (add_result_objects(m) == nullptr) {
    Py_DECREF(m);
    return nullptr;
  }

  if (add_exception_objects(m) == nullptr) {
    Py_DECREF(m);
    return nullptr;
  }

  if (add_logger_objects(m) == nullptr) {
    Py_DECREF(m);
    return nullptr;
  }

  add_core_enums(m);
  add_constants(m);
  return m;
}
