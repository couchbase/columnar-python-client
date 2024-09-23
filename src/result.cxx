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

#include "result.hxx"
#include "client.hxx"
#include "exceptions.hxx"

#include <core/columnar/error.hxx>
#include <core/columnar/query_result.hxx>

/* result type methods */

static void
result_dealloc([[maybe_unused]] result* self)
{
  if (self->dict) {
    PyDict_Clear(self->dict);
    Py_DECREF(self->dict);
  }
  // CB_LOG_DEBUG("pycbcc - dealloc result: result->refcnt: {}, result->dict->refcnt: {}",
  // Py_REFCNT(self), Py_REFCNT(self->dict));
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject*
result__get__(result* self, PyObject* args)
{
  const char* field_name = nullptr;
  PyObject* default_value = nullptr;

  if (!PyArg_ParseTuple(args, "s|O", &field_name, &default_value)) {
    PyErr_Print();
    PyErr_Clear();
    Py_RETURN_NONE;
  }
  // PyDict_GetItem will return NULL if key doesn't exist; also suppresses errors
  PyObject* val = PyDict_GetItemString(self->dict, field_name);

  if (val == nullptr && default_value == nullptr) {
    Py_RETURN_NONE;
  }
  if (val == nullptr) {
    val = default_value;
  }
  Py_INCREF(val);
  if (default_value != nullptr) {
    Py_XDECREF(default_value);
  }

  return val;
}

static PyObject*
result__str__(result* self)
{
  const char* format_string = "result:{value=%S}";
  return PyUnicode_FromFormat(format_string, self->dict);
}

static PyMethodDef result_methods[] = {
  { "get", (PyCFunction)result__get__, METH_VARARGS, PyDoc_STR("get field in result object") },
  { NULL, NULL, 0, NULL }
};

static struct PyMemberDef result_members[] = { { "raw_result",
                                                 T_OBJECT_EX,
                                                 offsetof(result, dict),
                                                 0,
                                                 PyDoc_STR("Object for the raw result data.\n") },
                                               { NULL } };

static PyObject*
result__new__(PyTypeObject* type, PyObject*, PyObject*)
{
  result* self = reinterpret_cast<result*>(type->tp_alloc(type, 0));
  self->dict = PyDict_New();
  return reinterpret_cast<PyObject*>(self);
}

static PyTypeObject
init_result_type()
{
  PyTypeObject obj = {};
  obj.ob_base = PyVarObject_HEAD_INIT(NULL, 0) obj.tp_name = "pycbcc_core.result";
  obj.tp_doc = PyDoc_STR("Result of operation on client");
  obj.tp_basicsize = sizeof(result);
  obj.tp_itemsize = 0;
  obj.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  obj.tp_new = result__new__;
  obj.tp_dealloc = (destructor)result_dealloc;
  obj.tp_methods = result_methods;
  obj.tp_members = result_members;
  obj.tp_repr = (reprfunc)result__str__;
  return obj;
}

static PyTypeObject result_type = init_result_type();

PyObject*
create_result_obj()
{
  return PyObject_CallObject(reinterpret_cast<PyObject*>(&result_type), nullptr);
}

/* columnar_query_iterator type methods */

using columnar_query_result_variant = std::variant<std::monostate,
                                                   couchbase::core::columnar::query_result_row,
                                                   couchbase::core::columnar::query_result_end>;

PyObject*
get_columnar_metrics(couchbase::core::columnar::query_metrics metrics)
{
  PyObject* pyObj_metrics = PyDict_New();
  std::chrono::duration<unsigned long long, std::nano> int_nsec = metrics.elapsed_time;
  PyObject* pyObj_tmp = PyLong_FromUnsignedLongLong(int_nsec.count());
  if (-1 == PyDict_SetItemString(pyObj_metrics, "elapsed_time", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  int_nsec = metrics.execution_time;
  pyObj_tmp = PyLong_FromUnsignedLongLong(int_nsec.count());
  if (-1 == PyDict_SetItemString(pyObj_metrics, "execution_time", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.result_count);
  if (-1 == PyDict_SetItemString(pyObj_metrics, "result_count", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.result_size);
  if (-1 == PyDict_SetItemString(pyObj_metrics, "result_size", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.processed_objects);
  if (-1 == PyDict_SetItemString(pyObj_metrics, "processed_objects", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  return pyObj_metrics;
}

PyObject*
get_columnar_query_metadata(couchbase::core::columnar::query_metadata metadata)
{
  PyObject* pyObj_metadata = PyDict_New();
  PyObject* pyObj_tmp = PyUnicode_FromString(metadata.request_id.c_str());
  if (-1 == PyDict_SetItemString(pyObj_metadata, "request_id", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  PyObject* pyObj_warnings = PyList_New(static_cast<Py_ssize_t>(0));
  for (auto const& warning : metadata.warnings) {
    PyObject* pyObj_warning = PyDict_New();

    pyObj_tmp = PyLong_FromLong(warning.code);
    if (-1 == PyDict_SetItemString(pyObj_warning, "code", pyObj_tmp)) {
      PyErr_Print();
      PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(warning.message.c_str());
    if (-1 == PyDict_SetItemString(pyObj_warning, "message", pyObj_tmp)) {
      PyErr_Print();
      PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    if (-1 == PyList_Append(pyObj_warnings, pyObj_warning)) {
      PyErr_Print();
      PyErr_Clear();
    }
    Py_XDECREF(pyObj_warning);
  }

  if (-1 == PyDict_SetItemString(pyObj_metadata, "warnings", pyObj_warnings)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_warnings);

  PyObject* pyObject_metrics = get_columnar_metrics(metadata.metrics);
  if (-1 == PyDict_SetItemString(pyObj_metadata, "metrics", pyObject_metrics)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObject_metrics);

  return pyObj_metadata;
}

static void
columnar_query_iterator_dealloc(columnar_query_iterator* self)
{
  Py_XDECREF(self->row_callback);
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject*
columnar_query_iterator__cancel__(columnar_query_iterator* self)
{
  columnar_query_iterator* query_iter = reinterpret_cast<columnar_query_iterator*>(self);
  if (query_iter->pending_op_ && !query_iter->query_result_) {
    query_iter->pending_op_->cancel();
  } else if (query_iter->query_result_) {
    query_iter->query_result_->cancel();
  }

  Py_RETURN_NONE;
}

static PyObject*
columnar_query_iterator__wait_for_core_query_result__(columnar_query_iterator* self)
{
  columnar_query_iterator* query_iter = reinterpret_cast<columnar_query_iterator*>(self);
  auto future = query_iter->barrier_->get_future();
  PyObject* ret = nullptr;
  Py_BEGIN_ALLOW_THREADS ret = future.get();
  Py_END_ALLOW_THREADS return ret;
}

static PyObject*
columnar_query_iterator__metadata__(columnar_query_iterator* self)
{
  columnar_query_iterator* query_iter = reinterpret_cast<columnar_query_iterator*>(self);
  auto metadata = query_iter->query_result_->metadata();
  if (metadata.has_value()) {
    return get_columnar_query_metadata(metadata.value());
  }
  Py_RETURN_NONE;
}

// static PyObject*
// columnar_query_iterator__is_cancelled__(columnar_query_iterator* self)
// {
//   columnar_query_iterator* query_iter = reinterpret_cast<columnar_query_iterator*>(self);
//   if (query_iter->query_result_->is_cancelled()) {
//     Py_INCREF(Py_True);
//     return Py_True;
//   } else {
//     Py_INCREF(Py_False);
//     return Py_False;
//   }
// }

static PyMethodDef columnar_query_iterator_TABLE_methods[] = {
  { "cancel",
    (PyCFunction)columnar_query_iterator__cancel__,
    METH_NOARGS,
    PyDoc_STR("Cancel Columnar query stream.") },
  { "wait_for_core_query_result",
    (PyCFunction)columnar_query_iterator__wait_for_core_query_result__,
    METH_NOARGS,
    PyDoc_STR("Wait for query stream's query result.") },
  { "metadata",
    (PyCFunction)columnar_query_iterator__metadata__,
    METH_NOARGS,
    PyDoc_STR("Get Columnar query metadat.") },
  { NULL }
};

PyObject*
columnar_query_iterator_iter(PyObject* self)
{
  Py_INCREF(self);
  return self;
}

void
get_next_row(columnar_query_result_variant result,
             couchbase::core::columnar::error err,
             PyObject* pyObj_row_callback,
             std::shared_ptr<std::promise<PyObject*>> barrier = nullptr)
{
  auto set_exception = false;
  PyObject* pyObj_exc = nullptr;
  PyObject* pyObj_args = NULL;
  PyObject* pyObj_func = NULL;
  PyObject* pyObj_result = nullptr;
  PyObject* pyObj_callback_res = nullptr;

  PyGILState_STATE state = PyGILState_Ensure();
  if (err.ec) {
    pyObj_exc = pycbcc_build_exception(err, __FILE__, __LINE__);
    if (pyObj_row_callback == nullptr) {
      barrier->set_value(pyObj_exc);
    } else {
      pyObj_func = pyObj_row_callback;
      pyObj_args = PyTuple_New(1);
      PyTuple_SET_ITEM(pyObj_args, 0, pyObj_exc);
    }
    // lets clear any errors
    PyErr_Clear();
  } else {
    if (std::holds_alternative<couchbase::core::columnar::query_result_row>(result)) {
      auto row = std::get<couchbase::core::columnar::query_result_row>(result);
      pyObj_result = PyBytes_FromStringAndSize(row.content.c_str(), row.content.length());
    } else if (std::holds_alternative<couchbase::core::columnar::query_result_end>(result)) {
      Py_INCREF(Py_None);
      pyObj_result = Py_None;
    } else {
      pyObj_result = pycbcc_build_exception(err, __FILE__, __LINE__);
    }

    if (pyObj_row_callback == nullptr) {
      barrier->set_value(pyObj_result);
    } else {
      pyObj_func = pyObj_row_callback;
      pyObj_args = PyTuple_New(1);
      PyTuple_SET_ITEM(pyObj_args, 0, pyObj_result);
    }
  }

  if (pyObj_func != nullptr) {
    pyObj_callback_res = PyObject_CallObject(pyObj_func, pyObj_args);
    if (pyObj_callback_res) {
      Py_DECREF(pyObj_callback_res);
    } else {
      pycbcc_set_python_exception(CoreClientErrors::INTERNAL_SDK,
                                  __FILE__,
                                  __LINE__,
                                  "Columnar query next row callback failed.");
    }
    Py_DECREF(pyObj_args);
  }
  PyGILState_Release(state);
}

PyObject*
columnar_query_iterator_iternext(PyObject* self)
{
  columnar_query_iterator* query_iter = reinterpret_cast<columnar_query_iterator*>(self);
  PyObject* result = nullptr;
  std::shared_ptr<std::promise<PyObject*>> barrier = nullptr;
  std::future<PyObject*> fut;
  if (query_iter->row_callback == nullptr) {
    barrier = std::make_shared<std::promise<PyObject*>>();
    fut = barrier->get_future();
  }

  query_iter->query_result_->next_row(
    [row_callback = query_iter->row_callback,
     barrier](columnar_query_result_variant res, couchbase::core::columnar::error err) mutable {
      get_next_row(res, err, row_callback, barrier);
    });

  if (query_iter->row_callback == nullptr) {
    Py_BEGIN_ALLOW_THREADS result = fut.get();
    Py_END_ALLOW_THREADS

      if (result == nullptr)
    {
      PyObject* pyObj_exc = pycbcc_build_exception(
        CoreClientErrors::INTERNAL_SDK, __FILE__, __LINE__, "Error retrieving next query row.");
      return pyObj_exc;
    }
    return result;
  }
  // we don't want to return None as that signals we should retrieve metadata.
  Py_INCREF(Py_True);
  return Py_True;
}

static PyObject*
columnar_query_iterator__new__(PyTypeObject* type, PyObject*, PyObject*)
{
  columnar_query_iterator* self =
    reinterpret_cast<columnar_query_iterator*>(type->tp_alloc(type, 0));
  return reinterpret_cast<PyObject*>(self);
}

static PyTypeObject
init_columnar_query_iterator_type()
{
  PyTypeObject obj = {};
  obj.ob_base = PyVarObject_HEAD_INIT(NULL, 0) obj.tp_name = "pycbcc_core.columnar_query_iterator";
  obj.tp_doc = PyDoc_STR("Result of Columnar query operation on client");
  obj.tp_basicsize = sizeof(columnar_query_iterator);
  obj.tp_itemsize = 0;
  obj.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  obj.tp_new = columnar_query_iterator__new__;
  obj.tp_dealloc = (destructor)columnar_query_iterator_dealloc;
  obj.tp_methods = columnar_query_iterator_TABLE_methods;
  obj.tp_iter = columnar_query_iterator_iter;
  obj.tp_iternext = columnar_query_iterator_iternext;
  return obj;
}

static PyTypeObject columnar_query_iterator_type = init_columnar_query_iterator_type();

PyObject*
create_columnar_query_iterator_obj(PyObject* pyObj_row_callback)
{
  PyObject* pyObj_res =
    PyObject_CallObject(reinterpret_cast<PyObject*>(&columnar_query_iterator_type), nullptr);
  columnar_query_iterator* query_iter = reinterpret_cast<columnar_query_iterator*>(pyObj_res);
  if (pyObj_row_callback != nullptr) {
    query_iter->row_callback = pyObj_row_callback;
  }
  return reinterpret_cast<PyObject*>(query_iter);
}

PyObject*
add_result_objects(PyObject* pyObj_module)
{
  // result_type, need to DECREF previous types on failure
  if (PyType_Ready(&result_type) < 0) {
    return nullptr;
  }
  Py_INCREF(&result_type);
  if (PyModule_AddObject(pyObj_module, "result", reinterpret_cast<PyObject*>(&result_type)) < 0) {
    Py_DECREF(&result_type);
    return nullptr;
  }

  // columnar_query_iterator_type, need to DECREF previous types on failure
  if (PyType_Ready(&columnar_query_iterator_type) < 0) {
    Py_DECREF(&result_type);
    return nullptr;
  }
  Py_INCREF(&columnar_query_iterator_type);
  if (PyModule_AddObject(pyObj_module,
                         "columnar_query_iterator",
                         reinterpret_cast<PyObject*>(&columnar_query_iterator_type)) < 0) {
    Py_DECREF(&result_type);
    Py_DECREF(&columnar_query_iterator_type);
    return nullptr;
  }

  return pyObj_module;
}
