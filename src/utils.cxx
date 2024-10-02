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

#include "utils.hxx"
#include "exceptions.hxx"

couchbase::core::utils::binary
PyObject_to_binary(PyObject* pyObj_value)
{
  char* buf;
  Py_ssize_t nbuf;
  if (PyBytes_AsStringAndSize(pyObj_value, &buf, &nbuf) == -1) {
    throw std::invalid_argument("Unable to determine bytes object from provided value.");
  }
  auto size = py_ssize_t_to_size_t(nbuf);
  return couchbase::core::utils::to_binary(reinterpret_cast<const char*>(buf), size);
}

PyObject*
binary_to_PyObject(couchbase::core::utils::binary value)
{
  auto buf = reinterpret_cast<const char*>(value.data());
  auto nbuf = size_t_to_py_ssize_t(value.size());
  return PyBytes_FromStringAndSize(buf, nbuf);
}

PyObject*
binary_to_PyObject_unicode(couchbase::core::utils::binary value)
{
  auto buf = reinterpret_cast<const char*>(value.data());
  auto nbuf = size_t_to_py_ssize_t(value.size());
  return PyUnicode_FromStringAndSize(buf, nbuf);
}

std::string
binary_to_string(couchbase::core::utils::binary value)
{
  auto json = couchbase::core::utils::json::parse_binary(value);
  return couchbase::core::utils::json::generate(json);
}

std::size_t
py_ssize_t_to_size_t(Py_ssize_t value)
{
  if (value < 0) {
    throw std::invalid_argument("Cannot convert provided Py_ssize_t value to size_t.");
  }

  return static_cast<std::size_t>(value);
}

Py_ssize_t
size_t_to_py_ssize_t(std::size_t value)
{
  if (value > INT_MAX) {
    throw std::invalid_argument("Cannot convert provided size_t value to Py_ssize_t.");
  }
  return static_cast<Py_ssize_t>(value);
}

std::chrono::milliseconds
pyObj_to_duration(PyObject* pyObj_duration)
{
  auto duration_str = std::string(PyUnicode_AsUTF8(pyObj_duration));
  try {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
      couchbase::core::utils::parse_duration(duration_str));
  } catch (const couchbase::core::utils::duration_parse_error& dpe) {
    auto msg =
      fmt::format(R"(Unable to parse duration (value: "{}"): {})", duration_str, dpe.what());
    throw std::invalid_argument(msg);
  } catch (const std::invalid_argument& ex1) {
    auto msg = fmt::format(
      R"(Unable to parse duration (value "{}" is not a number): {})", duration_str, ex1.what());
    throw std::invalid_argument(msg);
  } catch (const std::out_of_range& ex2) {
    auto msg = fmt::format(
      R"(Unable to parse duration (value "{}" is out of range): {})", duration_str, ex2.what());
    throw std::invalid_argument(msg);
  }
}
