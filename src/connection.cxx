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

#include "connection.hxx"

#include <core/io/ip_protocol.hxx>
#include <core/utils/connection_string.hxx>

#include "exceptions.hxx"
#include "utils.hxx"

couchbase::core::io::ip_protocol
pyObj_to_ip_protocol(std::string ip_protocol)
{
  if (ip_protocol.compare("force_ipv4") == 0) {
    return couchbase::core::io::ip_protocol::force_ipv4;
  } else if (ip_protocol.compare("force_ipv6") == 0) {
    return couchbase::core::io::ip_protocol::force_ipv6;
  } else {
    return couchbase::core::io::ip_protocol::any;
  }
}

PyObject*
ip_protocol_to_pyObj(couchbase::core::io::ip_protocol ip_protocol)
{
  if (ip_protocol == couchbase::core::io::ip_protocol::force_ipv4) {
    return PyUnicode_FromString("force_ipv4");
  } else if (ip_protocol == couchbase::core::io::ip_protocol::force_ipv6) {
    return PyUnicode_FromString("force_ipv6");
  } else {
    return PyUnicode_FromString("any");
  }
}

couchbase::core::tls_verify_mode
pyObj_to_tls_verify_mode(std::string tls_verify_mode)
{
  if (tls_verify_mode.compare("none") == 0) {
    return couchbase::core::tls_verify_mode::none;
  } else if (tls_verify_mode.compare("peer") == 0) {
    return couchbase::core::tls_verify_mode::peer;
  } else {
    return couchbase::core::tls_verify_mode::none;
  }
}

PyObject*
tls_verify_mode_to_pyObj(couchbase::core::tls_verify_mode tls_verify_mode)
{
  if (tls_verify_mode == couchbase::core::tls_verify_mode::none) {
    return PyUnicode_FromString("none");
  } else if (tls_verify_mode == couchbase::core::tls_verify_mode::peer) {
    return PyUnicode_FromString("peer");
  } else {
    return PyUnicode_FromString("none");
  }
}

static void
dealloc_conn(PyObject* obj)
{
  auto conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(obj, "conn_"));
  if (conn) {
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    conn->cluster_.close([barrier]() {
      barrier->set_value();
    });
    f.get();
    conn->io_.stop();
    for (auto& t : conn->io_threads_) {
      if (t.joinable()) {
        t.join();
      }
    }
  }
  CB_LOG_DEBUG("{}: dealloc_conn completed", "PYCBCC");
  delete conn;
}

void
close_connection_callback(PyObject* pyObj_conn,
                          PyObject* pyObj_callback,
                          PyObject* pyObj_errback,
                          std::shared_ptr<std::promise<PyObject*>> barrier)
{
  PyObject* pyObj_args = NULL;
  PyObject* pyObj_func = NULL;
  PyObject* pyObj_callback_res = nullptr;

  PyGILState_STATE state = PyGILState_Ensure();

  if (pyObj_callback == nullptr) {
    barrier->set_value(PyBool_FromLong(static_cast<long>(1)));
  } else {
    pyObj_func = pyObj_callback;
    pyObj_args = PyTuple_New(1);
    PyTuple_SET_ITEM(pyObj_args, 0, PyBool_FromLong(static_cast<long>(1)));
  }

  if (pyObj_func != nullptr) {
    pyObj_callback_res = PyObject_CallObject(pyObj_func, pyObj_args);
    CB_LOG_DEBUG("{}: return from close conn callback.", "PYCBCC");
    if (pyObj_callback_res) {
      Py_DECREF(pyObj_callback_res);
    } else {
      pycbcc_set_python_exception(
        CoreClientErrors::INTERNAL_SDK, __FILE__, __LINE__, "Close connection callback failed.");
    }
    Py_DECREF(pyObj_args);
    Py_XDECREF(pyObj_callback);
    Py_XDECREF(pyObj_errback);
  }
  CB_LOG_DEBUG("{}: close conn callback completed", "PYCBCC");
  auto conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
  conn->io_.stop();
  // the pyObj_conn was incref'd before being passed into this callback, decref it here
  Py_DECREF(pyObj_conn);
  PyGILState_Release(state);
}

void
create_connection_callback(PyObject* pyObj_conn,
                           std::error_code ec,
                           std::shared_ptr<std::promise<PyObject*>> barrier)
{
  PyObject* pyObj_exc = nullptr;

  PyGILState_STATE state = PyGILState_Ensure();
  if (ec.value()) {
    auto error = couchbase::core::columnar::error{ ec, ec.message() };
    pyObj_exc = pycbcc_build_exception(error, __FILE__, __LINE__);
    barrier->set_value(pyObj_exc);
  } else {
    barrier->set_value(pyObj_conn);
  }
  Py_DECREF(pyObj_conn);
  CB_LOG_DEBUG("{}: create conn callback completed", "PYCBCC");
  PyGILState_Release(state);
}

couchbase::core::cluster_credentials
get_cluster_credentials(PyObject* pyObj_credentials)
{
  couchbase::core::cluster_credentials creds{};
  PyObject* pyObj_username = PyDict_GetItemString(pyObj_credentials, "username");
  if (pyObj_username != nullptr) {
    auto username = std::string(PyUnicode_AsUTF8(pyObj_username));
    creds.username = username;
  }

  PyObject* pyObj_password = PyDict_GetItemString(pyObj_credentials, "password");
  if (pyObj_password != nullptr) {
    auto pw = std::string(PyUnicode_AsUTF8(pyObj_password));
    creds.password = pw;
  }

  // columnar default for SASL mechanism
  creds.allowed_sasl_mechanisms = { "PLAIN" };

  return creds;
}

void
update_timeout_config(couchbase::core::cluster_options& options,
                      couchbase::core::columnar::timeout_config& timeouts,
                      PyObject* pyObj_connstr_timeout_opts)
{
  size_t nargs = static_cast<size_t>(PyList_Size(pyObj_connstr_timeout_opts));
  size_t ii;
  for (ii = 0; ii < nargs; ++ii) {
    PyObject* pyObj_opt = PyList_GetItem(pyObj_connstr_timeout_opts, ii);
    if (!pyObj_opt) {
      throw std::runtime_error("Unable to parse connstr timeout option key.");
    }
    std::string k;
    if (PyUnicode_Check(pyObj_opt)) {
      k = std::string(PyUnicode_AsUTF8(pyObj_opt));
    }
    if (k.empty()) {
      throw std::runtime_error("Unable to parse connstr timeout option key.");
    }
    if (k == "query_timeout") {
      timeouts.query_timeout = options.query_timeout;
    }
  }
}

void
update_cluster_timeout_options(couchbase::core::cluster_options& options,
                               couchbase::core::columnar::timeout_config& timeouts,
                               PyObject* pyObj_timeout_opts,
                               PyObject* pyObj_connstr_timeout_opts)
{
  PyObject* pyObj_bootstrap_timeout = PyDict_GetItemString(pyObj_timeout_opts, "bootstrap_timeout");
  if (pyObj_bootstrap_timeout != nullptr) {
    auto bootstrap_timeout =
      static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_bootstrap_timeout));
    auto bootstrap_timeout_ms =
      std::chrono::milliseconds(std::max(0ULL, bootstrap_timeout / 1000ULL));
    options.bootstrap_timeout = bootstrap_timeout_ms;
  }

  PyObject* pyObj_dispatch_timeout = PyDict_GetItemString(pyObj_timeout_opts, "dispatch_timeout");
  if (pyObj_dispatch_timeout != nullptr) {
    auto dispatch_timeout =
      static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_dispatch_timeout));
    auto dispatch_timeout_ms =
      std::chrono::milliseconds(std::max(0ULL, dispatch_timeout / 1000ULL));
    options.dispatch_timeout = dispatch_timeout_ms;
  }

  PyObject* pyObj_resolve_timeout = PyDict_GetItemString(pyObj_timeout_opts, "resolve_timeout");
  if (pyObj_resolve_timeout != nullptr) {
    auto resolve_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_resolve_timeout));
    auto resolve_timeout_ms = std::chrono::milliseconds(std::max(0ULL, resolve_timeout / 1000ULL));
    options.resolve_timeout = resolve_timeout_ms;
  }

  PyObject* pyObj_connect_timeout = PyDict_GetItemString(pyObj_timeout_opts, "connect_timeout");
  if (pyObj_connect_timeout != nullptr) {
    auto connect_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_connect_timeout));
    auto connect_timeout_ms = std::chrono::milliseconds(std::max(0ULL, connect_timeout / 1000ULL));
    options.connect_timeout = connect_timeout_ms;
  }

  // TODO: Update this to set management_timeout in timeout_config instead, once C++ core is updated
  PyObject* pyObj_management_timeout =
    PyDict_GetItemString(pyObj_timeout_opts, "management_timeout");
  if (pyObj_management_timeout != nullptr) {
    auto management_timeout =
      static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_management_timeout));
    auto management_timeout_ms =
      std::chrono::milliseconds(std::max(0ULL, management_timeout / 1000ULL));
    options.management_timeout = management_timeout_ms;
  }

  // query_timeout should be set in the Columnar Agent's timeout_config
  PyObject* pyObj_query_timeout = PyDict_GetItemString(pyObj_timeout_opts, "query_timeout");
  if (pyObj_query_timeout != nullptr) {
    auto query_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_query_timeout));
    auto query_timeout_ms = std::chrono::milliseconds(std::max(0ULL, query_timeout / 1000ULL));
    timeouts.query_timeout = query_timeout_ms;
  } else if (pyObj_connstr_timeout_opts != nullptr && PyList_Check(pyObj_connstr_timeout_opts)) {
    // this means user specified query timeout in connstr
    update_timeout_config(options, timeouts, pyObj_connstr_timeout_opts);
  }
}

void
update_cluster_security_options(couchbase::core::cluster_options& options,
                                PyObject* pyObj_security_opts)
{
  PyObject* pyObj_trust_only_capella =
    PyDict_GetItemString(pyObj_security_opts, "trust_only_capella");
  if (pyObj_trust_only_capella != nullptr && pyObj_trust_only_capella == Py_False) {
    options.security_options.trust_only_capella = false;
  }

  PyObject* pyObj_trust_only_pem_file =
    PyDict_GetItemString(pyObj_security_opts, "trust_only_pem_file");
  if (pyObj_trust_only_pem_file != nullptr) {
    options.security_options.trust_only_capella = false;
    options.security_options.trust_only_pem_file = true;
    options.trust_certificate = std::string(PyUnicode_AsUTF8(pyObj_trust_only_pem_file));
  }

  PyObject* pyObj_trust_only_pem_str =
    PyDict_GetItemString(pyObj_security_opts, "trust_only_pem_str");
  if (pyObj_trust_only_pem_str != nullptr) {
    options.security_options.trust_only_capella = false;
    options.security_options.trust_only_pem_string = true;
    options.trust_certificate_value = std::string(PyUnicode_AsUTF8(pyObj_trust_only_pem_str));
  }

  PyObject* pyObj_trust_only_certificates =
    PyDict_GetItemString(pyObj_security_opts, "trust_only_certificates");
  if (pyObj_trust_only_certificates && PyList_Check(pyObj_trust_only_certificates)) {
    options.security_options.trust_only_capella = false;
    std::vector<std::string> certificates{};
    size_t nargs = static_cast<size_t>(PyList_Size(pyObj_trust_only_certificates));
    size_t ii;
    for (ii = 0; ii < nargs; ++ii) {
      PyObject* pyObj_cert = PyList_GetItem(pyObj_trust_only_certificates, ii);
      if (!pyObj_cert) {
        CB_LOG_WARNING(
          "{}: Unable to get certificate from certificate list.  Index={}", "PYCBCC", ii);
        continue;
      }
      Py_INCREF(pyObj_cert);
      certificates.emplace_back(std::string(PyUnicode_AsUTF8(pyObj_cert)));
      Py_DECREF(pyObj_cert);
      pyObj_cert = nullptr;
    }
    if (!certificates.empty()) {
      options.security_options.trust_only_certificates = certificates;
    }
  }

  PyObject* pyObj_trust_only_platform =
    PyDict_GetItemString(pyObj_security_opts, "trust_only_platform");
  if (pyObj_trust_only_platform != nullptr && pyObj_trust_only_platform == Py_True) {
    options.security_options.trust_only_capella = false;
    options.security_options.trust_only_platform = true;
  }

  // TODO: Remove until supported at a later date
  // PyObject* pyObj_cipher_suites = PyDict_GetItemString(pyObj_security_opts, "cipher_suites");
  // if (pyObj_cipher_suites && PyList_Check(pyObj_cipher_suites)) {
  //   std::vector<std::string> ciphers{};
  //   size_t nargs = static_cast<size_t>(PyList_Size(pyObj_cipher_suites));
  //   size_t ii;
  //   for (ii = 0; ii < nargs; ++ii) {
  //     PyObject* pyObj_cipher = PyList_GetItem(pyObj_cipher_suites, ii);
  //     if (!pyObj_cipher) {
  //       CB_LOG_WARNING("{}: Unable to get cipher from cipher suite list.  Index={}", "PYCBCC",
  //       ii); continue;
  //     }
  //     Py_INCREF(pyObj_cipher);
  //     ciphers.emplace_back(std::string(PyUnicode_AsUTF8(pyObj_cipher)));
  //     Py_DECREF(pyObj_cipher);
  //     pyObj_cipher = nullptr;
  //   }
  //   if (!ciphers.empty()) {
  //     options.security_options.cipher_suites = ciphers;
  //   }
  // }

  PyObject* pyObj_verify_server_cert =
    PyDict_GetItemString(pyObj_security_opts, "disable_server_certificate_verification");
  if (pyObj_verify_server_cert != nullptr && pyObj_verify_server_cert == Py_True) {
    options.tls_verify = couchbase::core::tls_verify_mode::none;
  }
}

void
update_cluster_options(couchbase::core::cluster_options& options,
                       couchbase::core::columnar::timeout_config& timeouts,
                       PyObject* pyObj_options,
                       PyObject* pyObj_connstr_timeout_opts)
{
  PyObject* pyObj_timeout_opts = PyDict_GetItemString(pyObj_options, "timeout_options");
  if (pyObj_timeout_opts != nullptr) {
    update_cluster_timeout_options(
      options, timeouts, pyObj_timeout_opts, pyObj_connstr_timeout_opts);
  } else if (pyObj_connstr_timeout_opts != nullptr && PyList_Check(pyObj_connstr_timeout_opts)) {
    // this means user specified timeout in connstr
    update_timeout_config(options, timeouts, pyObj_connstr_timeout_opts);
  }

  PyObject* pyObj_security_opts = PyDict_GetItemString(pyObj_options, "security_options");
  if (pyObj_security_opts != nullptr) {
    update_cluster_security_options(options, pyObj_security_opts);
  }

  // if we set the certificate via the connstr, we need to also set the security_option bool
  if (!options.trust_certificate.empty() && !options.security_options.trust_only_pem_file) {
    options.security_options.trust_only_pem_file = true;
  }

  PyObject* pyObj_use_ip_protocol = PyDict_GetItemString(pyObj_options, "use_ip_protocol");
  if (pyObj_use_ip_protocol != nullptr) {
    options.use_ip_protocol =
      pyObj_to_ip_protocol(std::string(PyUnicode_AsUTF8(pyObj_use_ip_protocol)));
  }

  PyObject* pyObj_enable_dns_srv = PyDict_GetItemString(pyObj_options, "enable_dns_srv");
  if (pyObj_enable_dns_srv != nullptr && pyObj_enable_dns_srv == Py_False) {
    options.enable_dns_srv = false;
  }

  PyObject* pyObj_enable_clustermap_notification =
    PyDict_GetItemString(pyObj_options, "enable_clustermap_notification");
  if (pyObj_enable_clustermap_notification != nullptr &&
      pyObj_enable_clustermap_notification == Py_False) {
    options.enable_clustermap_notification = false;
  }

  PyObject* pyObj_network = PyDict_GetItemString(pyObj_options, "network");
  if (pyObj_network != nullptr) {
    auto network = std::string(PyUnicode_AsUTF8(pyObj_network));
    options.network = network;
  }

  PyObject* pyObj_config_poll_interval =
    PyDict_GetItemString(pyObj_options, "config_poll_interval");
  if (pyObj_config_poll_interval != nullptr) {
    auto config_poll_interval =
      static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_config_poll_interval));
    auto config_poll_interval_ms =
      std::chrono::milliseconds(std::max(0ULL, config_poll_interval / 1000ULL));
    options.config_poll_interval = config_poll_interval_ms;
  }

  PyObject* pyObj_config_poll_floor = PyDict_GetItemString(pyObj_options, "config_poll_floor");
  if (pyObj_config_poll_floor != nullptr) {
    auto config_poll_floor =
      static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_config_poll_floor));
    auto config_poll_floor_ms =
      std::chrono::milliseconds(std::max(0ULL, config_poll_floor / 1000ULL));
    options.config_poll_floor = config_poll_floor_ms;
  }

  PyObject* pyObj_user_agent_extra = PyDict_GetItemString(pyObj_options, "user_agent_extra");
  if (pyObj_user_agent_extra != nullptr) {
    auto user_agent_extra = std::string(PyUnicode_AsUTF8(pyObj_user_agent_extra));
    options.user_agent_extra = user_agent_extra;
  }

  PyObject* pyObj_dns_nameserver = PyDict_GetItemString(pyObj_options, "dns_nameserver");
  PyObject* pyObj_dns_port = PyDict_GetItemString(pyObj_options, "dns_port");
  PyObject* pyObj_dns_srv_timeout = PyDict_GetItemString(pyObj_options, "dns_srv_timeout");
  auto dns_timeout_from_connstr = false;
  if (pyObj_dns_srv_timeout != nullptr) {
    dns_timeout_from_connstr = true;
  } else if (pyObj_dns_srv_timeout == nullptr && pyObj_timeout_opts != nullptr) {
    pyObj_dns_srv_timeout = PyDict_GetItemString(pyObj_timeout_opts, "dns_srv_timeout");
  }
  if (pyObj_dns_srv_timeout != nullptr || pyObj_dns_nameserver != nullptr ||
      pyObj_dns_port != nullptr) {
    auto nameserver = pyObj_dns_nameserver != nullptr
                        ? std::string(PyUnicode_AsUTF8(pyObj_dns_nameserver))
                        : options.dns_config.nameserver();
    auto port = pyObj_dns_port != nullptr
                  ? static_cast<uint16_t>(PyLong_AsUnsignedLong(pyObj_dns_port))
                  : options.dns_config.port();
    auto dns_srv_timeout_ms = couchbase::core::timeout_defaults::dns_srv_timeout;
    if (pyObj_dns_srv_timeout != nullptr) {
      if (dns_timeout_from_connstr) {
        dns_srv_timeout_ms = pyObj_to_duration(pyObj_dns_srv_timeout);
      } else {
        auto dns_srv_timeout =
          static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_dns_srv_timeout));
        dns_srv_timeout_ms = std::chrono::milliseconds(std::max(0ULL, dns_srv_timeout / 1000ULL));
      }
    }
    options.dns_config = couchbase::core::io::dns::dns_config(nameserver, port, dns_srv_timeout_ms);
  }

  PyObject* pyObj_dump_configuration = PyDict_GetItemString(pyObj_options, "dump_configuration");
  if (pyObj_dump_configuration != nullptr && pyObj_dump_configuration == Py_True) {
    options.dump_configuration = true;
  }

  // disable tracing and metrics for now
  options.enable_tracing = false;
  options.enable_metrics = false;
}

std::optional<std::tuple<couchbase::core::utils::connection_string,
                         couchbase::core::cluster_credentials,
                         couchbase::core::columnar::timeout_config>>
get_connection_config(char* conn_str,
                      PyObject* pyObj_credential,
                      PyObject* pyObj_options,
                      PyObject* pyObj_connstr_timeout_opts)
{
  couchbase::core::utils::connection_string connection_str =
    couchbase::core::utils::parse_connection_string(conn_str);

  if (!connection_str.warnings.empty()) {
    auto msg = fmt::format(R"(Invalid option(s) found. Details: {})",
                           couchbase::core::utils::join_strings(connection_str.warnings, ","));
    pycbcc_set_python_exception(CoreClientErrors::VALUE, __FILE__, __LINE__, msg.c_str());
    return std::nullopt;
  }
  if (connection_str.error.has_value()) {
    auto msg =
      fmt::format("Error parsing connection string. Details: {}", connection_str.error.value());
    pycbcc_set_python_exception(CoreClientErrors::VALUE, __FILE__, __LINE__, msg.c_str());
    return std::nullopt;
  }

  couchbase::core::cluster_credentials auth = get_cluster_credentials(pyObj_credential);
  couchbase::core::columnar::timeout_config timeouts;

  try {
    update_cluster_options(
      connection_str.options, timeouts, pyObj_options, pyObj_connstr_timeout_opts);
    return std::make_tuple(connection_str, auth, timeouts);
  } catch (const std::invalid_argument& e) {
    pycbcc_set_python_exception(CoreClientErrors::VALUE, __FILE__, __LINE__, e.what());
    return std::nullopt;
  } catch (const std::exception& e) {
    PyErr_SetString(PyExc_Exception, e.what());
    return std::nullopt;
  }
}

PyObject*
handle_create_connection([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
  char* conn_str = nullptr;
  PyObject* pyObj_credential = nullptr;
  PyObject* pyObj_options = nullptr;
  PyObject* pyObj_connstr_timeout_opts = nullptr;
  PyObject* pyObj_result = nullptr;

  static const char* kw_list[] = {
    "", "credential", "options", "connstr_timeout_options", nullptr
  };

  const char* kw_format = "s|OOO";
  int ret = PyArg_ParseTupleAndKeywords(args,
                                        kwargs,
                                        kw_format,
                                        const_cast<char**>(kw_list),
                                        &conn_str,
                                        &pyObj_credential,
                                        &pyObj_options,
                                        &pyObj_connstr_timeout_opts);

  if (!ret) {
    std::string msg = "Cannot create connection. Unable to parse args/kwargs.";
    pycbcc_set_python_exception(CoreClientErrors::VALUE, __FILE__, __LINE__, msg.c_str());
    return nullptr;
  }

  auto connection_config =
    get_connection_config(conn_str, pyObj_credential, pyObj_options, pyObj_connstr_timeout_opts);
  if (!connection_config.has_value()) {
    // exception has already been set
    return nullptr;
  }
  PyObject* pyObj_num_io_threads = PyDict_GetItemString(pyObj_options, "num_io_threads");
  int num_io_threads = 1;
  if (pyObj_num_io_threads != nullptr) {
    num_io_threads = static_cast<uint32_t>(PyLong_AsUnsignedLong(pyObj_num_io_threads));
  }

  connection* const conn = new connection(num_io_threads, std::get<2>(connection_config.value()));
  PyObject* pyObj_conn = PyCapsule_New(conn, "conn_", dealloc_conn);

  if (pyObj_conn == nullptr) {
    pycbcc_set_python_exception(CoreClientErrors::INTERNAL_SDK,
                                __FILE__,
                                __LINE__,
                                "Cannot create connection. Unable to create PyCapsule.");
    return nullptr;
  }

  Py_XINCREF(pyObj_conn);
  auto barrier = std::make_shared<std::promise<PyObject*>>();
  auto f = barrier->get_future();
  int callback_count = 0;
  Py_BEGIN_ALLOW_THREADS conn->cluster_.open_in_background(
    couchbase::core::origin(std::get<1>(connection_config.value()),
                            std::get<0>(connection_config.value())),
    [pyObj_conn, callback_count, barrier](std::error_code ec) mutable {
      if (callback_count == 0) {
        create_connection_callback(pyObj_conn, ec, barrier);
      }
      callback_count++;
    });
  pyObj_result = f.get();
  Py_END_ALLOW_THREADS return pyObj_result;
}

PyObject*
handle_create_connection_test([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
  char* conn_str = nullptr;
  PyObject* pyObj_credential = nullptr;
  PyObject* pyObj_options = nullptr;
  PyObject* pyObj_connstr_timeout_opts = nullptr;

  static const char* kw_list[] = {
    "", "credential", "options", "connstr_timeout_options", nullptr
  };

  const char* kw_format = "s|OOO";
  int ret = PyArg_ParseTupleAndKeywords(args,
                                        kwargs,
                                        kw_format,
                                        const_cast<char**>(kw_list),
                                        &conn_str,
                                        &pyObj_credential,
                                        &pyObj_options,
                                        &pyObj_connstr_timeout_opts);

  if (!ret) {
    std::string msg = "Cannot create connection. Unable to parse args/kwargs.";
    pycbcc_set_python_exception(CoreClientErrors::VALUE, __FILE__, __LINE__, msg.c_str());
    return nullptr;
  }

  auto connection_config =
    get_connection_config(conn_str, pyObj_credential, pyObj_options, pyObj_connstr_timeout_opts);
  if (!connection_config.has_value()) {
    return nullptr;
  }

  PyObject* pyObj_conn_options = PyDict_New();
  PyObject* pyObj_timeout_opts = PyDict_New();
  couchbase::core::cluster_options opts = std::get<0>(connection_config.value()).options;
  couchbase::core::columnar::timeout_config timeouts = std::get<2>(connection_config.value());
  std::chrono::duration<unsigned long long, std::milli> int_msec = opts.bootstrap_timeout;
  PyObject* pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
  if (-1 == PyDict_SetItemString(pyObj_timeout_opts, "connect_timeout", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  int_msec = opts.resolve_timeout;
  pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
  if (-1 == PyDict_SetItemString(pyObj_timeout_opts, "resolve_timeout", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  int_msec = opts.connect_timeout;
  pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
  if (-1 == PyDict_SetItemString(pyObj_timeout_opts, "socket_connect_timeout", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  int_msec = opts.dispatch_timeout;
  pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
  if (-1 == PyDict_SetItemString(pyObj_timeout_opts, "dispatch_timeout", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  int_msec = opts.management_timeout;
  pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
  if (-1 == PyDict_SetItemString(pyObj_timeout_opts, "management_timeout", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  int_msec = opts.dns_config.timeout();
  pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
  if (-1 == PyDict_SetItemString(pyObj_timeout_opts, "dns_srv_timeout", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  int_msec = timeouts.query_timeout;
  pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
  if (-1 == PyDict_SetItemString(pyObj_timeout_opts, "query_timeout", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  if (-1 == PyDict_SetItemString(pyObj_conn_options, "timeout_options", pyObj_timeout_opts)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_timeout_opts);

  PyObject* pyObj_security_opts = PyDict_New();

  pyObj_tmp = PyUnicode_FromString(opts.trust_certificate.c_str());
  if (-1 == PyDict_SetItemString(pyObj_security_opts, "trust_certificate", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  pyObj_tmp = tls_verify_mode_to_pyObj(opts.tls_verify);
  if (-1 == PyDict_SetItemString(pyObj_security_opts, "tls_verify", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  if (-1 == PyDict_SetItemString(pyObj_conn_options, "security_options", pyObj_security_opts)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_security_opts);

  PyObject* pyObj_credentials = PyDict_New();
  couchbase::core::cluster_credentials creds = std::get<1>(connection_config.value());
  pyObj_tmp = PyUnicode_FromString(creds.username.c_str());
  if (-1 == PyDict_SetItemString(pyObj_credentials, "username", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  pyObj_tmp = PyUnicode_FromString(creds.password.c_str());
  if (-1 == PyDict_SetItemString(pyObj_credentials, "password", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  if (-1 == PyDict_SetItemString(pyObj_conn_options, "credentials", pyObj_credentials)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_credentials);

  PyObject* pyObj_general_opts = PyDict_New();
  pyObj_tmp = ip_protocol_to_pyObj(opts.use_ip_protocol);
  if (-1 == PyDict_SetItemString(pyObj_general_opts, "ip_protocol", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  if (-1 == PyDict_SetItemString(
              pyObj_general_opts, "enable_dns_srv", opts.enable_dns_srv ? Py_True : Py_False)) {
    PyErr_Print();
    PyErr_Clear();
  }

  if (-1 == PyDict_SetItemString(pyObj_general_opts,
                                 "enable_clustermap_notification",
                                 opts.enable_clustermap_notification ? Py_True : Py_False)) {
    PyErr_Print();
    PyErr_Clear();
  }

  pyObj_tmp = PyUnicode_FromString(opts.network.c_str());
  if (-1 == PyDict_SetItemString(pyObj_general_opts, "network", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  int_msec = opts.config_poll_interval;
  pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
  if (-1 == PyDict_SetItemString(pyObj_general_opts, "config_poll_interval", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  int_msec = opts.config_poll_floor;
  pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
  if (-1 == PyDict_SetItemString(pyObj_general_opts, "config_poll_floor", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  pyObj_tmp = PyUnicode_FromString(opts.user_agent_extra.c_str());
  if (-1 == PyDict_SetItemString(pyObj_general_opts, "user_agent_extra", pyObj_tmp)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_tmp);

  if (-1 == PyDict_SetItemString(pyObj_general_opts,
                                 "dump_configuration",
                                 opts.dump_configuration ? Py_True : Py_False)) {
    PyErr_Print();
    PyErr_Clear();
  }

  if (-1 == PyDict_SetItemString(pyObj_conn_options, "general", pyObj_general_opts)) {
    PyErr_Print();
    PyErr_Clear();
  }
  Py_XDECREF(pyObj_general_opts);

  return pyObj_conn_options;
}

PyObject*
handle_close_connection([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* pyObj_conn = nullptr;
  PyObject* pyObj_callback = nullptr;
  PyObject* pyObj_errback = nullptr;
  PyObject* pyObj_result = nullptr;

  static const char* kw_list[] = { "", "callback", "errback", nullptr };

  const char* kw_format = "O!|OO";
  int ret = PyArg_ParseTupleAndKeywords(args,
                                        kwargs,
                                        kw_format,
                                        const_cast<char**>(kw_list),
                                        &PyCapsule_Type,
                                        &pyObj_conn,
                                        &pyObj_callback,
                                        &pyObj_errback);

  if (!ret) {
    std::string msg = "Cannot close connection. Unable to parse args/kwargs.";
    pycbcc_set_python_exception(CoreClientErrors::VALUE, __FILE__, __LINE__, msg.c_str());
    return nullptr;
  }

  connection* conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
  if (nullptr == conn) {
    pycbcc_set_python_exception(CoreClientErrors::VALUE, __FILE__, __LINE__, NULL_CONN_OBJECT);
    return nullptr;
  }

  // PyObjects that need to be around for the cxx client lambda
  // have their increment/decrement handled w/in the callback_context struct
  // struct callback_context callback_ctx = { pyObj_callback, pyObj_errback };
  Py_XINCREF(pyObj_callback);
  Py_XINCREF(pyObj_errback);
  Py_XINCREF(pyObj_conn);
  auto barrier = std::make_shared<std::promise<PyObject*>>();
  auto f = barrier->get_future();
  {
    int callback_count = 0;
    Py_BEGIN_ALLOW_THREADS conn->cluster_.close(
      [pyObj_conn, pyObj_callback, pyObj_errback, callback_count, barrier]() mutable {
        if (callback_count == 0) {
          close_connection_callback(pyObj_conn, pyObj_callback, pyObj_errback, barrier);
        } else {
          CB_LOG_DEBUG("close callback called {} times already!", callback_count);
          callback_count++;
        }
      });
    Py_END_ALLOW_THREADS
  }
  if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
    PyObject* ret = nullptr;
    Py_BEGIN_ALLOW_THREADS ret = f.get();
    Py_END_ALLOW_THREADS return ret;
  }
  Py_RETURN_NONE;
}
