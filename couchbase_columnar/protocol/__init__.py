#  Copyright 2016-2024. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
import sys
from functools import partial, partialmethod
from typing import (Dict,
                    Optional,
                    Union)

try:
    # Importing the ssl package allows us to utilize some Python voodoo to find OpenSSL.
    # This is particularly helpful on M1 macs (PYCBC-1386).
    import ssl  # noqa: F401 # nopep8 # isort:skip # noqa: E402
    import couchbase_columnar.protocol.pycbcc_core  # noqa: F401 # nopep8 # isort:skip # noqa: E402
except ImportError:
    # should only need to do this on Windows w/ Python >= 3.8 due to the changes made for how DLLs are resolved
    if sys.platform.startswith('win32') and (3, 8) <= sys.version_info:
        open_ssl_dir = os.getenv('PYCBCC_OPENSSL_DIR')
        # if not set by environment, try to use libcrypto and libssl that comes w/ Windows Python install
        if not open_ssl_dir:
            for p in sys.path:
                if os.path.split(p)[-1] == 'DLLs':
                    open_ssl_dir = p
                    break

        if open_ssl_dir:
            os.add_dll_directory(open_ssl_dir)
        else:
            print(('PYCBCC: Caught import error. '
                   'Most likely due to not finding OpenSSL libraries. '
                   'Set PYCBCC_OPENSSL_DIR to location where OpenSSL libraries can be found.'))

try:
    from couchbase_columnar._version import __version__
except ImportError:
    __version__ = '0.0.0-could-not-find-version'

PYCBCC_VERSION = f'pycbcc/{__version__}'

try:
    python_version_info = sys.version.split(' ')
    if len(python_version_info) > 0:
        PYCBCC_VERSION = f'{PYCBCC_VERSION} (python/{python_version_info[0]})'
except Exception:  # nosec
    pass

""" Add support for logging, adding a TRACE level to logging """
import json  # nopep8 # isort:skip # noqa: E402
import logging  # nopep8 # isort:skip # noqa: E402

from couchbase_columnar.protocol.pycbcc_core import CXXCBC_METADATA, pycbcc_logger  # nopep8 # isort:skip # noqa: E402

_PYCBCC_LOGGER = pycbcc_logger()
_CXXCBC_METADATA_JSON = json.loads(CXXCBC_METADATA)
logging.TRACE = 5  # type: ignore
logging.addLevelName(logging.TRACE, 'TRACE')  # type: ignore
logging.Logger.trace = partialmethod(logging.Logger.log, logging.TRACE)  # type: ignore
logging.trace = partial(logging.log, logging.TRACE)  # type: ignore

"""

pycbcc teardown methods

"""
import atexit  # nopep8 # isort:skip # noqa: E402


def _pycbcc_teardown(**kwargs: object) -> None:
    """**INTERNAL**"""
    global _PYCBCC_LOGGER
    if _PYCBCC_LOGGER:
        # TODO:  see about synchronizing the logger's shutdown here
        _PYCBCC_LOGGER = None  # type: ignore


atexit.register(_pycbcc_teardown)

"""

Metadata + version methods

"""
_METADATA_KEYS = ['openssl_default_cert_dir',
                  'openssl_default_cert_file',
                  'openssl_headers',
                  'openssl_runtime',
                  'version']


def get_metadata(as_str: Optional[bool] = False, detailed: Optional[bool] = False) -> Union[Dict[str, str], str]:
    metadata = _CXXCBC_METADATA_JSON if detailed is True else {
        k: v for k, v in _CXXCBC_METADATA_JSON.items() if k in _METADATA_KEYS}
    return json.dumps(metadata) if as_str is True else metadata


"""

Logging methods

"""


def configure_console_logger() -> None:
    import os
    log_level = os.getenv('PYCBCC_LOG_LEVEL', None)
    if log_level:
        _PYCBCC_LOGGER.create_console_logger(log_level.lower())
        logger = logging.getLogger()
        logger.info(f'Python Couchbase Columnar Client ({PYCBCC_VERSION})')
        logging.getLogger().debug(get_metadata(as_str=True))


def configure_logging(name: str,
                      level: Optional[int] = logging.INFO,
                      parent_logger: Optional[logging.Logger] = None) -> None:
    if parent_logger:
        name = f'{parent_logger.name}.{name}'
    logger = logging.getLogger(name)
    _PYCBCC_LOGGER.configure_logging_sink(logger, level)
    logger.info(f'Python Couchbase Columnar Client ({PYCBCC_VERSION})')
    logger.debug(get_metadata(as_str=True))


def enable_protocol_logger_to_save_network_traffic_to_file(filename: str) -> None:
    """
    **VOLATILE** This API is subject to change at any time.

    Exposes the underlying couchbase++ library protocol logger.  This method is for logging/debugging
    purposes and must be used with caution as network details will be logged to the provided file.

    Args:
        filename (str): The name of the file the protocol logger will write to.

    Raises:
        `ValueError`: If a filename is not provided.
    """
    _PYCBCC_LOGGER.enable_protocol_logger(filename)


configure_console_logger()
