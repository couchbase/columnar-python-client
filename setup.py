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
import platform
import sys

from setuptools import find_packages, setup

sys.path.append('.')
import couchbase_columnar_version  # nopep8 # isort:skip # noqa: E402
from pycbcc_build_setup import (BuildCommand,  # nopep8 # isort:skip # noqa: E402
                                CMakeBuildExt,
                                CMakeConfigureExt,
                                CMakeExtension)

try:
    couchbase_columnar_version.gen_version()
except couchbase_columnar_version.CantInvokeGit:
    pass

PYCBCC_README = os.path.join(os.path.dirname(__file__), 'README.md')
PYCBCC_VERSION = couchbase_columnar_version.get_version()


package_data = {'couchbase_columnar.common.core.nonprod_certificates': ['*.pem']}
# some more Windows tomfoolery...
if platform.system() == 'Windows':
    package_data.update(**{'couchbase_columnar.protocol': ['pycbcc_core.pyd']})


print(f'Python Columnar SDK version: {PYCBCC_VERSION}')

setup(name='couchbase-columnar',
      version=PYCBCC_VERSION,
      ext_modules=[CMakeExtension('couchbase_columnar.protocol.pycbcc_core')],
      cmdclass={'build': BuildCommand,
                'build_ext': CMakeBuildExt,
                'configure_ext': CMakeConfigureExt},
      python_requires='>=3.8',
      install_requires=[
          'typing-extensions~=4.11; python_version<"3.11"'
      ],
      packages=find_packages(
          include=['acouchbase_columnar', 'couchbase_columnar', 'acouchbase_columnar.*', 'couchbase_columnar.*'],
          exclude=['acouchbase_columnar.tests', 'couchbase_columnar.tests']),
      package_data=package_data,
      url="https://github.com/couchbase/columnar-python-client",
      author="Couchbase, Inc.",
      author_email="PythonPackage@couchbase.com",
      license="Apache License 2.0",
      description="Python Client for Couchbase Columnar",
      long_description=open(PYCBCC_README, "r").read(),
      long_description_content_type='text/markdown',
      keywords=["couchbase", "nosql", "pycouchbase", "couchbase++", "columnar"],
      classifiers=[
          "Development Status :: 5 - Production/Stable",
          "License :: OSI Approved :: Apache Software License",
          "Intended Audience :: Developers",
          "Operating System :: OS Independent",
          "Programming Language :: Python",
          "Programming Language :: Python :: 3",
          "Programming Language :: Python :: Implementation :: CPython",
          "Topic :: Database",
          "Topic :: Software Development :: Libraries",
          "Topic :: Software Development :: Libraries :: Python Modules"],
      )
