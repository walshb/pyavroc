#!/usr/bin/python

# Copyright 2015 Byhiras (Europe) Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from setuptools import setup, Extension
import subprocess

# can't import because we don't have shared library built yet.
version_str = open('pyavroc/_version.py').read().strip()
version = version_str.split("'")[1]

try:
    long_description \
        = subprocess.Popen(['pandoc', '-t', 'rst', '-o', '-', 'README.md'],
                           stdout=subprocess.PIPE).communicate()[0].decode('utf-8')
except OSError:
    long_description = open('README.md').read()

# bdist --format=rpm calls this with CFLAGS overridden,
# so have to use PYAVROC_CFLAGS
cflags = os.environ.get('PYAVROC_CFLAGS', None)
if cflags:
    os.environ['CFLAGS'] = os.environ.get('CFLAGS', '') + ' ' + cflags

extra_libs = os.environ.get('PYAVROC_LIBS', '').split()

ext_modules = [Extension('pyavroc._pyavroc',
                         ['src/pyavro.c',
                          'src/filereader.c',
                          'src/filewriter.c',
                          'src/serializer.c',
                          'src/deserializer.c',
                          'src/convert.c',
                          'src/record.c',
                          'src/avroenum.c',
                          'src/util.c',
                          'src/error.c'],
                         libraries=['avro'] + extra_libs)]

setup(name='pyavroc',
      version=version,
      url='http://github.com/Byhiras/pyavroc',
      license='Apache License 2.0',
      author='Ben Walsh',
      author_email='ben.walsh@byhiras.com',
      description='Avro file reader/writer',
      long_description=long_description,
      keywords='avro serialization',
      platforms='any',
      classifiers = [
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python',
          'Programming Language :: Python :: 2',
          'Topic :: Software Development :: Libraries',
          'Topic :: Software Development :: Libraries :: Python Modules',
      ],
      tests_require=['pytest'],
      packages=['pyavroc'],
      package_data={'pyavroc': ['avro/NOTICE.txt']},  # in case it was included
      ext_modules=ext_modules)
