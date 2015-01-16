#!/usr/bin/env python

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


import sys
import os
import pytest
import shutil
import tempfile

import avro.schema
import avro.datafile
import avro.io

import pyavroc

import _testhelper


json_schema = '''{"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": ["int", "null"]},
     {"name": "favorite_color", "type": ["string", "null"]}
 ]
}'''


def _python_create_file(filename):
    if sys.version_info >= (3,):
        schema = avro.schema.Parse(json_schema)
    else:
        schema = avro.schema.parse(json_schema)

    fp = open(filename, 'wb')

    writer = avro.datafile.DataFileWriter(fp, avro.io.DatumWriter(), schema)

    for i in range(1):
        writer.append({"name": "Alyssa", "favorite_number": 256})
        writer.append({"name": "Ben", "favorite_number": 7, "favorite_color": "red"})

    writer.close()

    fp.close()


def _pyavroc_create_file(filename):
    avtypes = pyavroc.create_types(json_schema)

    fp = open(filename, 'w')

    writer = pyavroc.AvroFileWriter(fp, json_schema)

    for i in range(1):
        writer.write(avtypes.User(name='Alyssa', favorite_number=256))
        writer.write(avtypes.User(name='Ben', favorite_number=7, favorite_color='red'))

    writer.close()

    fp.close()


def _create_files():
    dirname = tempfile.mkdtemp()

    python_filename = os.path.join(dirname, "test_python.avro")
    pyavroc_filename = os.path.join(dirname, "test_pyavroc.avro")

    _python_create_file(python_filename)
    _pyavroc_create_file(pyavroc_filename)

    return (dirname, python_filename, pyavroc_filename)


def _delete_files(dirname):
    shutil.rmtree(dirname)


def _python_read(filename):
    fp = avro.datafile.DataFileReader(open(filename, 'rb'), avro.io.DatumReader())

    return list(fp)


def _pyavroc_read(filename, types):
    fp = pyavroc.AvroFileReader(open(filename), types=types)

    return list(fp)


def test_load_same():
    dirname, python_filename, pyavroc_filename = _create_files()

    python_dicts = _python_read(python_filename)
    pyavroc_objs = _pyavroc_read(pyavroc_filename, True)
    pyavroc_dicts = _pyavroc_read(pyavroc_filename, False)

    assert _testhelper.objs_to_dicts(pyavroc_objs) == python_dicts
    assert pyavroc_dicts == python_dicts

    _delete_files(dirname)


def test_load_swap_same():
    dirname, python_filename, pyavroc_filename = _create_files()

    python_dicts = _python_read(pyavroc_filename)
    pyavroc_objs = _pyavroc_read(python_filename, True)
    pyavroc_dicts = _pyavroc_read(pyavroc_filename, False)

    assert _testhelper.objs_to_dicts(pyavroc_objs) == python_dicts
    assert pyavroc_dicts == python_dicts

    _delete_files(dirname)
