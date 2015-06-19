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
import shutil
import tempfile

import pytest

import pyavroc

import _testhelper


def test_write_union_obj():
    schema = '''[{"name": "Rec1", "type": "record",
"fields": [ {"name": "attr1", "type": "int"} ] },
{"name": "Rec2", "type": "record",
"fields": [ {"name": "attr2", "type": "string"} ]}
]'''

    dirname = tempfile.mkdtemp()
    filename = os.path.join(dirname, 'test.avro')

    avtypes = pyavroc.create_types(schema)

    assert avtypes.Rec1._fieldtypes == {'attr1': int}
    assert avtypes.Rec2._fieldtypes == {'attr2': str}

    recs = [avtypes.Rec1(attr1=123), avtypes.Rec2(attr2='hello')]

    with open(filename, 'w') as fp:
        writer = pyavroc.AvroFileWriter(fp, schema)
        for rec in recs:
            writer.write(rec)
        writer.close()

    orig_rec1 = avtypes.Rec1
    orig_rec2 = avtypes.Rec2

    # read using existing types
    with open(filename) as fp:
        reader = pyavroc.AvroFileReader(fp, types=avtypes)
        read_recs = list(reader)

    assert reader.types.Rec1 is orig_rec1
    assert reader.types.Rec2 is orig_rec2

    assert read_recs == recs

    # read and create new types
    with open(filename) as fp:
        reader = pyavroc.AvroFileReader(fp, types=True)
        read_recs = list(reader)

    assert reader.types.Rec1 is not orig_rec1
    assert reader.types.Rec2 is not orig_rec2

    assert read_recs != recs
    assert _testhelper.objs_to_dicts(read_recs) == _testhelper.objs_to_dicts(recs)

    shutil.rmtree(dirname)


def test_write_closed():
    schema = '''[{"name": "Rec1", "type": "record",
"fields": [ {"name": "attr1", "type": "int"} ] },
{"name": "Rec2", "type": "record",
"fields": [ {"name": "attr2", "type": "string"} ]}
]'''

    dirname = tempfile.mkdtemp()
    filename = os.path.join(dirname, 'test.avro')

    avtypes = pyavroc.create_types(schema)

    fp = open(filename, 'w')
    writer = pyavroc.AvroFileWriter(fp, schema)
    writer.write(avtypes.Rec1(attr1=123))
    writer.close()
    fp.close()

    with pytest.raises(IOError):
        writer.write(avtypes.Rec1(attr1=456))

    shutil.rmtree(dirname)


def test_write_read_empty():
    schema = '''[{"name": "Rec1", "type": "record",
"fields": [ {"name": "attr1", "type": "int"} ] },
{"name": "Rec2", "type": "record",
"fields": [ {"name": "attr2", "type": "string"} ]}
]'''

    dirname = tempfile.mkdtemp()
    filename = os.path.join(dirname, 'test.avro')

    avtypes = pyavroc.create_types(schema)

    with open(filename, 'w') as fp:
        writer = pyavroc.AvroFileWriter(fp, schema)
        writer.close()

    # read using existing types
    with open(filename) as fp:
        reader = pyavroc.AvroFileReader(fp, types=avtypes)
        read_recs = list(reader)

    assert len(read_recs) == 0

    shutil.rmtree(dirname)


def test_write_union_of_dicts():
    schema = '''[{"name": "Rec1", "type": "record",
"fields": [ {"name": "attr1", "type": "int"} ] },
{"name": "Rec2", "type": "record",
"fields": [ {"name": "attr2", "type": "string"} ]}
]'''

    dirname = tempfile.mkdtemp()
    filename = os.path.join(dirname, 'test.avro')

    recs = [{'attr1': 123}, {'attr2': 'hello'}]

    with open(filename, 'w') as fp:
        writer = pyavroc.AvroFileWriter(fp, schema)
        for rec in recs:
            writer.write(rec)
        writer.close()

    with open(filename) as fp:
        reader = pyavroc.AvroFileReader(fp, types=False)
        read_recs = list(reader)

    assert read_recs == recs

    shutil.rmtree(dirname)


def test_bad_file_argument():
    try:
        with tempfile.NamedTemporaryFile() as fp:
            writer = pyavroc.AvroFileWriter(fp, '["null", "int"]')
            writer.close()
    except TypeError:
        pass
