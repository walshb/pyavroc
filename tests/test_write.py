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
import re

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


def test_write_wrong_value():
    schema = '''[{"name": "Rec1", "type": "record",
"fields": [ {"name": "attr1", "type": "int"} ] },
{"name": "Rec2", "type": "record",
"fields": [ {"name": "attr2", "type": "string"} ] },
{"name": "Rec3", "type": "record",
"fields": [ {"name": "attr3", "type": {"type": "map", "values": "int"}} ] },
{"name": "Rec4", "type": "record",
"fields": [ {"name": "attr4", "type": {"type": "array", "items": "int"}} ] }
]'''

    dirname = tempfile.mkdtemp()
    filename = os.path.join(dirname, 'test.avro')

    avtypes = pyavroc.create_types(schema)

    with pytest.raises(TypeError) as excinfo:
        with open(filename, 'w') as fp:
            writer = pyavroc.AvroFileWriter(fp, schema)
            writer.write(avtypes.Rec1(attr1='x' * 120))
            writer.close()

    expected_error = "when writing to Rec1.attr1, invalid python object '" \
                     + ('x' * 99) + ", an integer is required"
    expected_error2 = "when writing to Rec1.attr1, invalid python object '" \
                      + ('x' * 120) + "', an integer is required"

    assert expected_error in str(excinfo.value) \
        or expected_error2 in str(excinfo.value)

    with pytest.raises(TypeError) as excinfo:
        with open(filename, 'w') as fp:
            writer = pyavroc.AvroFileWriter(fp, schema)
            writer.write(avtypes.Rec2(attr2=123))
            writer.close()

    expected_error = "when writing to Rec2.attr2, invalid python object 123," \
                     " expected.*Unicode.*, int found"

    assert re.search(expected_error, str(excinfo.value))

    with pytest.raises(TypeError) as excinfo:
        with open(filename, 'w') as fp:
            writer = pyavroc.AvroFileWriter(fp, schema)
            writer.write(avtypes.Rec3(attr3=123))
            writer.close()

    expected_error = "when writing to Rec3.attr3, expected dict-like object, " \
                     "int found"

    assert expected_error in str(excinfo.value)

    with pytest.raises(TypeError) as excinfo:
        with open(filename, 'w') as fp:
            writer = pyavroc.AvroFileWriter(fp, schema)
            writer.write(avtypes.Rec4(attr4=123))
            writer.close()

    expected_error = "when writing to Rec4.attr4, expected list, int found"

    assert expected_error in str(excinfo.value)

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

def test_write_wrong_type_primitive():
    schema = '''{
  "type": "record",
  "name": "Obj",
  "fields": [
    {"name": "string", "type": "string"},
    {"name": "number", "type": "int"}
  ]
}'''
    avtypes = pyavroc.create_types(schema)
    serializer = pyavroc.AvroSerializer(schema)

    # this shouldn't raise
    serializer.serialize(avtypes.Obj(string="pippo", number=1))
    # place an int in place of a str
    u = avtypes.Obj(string=1, number=1)
    with pytest.raises(TypeError):
        serializer.serialize(u)
    # string in place of int
    u = avtypes.Obj(string="a", number="a")
    with pytest.raises(TypeError):
        serializer.serialize(u)

def test_coerce_int_long_in_unions():
    schema = ''' [ "null", "long"] '''

    with open('/dev/null', 'w') as fp:
        writer = pyavroc.AvroFileWriter(fp, schema)
        writer.write(33) # an integer.  Should be coerced to long without an error
        writer.close()

def test_coerce_int_long():
    schema = '''{
        "type": "record",
        "name": "Rec",
        "fields": [ {"name": "attr1", "type": "long"} ]
        }'''
    av_types = pyavroc.create_types(schema)
    rec = av_types.Rec(attr1=33) # an integer.  Should be coerced to long without an error
    with open('/dev/null', 'w') as fp:
        writer = pyavroc.AvroFileWriter(fp, schema)
        writer.write(rec)
        writer.close()

def test_union_with_bool():
    schema = '''{
        "type": "record",
        "name": "Rec",
        "fields": [ {"name": "attr1", "type": [ "null", "boolean" ]} ]
        }'''
    av_types = pyavroc.create_types(schema)
    with tempfile.NamedTemporaryFile() as tmpfile:
        writer = pyavroc.AvroFileWriter(tmpfile.file, schema)
        # Try writing null
        writer.write(av_types.Rec(attr1=None))
        # Try writing a boolean value
        writer.write(av_types.Rec(attr1=True))
        # Try writing an integer.  Should be coerced to boolean without an error
        writer.write(av_types.Rec(attr1=33))
        writer.write(av_types.Rec(attr1=0))
        writer.close()

        tmpfile.flush()
        tmpfile.seek(0)
        reader = pyavroc.AvroFileReader(tmpfile.file, types=av_types)
        read_recs = list(reader)
        attr_values = [ r.attr1 for r in read_recs ]
        assert attr_values == [ None, True, True, False ]

def test_bad_file_type():
    irrelevant = '''{
        "type": "boolean",
        "name": "x"
        }'''
    av_types = pyavroc.create_types(irrelevant)
    with pytest.raises(TypeError):
        # try to open a reader on a list
        reader = pyavroc.AvroFileReader(list(), types=av_types)
