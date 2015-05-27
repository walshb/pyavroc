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
