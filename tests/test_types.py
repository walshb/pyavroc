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

json_schema = '''[{"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": ["int", "null"]},
     {"name": "favorite_color", "type": ["string", "null"]}
 ]
},
{"namespace": "example.avro",
 "type": "enum",
 "name": "Color",
 "symbols": ["BLUE", "GREEN", "BROWN"]
}
]'''


avtypes = pyavroc.create_types(json_schema)


def test_types_container():
    assert isinstance(avtypes, pyavroc.AvroTypes)


def test_record_init_no_args():
    v = avtypes.User()
    assert v.name is None
    assert v.favorite_number is None
    assert v.favorite_color is None


def test_record_init_some_args():
    v = avtypes.User('Fred')
    assert v.name == 'Fred'
    assert v.favorite_number is None
    assert v.favorite_color is None


def test_record_init_other_args():
    v = avtypes.User(None, 123)
    assert v.name is None
    assert v.favorite_number == 123
    assert v.favorite_color is None


def test_record_init_kwargs():
    v = avtypes.User(name='Fred')
    assert v.name == 'Fred'
    assert v.favorite_number is None
    assert v.favorite_color is None


def test_enum_value():
    assert isinstance(avtypes.Color.BLUE.value, int)
    assert isinstance(avtypes.Color.GREEN.value, int)
    assert isinstance(avtypes.Color.BROWN.value, int)
    assert avtypes.Color.BLUE.value == 0
    assert avtypes.Color.GREEN.value == 1
    assert avtypes.Color.BROWN.value == 2


def test_enum_eq():
    assert avtypes.Color.BLUE == 0
    assert avtypes.Color.BLUE == avtypes.Color.BLUE
    assert avtypes.Color.GREEN == 1
    assert avtypes.Color.GREEN == avtypes.Color.GREEN
    assert avtypes.Color.BROWN == 2
    assert avtypes.Color.BROWN == avtypes.Color.BROWN


def test_enum_ne():
    assert avtypes.Color.BLUE != 1
    assert avtypes.Color.BLUE != avtypes.Color.GREEN
    assert avtypes.Color.BLUE != 2
    assert avtypes.Color.BLUE != avtypes.Color.BROWN
    assert avtypes.Color.GREEN != 0
    assert avtypes.Color.GREEN != avtypes.Color.BLUE
    assert avtypes.Color.GREEN != 2
    assert avtypes.Color.GREEN != avtypes.Color.BROWN
    assert avtypes.Color.BROWN != 0
    assert avtypes.Color.BROWN != avtypes.Color.BLUE
    assert avtypes.Color.BROWN != 1
    assert avtypes.Color.BROWN != avtypes.Color.GREEN


def test_enum_constructor():
    assert avtypes.Color(0) is avtypes.Color.BLUE
    assert avtypes.Color(1) is avtypes.Color.GREEN
    assert avtypes.Color(2) is avtypes.Color.BROWN


def test_enum_symbols():
    assert avtypes.Color._symbols[0] is avtypes.Color.BLUE
    assert avtypes.Color._symbols[1] is avtypes.Color.GREEN
    assert avtypes.Color._symbols[2] is avtypes.Color.BROWN


def test_enum_hash():
    assert hash(avtypes.Color.BLUE) == 0
    assert hash(avtypes.Color.GREEN) == 1
    assert hash(avtypes.Color.BROWN) == 2


def test_enum_as_key():
    d = {}
    d[avtypes.Color.GREEN] = 'a'
    assert avtypes.Color.GREEN in d
    assert avtypes.Color.BLUE not in d
    assert d[avtypes.Color.GREEN] == 'a'


if __name__ == '__main__':
    ##test_enum_as_key()
    ##ac = avtypes.Color.BLUE
    print 'setting avtypes = None'
    avtypes = None
    ##print sys.getrefcount(ac)
