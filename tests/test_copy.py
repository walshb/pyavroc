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


import pytest

import sys
import os
import shutil
import tempfile
import copy

import pyavroc

json_schema = '''[{"name": "SubRec",
"type": "record",
"fields": [
 {"name": "bar", "type": "int"}
] },
{"name": "Rec",
"type": "record",
"fields": [
 {"name": "foo", "type": "string"},
 {"name": "subrec", "type": "SubRec"}
]},
{"name": "Enu",
"type": "enum",
"symbols": ["ZERO", "ONE", "TWO"]
}
]'''

avtypes = pyavroc.create_types(json_schema)


def test_reduce():
    v = avtypes.Rec('hello', avtypes.SubRec(123))
    red = v.__reduce__()
    assert red == (avtypes.Rec, ('hello', avtypes.SubRec(123)))


def test_deepcopy():
    v = avtypes.Rec('hello', avtypes.SubRec(123))
    cv = copy.deepcopy(v)

    assert cv is not v
    assert cv == v

    assert cv.subrec is not v.subrec
    assert cv.subrec == v.subrec


def test_enum_reduce():
    v = avtypes.Enu.ONE
    red = v.__reduce__()
    assert red == (avtypes.Enu, (1,))


def test_enum_deepcopy():
    v = avtypes.Enu.ONE
    cv = copy.deepcopy(v)

    assert cv is v
