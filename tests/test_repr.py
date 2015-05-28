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


def test_default_repr():
    pyavroc.set_record_repr_helper(None)

    v = avtypes.Rec('hello', avtypes.SubRec(123))

    assert repr(v) == "avtypes.Rec(foo='hello', subrec=avtypes.SubRec(bar=123))"


class MyRepr(object):
    def __init__(self):
        self._indent = 0

    def do_repr(self, typename, fieldnames, obj):
        indentstr = '  ' * self._indent
        self._indent += 1
        res = ['%s(' % (typename,)]
        for fieldname in fieldnames:
            res.append('  %s%s=%r' % (indentstr, fieldname, getattr(obj, fieldname)))
        res.append('%s)' % indentstr)
        self._indent -= 1
        return '\n'.join(res)


def test_user_repr():
    pyavroc.set_record_repr_helper(MyRepr().do_repr)

    v = avtypes.Rec('hello', avtypes.SubRec(123))

    assert repr(v) == "Rec(\n  foo='hello'\n  subrec=SubRec(\n    bar=123\n  )\n)"


if __name__ == '__main__':
    test_user_repr()
