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

json_schema = '''[{"type": "record",
 "name": "MyRec",
 "fields": [ {"name": "color", "type": {"type": "enum",
  "name": "Color",
  "symbols": ["BLUE", "GREEN", "BROWN"]
}} ] }]'''


def test_read_enum_int():
    dirname = tempfile.mkdtemp()

    filename = os.path.join(dirname, "test_python.avro")

    avtypes = pyavroc.create_types(json_schema)

    with open(filename, 'w') as fp:
        writer = pyavroc.AvroFileWriter(fp, json_schema)

        writer.write(avtypes.MyRec(color=avtypes.Color.BROWN))
        writer.close()

    with open(filename, 'r') as fp:
        reader = pyavroc.AvroFileReader(fp, types=False)

        rec = reader.next()

        assert rec['color'] == 2

    with open(filename, 'r') as fp:
        reader = pyavroc.AvroFileReader(fp, types=False, enum_strings=True)

        rec = reader.next()

        assert rec['color'] == "BROWN"

    shutil.rmtree(dirname)
