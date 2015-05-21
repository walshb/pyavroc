#!/usr/bin/env python

# Copyright 2015 CRS4
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


from cStringIO import StringIO

import avro.schema
from avro.io import DatumReader, BinaryDecoder

import pyavroc


class Deserializer(object):

    def __init__(self, schema_str):
        schema = avro.schema.parse(schema_str)
        self.reader = DatumReader(schema)

    def deserialize(self, rec_bytes):
        return self.reader.read(BinaryDecoder(StringIO(rec_bytes)))


def test_serialize_record():
    n_recs = 10
    schema = '''{
      "type": "record",
      "name": "User",
      "fields": [
        {"name": "office", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "favorite_number",  "type": ["int", "null"]}
      ]
    }'''
    avtypes = pyavroc.create_types(schema)
    serializer = pyavroc.AvroSerializer(schema)
    deserializer = Deserializer(schema)
    for i in xrange(n_recs):
        name, office = "name-%d" % i, "office-%d" % i
        avro_obj = avtypes.User(name=name, office=office)
        rec_bytes = serializer.serialize(avro_obj)
        deser_rec = deserializer.deserialize(rec_bytes)
        assert set(deser_rec) == set(['name', 'office', 'favorite_number'])
        assert deser_rec['name'] == name
        assert deser_rec['office'] == office
        assert deser_rec['favorite_number'] is None
