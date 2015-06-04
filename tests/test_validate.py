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

import pytest
from pyavroc import validate

# copied from pure Python Apache Avro tests, except for the fact that
# the C-level JSON parser expects a top-level '{' or '['
TEST_CASES = (
    ('["null"]', None),
    ('["boolean"]', True),
    ('["string"]', unicode('adsfasdf09809dsf-=adsf')),
    ('["bytes"]', '12345abcd'),
    ('["int"]', 1234),
    ('["long"]', 1234),
    ('["float"]', 1234.0),
    ('["double"]', 1234.0),
    ('{"type": "fixed", "name": "Test", "size": 1}', 'B'),
    ('{"type": "enum", "name": "Test", "symbols": ["A", "B"]}', 'B'),
    ('{"type": "array", "items": "long"}', [1, 3, 2]),
    ('{"type": "map", "values": "long"}', {'a': 1, 'b': 3, 'c': 2}),
    ('["string", "null", "long"]', None),
    ("""\
    {"type": "record",
    "name": "Test",
    "fields": [{"name": "f", "type": "long"}]}
    """, {'f': 5}),
)


def test_validate_schemas():
    for schema, datum in TEST_CASES:
        assert validate(datum, schema)


@pytest.mark.xfail(reason="not supported (yet)")
def test_validate_recursive():
    schema = """\
    {"type": "record",
    "name": "Lisp",
    "fields": [{"name": "value",
                "type": ["null", "string",
                         {"type": "record",
                          "name": "Cons",
                          "fields": [{"name": "car", "type": "Lisp"},
                                     {"name": "cdr", "type": "Lisp"}]}]}]}
    """
    datum = {'value': {'car': {'value': 'head'}, 'cdr': {'value': None}}}
    assert validate(datum, schema)
