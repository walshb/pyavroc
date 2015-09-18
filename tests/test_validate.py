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

import sys

from pyavroc import validate

# Based on the test_io module from the official Python API.  One
# difference is that the C-level JSON parser expects a top-level '{'
# or '[', so primitive schemas must be passed as 1-element unions.
TEST_CASES = [
    ('["null"]', None, 0),
    ('["boolean"]', True, 0),
    ('["string"]', u'adsfasdf09809dsf-=adsf', 0),
    ('["int"]', 1234, 0),
    ('["long"]', 1234, 0),
    ('["float"]', 1234.0, 0),
    ('["double"]', 1234.0, 0),
    ('{"type": "fixed", "name": "Test", "size": 1}', 'B', 0),
    ('{"type": "enum", "name": "Test", "symbols": ["A", "B"]}', 'A', 0),
    ('{"type": "enum", "name": "Test", "symbols": ["A", "B"]}', 'B', 1),
    ('{"type": "array", "items": "long"}', [1, 3, 2], 0),
    ('{"type": "map", "values": "long"}', {'a': 1, 'b': 3, u'c': 2}, 0),
    ('["string", "null", "long"]', 'spam', 0),
    ('["string", "null", "long"]', None, 1),
    ("""\
    {"type": "record",
    "name": "Test",
    "fields": [{"name": "f", "type": "long"}]}
    """, {'f': 5}, 0),
]

if sys.version_info < (3,):
    TEST_CASES.append(('["bytes"]', '12345abcd', 0))
else:
    TEST_CASES.append(('["bytes"]', bytes('12345abcd', 'utf-8'), 0))

if sys.version_info < (3,):
    TEST_CASES.append(('["string", "null", "long"]', long(42), 2))


def test_validate_schemas():
    for schema, datum, exp_res in TEST_CASES:
        assert validate(datum, schema) == exp_res


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
    assert validate(datum, schema) == 0
