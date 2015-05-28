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


import pyavroc

json_schema = '''{"namespace": "example.avro",
               "type": "record",
               "name": "User",
               "fields": [
                   {"name": "friend",
                    "type": ["null", "User"]}
               ]
           }'''

def test_create_recursive_types():
    avtypes = pyavroc.create_types(json_schema)

    assert 'friend' in avtypes.User._fieldtypes
