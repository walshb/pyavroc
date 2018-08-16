pyavroc
=======

[![Build Status](https://travis-ci.org/Byhiras/pyavroc.svg?branch=master)](https://travis-ci.org/Byhiras/pyavroc)

An Avro file reader/writer for Python.

Usage
-----

```python
>>> import pyavroc
>>> with open('myfile.avro') as fp:
>>>     reader = pyavroc.AvroFileReader(fp, types=True)
>>>     for record in reader:
>>>         print record
```

Comparison with original Avro Python API
----------------------------------------

pyavroc is a Python API on top of upstream Avro-C. This means it reads about 40 times faster than Avro's Python implementation. (The exact timings depend on the version of Python used).

Name                                              | Description                         | Relative speed (bigger is better)
--------------------------------------------------|-------------------------------------|----------------------------------
[python-avro](https://github.com/apache/avro.git) | Avro's implementation (pure Python) | 1
[fastavro](https://bitbucket.org/tebeka/fastavro) | python-avro improved, using Cython  | 10
[pyavroc](https://github.com/Byhiras/pyavroc.git) | Python/C API on upstream Avro-C     | 40


Installating the module
-----------------------
- Download and run cmake installation file from https://cmake.org/ (e.g. https://cmake.org/files/v3.12/cmake-3.12.1-Linux-x86_64.sh)
- Once extracted just add its bin folder to the PATH so that `cmake` command is available
- Clone this repo - `git clone https://github.com/Byhiras/pyavroc`
- Run `./clone_avro_and_build.sh` in the cloned folder
- This generates a build folder
- `python setup.py bdist_wheel -d build` makes an executable wheel of the project and saves in the build/ folder
- `cd build`
- `pip install pyavroc-0.7.2-cp36-cp36m-linux_x86_64.whl`

Building the module
-------------------

You will need to build Avro-C with a number of patches applied. This is available at https://github.com/Byhiras/avro.git, branch "patches".

Then you can build pyavroc, linking against the Avro-C shared library.

The pyavroc repository contains the script `clone_avro_and_build.sh` which automates this process:

```bash
./clone_avro_and_build.sh
```

Writing records
---------------

pyavroc supports writing, both for records created as dictionaries, and for records created as Python objects.

More examples
-------------

More examples are available in the `tests` directory.

License
-------

Copyright 2015 Byhiras (Europe) Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at:

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
