pyavroc
=======

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

pyavroc is a Python API on top of upstream Avro-C. This means it reads 20 times faster than the upstream Python version when using dictionaries to represent objects (`types=False`), and 70 times faster when creating Python objects (`types=True`).

Name                                              | Description                         | Relative speed (bigger is better)
--------------------------------------------------|-------------------------------------|-------------------------------------
[python-avro](https://github.com/apache/avro.git) | Avro's implementation (pure Python) | 1
[fastavro](https://bitbucket.org/tebeka/fastavro) | python-avro improved, using Cython  | 10
[pyavroc](https://github.com/Byhiras/pyavroc.git) | Python/C API on upstream Avro-C     | 70 (`types=True`) 20 (`types=False`)

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

pyavroc supports writing records but this only works when the objects are represented as Python objects, not as dictionaries. Dictionary writing may be added in the future.

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
