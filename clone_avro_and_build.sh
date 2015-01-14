#!/bin/bash

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


set -eux

MYDIR=$(dirname $(readlink -f "$0"))

cd $MYDIR

AVRO=$MYDIR/local_avro

[ -d $AVRO ] || git clone -b patches http://github.com/Byhiras/avro $(basename $AVRO)

# build avro

if ! [ -f $AVRO/dist/lib/libavro.so ]
then
    mkdir -p $AVRO/build $AVRO/dist
    cd $AVRO/build
    cmake $AVRO/lang/c -DCMAKE_VERBOSE_MAKEFILE=ON -DCMAKE_INSTALL_PREFIX=$AVRO/dist -DCMAKE_BUILD_TYPE=Release -DTHREADSAFE=true
    make
    make test
    # workaround older cmake
    mv $AVRO/build/avro-c.pc $AVRO/build/src/ || true
    make install
fi

# build avro python

cd $AVRO/lang/py

python setup.py build

# build pyavroc

cd $MYDIR

rm -rf build dist

export PYAVROC_CFLAGS="-I$AVRO/dist/include"
export LDFLAGS="-L$AVRO/dist/lib -Wl,-rpath,$AVRO/dist/lib"

python setup.py build

export PYTHONPATH=$(readlink -f build/lib*):$AVRO/lang/py/build/lib

py.test -sv tests
