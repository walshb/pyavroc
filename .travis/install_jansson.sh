#!/bin/sh

# Copyright 2017 Ben Walsh
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

TOPDIR=$(/bin/pwd)

VERSION=2.10

URL=http://www.digip.org/jansson/releases/jansson-${VERSION}.tar.gz

if ! [ -d local_jansson ]
then
    mkdir -p local_jansson

    cd local_jansson

    curl -LO $URL
    tar xvf jansson-${VERSION}.tar.gz
fi

cd $TOPDIR/local_jansson/jansson-${VERSION}

CFLAGS='-fPIC' ./configure --prefix=$TOPDIR/local_jansson/build

make clean
make
make install

# ensure static libs used
rm -f $TOPDIR/local_jansson/build/lib/*.so* $TOPDIR/local_jansson/build/lib/*.dylib*
