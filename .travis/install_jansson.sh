#!/bin/sh

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
