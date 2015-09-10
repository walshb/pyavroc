#!/bin/sh

set -eux

MYDIR=$(dirname $(readlink -f "$0"))

AVRO=$MYDIR/local_avro

PYTHON=${PYTHON:-python}

case $($PYTHON -c 'import sys; print(sys.version_info.major)') in
    3) AVROPY=$AVRO/lang/py3
        ;;
    *) AVROPY=$AVRO/lang/py
       ;;
esac

export PYTHONPATH=$(readlink -e build/lib*):$(readlink -e $AVROPY/build/lib*):${PYTHONPATH:-}

$PYTHON examples/benchmark.py
