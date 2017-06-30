#!/bin/sh

set -eux

cd $(dirname "$0")
MYDIR=$(/bin/pwd)

AVRO=$MYDIR/local_avro

PYTHON=${PYTHON:-python}

case $($PYTHON -c 'import sys; print(sys.version_info.major)') in
    3) AVROPY=$AVRO/lang/py3
        ;;
    *) AVROPY=$AVRO/lang/py
       ;;
esac

export PYTHONPATH=$(echo build/lib*):$(echo $AVROPY/build/lib*):${PYTHONPATH:-}

$PYTHON examples/benchmark.py
