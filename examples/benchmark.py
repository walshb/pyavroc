#!/usr/bin/env python

from __future__ import print_function

import sys
import os
import shutil
import tempfile
import datetime
import subprocess

import avro.schema
import avro.datafile
import avro.io

try:
    import fastavro
    # ensure we're using Cython version
    assert '._reader.' in str(fastavro.reader)
except:
    fastavro = None

import pyavroc

nrecords = 1000000

def create_file(filename):
    print('creating file...')

    schema = '''{"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": ["int", "null"]},
     {"name": "favorite_color", "type": ["string", "null"]}
 ]
}'''

    with open(filename, 'wb') as fp:
        writer = pyavroc.AvroFileWriter(fp, schema)

        for i in range(nrecords // 2):
            writer.write({"name": "Ermintrude", "favorite_number": 256})
            writer.write({"name": "Zebedee", "favorite_number": 7, "favorite_color": "red"})

        writer.close()


def test_avro():
    print('Python avro: reading file...')

    with open(filename, 'rb') as fp:
        av = avro.datafile.DataFileReader(fp, avro.io.DatumReader())

        t0 = datetime.datetime.now()
        res = list(av)
        t1 = datetime.datetime.now()

    return (t1 - t0, len(res))


def test_pyavroc(types):
    print('pyavroc(types=%s): reading file...' % types)

    with open(filename, 'rb') as fp:
        av = pyavroc.AvroFileReader(fp, types=types)

        t0 = datetime.datetime.now()
        res = list(av)
        t1 = datetime.datetime.now()

    return (t1 - t0, len(res))


def test_pyavroc_pipe():
    print('pyavroc(via pipe): reading file...')

    proc = subprocess.Popen(['/bin/cat', filename], stdout=subprocess.PIPE)

    av = pyavroc.AvroFileReader(proc.stdout)

    t0 = datetime.datetime.now()
    res = list(av)
    t1 = datetime.datetime.now()

    proc.wait()

    return (t1 - t0, len(res))


def test_fastavro():
    print('fastavro: reading file...')

    with open(filename, 'rb') as fp:
        av = fastavro.reader(fp)

        t0 = datetime.datetime.now()
        res = list(av)
        t1 = datetime.datetime.now()

    return (t1 - t0, len(res))


def _micros(timing):
    return float(timing.seconds) * 1e6 + float(timing.microseconds)


def run_test(test_fn, base_timing=None):
    timing, n = test_fn()
    assert n == nrecords
    print(timing)
    if base_timing:
        print('  (%s times faster than Python avro)' % (_micros(base_timing) / _micros(timing)))

    return timing


def main():
    global filename

    dirname = tempfile.mkdtemp()
    filename = os.path.join(dirname, 'test.avro')

    create_file(filename)

    base_timing = run_test(test_avro)
    if fastavro:
        run_test(test_fastavro, base_timing)
    run_test(lambda: test_pyavroc(False), base_timing)
    run_test(lambda: test_pyavroc(True), base_timing)
    run_test(test_pyavroc_pipe, base_timing)

    shutil.rmtree(dirname)


if __name__ == '__main__':
    main()
