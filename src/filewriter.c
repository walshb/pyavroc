/*
 * Copyright 2015 Byhiras (Europe) Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "util.h"
#include "filewriter.h"
#include "convert.h"
#include "structmember.h"
#include "error.h"

#define PYAVROC_BLOCK_SIZE (128 * 1024)

static int
AvroFileWriter_init(AvroFileWriter *self, PyObject *args, PyObject *kwds)
{
    int rval;
    PyObject *pyfile;
    PyObject *schema_json;
    PyObject *schema_json_bytes;
    FILE *file;
    char *codec = "null";
    int block_size = PYAVROC_BLOCK_SIZE;

    self->pyfile = NULL;
    self->flags = 0;
    self->iface = NULL;

    static char *kwlist[] = { "pyfile", "schema_json", "codec", "block_size", NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OO|si", kwlist, &pyfile, &schema_json, &codec, &block_size)) {
        return -1;
    }

    schema_json_bytes = pystring_to_pybytes(schema_json);
    rval = avro_schema_from_json(pybytes_to_chars(schema_json_bytes), 0, &self->schema, NULL);
    Py_DECREF(schema_json_bytes);

    if (rval != 0 || self->schema == NULL) {
        PyErr_Format(PyExc_IOError, "Error reading schema: %s", avro_strerror());
        return -1;
    }

    self->flags |= AVROFILE_SCHEMA_OK;

    file = pyfile_to_file(pyfile, "wb");

    if (file == NULL) {
        PyErr_Format(PyExc_TypeError, "Error accessing file object.  Is it a file or file-like object?");
        return -1;
    }

    self->pyfile = pyfile;
    Py_INCREF(pyfile);

    if (avro_file_writer_create_with_codec_fp(file, "pyfile", 0, self->schema, &self->writer, codec, block_size)) {
        PyErr_Format(PyExc_IOError, "Error opening file: %s", avro_strerror());
        return -1;
    }

    self->flags |= AVROFILE_READER_OK;

    self->iface = avro_generic_class_from_schema(self->schema);

    if (self->iface == NULL) {
        PyErr_SetString(PyExc_IOError, "Error creating generic class interface");
        return -1;
    }

    return 0;
}

static int
is_open(AvroFileWriter *self)
{
    if (self->pyfile != NULL) {
        FILE *file = pyfile_to_file(self->pyfile, "w");
        return (file != NULL && (self->flags & AVROFILE_READER_OK));
    }

    return 0;
}

static int
do_close(AvroFileWriter *self)
{
    if (self->iface != NULL) {
        avro_value_iface_decref(self->iface);
        self->iface = NULL;
    }
    if (self->flags & AVROFILE_SCHEMA_OK) {
        avro_schema_decref(self->schema);
        self->flags &= ~AVROFILE_SCHEMA_OK;
    }

    if (self->pyfile != NULL) {
        if (is_open(self)) {
            avro_file_writer_close(self->writer);
            self->flags &= ~AVROFILE_READER_OK;
        }

        Py_CLEAR(self->pyfile);
    }

    return 0;
}

static void
AvroFileWriter_dealloc(AvroFileWriter *self)
{
    do_close(self);

    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject *
AvroFileWriter_write(AvroFileWriter *self, PyObject *args)
{
    int rval;
    avro_value_t value;
    PyObject *pyobj;

    if (!PyArg_ParseTuple(args, "O", &pyobj)) {
        return NULL;
    }

    if (!is_open(self)) {
        PyErr_SetString(PyExc_IOError, "file closed");
        return NULL;
    }

    avro_generic_value_new(self->iface, &value);

    rval = python_to_avro(NULL, pyobj, &value);

    if (!rval) {
        rval = avro_file_writer_append_value(self->writer, &value);
    }

    if (rval) {
        avro_value_decref(&value);

        set_error_prefix("Error writing: ");
        return NULL;
    }

    avro_value_decref(&value);

    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
AvroFileWriter_close(AvroFileWriter *self, PyObject *args)
{
    do_close(self);

    Py_INCREF(Py_None);
    return Py_None;
}

static PyMethodDef AvroFileWriter_methods[] = {
    {"close", (PyCFunction)AvroFileWriter_close, METH_VARARGS,
     "Close Avro file writer."
    },
    {"write", (PyCFunction)AvroFileWriter_write, METH_VARARGS,
     "Write a record."
    },
    {NULL}  /* Sentinel */
};

PyTypeObject avroFileWriterType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pyavro.AvroFileWriter",           /* tp_name */
    sizeof(AvroFileWriter), /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)AvroFileWriter_dealloc,    /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_compare */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash */
    0,                         /* tp_call */
    0,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
    "AvroFileWriter objects",          /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    AvroFileWriter_methods,    /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)AvroFileWriter_init,     /* tp_init */
    0,                         /* tp_alloc */
    0,                         /* tp_new */
};
