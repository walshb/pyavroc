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

#include "filereader.h"
#include "convert.h"
#include "structmember.h"

static int
AvroFileReader_init(AvroFileReader *self, PyObject *args, PyObject *kwds)
{
    int rval;
    PyObject *pyfile;
    PyObject *types = NULL;
    FILE *file;
    char *schema_json;
    avro_writer_t schema_json_writer;
    size_t len;
    static char *kwlist[] = {"file", "types", NULL};

    self->pyfile = NULL;
    self->flags = 0;
    self->iface = NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|O", kwlist,
                                     &pyfile, &types)) {
        return -1;
    }

    file = PyFile_AsFile(pyfile);

    if (file == NULL) {
        return -1;
    }

    self->pyfile = pyfile;
    Py_INCREF(pyfile);

    if (avro_file_reader_fp(file, "pyfile", 0, &self->reader)) {
        PyErr_Format(PyExc_IOError, "Error opening file: %s", avro_strerror());
        return -1;
    }

    self->flags |= AVROFILE_READER_OK;

    self->schema = avro_file_reader_get_writer_schema(self->reader);

    if (self->schema == NULL) {
        PyErr_Format(PyExc_IOError, "Error reading schema: %s", avro_strerror());
        return -1;
    }

    len = 256;
    do {
        /* XXX horrible loop to get a big enough buffer for schema. */
        len *= 2;
        schema_json = (char *)PyMem_Malloc(len);
        schema_json_writer = avro_writer_memory(schema_json, len);
        rval = avro_schema_to_json(self->schema, schema_json_writer);
        if (!rval) {
            rval = avro_write(schema_json_writer, (void *)"", 1);  /* zero terminate */
            if (!rval) {
                self->schema_json = PyString_FromString(schema_json);
            }
        }
        avro_writer_free(schema_json_writer);
        PyMem_Free(schema_json);
    } while (rval == ENOSPC);

    if (rval) {
        PyErr_Format(PyExc_IOError, "Error saving schema: %s", avro_strerror());
        return -1;
    }

    self->flags |= AVROFILE_SCHEMA_OK;

    self->iface = avro_generic_class_from_schema(self->schema);

    if (self->iface == NULL) {
        PyErr_SetString(PyExc_IOError, "Error creating generic class interface");
        return -1;
    }

    if (types != NULL && PyObject_IsTrue(types)) {
        /* we still haven't incref'ed types here */
        if (Py_TYPE(types) == get_avro_types_type()) {
            Py_INCREF(types);
            self->info.types = types;
        } else {
            self->info.types = PyObject_CallFunctionObjArgs((PyObject *)get_avro_types_type(), NULL);
            if (self->info.types == NULL) {
                return -1;
            }
            declare_types(&self->info, self->schema);
        }
    } else {
        self->info.types = NULL;
    }

    return 0;
}

static void
AvroFileReader_dealloc(AvroFileReader *self)
{
    if (self->iface != NULL) {
        avro_value_iface_decref(self->iface);
    }
    if (self->flags & AVROFILE_SCHEMA_OK) {
        avro_schema_decref(self->schema);
        Py_CLEAR(self->schema_json);
    }
    if (self->flags & AVROFILE_READER_OK) {
        avro_file_reader_close(self->reader);
    }
    if (self->pyfile != NULL) {
        Py_CLEAR(self->pyfile);
    }

    self->ob_type->tp_free((PyObject*)self);
}

static PyObject *
AvroFileReader_self(AvroFileReader *self)
{
    Py_INCREF(self);
    return (PyObject *)self;
}

static PyObject *
AvroFileReader_iternext(AvroFileReader *self)
{
    avro_value_t value;
    PyObject *result;

    avro_generic_value_new(self->iface, &value);

    int rval = avro_file_reader_read_value(self->reader, &value);

    if (rval) {
        avro_value_decref(&value);

        if (rval == EOF) {
            return NULL;
        }
        PyErr_Format(PyExc_IOError, "Error reading: %s", avro_strerror());
        return NULL;
    }

    result = avro_to_python(&self->info, &value);

    avro_value_decref(&value);

    return result;
}

static PyMethodDef AvroFileReader_methods[] = {
    /*
    {"next", (PyCFunction)AvroFileReader_next, METH_VARARGS,
     "Read a record."
    },
    */
    {NULL}  /* Sentinel */
};

static PyMemberDef AvroFileReader_members[] = {
    {"types", T_OBJECT, offsetof(AvroFileReader, info.types), 0,
     "types from file"},
    {"schema_json", T_OBJECT, offsetof(AvroFileReader, schema_json), 0,
     "schema as json"},
    {NULL}  /* Sentinel */
};

PyTypeObject avroFileReaderType = {
    PyObject_HEAD_INIT(NULL)
    0,                         /* ob_size */
    "_pyavro.AvroFileReader",           /* tp_name */
    sizeof(AvroFileReader), /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)AvroFileReader_dealloc,    /* tp_dealloc */
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
    "AvroFileReader objects",          /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    (getiterfunc)AvroFileReader_self,         /* tp_iter */
    (iternextfunc)AvroFileReader_iternext,    /* tp_iternext */
    AvroFileReader_methods,                   /* tp_methods */
    AvroFileReader_members,                   /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)AvroFileReader_init,     /* tp_init */
    0,                         /* tp_alloc */
    0,                         /* tp_new */
};
