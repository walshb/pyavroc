/*
 * Copyright 2015 CRS4
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

#include "serializer.h"
#include "convert.h"
#include "structmember.h"
#include "error.h"

#define PYAVROC_BUFFER_SIZE (128 * 1024)


static int
AvroSerializer_init(AvroSerializer *self, PyObject *args, PyObject *kwds)
{
    int rval;
    const char *schema_json;

    self->flags = 0;
    self->iface = NULL;

    if (!PyArg_ParseTuple(args, "s", &schema_json)) {
        return -1;
    }

    rval = avro_schema_from_json(schema_json, 0, &self->schema, NULL);
    if (rval != 0 || self->schema == NULL) {
        PyErr_Format(PyExc_IOError, "Error reading schema: %s",
                     avro_strerror());
        return -1;
    }
    self->flags |= SERIALIZER_SCHEMA_OK;

    self->iface = avro_generic_class_from_schema(self->schema);
    if (self->iface == NULL) {
        PyErr_SetString(PyExc_IOError,
                        "Error creating generic class interface");
        return -1;
    }

    self->buffer_size = PYAVROC_BUFFER_SIZE;  /* Initial size */
    self->buffer = (char *) avro_malloc(self->buffer_size);
    if (!self->buffer) {
        PyErr_NoMemory();
        return -1;
    }
    self->flags |= SERIALIZER_BUFFER_OK;

    self->datum_writer = avro_writer_memory(self->buffer, self->buffer_size);
    if (!self->datum_writer) {
        avro_free(self->buffer, self->buffer_size);
        PyErr_NoMemory();
        return -1;
    }
    self->flags |= SERIALIZER_WRITER_OK;

    return 0;
}

static int
do_close(AvroSerializer* self)
{
    if (self->flags & SERIALIZER_WRITER_OK) {
        avro_writer_free(self->datum_writer);
        self->flags &= ~SERIALIZER_WRITER_OK;
    }
    if (self->flags & SERIALIZER_BUFFER_OK) {
        avro_free(self->buffer, self->buffer_size);
        self->flags &= ~SERIALIZER_BUFFER_OK;
    }
    if (self->flags & SERIALIZER_SCHEMA_OK) {
        avro_schema_decref(self->schema);
        self->flags &= ~SERIALIZER_SCHEMA_OK;
    }
    if (self->iface != NULL) {
        avro_value_iface_decref(self->iface);
        self->iface = NULL;
    }
    return 0;
}

static void
AvroSerializer_dealloc(AvroSerializer *self)
{
    do_close(self);
    Py_TYPE(self)->tp_free((PyObject*) self);
}

static PyObject *
AvroSerializer_serialize(AvroSerializer *self, PyObject *args)
{
    int rval;
    size_t new_size;
    avro_value_t value;
    PyObject *pyvalue;
    PyObject *serialized;

    if (!PyArg_ParseTuple(args, "O", &pyvalue)) {
        return NULL;
    }
    avro_generic_value_new(self->iface, &value);
    rval = python_to_avro(NULL, pyvalue, &value);
    if (!rval) {
        rval = avro_value_write(self->datum_writer, &value);
    }

    while(rval == ENOSPC) {
        new_size = self->buffer_size * 2;
        self->buffer = (char*) avro_realloc(
            self->buffer, self->buffer_size, new_size);
        if (!self->buffer) {
            PyErr_NoMemory();
            return NULL;
        }
        self->buffer_size = new_size;
        avro_writer_memory_set_dest(
              self->datum_writer, self->buffer, self->buffer_size);
        rval = avro_value_write(self->datum_writer, &value);
    }

    if (rval) {
        avro_value_decref(&value);
        set_error_prefix("Write error: ");
        return NULL;
    }

    serialized = Py_BuildValue("s#", self->buffer,
                               avro_writer_tell(self->datum_writer));
    avro_writer_reset(self->datum_writer);
    avro_value_decref(&value);
    return serialized;
}

static PyObject *
AvroSerializer_close(AvroSerializer *self, PyObject *args)
{
    do_close(self);

    Py_INCREF(Py_None);
    return Py_None;
}

static PyMethodDef AvroSerializer_methods[] = {
    {"close", (PyCFunction)AvroSerializer_close, METH_VARARGS,
     "Close Avro serializer."
    },
    {"serialize", (PyCFunction)AvroSerializer_serialize, METH_VARARGS,
     "Serialize a record."
    },
    {NULL}  /* Sentinel */
};

PyTypeObject avroSerializerType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pyavro.AvroSerializer",            /* tp_name */
    sizeof(AvroSerializer),              /* tp_basicsize */
    0,                                   /* tp_itemsize */
    (destructor)AvroSerializer_dealloc,  /* tp_dealloc */
    0,                                   /* tp_print */
    0,                                   /* tp_getattr */
    0,                                   /* tp_setattr */
    0,                                   /* tp_compare */
    0,                                   /* tp_repr */
    0,                                   /* tp_as_number */
    0,                                   /* tp_as_sequence */
    0,                                   /* tp_as_mapping */
    0,                                   /* tp_hash */
    0,                                   /* tp_call */
    0,                                   /* tp_str */
    0,                                   /* tp_getattro */
    0,                                   /* tp_setattro */
    0,                                   /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                  /* tp_flags */
    "AvroSerializer objects",            /* tp_doc */
    0,                                   /* tp_traverse */
    0,                                   /* tp_clear */
    0,                                   /* tp_richcompare */
    0,                                   /* tp_weaklistoffset */
    0,                                   /* tp_iter */
    0,                                   /* tp_iternext */
    AvroSerializer_methods,              /* tp_methods */
    0,                                   /* tp_members */
    0,                                   /* tp_getset */
    0,                                   /* tp_base */
    0,                                   /* tp_dict */
    0,                                   /* tp_descr_get */
    0,                                   /* tp_descr_set */
    0,                                   /* tp_dictoffset */
    (initproc)AvroSerializer_init,       /* tp_init */
    0,                                   /* tp_alloc */
    0,                                   /* tp_new */
};
