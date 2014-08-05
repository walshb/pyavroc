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

#include "Python.h"

#include "util.h"
#include "filereader.h"
#include "filewriter.h"
#include "serializer.h"
#include "deserializer.h"
#include "convert.h"

static PyObject *
create_types_func(PyObject *self, PyObject *args)
{
    int rval;
    avro_schema_t schema;
    PyObject *schema_json;
    ConvertInfo info;
    PyObject *schema_json_bytes;

    if (!PyArg_ParseTuple(args, "O", &schema_json)) {
        return NULL;
    }

    schema_json_bytes = pystring_to_pybytes(schema_json);
    rval = avro_schema_from_json(pybytes_to_chars(schema_json_bytes), 0, &schema, NULL);
    Py_DECREF(schema_json_bytes);

    if (rval != 0 || schema == NULL) {
        PyErr_Format(PyExc_IOError, "Error reading schema: %s", avro_strerror());
        return NULL;
    }

    info.types = PyObject_CallFunctionObjArgs((PyObject *)get_avro_types_type(), NULL);
    if (info.types == NULL) {
        /* XXX: is the exception already set? */
        return NULL;
    }

    declare_types(&info, schema);

    return info.types;
}

static PyObject *
validate_func(PyObject *self, PyObject *args) {
    int rval;
    PyObject *datum;
    char *schema_json;
    avro_schema_t schema;

    if (!PyArg_ParseTuple(args, "Os", &datum, &schema_json)) {
        return NULL;
    }

    rval = avro_schema_from_json(schema_json, 0, &schema, NULL);
    if (rval != 0 || schema == NULL) {
        PyErr_Format(PyExc_IOError, "Error reading schema: %s",
                     avro_strerror());
        return NULL;
    }
    return Py_BuildValue("i", validate(datum, schema));
}


static PyMethodDef mod_methods[] = {
    {"create_types", (PyCFunction)create_types_func, METH_VARARGS,
     "Take a JSON schema and return a structure of Python types."
    },
    {"validate", (PyCFunction)validate_func, METH_VARARGS,
     "validate(datum, schema): check if datum matches schema. If it doesn't,\n"
     "return -1; if it matches one of the n branches in a union (or one of\n"
     "the n elements in an enum), return the corresponding index (0...n-1);\n"
     "if it matches, but it's not a union or enum, return 0."
    },
    {NULL}  /* Sentinel */
};

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,
        "_pyavroc",
        "Python wrapper around Avro-C",
        0,
        mod_methods,
        NULL,
        NULL,
        NULL,
        NULL
};
#endif

#ifndef PyMODINIT_FUNC  /* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif

PyMODINIT_FUNC
#if PY_MAJOR_VERSION >= 3
#define INIT_RETURN(V) return V;
PyInit__pyavroc(void)
#else
#define INIT_RETURN(V) return;
init_pyavroc(void)
#endif
{
    PyObject* m;

    avroFileReaderType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&avroFileReaderType) < 0) {
        INIT_RETURN(NULL);
    }

    avroFileWriterType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&avroFileWriterType) < 0) {
        INIT_RETURN(NULL);
    }

    avroSerializerType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&avroSerializerType) < 0) {
        INIT_RETURN(NULL);
    }

    if (avroenum_init() < 0) {
        return;
    }

    avroDeserializerType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&avroDeserializerType) < 0) {
        INIT_RETURN(NULL);
    }

#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&moduledef);
#else
    m = Py_InitModule3("_pyavroc", mod_methods,
                       "Python wrapper around Avro-C");
#endif

    Py_INCREF(&avroFileReaderType);
    PyModule_AddObject(m, "AvroFileReader", (PyObject *)&avroFileReaderType);

    Py_INCREF(&avroFileWriterType);
    PyModule_AddObject(m, "AvroFileWriter", (PyObject *)&avroFileWriterType);

    Py_INCREF(&avroSerializerType);
    PyModule_AddObject(m, "AvroSerializer", (PyObject *)&avroSerializerType);

    Py_INCREF(&avroDeserializerType);
    PyModule_AddObject(m, "AvroDeserializer",
                       (PyObject *)&avroDeserializerType);

    PyModule_AddObject(m, "AvroTypes", (PyObject*)get_avro_types_type());

    INIT_RETURN(m);
}
