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

#include "avroenum.h"
#include "util.h"
#include "structmember.h"

static PyObject *
avro_enum_new(PyTypeObject *subtype, PyObject *args, PyObject *kwds)
{
    PyObject *obj;
    PyObject *symbols;
    int value;

    if (!PyArg_ParseTuple(args, "i", &value)) {
        return NULL;
    }

    symbols = PyObject_GetAttrString((PyObject *)subtype, "_symbols");
    if (symbols == NULL) {
        return NULL;
    }

    obj = PySequence_GetItem(symbols, value);

    Py_DECREF(symbols);

    return obj;
}

static void
avro_enum_dealloc(AvroEnum *self)
{
    PyMem_Free(self->name);
    self->ob_type->tp_free((PyObject*)self);
}

static PyObject *
avro_enum_repr(AvroEnum *self)
{
    PyObject *result;

    result = PyString_FromString("avtypes.");
    PyString_ConcatAndDel(&result, PyString_FromString(self->ob_type->tp_name));
    PyString_ConcatAndDel(&result, PyString_FromString("."));
    PyString_ConcatAndDel(&result, PyString_FromString(self->name));

    return result;
}

static PyObject *
avro_enum_reduce(AvroEnum *self, PyObject *args)
{
    PyObject *result;
    PyObject *conargs;

    result = PyTuple_New(2);

    Py_INCREF(self->ob_type);
    PyTuple_SET_ITEM(result, 0, (PyObject *)self->ob_type);  /* steals ref */

    conargs = PyTuple_New(1);
    PyTuple_SET_ITEM(conargs, 0, PyInt_FromLong(self->value));
    PyTuple_SET_ITEM(result, 1, conargs);  /* steals ref */

    return result;
}

static long
avro_enum_hash(AvroEnum *self)
{
    return (long)self->value;
}

static int
equal(AvroEnum *a, PyObject *b)
{
    if (PyInt_Check(b)) {
        return (((AvroEnum *)a)->value == PyInt_AsLong(b));
    }

    /* must be an AvroEnum */
    return (((AvroEnum *)a)->value == ((AvroEnum *)b)->value);
}

static PyObject *
avro_enum_richcompare(PyObject *a, PyObject *b, int op)
{
    PyObject *res = Py_NotImplemented;

    if (!PyInt_Check(b) && PyObject_Type(a) != PyObject_Type(b)) {
        Py_INCREF(Py_False);
        return Py_False;
    }

    switch (op) {
    case Py_EQ:
        res = PyBool_FromLong(equal((AvroEnum *)a, b));
        break;
    case Py_NE:
        res = PyBool_FromLong(!equal((AvroEnum *)a, b));
        break;
    default:
        Py_INCREF(res);
        break;
    }

    return res;
}

static PyMethodDef avro_enum_methods[] = {
    {"__reduce__", (PyCFunction)avro_enum_reduce, METH_VARARGS, ""},
    {0}
};

static PyMemberDef avro_enum_members[] = {
    {"value", T_INT, offsetof(AvroEnum, value), 0, "enum integer value"},
    {NULL}  /* Sentinel */
};

static PyTypeObject empty_type_object = {
    PyObject_HEAD_INIT(NULL)
    0,                         /* ob_size */
    0,                         /* tp_name */
    sizeof(AvroEnum),          /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)avro_enum_dealloc,  /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_compare */
    (reprfunc)avro_enum_repr,  /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    (hashfunc)avro_enum_hash,  /* tp_hash */
    0,                         /* tp_call */
    0,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
    0,                         /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    (richcmpfunc)avro_enum_richcompare,  /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    avro_enum_methods,         /* tp_methods */
    avro_enum_members,         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    0,                         /* tp_init */
    0,                         /* tp_alloc */
    (newfunc)avro_enum_new,    /* tp_new */
};

static PyObject *
create_new_type(avro_schema_t schema)
{
    size_t i;
    const char *enum_name = avro_schema_name(schema);
    PyObject *symbols;
    AvroEnum *obj;
    int nsymbols;

    PyTypeObject *type = (PyTypeObject *)PyMem_Malloc(sizeof(PyTypeObject));
    memcpy(type, &empty_type_object, sizeof(PyTypeObject));

    type->tp_name = strdup(enum_name);
    type->tp_dict = PyDict_New();

    symbols = PyList_New(0);

    PyMapping_SetItemString(type->tp_dict, "_symbols", symbols);

    if (PyType_Ready(type) < 0) {
        return NULL;
    }

    nsymbols = avro_schema_enum_number_of_symbols(schema);
    for (i = 0; i < nsymbols; i++) {
        const char *name = avro_schema_enum_get(schema, i);
        if (name == NULL) {
            break;
        }

        /* create an object for the particular enum symbol */
        obj = (AvroEnum *)type->tp_alloc(type, 1);
        obj->value = i;
        obj->name = pymem_strdup(name);

        PyMapping_SetItemString((PyObject *)type->tp_dict, name, (PyObject *)obj);
        PyList_Append(symbols, (PyObject *)obj);

        Py_DECREF((PyObject *)obj);
    }

    return (PyObject *)type;
}

PyObject *
get_python_enum_type(PyObject *types, avro_schema_t schema)
{
    const char *enum_name = avro_schema_name(schema);
    PyObject *type;

    type = PyObject_GetAttrString(types, enum_name);

    if (type == NULL) {
        PyErr_Clear();
        type = create_new_type(schema);
        PyObject_SetAttrString(types, enum_name, type);
    }

    return type;
}
