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

#include "record.h"
#include "util.h"
#include "structmember.h"

static PyObject *record_repr_helper = NULL;

PyObject *
set_record_repr_helper_func(PyObject *self, PyObject *args)
{
    PyObject *helper;

    if (!PyArg_ParseTuple(args, "O", &helper)) {
        return NULL;
    }

    Py_XDECREF(record_repr_helper);
    Py_INCREF(helper);
    record_repr_helper = helper;

    Py_INCREF(Py_None);
    return Py_None;
}

static int
avro_record_init(AvroRecord *self, PyObject *args, PyObject *kwds)
{
    Py_ssize_t i;
    Py_ssize_t nargs, nkwds;
    PyObject *keys;
    PyObject *vals;
    PyObject *key;
    PyObject *val;
    size_t basicsize = Py_TYPE(self)->tp_basicsize;
    size_t nfields = (basicsize - sizeof(AvroRecord)) / sizeof(PyObject *);
    int rc = 0;

    nargs = PySequence_Length(args);

    if (nargs > nfields) {
        PyErr_Format(PyExc_ValueError, "too many constructor args");
        return -1;
    }

    for (i = 0; i < nargs; i++) {
        val = PySequence_GetItem(args, i);
        self->fields[i] = val;
    }
    for ( ; i < nfields; i++) {
        Py_INCREF(Py_None);
        self->fields[i] = Py_None;
    }

    if (kwds != NULL) {
        nkwds = PyMapping_Length(kwds);
        keys = PyMapping_Keys(kwds);
        vals = PyMapping_Values(kwds);
        for (i = 0; rc >= 0 && i < nkwds; i++) {
            key = PySequence_GetItem(keys, i);
            val = PySequence_GetItem(vals, i);
            if (PyObject_SetAttr((PyObject *)self, key, val) < 0) {
                rc = -1;
            }
            Py_DECREF(key);
            Py_DECREF(val);
        }
        Py_DECREF(keys);
        Py_DECREF(vals);
    }

    return rc;
}

static void
avro_record_dealloc(AvroRecord *self)
{
    size_t i;
    size_t basicsize = Py_TYPE(self)->tp_basicsize;
    size_t nfields = (basicsize - sizeof(AvroRecord)) / sizeof(PyObject *);

    for (i = 0; i < nfields; i++) {
        Py_CLEAR(self->fields[i]);
    }

    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject *
avro_record_repr(AvroRecord *self)
{
    size_t i;
    size_t basicsize = Py_TYPE(self)->tp_basicsize;
    size_t nfields = (basicsize - sizeof(AvroRecord)) / sizeof(PyObject *);
    PyObject *result;
    PyObject *type_name;
    PyObject *field_names;

    if (record_repr_helper != NULL && record_repr_helper != Py_None) {
        type_name = PyString_FromString(Py_TYPE(self)->tp_name);
        field_names = PyList_New(nfields);
        for (i = 0; i < nfields; i++) {
            /* steals a ref to the new string */
            PyList_SET_ITEM(field_names, i, PyString_FromString(Py_TYPE(self)->tp_members[i].name));
        }
        PyObject *result = PyObject_CallFunctionObjArgs(record_repr_helper, type_name, field_names, self, NULL);
        Py_DECREF(type_name);
        Py_DECREF(field_names);
        return result;
    }

    result = chars_to_pystring("avtypes.");
    pystring_concat(&result, Py_TYPE(self)->tp_name);
    pystring_concat(&result, "(");
    for (i = 0; i < nfields; i++) {
        if (i > 0) {
            pystring_concat(&result, ", ");
        }
        pystring_concat(&result, Py_TYPE(self)->tp_members[i].name);
        pystring_concat(&result, "=");
        pystring_concat_repr(&result, self->fields[i]);
    }
    pystring_concat(&result, ")");

    return result;
}

static PyObject *
avro_record_reduce(AvroRecord *self, PyObject *args)
{
    size_t i;
    size_t basicsize = Py_TYPE(self)->tp_basicsize;
    size_t nfields = (basicsize - sizeof(AvroRecord)) / sizeof(PyObject *);
    PyObject *result;
    PyObject *conargs;

    result = PyTuple_New(2);

    Py_INCREF(Py_TYPE(self));
    PyTuple_SET_ITEM(result, 0, (PyObject *)Py_TYPE(self));  /* steals ref */

    conargs = PyTuple_New(nfields);
    for (i = 0; i < nfields; i++) {
        if (self->fields[i] == NULL) {
            Py_INCREF(Py_None);
            PyTuple_SET_ITEM(conargs, i, Py_None);  /* steals ref */
        } else {
            Py_INCREF(self->fields[i]);
            PyTuple_SET_ITEM(conargs, i, self->fields[i]);  /* steals ref */
        }
    }
    PyTuple_SET_ITEM(result, 1, conargs);  /* steals ref */

    return result;
}

static PyObject *
equal(AvroRecord *a, AvroRecord *b)
{
    size_t i;
    size_t basicsize;
    size_t nfields;

    basicsize = Py_TYPE(a)->tp_basicsize;
    nfields = (basicsize - sizeof(AvroRecord)) / sizeof(PyObject *);

    for (i = 0; i < nfields; i++) {
        PyObject *cmp = PyObject_RichCompare(a->fields[i], b->fields[i], Py_EQ);
        if (!PyObject_IsTrue(cmp)) {
            return cmp;
        }
        Py_DECREF(cmp);
    }

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject *
avro_record_richcompare(PyObject *a, PyObject *b, int op)
{
    PyObject *cmp;
    PyObject *res = Py_NotImplemented;

    if (PyObject_Type(a) != PyObject_Type(b)) {
        Py_INCREF(Py_False);
        return Py_False;
    }

    switch (op) {
    case Py_EQ:
        res = equal((AvroRecord *)a, (AvroRecord *)b);
        break;
    case Py_NE:
        cmp = equal((AvroRecord *)a, (AvroRecord *)b);
        if (cmp == Py_True) {
            res = Py_False;
        } else if (cmp == Py_False) {
            res = Py_True;
        }
        Py_DECREF(cmp);
        break;
    default:
        break;
    }

    Py_INCREF(res);
    return res;
}

static PyMethodDef avro_record_methods[] = {
    {"__reduce__", (PyCFunction)avro_record_reduce, METH_VARARGS, ""},
    {0}
};

static PyTypeObject empty_type_object = {
    PyVarObject_HEAD_INIT(NULL, 0)
    0,                         /* tp_name */
    0,                         /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)avro_record_dealloc,  /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_compare */
    (reprfunc)avro_record_repr,  /* tp_repr */
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
    0,                         /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    (richcmpfunc)avro_record_richcompare,  /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    avro_record_methods,       /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)avro_record_init,  /* tp_init */
    0,                         /* tp_alloc */
    0,                         /* tp_new */
};

static PyObject *
create_new_type(avro_schema_t schema)
{
    size_t i;
    const char *record_name = avro_schema_name(schema);
    size_t field_count = avro_schema_record_size(schema);

    PyMemberDef *member_defs = (PyMemberDef *)PyMem_Malloc((field_count + 1) * sizeof(PyMemberDef));

    for (i = 0; i < field_count; i++) {
        const char *field_name = avro_schema_record_field_name(schema, i);
        member_defs[i].name = strdup(field_name);
        member_defs[i].type = T_OBJECT;
        member_defs[i].offset = sizeof(AvroRecord) + i * sizeof(PyObject *);
        member_defs[i].flags = 0;
        member_defs[i].doc = "";
    }
    member_defs[field_count].name = NULL;

    PyTypeObject *type = (PyTypeObject *)PyMem_Malloc(sizeof(PyTypeObject));
    memcpy(type, &empty_type_object, sizeof(PyTypeObject));

    type->tp_name = strdup(record_name);
    type->tp_basicsize = sizeof(AvroRecord) + field_count * sizeof(PyObject *);
    type->tp_doc = strdup(record_name);
    type->tp_members = member_defs;
    type->tp_new = PyType_GenericNew;

    type->tp_dict = PyDict_New();
    PyMapping_SetItemString(type->tp_dict, "_fieldtypes", PyDict_New());

    if (PyType_Ready(type) < 0) {
        return NULL;
    }

    return (PyObject *)type;
}

PyObject *
get_python_obj_type(PyObject *types, avro_schema_t schema)
{
    const char *record_name = avro_schema_name(schema);
    PyObject *type;

    type = PyObject_GetAttrString(types, record_name);

    if (type == NULL) {
        PyErr_Clear();
        type = create_new_type(schema);
        PyObject_SetAttrString(types, record_name, type);
    }

    return type;
}
