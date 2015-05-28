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

#include "convert.h"
#include "record.h"
#include "avroenum.h"

static PyObject *avro_types_type = NULL;

/* type of the object which has the Avro types setattred onto it */
PyObject *
get_avro_types_type()
{
    if (avro_types_type == NULL) {
        avro_types_type = PyObject_CallFunction((PyObject *)&PyType_Type,
                                                "s(O)N",
                                                "AvroTypes",
                                                (PyObject *)&PyBaseObject_Type,
                                                PyDict_New());
    }

    return avro_types_type;
}

PyObject *
declare_types(ConvertInfo *info, avro_schema_t schema)
{
    avro_type_t type = schema->type;

    switch (type) {
    case AVRO_NULL:
        /* PyNone_Type is not publicly visible */
        return (PyObject *)Py_TYPE(Py_None);
    case AVRO_BOOLEAN:
        return (PyObject *)&PyBool_Type;
    case AVRO_BYTES:
    case AVRO_STRING:
    case AVRO_FIXED:
        return (PyObject *)&PyString_Type;
    case AVRO_DOUBLE:
    case AVRO_FLOAT:
        return (PyObject *)&PyFloat_Type;
    case AVRO_INT32:
        return (PyObject *)&PyInt_Type;
    case AVRO_ENUM:
        return get_python_enum_type(info->types, schema);
    case AVRO_INT64:
        return (PyObject *)&PyLong_Type;
    case AVRO_ARRAY:
        declare_types(info, avro_schema_array_items(schema));
        return (PyObject *)&PyList_Type;
    case AVRO_MAP:
        declare_types(info, avro_schema_map_values(schema));
        return (PyObject *)&PyDict_Type;
    case AVRO_UNION:
        {
            size_t union_size = avro_schema_union_size(schema);
            size_t i;
            for (i = 0; i < union_size; i++) {
                declare_types(info, avro_schema_union_branch(schema, i));
            }
        }
        return (PyObject *)&PyBaseObject_Type;  /* XXX list of types might be better */
    case AVRO_RECORD:
        {
            size_t field_count = avro_schema_record_size(schema);
            size_t i;
            /* create the Python type for this schema */
            PyObject *record_type = get_python_obj_type(info->types, schema);
            PyObject *field_types = PyObject_GetAttrString(record_type, "_fieldtypes");
            for (i = 0; i < field_count; i++) {
                PyObject *field_type = declare_types(info, avro_schema_record_field_get_by_index(schema, i));
                /* this will INCREF, so takes hold of the object */
                PyMapping_SetItemString(field_types, avro_schema_record_field_name(schema, i), field_type);
            }
            return record_type;
        }
    case AVRO_LINK:
        return declare_types(info, avro_schema_link_target(schema));
    default:
        /* other types don't hold records */
        break;
    }
    return NULL;
}

static PyObject *
array_to_python(ConvertInfo *info, avro_value_t *value)
{
    size_t  element_count;
    PyObject *result;
    size_t  i;

    avro_value_get_size(value, &element_count);

    result = PyList_New(element_count);

    for (i = 0; i < element_count; i++) {
        avro_value_t  element_value;
        PyObject *pyelement_value;

        avro_value_get_by_index(value, i, &element_value, NULL);

        pyelement_value = avro_to_python(info, &element_value);

        /* steals a ref to pyelement_value */
        PyList_SET_ITEM(result, i, pyelement_value);
    }

    return result;
}

static PyObject *
enum_to_python(ConvertInfo *info, avro_value_t *value)
{
    int  val;

    avro_value_get_enum(value, &val);

    /*
     * avro_schema_t  schema = avro_value_get_schema(value);
     * symbol_name = avro_schema_enum_get(schema, val);
     */

    return PyInt_FromLong(val);
}

static PyObject *
enum_to_python_object(ConvertInfo *info, avro_value_t *value)
{
    int val;
    const char *name;

    avro_schema_t schema = avro_value_get_schema(value);
    PyObject *type = (PyObject *)get_python_enum_type(info->types, schema);

    avro_value_get_enum(value, &val);
    name = avro_schema_enum_get(schema, val);

    AvroEnum *obj = (AvroEnum *)PyObject_GetAttrString(type, name);

    return obj;
}

static PyObject *
record_to_python(ConvertInfo *info, avro_value_t *value)
{
    size_t  field_count;
    size_t  i;
    PyObject *result = PyDict_New();

    avro_value_get_size(value, &field_count);

    for (i = 0; i < field_count; i++) {
        avro_value_t  field_value;
        const char  *field_name;
        PyObject *pykey;
        PyObject *pyelement_value;

        avro_value_get_by_index(value, i, &field_value, &field_name);

        pykey = (PyObject *)PyString_FromString(field_name);
        pyelement_value = avro_to_python(info, &field_value);

        /* increfs key and value */
        PyDict_SetItem(result, pykey, pyelement_value);

        Py_DECREF(pykey);
        Py_DECREF(pyelement_value);
    }

    return result;
}

static PyObject *
record_to_python_object(ConvertInfo *info, avro_value_t *value)
{
    size_t field_count;
    size_t i;

    avro_schema_t schema = avro_value_get_schema(value);
    PyObject *type = get_python_obj_type(info->types, schema);

    AvroRecord *obj = (AvroRecord *)PyObject_CallFunctionObjArgs(type, NULL);

    Py_DECREF(type);

    avro_value_get_size(value, &field_count);

    for (i = 0; i < field_count; i++) {
        avro_value_t field_value;
        PyObject *pyelement_value;

        avro_value_get_by_index(value, i, &field_value, NULL);

        pyelement_value = avro_to_python(info, &field_value);

        obj->fields[i] = pyelement_value;
    }

    return (PyObject *)obj;
}

static PyObject *
map_to_python(ConvertInfo *info, avro_value_t *value)
{
    size_t  element_count;
    PyObject *result = PyDict_New();
    size_t  i;

    avro_value_get_size(value, &element_count);

    for (i = 0; i < element_count; i++) {
        const char  *key;
        avro_value_t  element_value;
        PyObject *pykey;
        PyObject *pyelement_value;

        avro_value_get_by_index(value, i, &element_value, &key);

        pykey = (PyObject *)PyString_FromString(key);
        pyelement_value = avro_to_python(info, &element_value);

        /* increfs key and value */
        PyDict_SetItem(result, pykey, pyelement_value);

        Py_DECREF(pykey);
        Py_DECREF(pyelement_value);
    }

    return result;
}

static PyObject *
union_to_python(ConvertInfo *info, avro_value_t *value)
{
    avro_value_t  branch_value;
    avro_value_get_current_branch(value, &branch_value);

    return avro_to_python(info, &branch_value);
}

/* returns new reference */
PyObject *
avro_to_python(ConvertInfo *info, avro_value_t *value)
{
    avro_type_t  type = avro_value_get_type(value);
    switch (type) {
    case AVRO_BOOLEAN:
        {
            int  val;
            avro_value_get_boolean(value, &val);
            return PyBool_FromLong(val);
        }

    case AVRO_BYTES:
        {
            const void  *buf;
            size_t  size;
            avro_value_get_bytes(value, &buf, &size);
            /* got pointer into underlying value. no need to free */
            return PyString_FromStringAndSize(buf, size);
        }

    case AVRO_DOUBLE:
        {
            double  val;
            avro_value_get_double(value, &val);
            return PyFloat_FromDouble(val);
        }

    case AVRO_FLOAT:
        {
            float  val;
            avro_value_get_float(value, &val);
            return PyFloat_FromDouble(val);
        }

    case AVRO_INT32:
        {
            int32_t  val;
            avro_value_get_int(value, &val);
            return PyInt_FromLong(val);
        }

    case AVRO_INT64:
        {
            int64_t  val;
            avro_value_get_long(value, &val);
            return PyLong_FromLongLong((PY_LONG_LONG)val);
        }

    case AVRO_NULL:
        {
            avro_value_get_null(value);
            Py_INCREF(Py_None);
            return Py_None;
        }

    case AVRO_STRING:
        {
            /* TODO: Convert the UTF-8 to the current
             * locale's character set */
            const char  *buf;
            size_t  size;
            avro_value_get_string(value, &buf, &size);
            /* For strings, size includes the NUL terminator. */
            return PyString_FromStringAndSize(buf, size - 1);
        }

    case AVRO_ARRAY:
        return array_to_python(info, value);

    case AVRO_ENUM:
        if (info->types == NULL) {
            /* just an int */
            return enum_to_python(info, value);
        } else {
            /* a fancy object */
            return enum_to_python_object(info, value);
        }

    case AVRO_FIXED:
        {
            const void  *buf;
            size_t  size;
            avro_value_get_fixed(value, &buf, &size);
            return PyString_FromStringAndSize((const char *)buf, size);
        }

    case AVRO_MAP:
        return map_to_python(info, value);

    case AVRO_RECORD:
        if (info->types == NULL) {
            /* just a dictionary */
            return record_to_python(info, value);
        } else {
            /* a fancy object */
            return record_to_python_object(info, value);
        }

    case AVRO_UNION:
        return union_to_python(info, value);

    default:
        return NULL;
    }
}

/* assumes dest is already correct type. */
static int
python_to_array(ConvertInfo *info, PyObject *pyobj, avro_value_t *dest)
{
    int rval;
    size_t i;
    size_t element_count;

    element_count = PyObject_Length(pyobj);

    for (i = 0; i < element_count; i++) {
        PyObject *pyval = PySequence_GetItem(pyobj, i);
        avro_value_t child;

        avro_value_append(dest, &child, NULL);
        rval = python_to_avro(info, pyval, &child);
        Py_DECREF(pyval);
        if (rval) {
            return rval;
        }
    }

    return 0;
}

static int
python_to_map(ConvertInfo *info, PyObject *pyobj, avro_value_t *dest)
{
    int rval = 0;
    size_t i;
    size_t element_count;
    PyObject *keys;
    PyObject *vals;

    element_count = PyMapping_Length(pyobj);

    keys = PyMapping_Keys(pyobj);
    vals = PyMapping_Values(pyobj);

    for (i = 0; !rval && i < element_count; i++) {
        PyObject *key = PySequence_GetItem(keys, i);
        PyObject *val = PySequence_GetItem(vals, i);
        avro_value_t child;

        rval = avro_value_add(dest, PyString_AsString(key), &child, NULL, NULL);
        if (!rval) {
            rval = python_to_avro(info, val, &child);
        }

        Py_DECREF(key);
        Py_DECREF(val);
    }

    Py_DECREF(keys);
    Py_DECREF(vals);

    return rval;
}

static int
get_branch_index(ConvertInfo *info, PyObject *pyobj, avro_schema_t schema)
{
    const char *typename;
    avro_schema_t branch_schema;
    int branch_index;

    if (pyobj == Py_None) {
        typename = "null";
    } else {
        if (PyString_CheckExact(pyobj)) {
            typename = "string";
        } else if (PyList_CheckExact(pyobj)) {
            typename = "array";
        } else {
            /* "long", "float" and Object types are the same for both. */
            typename = ((PyTypeObject *)PyObject_Type(pyobj))->tp_name;
        }
    }

    branch_schema = avro_schema_union_branch_by_name(schema,
                                                     &branch_index,
                                                     typename);
    if (branch_schema == NULL) {
        /* maybe Python "float" vs avro "double" */
        if (PyFloat_CheckExact(pyobj)) {
            branch_schema = avro_schema_union_branch_by_name(schema,
                                                             &branch_index,
                                                             "double");
        }
        else if (PyInt_CheckExact(pyobj)) { /* may also be Python int vs long */
            branch_schema = avro_schema_union_branch_by_name(schema,
                                                             &branch_index,
                                                             "long");
        }
    }

    /* As a last resort, check to see if 'boolean' is in the union. Then we should try
     * interpreting the value as one. */
    if (branch_schema == NULL) {
        branch_schema = avro_schema_union_branch_by_name(schema,
                                                         &branch_index,
                                                         "boolean");
    }
    if (branch_schema == NULL) {
        avro_set_error("no type in union suitable for type %s.", typename);
        return -1;  /* fail */
    }

    return branch_index;
}

static int
python_to_union(ConvertInfo *info, PyObject *pyobj, avro_value_t *dest)
{
    int branch_index = get_branch_index(info, pyobj, avro_value_get_schema(dest));
    avro_value_t branch;

    if (branch_index < 0) {
        return -1;
    }

    avro_value_set_branch(dest, branch_index, &branch);

    return python_to_avro(info, pyobj, &branch);
}

static int
python_to_record(ConvertInfo *info, PyObject *pyobj, avro_value_t *dest)
{
    int rval;
    size_t i;
    size_t field_count;

    avro_value_get_size(dest, &field_count);
    for (i = 0; i < field_count; i++) {
        const char *field_name;
        avro_value_t field_value;
        PyObject *pyval;

        avro_value_get_by_index(dest, i, &field_value, &field_name);

        pyval = PyObject_GetAttrString(pyobj, field_name);
        if (pyval == NULL) {
            PyErr_Clear();
            continue;
        }

        rval = python_to_avro(info, pyval, &field_value);
        Py_DECREF(pyval);
        if (rval) {
            return rval;
        }
    }

    return 0;
}

int
python_to_avro(ConvertInfo *info, PyObject *pyobj, avro_value_t *dest)
{
    switch (avro_value_get_type(dest)) {
    case AVRO_BOOLEAN:
        return avro_value_set_boolean(dest, PyObject_IsTrue(pyobj));
    case AVRO_BYTES:
        {
            char *buf;
            Py_ssize_t len;
            PyString_AsStringAndSize(pyobj, &buf, &len);
            /* we're holding internal data so use "set" not "give" */
            return avro_value_set_bytes(dest, buf, len);
        }
    case AVRO_DOUBLE:
        return avro_value_set_double(dest, PyFloat_AsDouble(pyobj));
    case AVRO_FLOAT:
        return avro_value_set_float(dest, PyFloat_AsDouble(pyobj));
    case AVRO_INT32:
        return avro_value_set_int(dest, PyInt_AsLong(pyobj));
    case AVRO_INT64:
        return avro_value_set_long(dest, PyLong_AsLongLong(pyobj));
    case AVRO_NULL:
        return avro_value_set_null(dest);
    case AVRO_STRING:
        {
            char *buf;
            Py_ssize_t len;
            PyString_AsStringAndSize(pyobj, &buf, &len);
            return avro_value_set_string_len(dest, buf, len + 1);
        }
    case AVRO_ARRAY:
        return python_to_array(info, pyobj, dest);
    case AVRO_ENUM:
        return avro_value_set_enum(dest, PyInt_AsLong(pyobj));
    case AVRO_FIXED:
        {
            char *buf;
            Py_ssize_t len;
            PyString_AsStringAndSize(pyobj, &buf, &len);
            return avro_value_set_fixed(dest, buf, len);
        }
    case AVRO_MAP:
        return python_to_map(info, pyobj, dest);
    case AVRO_RECORD:
        return python_to_record(info, pyobj, dest);
    case AVRO_UNION:
        return python_to_union(info, pyobj, dest);
    default:
        return -1;
    }

    return 0;
}
