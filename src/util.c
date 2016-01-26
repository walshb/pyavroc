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

#include "Python.h"

char *
pymem_strdup(const char *str)
{
    char *res = (char *)PyMem_Malloc(strlen(str) + 1);
    if (res != NULL) {
        return strcpy(res, str);
    }
    return NULL;
}

PyObject *pystring_to_pybytes(PyObject *pystr)
{
    if (PyUnicode_Check(pystr)) {
        return PyUnicode_AsUTF8String(pystr);
    }
#if PY_MAJOR_VERSION >= 3
    PyErr_Format(PyExc_TypeError,
                 "expected Unicode, %.200s found", Py_TYPE(pystr)->tp_name);
    return NULL;
#else
    Py_INCREF(pystr);
    return pystr;
#endif
}

/**
 * Return the file object associated with p as a FILE*, if possible.
 *
 * Works with Python 2 and 3.  Note that it does not set a Python exception
 * if the conversion fails.
 */
FILE *
pyfile_to_file(PyObject *pyfile, const char *mode)
{
#if PY_MAJOR_VERSION >= 3
    int fd = PyObject_AsFileDescriptor(pyfile);
    if (fd < 0) {
        PyErr_Clear();
        return NULL;
    }
    return fdopen(fd, mode);
#else
    return PyFile_AsFile(pyfile);
#endif
}

#if PY_MAJOR_VERSION >= 3
/*
 * Private helper implementing logic akin to Python 2's PyString_ConcatAndDel.
 * Concats two unicode strings and sets pystr to point to the new resulting
 * string;  then it dereferences both original parameters.
 *
 * In case of error it sets pystr to NULL and sets a Python exception.
 */
static void
concat_unicode_and_decref_all(PyObject** pystr, PyObject* obj)
{
    PyObject *newstr = NULL;
    if (obj) {
        newstr = PyUnicode_Concat(*pystr, obj);
        Py_DECREF(obj);
    }
    Py_XDECREF(*pystr);
    *pystr = newstr;

}
#endif

void
pystring_concat(PyObject **pystr, const char *chars)
{
#if PY_MAJOR_VERSION >= 3
    concat_unicode_and_decref_all(pystr, PyUnicode_FromString(chars));
#else
    PyString_ConcatAndDel(pystr, PyString_FromString(chars));
#endif
}

void
pystring_concat_repr(PyObject **pystr, PyObject *obj)
{
#if PY_MAJOR_VERSION >= 3
    concat_unicode_and_decref_all(pystr, PyObject_Repr(obj));
#else
    PyString_ConcatAndDel(pystr, PyObject_Repr(obj));
#endif
}

void
pystring_concat_str(PyObject **pystr, PyObject *obj)
{
#if PY_MAJOR_VERSION >= 3
    concat_unicode_and_decref_all(pystr, PyObject_Str(obj));
#else
    PyString_ConcatAndDel(pystr, PyObject_Str(obj));
#endif
}
