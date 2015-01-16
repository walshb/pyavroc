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
#if PY_MAJOR_VERSION >= 3
    return PyUnicode_AsEncodedString(pystr, "utf-8", "strict");
#else
    Py_INCREF(pystr);
    return pystr;
#endif
}

FILE *
pyfile_to_file(PyObject *pyfile, const char *mode)
{
#if PY_MAJOR_VERSION >= 3
    int fd = PyObject_AsFileDescriptor(pyfile);
    return (fd < 0) ? NULL : fdopen(fd, mode);
#else
    return PyFile_AsFile(pyfile);
#endif
}

void
pystring_concat(PyObject **pystr, const char *chars)
{
#if PY_MAJOR_VERSION >= 3
    PyObject *newstr = PyUnicode_Concat(*pystr, PyUnicode_FromString(chars));
    Py_DECREF(*pystr);
    *pystr = newstr;
#else
    PyString_ConcatAndDel(pystr, PyString_FromString(chars));
#endif
}

void
pystring_concat_repr(PyObject **pystr, PyObject *obj)
{
#if PY_MAJOR_VERSION >= 3
    PyObject *repr = PyObject_Repr(obj);
    PyObject *newstr = PyUnicode_Concat(*pystr, repr);
    Py_DECREF(repr);
    Py_DECREF(*pystr);
    *pystr = newstr;
#else
    PyString_ConcatAndDel(pystr, PyObject_Repr(obj));
#endif
}
