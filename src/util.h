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

#ifndef INC_UTIL_H
#define INC_UTIL_H

#include "Python.h"

char *pymem_strdup(const char *);

PyObject *pystring_to_pybytes(PyObject *);

PyObject *chars_size_to_pystring(char *, size_t);

PyObject *chars_size_to_pybytes(char *, size_t);

FILE *pyfile_to_file(PyObject *, const char*);

void pystring_concat(PyObject **, const char*);

void pystring_concat_repr(PyObject **, PyObject *);

#if PY_MAJOR_VERSION >= 3
#define long_to_pyint(L) PyLong_FromLong(L)
#define pyint_to_long(P) PyLong_AsLong(P)
#define is_pystring(P) PyUnicode_CheckExact(P)
#define is_pyint(P) PyLong_Check(P)
#define chars_to_pystring(C) PyUnicode_FromString(C)
#define chars_size_to_pystring(C, N) PyUnicode_FromStringAndSize(C, N)
#define chars_size_to_pybytes(C, N) PyBytes_FromStringAndSize(C, N)
#define pybytes_to_chars(P) PyBytes_AsString(P)
#define pybytes_to_chars_size(P, C, N) PyBytes_AsStringAndSize(P, C, N)
#else
#define long_to_pyint(L) PyInt_FromLong(L)
#define pyint_to_long(P) PyInt_AsLong(P)
#define is_pystring(P) PyString_CheckExact(P)
#define is_pyint(P) PyInt_Check(P)
#define chars_to_pystring(C) PyString_FromString(C)
#define chars_size_to_pystring(C, N) PyString_FromStringAndSize(C, N)
#define chars_size_to_pybytes(C, N) PyString_FromStringAndSize(C, N)
#define pybytes_to_chars(P) PyString_AsString(P)
#define pybytes_to_chars_size(P, C, N) PyString_AsStringAndSize(P, C, N)
#endif

#endif
