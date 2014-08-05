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

#ifndef INC_ENUM_H
#define INC_ENUM_H

#include "Python.h"
#include "avro.h"

typedef struct {
    PyObject_HEAD

    int value;
    char *name;
} AvroEnum;

PyObject *get_python_enum_type(PyObject *types, avro_schema_t schema);

int avroenum_init();

#endif
