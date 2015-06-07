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

#ifndef INC_SERIALIZER_H
#define INC_SERIALIZER_H

#include "Python.h"
#include "convert.h"
#include "avro.h"

#define SERIALIZER_WRITER_OK 0x1
#define SERIALIZER_SCHEMA_OK 0x2
#define SERIALIZER_BUFFER_OK 0x4

typedef struct {
    PyObject_HEAD

    int flags;
    char *buffer;
    size_t buffer_size;

    avro_schema_t schema;
    avro_value_iface_t *iface;
    avro_writer_t datum_writer;
} AvroSerializer;

extern PyTypeObject avroSerializerType;

#endif
