/*
** Copyright (C) 2013, wormtable developers (see AUTHORS.txt).
**
** This file is part of wormtable.
**
** Wormtable is free software: you can redistribute it and/or modify
** it under the terms of the GNU Lesser General Public License as published by
** the Free Software Foundation, either version 3 of the License, or
** (at your option) any later version.
**
** Wormtable is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
** GNU Lesser General Public License for more details.
**
** You should have received a copy of the GNU Lesser General Public License
** along with wormtable.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <Python.h>
#include <structmember.h>
#include <db.h>
#include "halffloat.h"

#ifdef _WIN32
#ifdef __MINGW32__
#define fseeko fseeko64
#define ftello ftello64
#else
#define fseeko _fseeki64
#define ftello _ftelli64
#endif
#endif

#if PY_MAJOR_VERSION >= 3
#define IS_PY3K
#endif

#define WT_READ 0
#define WT_WRITE 1

#define WT_UINT 0
#define WT_INT 1
#define WT_FLOAT 2
#define WT_CHAR 3

#define WT_VAR_1 0
#define WT_VAR_2 (-1)
#define WT_VAR_1_MAX_ELEMENTS 254
#define WT_VAR_2_MAX_ELEMENTS 65534
#define MAX_ROW_SIZE 65536
#define WT_MISSING_VALUE 1
#define OFFSET_LEN_RECORD_SIZE 10

/* This is the default defined by the linux fopen man pages. */
#define WT_DB_FILE_PERMS 0666

#define MODULE_DOC \
"Low level Berkeley DB interface for wormtable"

static PyObject *WormtableError;

typedef struct Column_t {
    PyObject_HEAD
    PyObject *name;
    PyObject *description;
    PyObject *min_element;
    PyObject *max_element;
    int position;
    int element_type;
    int element_size;
    int num_elements;
    int fixed_region_offset;
    void **input_elements; /* pointer to each elements in input format */
    void *element_buffer; /* parsed input elements in native CPU format */
    int num_buffered_elements;
    int (*string_to_native)(struct Column_t*, char *);
    int (*python_to_native)(struct Column_t*, PyObject *);
    int (*verify_elements)(struct Column_t*);
    int (*truncate_elements)(struct Column_t*, double);
    int (*pack_elements)(struct Column_t*, void *);
    int (*unpack_elements)(struct Column_t*, void *);
    PyObject *(*native_to_python)(struct Column_t *, int);
} Column;

/*
 * We're using a mixture of fixed and non-fixed size integer types because
 * of the conflicting needs of being exact about the sizes and interfacing
 * with Python. Ideally, all of the types would be fixed size for simplicity
 */

typedef struct {
    PyObject_HEAD
    DB *db;
    PyObject *db_filename;
    FILE *data_file;
    PyObject *data_filename;
    Column **columns;
    unsigned long long cache_size;
    unsigned int fixed_region_size;
    unsigned int num_columns;
    void *row_buffer;
    uint32_t row_buffer_size;     /* max size */
    uint32_t current_row_size;    /* current size */
    unsigned long long num_rows;
    /* row stats */
    unsigned long long total_row_size;
    unsigned int min_row_size;
    unsigned int max_row_size;
} Table;


typedef struct {
    PyObject_HEAD
    Table *table;
    DB *db;
    PyObject *db_filename;
    unsigned long long cache_size;
    uint32_t *columns;
    uint32_t num_columns;
    void *key_buffer;
    uint32_t key_buffer_size;
    double *bin_widths;
} Index;

typedef struct {
    PyObject_HEAD
    Index *index;
    DBC *cursor;
    int completed;
    uint32_t *read_columns;
    uint32_t num_read_columns;
    void *min_key;
    uint32_t min_key_size;
    void *max_key;
    uint32_t max_key_size;
} IndexRowIterator;


typedef struct {
    PyObject_HEAD
    Table *table;
    DBC *cursor;
    int completed;
    uint32_t *read_columns;
    uint32_t num_read_columns;
    void *min_key;
    uint32_t min_key_size;
    void *max_key;
    uint32_t max_key_size;
} TableRowIterator;


typedef struct {
    PyObject_HEAD
    Index *index;
    DBC *cursor;
} IndexKeyIterator;


static void
handle_bdb_error(int err)
{
    PyErr_SetString(WormtableError, db_strerror(err));
}

static void
handle_io_error(void)
{
    PyErr_SetFromErrno(WormtableError);
}

#ifndef WORDS_BIGENDIAN
/*
 * Copies n bytes of source into destination, swapping the order of the
 * bytes.
 */
static void
byteswap_copy(void* dest, void *source, size_t n)
{
    size_t j = 0;
    unsigned char *dest_c = (unsigned char *) dest;
    unsigned char *source_c = (unsigned char *) source;
    for (j = 0; j < n; j++) {
        dest_c[j] = source_c[n - j - 1];
    }
}
#endif

/*
 * Floating point packing and unpacking. Floating point values are stored as a
 * slight perturbation to the IEEE standards so that they sort correctly. This
 * is done by a simple rule: if the number is positive, flip the sign bit; if
 * it is negative, flip all bits. This procedure is reversed when we unpack
 * to recover the original values exactly.
 */

static void
pack_half(double value, void *dest)
{
    union { uint16_t value; int16_t bits; } conv;
    int16_t bits;
    conv.value = npy_double_to_half(value);
    bits = conv.bits;
    bits ^= (bits < 0) ? 0xffff: 0x8000;
#ifdef WORDS_BIGENDIAN
    memcpy(dest, &bits, 2);
#else
    byteswap_copy(dest, &bits, 2);
#endif
}

static double
unpack_half(void *src)
{
    int16_t bits;
    double v;
#ifdef WORDS_BIGENDIAN
    memcpy(&bits, src, 2);
#else
    byteswap_copy(&bits, src, 2);
#endif
    bits ^= (bits < 0) ? 0x8000: 0xffff;
    v = npy_half_to_double(bits);
    return v;
}

static void
pack_float(double value, void *dest)
{
    int32_t bits;
    union { float value; int32_t bits; } conv;
    conv.value = (float) value;
    bits = conv.bits;
    bits ^= (bits < 0) ? 0xffffffffL: 0x80000000L;
#ifdef WORDS_BIGENDIAN
    memcpy(dest, &bits, sizeof(float));
#else
    byteswap_copy(dest, &bits, sizeof(float));
#endif
}

static double
unpack_float(void *src)
{
    union { float value; int32_t bits; } conv;
    int32_t bits;
#ifdef WORDS_BIGENDIAN
    memcpy(&bits, src, sizeof(float));
#else
    byteswap_copy(&bits, src, sizeof(float));
#endif
    bits ^= (bits < 0) ? 0x80000000L: 0xffffffffL;
    conv.bits = bits;
    return (double) conv.value;
}

static void
pack_double(double value, void *dest)
{
    union { double value; int64_t bits; } conv;
    int64_t bits;
    conv.value = value;
    bits = conv.bits;
    bits ^= (bits < 0) ? 0xffffffffffffffffLL: 0x8000000000000000LL;
#ifdef WORDS_BIGENDIAN
    memcpy(dest, &bits, sizeof(double));
#else
    byteswap_copy(dest, &bits, sizeof(double));
#endif
}

static double
unpack_double(void *src)
{
    union { double value; int64_t bits; } conv;
    int64_t bits;
#ifdef WORDS_BIGENDIAN
    memcpy(&bits, src, sizeof(double));
#else
    byteswap_copy(&bits, src, sizeof(double));
#endif
    bits ^= (bits < 0) ? 0x8000000000000000LL: 0xffffffffffffffffLL;
    conv.bits = bits;
    return conv.value;
}


/* Integer packing and unpacking.
 * TODO document the format.
 */

static void
pack_uint(uint64_t value, void *dest, uint8_t size)
{
    char *src;
    uint64_t u = value;
    /* increment before storing */
    u += 1;
    src = (char *) &u;
#ifdef WORDS_BIGENDIAN
    memcpy(dest, src + (8 - size), size);
#else
    byteswap_copy(dest, src, size);
#endif
}

static uint64_t
unpack_uint(void *src, uint8_t size)
{
    uint64_t dest = 0;
    char *v = (char *) &dest;
#ifdef WORDS_BIGENDIAN
    memcpy(v + 8 - size, src, size);
#else
    byteswap_copy(v, src, size);
#endif
    /* decrement and return */
    dest -= 1;
    return dest;
}


static void
pack_int(int64_t value, void *dest, uint8_t size)
{
    char *src;
    int64_t u = value;
    const int64_t m = 1LL << (size * 8 - 1);
    /* flip the sign bit */
    u ^= m;
    src = (char *) &u;
#ifdef WORDS_BIGENDIAN
    memcpy(dest, src + (8 - size), size);
#else
    byteswap_copy(dest, src, size);
#endif
}

static int64_t
unpack_int(void *src, uint8_t size)
{
    int64_t dest = 0;
    char *v = (char *) &dest;
    const int64_t m = 1LL << (size * 8 - 1);
#ifdef WORDS_BIGENDIAN
    memcpy(v + 8 - size, src, size);
#else
    byteswap_copy(v, src, size);
#endif
    /* flip the sign bit */
    dest ^= m;
    /* sign extend and return */
    dest = (dest ^ m) - m;
    return dest;
}


/*
 * Returns the missing value for a k byte integer.
 */
static int64_t
missing_int(uint32_t k)
{
    int64_t v = (-1) * (1ll << (8 * k - 1));
    return v;
}

/*
 * Returns the minimum value for a k byte integer.
 */
static int64_t
min_int(uint32_t k)
{
    int64_t v = (-1) * (1ll << (8 * k - 1)) + 1;
    return v;
}

/*
 * Returns the maximum value for a k byte integer.
 */
static int64_t
max_int(uint32_t k)
{
    int64_t v = (1ll << (8 * k - 1)) - 1;
    return v;
}

/*
 * Returns the missing value for a k byte unsigned integer.
 */
static uint64_t
missing_uint(uint32_t k)
{
    return (uint64_t) -1ll;
}

/*
 * Returns the maximum value for a k byte unsigned integer.
 */
static uint64_t
max_uint(uint32_t k)
{
    uint64_t v = (uint64_t) -1ll;
    if (k < 8) {
        v = (1ll << (8 * k)) - 1;
    }
    return v - 1;
}

/*
 * Returns the minumum value for a k byte unsigned integer.
 */
static uint64_t
min_uint(uint32_t k)
{
    uint64_t v = 0ll;
    return v;
}


/*
 * Returns the missing value for a k byte float. This
 * is returned as an unsigned integer since we cannot
 * compare NaN values as doubles.
 */
static uint64_t
missing_float(uint32_t k)
{
    uint64_t zero = 0uLL;
    union { double value; uint64_t bits; } conv;
    conv.bits = 0;
    switch (k) {
        case 2:
            conv.value = unpack_half(&zero);
            break;
        case 4:
            conv.value = unpack_float(&zero);
            break;
        case 8:
            conv.value = unpack_double(&zero);
            break;
    }
    return conv.bits;
}


/*==========================================================
 * Column object
 *==========================================================
 */

/*
 * Returns true if this Column is variable length.
 */
static int
Column_is_variable(Column *self)
{
    return self->num_elements == WT_VAR_1 || self->num_elements == WT_VAR_2;
}

/*
 * Returns the maximum number of elements that can be stored in this Column.
 */
static uint32_t
Column_get_max_num_elements(Column *self)
{
    uint32_t ret = self->num_elements;
    if (Column_is_variable(self)) {
        if (self->num_elements == WT_VAR_1) {
            ret = WT_VAR_1_MAX_ELEMENTS;
        } else {
            ret = WT_VAR_2_MAX_ELEMENTS;
        }
    }
    return ret;
}

/**************************************
 *
 * Native values to Python conversion.
 *
 *************************************/

static PyObject *
Column_native_to_python_uint(Column *self, int index)
{
    PyObject *ret = NULL;
    uint64_t *elements = (uint64_t *) self->element_buffer;
    uint64_t missing_value = missing_uint(self->element_size);
    if (elements[index] == missing_value) {
        Py_INCREF(Py_None);
        ret = Py_None;
    } else {
        ret = PyLong_FromUnsignedLongLong((unsigned long long) elements[index]);
        if (ret == NULL) {
            PyErr_NoMemory();
        }
    }
    return ret;
}

static PyObject *
Column_native_to_python_int(Column *self, int index)
{
    PyObject *ret = NULL;
    int64_t *elements = (int64_t *) self->element_buffer;
    int64_t missing_value = missing_int(self->element_size);
    if (elements[index] == missing_value) {
        Py_INCREF(Py_None);
        ret = Py_None;
    } else {
        ret = PyLong_FromLongLong((long long) elements[index]);
        if (ret == NULL) {
            PyErr_NoMemory();
        }
    }
    return ret;
}

static PyObject *
Column_native_to_python_float(Column *self, int index)
{
    PyObject *ret = NULL;
    union { double value; uint64_t bits; } conv;
    double *elements = (double *) self->element_buffer;
    uint64_t missing_bits = missing_float(self->element_size);
    conv.value = elements[index];
    if (conv.bits == missing_bits) {
        Py_INCREF(Py_None);
        ret = Py_None;
    } else {
        ret = PyFloat_FromDouble(elements[index]);
        if (ret == NULL) {
            PyErr_NoMemory();
        }
    }
    return ret;
}

static PyObject *
Column_native_to_python_char(Column *self, int index)
{
    PyObject *ret = NULL;
    int j = self->num_buffered_elements;
    char *str = (char *) self->element_buffer;
    ret = PyBytes_FromStringAndSize(str, j);
    if (ret == NULL) {
        PyErr_NoMemory();
    }
    return ret;
}

/**************************************
 *
 * Unpacking from a row to the element buffer.
 *
 *************************************/

/*
 * Unpack values starting at the specified pointer, and return the
 * number of missing_values encountered.
 */
static int
Column_unpack_elements_uint(Column *self, void *source)
{
    int j;
    int ret = 0;
    char *v = (char *) source;
    uint64_t *elements = (uint64_t *) self->element_buffer;
    uint64_t missing_value = missing_uint(self->element_size);
    int size = self->element_size;
    for (j = 0; j < self->num_buffered_elements; j++) {
        elements[j] = unpack_uint(v + j * size, size);
        if (elements[j] == missing_value) {
            ret += 1;
        }
    }
    return ret;
}


static int
Column_unpack_elements_int(Column *self, void *source)
{
    int j;
    int ret = 0;
    char *v = (char *) source;
    int64_t *elements = (int64_t *) self->element_buffer;
    int64_t missing_value = missing_int(self->element_size);
    int size = self->element_size;
    for (j = 0; j < self->num_buffered_elements; j++) {
        elements[j] = unpack_int(v + j * size, size);
        if (elements[j] == missing_value) {
            ret += 1;
        }
    }
    return ret;
}

static int
Column_unpack_elements_float_2(Column *self, void *source)
{
    int j;
    int ret = 0;
    char *v = (char *) source;
    double *elements = (double *) self->element_buffer;
    union { double value; uint64_t bits; } conv;
    uint64_t missing_bits = missing_float(self->element_size);
    for (j = 0; j < self->num_buffered_elements; j++) {
        elements[j] = unpack_half(v);
        v += self->element_size;
        conv.value = elements[j];
        if (conv.bits == missing_bits) {
            ret += 1;
        }
    }
    return ret;
}

static int
Column_unpack_elements_float_4(Column *self, void *source)
{
    int j;
    int ret = 0;
    char *v = (char *) source;
    double *elements = (double *) self->element_buffer;
    union { double value; uint64_t bits; } conv;
    uint64_t missing_bits = missing_float(self->element_size);
    for (j = 0; j < self->num_buffered_elements; j++) {
        elements[j] = unpack_float(v);
        v += self->element_size;
        conv.value = elements[j];
        if (conv.bits == missing_bits) {
            ret += 1;
        }
    }
    return ret;
}

static int
Column_unpack_elements_float_8(Column *self, void *source)
{
    int j;
    int ret = 0;
    char *v = (char *) source;
    double *elements = (double *) self->element_buffer;
    union { double value; uint64_t bits; } conv;
    uint64_t missing_bits = missing_float(self->element_size);
    for (j = 0; j < self->num_buffered_elements; j++) {
        elements[j] = unpack_double(v);
        v += self->element_size;
        conv.value = elements[j];
        if (conv.bits == missing_bits) {
            ret += 1;
        }
    }
    return ret;
}

static int
Column_unpack_elements_char(Column *self, void *source)
{
    int ret = 0;
    int j;
    char *v = (char *) source;
    memcpy(self->element_buffer, source, self->num_buffered_elements);
    /* scan for zero bytes to get missing values */
    for (j = 0; j < self->num_buffered_elements; j++) {
        if (v[j] == 0) {
            ret += 1;
        }
    }
    return ret;
}

/**************************************
 *
 * Packing native values from the element_buffer to a row.
 *
 *************************************/

static int
Column_pack_elements_uint(Column *self, void *dest)
{
    int j;
    int ret = -1;
    char *v = (char *) dest;
    uint64_t *elements = (uint64_t *) self->element_buffer;
    for (j = 0; j < self->num_buffered_elements; j++) {
        pack_uint(elements[j], v, self->element_size);
        v += self->element_size;
    }
    ret = 0;
    return ret;
}

static int
Column_pack_elements_int(Column *self, void *dest)
{
    int j;
    int ret = -1;
    char *v = (char *) dest;
    int64_t *elements = (int64_t *) self->element_buffer;
    for (j = 0; j < self->num_buffered_elements; j++) {
        pack_int(elements[j], v, self->element_size);
        v += self->element_size;
    }
    ret = 0;
    return ret;
}

static int
Column_pack_elements_float_2(Column *self, void *dest)
{
    int j;
    int ret = -1;
    char *v = (char *) dest;
    double *elements = (double *) self->element_buffer;
    for (j = 0; j < self->num_buffered_elements; j++) {
        pack_half(elements[j], v);
        v += self->element_size;
    }
    ret = 0;
    return ret;
}

static int
Column_pack_elements_float_4(Column *self, void *dest)
{
    int j;
    int ret = -1;
    char *v = (char *) dest;
    double *elements = (double *) self->element_buffer;
    for (j = 0; j < self->num_buffered_elements; j++) {
        pack_float(elements[j], v);
        v += self->element_size;
    }
    ret = 0;
    return ret;
}

static int
Column_pack_elements_float_8(Column *self, void *dest)
{
    int j;
    int ret = -1;
    char *v = (char *) dest;
    double *elements = (double *) self->element_buffer;
    for (j = 0; j < self->num_buffered_elements; j++) {
        pack_double(elements[j], v);
        v += self->element_size;
    }
    ret = 0;
    return ret;
}

static int
Column_pack_elements_char(Column *self, void *dest)
{
    int ret = -1;
    memcpy(dest, self->element_buffer, self->num_buffered_elements);
    ret = 0;
    return ret;
}



/**************************************
 *
 * Verify elements in the buffer.
 *
 *************************************/

/* This doesn't really have any useful function any more, as we were forced
 * to put the range checking functionality into the native_to_python_int
 * function. This is was because there was no way to exclude the user
 * from using the missing_value as an input parameter, and this would
 * have led to very annoying bugs. This doesn't do any harm here though,
 * so let's leave it in for now.
 */
static int
Column_verify_elements_uint(Column *self)
{
    int j;
    int ret = -1;
    uint64_t *elements = (uint64_t *) self->element_buffer;
    uint64_t min_value = min_uint(self->element_size);
    uint64_t max_value = max_uint(self->element_size);
    uint64_t missing_value = missing_uint(self->element_size);
    for (j = 0; j < self->num_buffered_elements; j++) {
        if (elements[j] != missing_value &&
                (elements[j] < min_value || elements[j] > max_value)) {
            PyErr_Format(PyExc_OverflowError,
                    "Values for column '%s' must be between %lld and %lld",
                    PyBytes_AsString(self->name), (long long) min_value,
                    (long long) max_value);
            goto out;
        }
    }
    ret = 0;
out:
    return ret;

}

static int
Column_verify_elements_int(Column *self)
{
    int j;
    int ret = -1;
    int64_t *elements = (int64_t *) self->element_buffer;
    int64_t min_value = min_int(self->element_size);
    int64_t max_value = max_int(self->element_size);
    int64_t missing_value = missing_int(self->element_size);
    for (j = 0; j < self->num_buffered_elements; j++) {
        if (elements[j] != missing_value &&
                (elements[j] < min_value || elements[j] > max_value)) {
            PyErr_Format(PyExc_OverflowError,
                    "Values for column '%s' must be between %lld and %lld",
                    PyBytes_AsString(self->name), (long long) min_value,
                    (long long) max_value);
            goto out;
        }
    }
    ret = 0;
out:
    return ret;

}

static int
Column_verify_elements_float(Column *self)
{
    return 0;
}

static int
Column_verify_elements_char(Column *self)
{
    return 0;
}

/**************************************
 *
 * Truncate elements in the buffer.
 *
 *************************************/
static int
Column_truncate_elements_uint(Column *self, double bin_width)
{
    int ret = 0;
    unsigned int j;
    uint64_t w = (uint64_t) bin_width;
    uint64_t *elements = (uint64_t *) self->element_buffer;
    uint64_t missing_value = missing_uint(self->element_size);
    uint64_t u;
    if (bin_width <= 0.0) {
        PyErr_Format(PyExc_SystemError, "bin_width for column '%s' must > 0",
                PyBytes_AsString(self->name));
        goto out;
    }
    for (j = 0; j < self->num_buffered_elements; j++) {
        u = elements[j];
        if (u != missing_value) {
            elements[j] = u - (u % w);
        }
    }
    ret = 0;
out:
    return ret;
}



static int
Column_truncate_elements_int(Column *self, double bin_width)
{
    int ret = 0;
    unsigned int j;
    int64_t w = (int64_t) bin_width;
    int64_t *elements = (int64_t *) self->element_buffer;
    int64_t missing_value = missing_int(self->element_size);
    int64_t u;
    if (bin_width <= 0.0) {
        PyErr_Format(PyExc_SystemError, "bin_width for column '%s' must > 0",
                PyBytes_AsString(self->name));
        goto out;
    }
    for (j = 0; j < self->num_buffered_elements; j++) {
        u = elements[j];
        if (u != missing_value) {
            elements[j] = u - (u % w);
        }
    }
    ret = 0;
out:
    return ret;
}

static int
Column_truncate_elements_float(Column *self, double bin_width)
{
    int ret = -1;
    unsigned int j;
    union { double value; uint64_t bits; } conv;
    uint64_t missing_bits = missing_float(self->element_size);
    double *elements = (double *) self->element_buffer;
    double u;
    if (bin_width <= 0.0) {
        PyErr_Format(PyExc_SystemError, "bin_width for column '%s' must > 0",
                PyBytes_AsString(self->name));
        goto out;
    }
    for (j = 0; j < self->num_buffered_elements; j++) {
        u = elements[j];
        conv.value = u;
        if (conv.bits != missing_bits) {
            elements[j] = u - fmod(u, bin_width);
        }
    }
    ret = 0;
out:
    return ret;

}

static int
Column_truncate_elements_char(Column *self, double bin_width)
{
    return 0;
}


/**************************************
 *
 * Python input element parsing.
 *
 *************************************/


/*
 * Takes a Python sequence and places pointers to the Python
 * elements into the input_elements list. Checks for various
 * errors in the format of this sequence.
 */
static int
Column_parse_python_sequence(Column *self, PyObject *elements)
{
    int ret = -1;
    int j, num_elements;
    PyObject *seq = NULL;
    PyObject *v;
    self->num_buffered_elements = 0;
    if (self->num_elements == 1) {
        self->input_elements[0] = elements;
        num_elements = 1;
    } else {
        seq = PySequence_Fast(elements, "Sequence required");
        if (seq == NULL) {
            goto out;
        }
        num_elements = PySequence_Fast_GET_SIZE(seq);
        if (Column_is_variable(self)) {
            if (num_elements > Column_get_max_num_elements(self)) {
                PyErr_Format(PyExc_ValueError,
                        "too many elements for column '%s'",
                        PyBytes_AsString(self->name));
                goto out;
            }
        } else {
            if (num_elements != self->num_elements) {
                PyErr_Format(PyExc_ValueError,
                        "incorrect number of elements for column '%s'",
                        PyBytes_AsString(self->name));
                goto out;
            }
        }
        for (j = 0; j < num_elements; j++) {
            v = PySequence_Fast_GET_ITEM(seq, j);
            self->input_elements[j] = v;
        }
    }
    self->num_buffered_elements = num_elements;
    ret = 0;
out:
    Py_XDECREF(seq);
    return ret;
}

static int
Column_python_to_native_uint(Column *self, PyObject *elements)
{
    int ret = -1;
    uint64_t *native= (uint64_t *) self->element_buffer;
    uint64_t missing_value = missing_uint(self->element_size);
    uint64_t min_value = min_uint(self->element_size);
    uint64_t max_value = max_uint(self->element_size);
    PyObject *v;
    int j;
    if (elements == Py_None) {
        if (Column_is_variable(self)) {
            self->num_buffered_elements = 0;
        } else {
            for (j = 0; j < self->num_elements; j++) {
                native[j] = missing_value;
            }
            self->num_buffered_elements = self->num_elements;
        }
        ret = WT_MISSING_VALUE;
    } else {
        if (Column_parse_python_sequence(self, elements) < 0) {
            goto out;
        }
        for (j = 0; j < self->num_buffered_elements; j++) {
            v = (PyObject *) self->input_elements[j];
            if (!PyNumber_Check(v)) {
                PyErr_Format(PyExc_TypeError,
                        "Values for column '%s' must be numeric",
                        PyBytes_AsString(self->name));
                goto out;
            }
#ifdef IS_PY3K
            native[j] = (uint64_t) PyLong_AsUnsignedLongLong(v);
#else
            /* there's a problem with Python 2 in which we have to convert to
             * long first.
             */
            v = PyNumber_Long(v);
            if (v == NULL) {
                goto out;
            }
            native[j] = (uint64_t) PyLong_AsUnsignedLongLong(v);
            Py_DECREF(v);
#endif
            if (native[j] == -1) {
                /* PyLong_AsUnsignedLongLong return -1 and raises OverFlowError
                 * if the value cannot be represented as an unsigned long long
                 */
                if (PyErr_Occurred()) {
                    goto out;
                }
            }
            /* check if the values are in the right range for the column */
            if (native[j] < min_value || native[j] > max_value) {
                PyErr_Format(PyExc_OverflowError,
                        "Values for column '%s' must be between %lld and %lld",
                        PyBytes_AsString(self->name), (long long) min_value,
                        (long long) max_value);
                goto out;
            }
        }
        ret = 0;
    }
out:
    return ret;
}


static int
Column_python_to_native_int(Column *self, PyObject *elements)
{
    int ret = -1;
    int64_t *native= (int64_t *) self->element_buffer;
    int64_t missing_value = missing_int(self->element_size);
    int64_t min_value = min_int(self->element_size);
    int64_t max_value = max_int(self->element_size);
    PyObject *v;
    int j;
    if (elements == Py_None) {
        if (Column_is_variable(self)) {
            self->num_buffered_elements = 0;
        } else {
            for (j = 0; j < self->num_elements; j++) {
                native[j] = missing_value;
            }
            self->num_buffered_elements = self->num_elements;
        }
        ret = WT_MISSING_VALUE;
    } else {
        if (Column_parse_python_sequence(self, elements) < 0) {
            goto out;
        }
        for (j = 0; j < self->num_buffered_elements; j++) {
            v = (PyObject *) self->input_elements[j];
            if (!PyNumber_Check(v)) {
                PyErr_Format(PyExc_TypeError,
                        "Values for column '%s' must be numeric",
                        PyBytes_AsString(self->name));
                goto out;
            }
            native[j] = (int64_t) PyLong_AsLongLong(v);
            if (native[j] == -1) {
                /* PyLong_AsLongLong return -1 and raises OverFlowError if
                 * the value cannot be represented as a long long
                 */
                if (PyErr_Occurred()) {
                    goto out;
                }
            }
            /* check if the values are in the right range for the column */
            if (native[j] < min_value || native[j] > max_value) {
                PyErr_Format(PyExc_OverflowError,
                        "Values for column '%s' must be between %lld and %lld",
                        PyBytes_AsString(self->name), (long long) min_value,
                        (long long) max_value);
                goto out;
            }
        }
        ret = 0;
    }
out:
    return ret;
}


static int
Column_python_to_native_float(Column *self, PyObject *elements)
{
    int ret = -1;
    double *native = (double *) self->element_buffer;
    union { double value; uint64_t bits; } conv;
    PyObject *v;
    int j;
    conv.bits = missing_float(self->element_size);
    if (elements == Py_None) {
        if (Column_is_variable(self)) {
            self->num_buffered_elements = 0;
        } else {
            for (j = 0; j < self->num_elements; j++) {
                native[j] = conv.value;
            }
            self->num_buffered_elements = self->num_elements;
        }
        ret = WT_MISSING_VALUE;
    } else {
        if (Column_parse_python_sequence(self, elements) < 0) {
            goto out;
        }
        for (j = 0; j < self->num_buffered_elements; j++) {
            v = (PyObject *) self->input_elements[j];
            if (!PyNumber_Check(v)) {
                PyErr_Format(PyExc_TypeError,
                        "Values for column '%s' must be numeric",
                        PyBytes_AsString(self->name));
                goto out;
            }
            native[j] = (double) PyFloat_AsDouble(v);
        }
        ret = 0;
    }
out:
    return ret;
}

static int
Column_python_to_native_char(Column *self, PyObject *elements)
{
    int ret = -1;
    char *s;
    Py_ssize_t length;

    if (elements == Py_None) {
        if (Column_is_variable(self)) {
            self->num_buffered_elements = 0;
        } else {
            memset(self->element_buffer, 0, self->num_elements);
            self->num_buffered_elements = self->num_elements;
        }
        ret = WT_MISSING_VALUE;
    } else {
        /* Elements must be a single Python bytes object */
        if (!PyBytes_Check(elements)) {
            PyErr_Format(PyExc_TypeError,
                    "Values for column '%s' must be bytes",
                    PyBytes_AsString(self->name));
            goto out;
        }
        if (PyBytes_AsStringAndSize(elements, &s, &length) < 0) {
            PyErr_Format(PyExc_ValueError,
                    "String conversion failed for column '%s'",
                    PyBytes_AsString(self->name));
            goto out;
        }
        if (Column_is_variable(self)) {
            if (length > Column_get_max_num_elements(self)) {
                PyErr_Format(PyExc_ValueError,
                        "String too long for column '%s'",
                        PyBytes_AsString(self->name));
                goto out;
            }
        } else {
            if (length != self->num_elements) {
                PyErr_Format(PyExc_ValueError,
                        "String incorrect length for column '%s'",
                        PyBytes_AsString(self->name));
                goto out;
            }
        }
        memcpy(self->element_buffer, s, length);
        self->num_buffered_elements = length;
        ret = 0;
    }
out:
    return ret;
}



/**************************************
 *
 * String input element parsing.
 *
 *************************************/

static void
Column_encoded_elements_parse_error(Column *self, const char *message,
        char *source)
{
    PyErr_Format(PyExc_ValueError,
            "Parse error on column '%s': %s: '%s'",
            PyBytes_AsString(self->name), message, source);
}


/*
 * Takes a string sequence and places pointers to the start
 * of each individual element into the input_elements list.
 * Checks for various errors in the format of this sequence.
 */
static int
Column_parse_string_sequence(Column *self, char *s)
{
    int ret = -1;
    int j, num_elements, delimiter;
    int max_num_elements = Column_get_max_num_elements(self);
    self->num_buffered_elements = 0;
    if (self->num_elements == 1) {
        self->input_elements[0] = s;
        num_elements = 1;
    } else {
        j = 0;
        num_elements = 0;
        delimiter = -1;
        while (s[j] != '\0') {
            if (s[j] == ',' || s[j] == ';') {
                delimiter = j;
            }
            if (j == delimiter + 1) {
                if (num_elements >= max_num_elements) {
                    Column_encoded_elements_parse_error(self,
                            "incorrect number of elements", s);
                    goto out;
                }
                /* this is the start of a new element */
                self->input_elements[num_elements] = &s[j];
                num_elements++;
            }
            j++;
        }
    }
    if (!Column_is_variable(self)) {
        if (num_elements != self->num_elements) {
            Column_encoded_elements_parse_error(self,
                    "incorrect number of elements", s);
            goto out;
        }
    }
    self->num_buffered_elements = num_elements;
    ret = 0;
out:
    return ret;
}

static int
Column_string_to_native_uint(Column *self, char *string)
{
    int ret = -1;
    uint64_t *native= (uint64_t *) self->element_buffer;
    char *v, *tail;
    int j;
    if (Column_parse_string_sequence(self, string) < 0) {
        goto out;
    }
    for (j = 0; j < self->num_buffered_elements; j++) {
        v = (char *) self->input_elements[j];
        errno = 0;
        native[j] = (uint64_t) strtoull(v, &tail, 0);
        if (errno) {
            Column_encoded_elements_parse_error(self, "element overflow",
                    string);
            goto out;
        }
        if (v == tail) {
            Column_encoded_elements_parse_error(self, "parse error", string);
            goto out;
        }
        if (*tail != '\0') {
            if (!(isspace(*tail) || *tail == ',' || *tail == ';')) {
                Column_encoded_elements_parse_error(self, "parse error",
                        string);
                goto out;
            }
        }
    }
    ret = 0;
out:
    return ret;
}


static int
Column_string_to_native_int(Column *self, char *string)
{
    int ret = -1;
    int64_t *native= (int64_t *) self->element_buffer;
    char *v, *tail;
    int j;
    if (Column_parse_string_sequence(self, string) < 0) {
        goto out;
    }
    for (j = 0; j < self->num_buffered_elements; j++) {
        v = (char *) self->input_elements[j];
        errno = 0;
        native[j] = (int64_t) strtoll(v, &tail, 0);
        if (errno) {
            Column_encoded_elements_parse_error(self, "element overflow",
                    string);
            goto out;
        }
        if (v == tail) {
            Column_encoded_elements_parse_error(self, "parse error", string);
            goto out;
        }
        if (*tail != '\0') {
            if (!(isspace(*tail) || *tail == ',' || *tail == ';')) {
                Column_encoded_elements_parse_error(self, "parse error",
                        string);
                goto out;
            }
        }
    }
    ret = 0;
out:
    return ret;
}

static int
Column_string_to_native_float(Column *self, char *string)
{
    int ret = -1;
    double *native= (double *) self->element_buffer;
    char *v, *tail;
    int j;
    if (Column_parse_string_sequence(self, string) < 0) {
        goto out;
    }
    for (j = 0; j < self->num_buffered_elements; j++) {
        v = (char *) self->input_elements[j];
        errno = 0;
        native[j] = (double) strtod(v, &tail);
        if (errno) {
            Column_encoded_elements_parse_error(self, "element overflow",
                    string);
            goto out;
        }
        if (v == tail) {
            Column_encoded_elements_parse_error(self, "parse error", string);
            goto out;
        }
        if (*tail != '\0') {
            if (!(isspace(*tail) || *tail == ',' || *tail == ';')) {
                Column_encoded_elements_parse_error(self, "parse error",
                        string);
                goto out;
            }
        }
    }
    ret = 0;
out:
    return ret;
}

static int
Column_string_to_native_char(Column *self, char *string)
{
    int ret = -1;
    size_t length = strlen(string);
    if (Column_is_variable(self)) {
        if (length > Column_get_max_num_elements(self)) {
            PyErr_Format(PyExc_ValueError,
                    "String too long for column '%s'",
                    PyBytes_AsString(self->name));
            goto out;
        }
    } else {
        if (length != self->num_elements) {
            PyErr_Format(PyExc_ValueError,
                    "String incorrect length for column '%s'",
                    PyBytes_AsString(self->name));
            goto out;
        }
    }
    memcpy(self->element_buffer, string, length);
    self->num_buffered_elements = length;
    ret = 0;
out:
    return ret;
}


/*
 * Packs the address and number of elements in a variable length column at the
 * specified pointer.
 *
 */
static int
Column_pack_variable_elements_address(Column *self, void *dest,
        uint32_t offset, uint32_t num_elements)
{
    int ret = -1;
    char *v = (char *) dest;
    /* future version will support more general address size */
    unsigned int address_size = 2;
    unsigned int var_size = self->num_elements == WT_VAR_1 ? 1 : 2;
    if (offset >= MAX_ROW_SIZE) {
        PyErr_SetString(PyExc_SystemError, "Row overflow");
        goto out;
    }
    if (num_elements > Column_get_max_num_elements(self)) {
        PyErr_SetString(PyExc_SystemError, "too many elements");
        goto out;
    }
    pack_uint((uint64_t) offset, v, address_size);
    v += address_size;
    pack_uint((uint64_t) num_elements, v, var_size);
    ret = 0;
out:
    return ret;
}

/*
 * Unpacks the address and number of elements in a variable length column at the
 * specified pointer.
 */
static int
Column_unpack_variable_elements_address(Column *self, void *src,
        uint32_t *offset, uint32_t *num_elements)
{
    int ret = -1;
    char *v = (char *) src;
    uint64_t off = 0;
    uint64_t n = 0;
    /* future version will support more general address size */
    unsigned int address_size = 2;
    unsigned int var_size = self->num_elements == WT_VAR_1 ? 1 : 2;
    off = unpack_uint(v, address_size);
    if (off == missing_uint(address_size)) {
        off = 0;
    } else {
        v += address_size;
        n = unpack_uint(v, var_size);
        if (off >= MAX_ROW_SIZE) {
            PyErr_SetString(PyExc_SystemError, "Row overflow");
            goto out;
        }
        if (n > Column_get_max_num_elements(self)) {
            PyErr_SetString(PyExc_SystemError, "too many elements");
            goto out;
        }
    }
    *offset = (uint32_t) off;
    *num_elements = (uint32_t) n;
    ret = 0;
out:
    return ret;
}

/*
 * Inserts the values in the element buffer into the specified row which
 * is currently of the specified size, and return the number of bytes
 * used in the variable region. Returns -1 in the case of an error with
 * the appropriate Python exception set.
 */
static int
Column_update_row(Column *self, void *row, uint32_t row_size)
{
    int ret = -1;
    char *v = (char *) row;
    void *dest;
    int bytes_added = 0;
    uint32_t num_elements = (uint32_t) self->num_buffered_elements;
    int data_size = num_elements * self->element_size;
    if (self->verify_elements(self) < 0) {
        goto out;
    }
    dest = v + self->fixed_region_offset;
    if (Column_is_variable(self)) {
        bytes_added = data_size;
        if (row_size + bytes_added > MAX_ROW_SIZE) {
            PyErr_SetString(PyExc_ValueError, "Row overflow");
            goto out;
        }
        if (Column_pack_variable_elements_address(self, dest, row_size,
                num_elements) < 0) {
            goto out;
        }
        dest = v + row_size;
    }
    self->pack_elements(self, dest);
    ret = bytes_added;
out:
    return ret;
}


/*
 * Extract values from the specified key buffer starting at the specified
 * offset. Returns a positive value WT_MISSING_VALUE if the missing
 * value is detected, or 0 if not. Returns a negative value if an error
 * occured.
 */
static int
Column_extract_key(Column *self, void *key_buffer, uint32_t offset,
        uint32_t key_size)
{
    int ret = -1;
    char *v;
    char *kb = (char *) key_buffer;
    char s;
    uint32_t j, k;
    uint32_t element_size = self->element_size;
    uint32_t num_elements = self->num_elements;
    int not_done;

    if (Column_is_variable(self)) {
        /* count the number of elements until we hit the sentinel */
        num_elements = 0;
        not_done = 1;
        j = offset;
        v = kb;
        while (not_done) {
            s = 0;
            k = j + element_size;
            while (j < k) {
                if (j >= key_size) {
                    PyErr_SetString(PyExc_SystemError, "Key buffer overflow");
                    goto out;
                }
                s |= v[j];
                j++;
            }
            if (s == 0) {
                not_done = 0;
            } else {
                num_elements++;
            }
        }
        v = kb;
    }
    if (offset + num_elements * element_size > key_size) {
        PyErr_SetString(PyExc_SystemError, "Key offset too long");
        goto out;
    }
    v = kb + offset;
    self->num_buffered_elements = num_elements;
    ret = self->unpack_elements(self, v);
    if (ret > 0) {
        ret = WT_MISSING_VALUE;
    }
out:
    return ret;
}

/*
 * Extracts elements from the specified row and inserts them into the
 * element buffer. Returns a positive value WT_MISSING_VALUE if the
 * missing value was stored in this column, 0 if a non-missing value
 * was stored, and a negative value if an error occurs.
 */
static int
Column_extract_elements(Column *self, void *row)
{
    int ret = -1;
    char *v = (char *) row;
    void *src;
    uint32_t offset, num_elements;
    src = v + self->fixed_region_offset;
    if (Column_is_variable(self)) {
        if (Column_unpack_variable_elements_address(self, src, &offset,
                &num_elements) < 0) {
            goto out;
        }
        src = v + offset;
        self->num_buffered_elements = num_elements;
        ret = self->unpack_elements(self, src);
        if (ret > 0) {
            PyErr_SetString(PyExc_SystemError,
                    "Missing values detected within variable length column");
            goto out;
        }
        if (ret < 0) {
            goto out;
        }
        if (offset == 0) {
            ret = WT_MISSING_VALUE;
        }
    } else {
        self->num_buffered_elements = self->num_elements;;
        ret = self->unpack_elements(self, src);
        if (ret > 0) {
            ret = WT_MISSING_VALUE;
        }
    }
out:
    return ret;
}

/*
 * Converts the native values in the element buffer to the appropriate
 * Python types, and returns the result.
 */
static PyObject *
Column_get_python_elements(Column *self, int missing)
{
    PyObject *ret = NULL;
    PyObject *u, *t;
    Py_ssize_t j;
    if (missing) {
        Py_INCREF(Py_None);
        ret = Py_None;
    } else {
        if (self->element_type == WT_CHAR || self->num_elements == 1) {
            ret = self->native_to_python(self, 0);
            if (ret == NULL) {
                goto out;
            }
        } else {
            t = PyTuple_New(self->num_buffered_elements);
            if (t == NULL) {
                PyErr_NoMemory();
                goto out;
            }
            for (j = 0; j < self->num_buffered_elements; j++) {
                u = self->native_to_python(self, j);
                if (u == NULL) {
                    Py_DECREF(t);
                    PyErr_NoMemory();
                    goto out;
                }
                PyTuple_SET_ITEM(t, j, u);
            }
            ret = t;
        }
    }
out:
    return ret;
}

/*
 * Returns the number of bytes that this column occupies in the
 * fixed region of records.
 */
static int
Column_get_fixed_region_size(Column *self)
{
    int ret = self->element_size * self->num_elements;
    if (Column_is_variable(self)) {
        ret = 2; // TODO generalise for large address size.
        ret += self->num_elements == WT_VAR_1 ? 1 : 2;
    }
    return ret;
}

/**************************************
 *
 * Special methods for the row_id column
 *
 *************************************/

static int
Column_get_row_id(Column *self, uint64_t *key)
{
    int ret = -1;
    uint64_t *native = (uint64_t *) self->element_buffer;
    if (self->num_buffered_elements != 1) {
        PyErr_Format(PyExc_SystemError, "key retrieval error.");
        goto out;
    }
    *key = native[0];
    ret = 0;
out:
    return ret;
}

static int
Column_set_row_id(Column *self, uint64_t key)
{
    int ret = -1;
    uint64_t *native = (uint64_t *) self->element_buffer;
    native[0] = key;
    self->num_buffered_elements = 1;
    ret = 0;
    return ret;
}

static void
Column_dealloc(Column* self)
{
    Py_XDECREF(self->name);
    Py_XDECREF(self->description);
    Py_XDECREF(self->min_element);
    Py_XDECREF(self->max_element);
    PyMem_Free(self->element_buffer);
    PyMem_Free(self->input_elements);
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static int
Column_init(Column *self, PyObject *args, PyObject *kwds)
{
    int ret = -1;
    static char *kwlist[] = {"name", "description",  "element_type",
        "element_size", "num_elements", NULL};
    Py_ssize_t max_num_elements;
    Py_ssize_t native_element_size;
    PyObject *name = NULL;
    PyObject *description = NULL;
    self->position = -1;
    self->min_element = NULL;
    self->max_element = NULL;
    self->element_buffer = NULL;
    self->input_elements = NULL;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!O!iii", kwlist,
            &PyBytes_Type, &name,
            &PyBytes_Type, &description,
            &self->element_type, &self->element_size,
            &self->num_elements)) {
        goto out;
    }
    self->name = name;
    self->description = description;
    Py_INCREF(self->name);
    Py_INCREF(self->description);
    if (self->element_type == WT_UINT) {
        if (self->element_size < 1 || self->element_size > 8) {
            PyErr_SetString(PyExc_ValueError, "bad element size");
            goto out;
        }
        self->python_to_native = Column_python_to_native_uint;
        self->string_to_native = Column_string_to_native_uint;
        self->verify_elements = Column_verify_elements_uint;
        self->truncate_elements = Column_truncate_elements_uint;
        self->pack_elements = Column_pack_elements_uint;
        self->unpack_elements = Column_unpack_elements_uint;
        self->native_to_python = Column_native_to_python_uint;
        native_element_size = sizeof(uint64_t);
        self->min_element = PyLong_FromUnsignedLongLong(
                min_uint(self->element_size));
        self->max_element = PyLong_FromUnsignedLongLong(
                max_uint(self->element_size));
        if (self->min_element == NULL || self->max_element == NULL) {
            PyErr_NoMemory();
            goto out;
        }
    } else if (self->element_type == WT_INT) {
        if (self->element_size < 1 || self->element_size > 8) {
            PyErr_SetString(PyExc_ValueError, "bad element size");
            goto out;
        }
        self->python_to_native = Column_python_to_native_int;
        self->string_to_native = Column_string_to_native_int;
        self->verify_elements = Column_verify_elements_int;
        self->truncate_elements = Column_truncate_elements_int;
        self->pack_elements = Column_pack_elements_int;
        self->unpack_elements = Column_unpack_elements_int;
        self->native_to_python = Column_native_to_python_int;
        native_element_size = sizeof(int64_t);
        self->min_element = PyLong_FromLongLong(min_int(self->element_size));
        self->max_element = PyLong_FromLongLong(max_int(self->element_size));
        if (self->min_element == NULL || self->max_element == NULL) {
            PyErr_NoMemory();
            goto out;
        }
    } else if (self->element_type == WT_FLOAT) {
        if (self->element_size != 2 && self->element_size != sizeof(float)
                && self->element_size != sizeof(double)) {
            PyErr_SetString(PyExc_ValueError, "bad element size");
            goto out;
        }
        self->python_to_native = Column_python_to_native_float;
        self->string_to_native = Column_string_to_native_float;
        self->verify_elements = Column_verify_elements_float;
        self->truncate_elements = Column_truncate_elements_float;
        if (self->element_size == 2) {
            self->pack_elements = Column_pack_elements_float_2;
            self->unpack_elements = Column_unpack_elements_float_2;
        } else if (self->element_size == 4) {
            self->pack_elements = Column_pack_elements_float_4;
            self->unpack_elements = Column_unpack_elements_float_4;
        } else {
            self->pack_elements = Column_pack_elements_float_8;
            self->unpack_elements = Column_unpack_elements_float_8;
        }
        self->native_to_python = Column_native_to_python_float;
        native_element_size = sizeof(double);
        self->min_element = Py_None;
        self->max_element = Py_None;
        Py_INCREF(self->min_element);
        Py_INCREF(self->max_element);
    } else if (self->element_type == WT_CHAR) {
        if (self->element_size != 1) {
            PyErr_SetString(PyExc_ValueError, "bad element size");
            goto out;
        }
        self->python_to_native = Column_python_to_native_char;
        self->string_to_native = Column_string_to_native_char;
        self->verify_elements = Column_verify_elements_char;
        self->truncate_elements = Column_truncate_elements_char;
        self->pack_elements = Column_pack_elements_char;
        self->unpack_elements = Column_unpack_elements_char;
        self->native_to_python = Column_native_to_python_char;
        native_element_size = sizeof(char);
        self->min_element = Py_None;
        self->max_element = Py_None;
        Py_INCREF(self->min_element);
        Py_INCREF(self->max_element);
    } else {
        PyErr_SetString(PyExc_ValueError, "Unknown element type");
        goto out;
    }
    max_num_elements = self->num_elements;
    if (Column_is_variable(self)) {
        max_num_elements = Column_get_max_num_elements(self);
    } else if (self->num_elements > Column_get_max_num_elements(self)) {
        PyErr_SetString(PyExc_ValueError, "Too many elements");
        goto out;
    }
    if (self->num_elements < WT_VAR_2) { /* VAR_2 = -1 */
        PyErr_SetString(PyExc_ValueError, "negative num elements");
        goto out;
    }
    self->element_buffer = PyMem_Malloc(max_num_elements
            * native_element_size);
    if (self->element_buffer == NULL) {
        PyErr_NoMemory();
        goto out;
    }
    self->input_elements = PyMem_Malloc(max_num_elements * sizeof(void *));
    if (self->input_elements == NULL) {
        PyErr_NoMemory();
        goto out;
    }
    ret = 0;
out:
    return ret;
}

static PyMemberDef Column_members[] = {
    {"name", T_OBJECT_EX, offsetof(Column, name), READONLY, "name"},
    {"description", T_OBJECT_EX, offsetof(Column, description), READONLY, "description"},
    {"position", T_INT, offsetof(Column, position), READONLY, "position"},
    {"element_type", T_INT, offsetof(Column, element_type), READONLY, "element_type"},
    {"element_size", T_INT, offsetof(Column, element_size), READONLY, "element_size"},
    {"num_elements", T_INT, offsetof(Column, num_elements), READONLY, "num_elements"},
    {"fixed_region_offset", T_INT, offsetof(Column, fixed_region_offset),
        READONLY, "fixed_region_offset"},
    {"min_element", T_OBJECT_EX, offsetof(Column, min_element), READONLY, "minimum element"},
    {"max_element", T_OBJECT_EX, offsetof(Column, max_element), READONLY, "maximum element"},
    {NULL}  /* Sentinel */
};

PyDoc_STRVAR(Column_is_variable__doc__,
"is_variable() -> bool\n\n"
"Return True is this Column holds a variable number of elements.");
static PyObject *
Column_is_variable_py(Column *self)
{
    return PyBool_FromLong((long) Column_is_variable(self));
}

PyDoc_STRVAR(Column_get_max_num_elements__doc__,
"get_max_num_elements() -> int\n\n"
"Return the maximum number of elements that can be stored in this "
"Column.");
static PyObject *
Column_get_max_num_elements_py(Column *self)
{
    return PyLong_FromLong((long) Column_get_max_num_elements(self));
}

static PyMethodDef Column_methods[] = {
    {"is_variable", (PyCFunction) Column_is_variable_py, METH_NOARGS,
        Column_is_variable__doc__},
    {"get_max_num_elements", (PyCFunction) Column_get_max_num_elements_py,
        METH_NOARGS, Column_get_max_num_elements__doc__},
    {NULL}  /* Sentinel */
};

static PyTypeObject ColumnType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_wormtable.Column",             /* tp_name */
    sizeof(Column),             /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)Column_dealloc, /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_reserved */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash  */
    0,                         /* tp_call */
    0,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT |
        Py_TPFLAGS_BASETYPE,   /* tp_flags */
    "Column objects",           /* tp_doc */
    0,                     /* tp_traverse */
    0,                     /* tp_clear */
    0,                     /* tp_richcompare */
    0,                     /* tp_weaklistoffset */
    0,                     /* tp_iter */
    0,                     /* tp_iternext */
    Column_methods,             /* tp_methods */
    Column_members,             /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)Column_init,      /* tp_init */
};


/*==========================================================
 * Table object
 *==========================================================
 */

static void
Table_dealloc(Table* self)
{
    uint32_t j;
    Py_XDECREF(self->db_filename);
    Py_XDECREF(self->data_filename);
    /* make sure that the DB handles are closed. We can ignore errors here. */
    if (self->db != NULL) {
        self->db->close(self->db, 0);
    }
    if (self->data_file != NULL) {
        fclose(self->data_file);
    }
    if (self->row_buffer != NULL) {
        PyMem_Free(self->row_buffer);
    }
    if (self->columns != NULL) {
        /* columns must be decref'd but may be null */
        for (j = 0; j < self->num_columns; j++) {
            Py_XDECREF(self->columns[j]);
        }
        PyMem_Free(self->columns);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

/*
 * Checks the columns to ensure sanity.
 */
static int
Table_verify_columns(Table *self)
{
    int ret = -1;
    Column *col;
    uint32_t j, k;
    char *cj, *ck;
    if (self->num_columns < 2) {
        PyErr_SetString(PyExc_ValueError, "Two or more columns required");
        goto out;
    }
    for (j = 0; j < self->num_columns; j++) {
        if (!PyObject_TypeCheck(self->columns[j], &ColumnType)) {
            PyErr_SetString(PyExc_TypeError, "Must be Column objects");
            goto out;
        }
    }
    /* check the row_id column */
    col = self->columns[0];
    if (col->element_type != WT_UINT || col->num_elements != 1) {
        PyErr_SetString(PyExc_ValueError,
                "row_id column must be 1 element uint");
        goto out;
    }
    /* check for duplicate columns */
    /* TODO this is very slow for large numbers of columns - we should use a
     * python dictionary to check instead
     */
    for (j = 0; j < self->num_columns; j++) {
        cj = PyBytes_AsString(self->columns[j]->name);
        for (k = j + 1; k < self->num_columns; k++) {
            ck = PyBytes_AsString(self->columns[k]->name);
            if (self->columns[j] == self->columns[k]) {
                PyErr_SetString(PyExc_ValueError,
                        "Duplicate columns not permitted");
                goto out;
            }
            /* check for duplicate names */
            if (strcmp(cj, ck) == 0) {
                PyErr_SetString(PyExc_ValueError,
                        "Duplicate column names not permitted");
                goto out;
            }
        }
    }
    ret = 0;
out:
    return ret;
}

static int
Table_init(Table *self, PyObject *args, PyObject *kwds)
{
    int ret = -1;
    static char *kwlist[] = {"db_filename", "data_filename", "columns",
            "cache_size", NULL};
    Column *col;
    PyObject *db_filename = NULL;
    PyObject *data_filename = NULL;
    PyObject *columns = NULL;
    uint32_t j;
    self->db = NULL;
    self->row_buffer = NULL;
    self->columns = NULL;
    self->db_filename = NULL;
    self->cache_size = 0;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!O!O!K", kwlist,
            &PyBytes_Type, &db_filename,
            &PyBytes_Type, &data_filename,
            &PyList_Type,  &columns,
            &self->cache_size)) {
        goto out;
    }
    self->db_filename = db_filename;
    Py_INCREF(self->db_filename);
    self->data_filename = data_filename;
    Py_INCREF(self->data_filename);
    self->num_columns = PyList_GET_SIZE(columns);
    self->columns = PyMem_Malloc(self->num_columns * sizeof(Column *));
    if (self->columns == NULL) {
        PyErr_NoMemory();
        goto out;
    }
    for (j = 0; j < self->num_columns; j++) {
        col = (Column *) PyList_GET_ITEM(columns, j);
        Py_INCREF(col);
        self->columns[j] = col;
    }
    if (Table_verify_columns(self) != 0) {
        goto out;
    }
    self->row_buffer = PyMem_Malloc(MAX_ROW_SIZE);
    if (self->row_buffer == NULL) {
        PyErr_NoMemory();
        goto out;
    }
    self->row_buffer_size = MAX_ROW_SIZE;
    memset(self->row_buffer, 0, self->row_buffer_size);
    self->fixed_region_size = 0;
    for (j = 0; j < self->num_columns; j++) {
        col = self->columns[j];
        col->position = j;
        col->fixed_region_offset = self->fixed_region_size;
        self->fixed_region_size += Column_get_fixed_region_size(col);
        if (self->fixed_region_size > MAX_ROW_SIZE) {
            PyErr_SetString(WormtableError, "Columns exceed max row size");
            goto out;
        }
    }
    self->current_row_size = self->fixed_region_size;
    self->num_rows = 0;
    self->max_row_size = 0;
    self->min_row_size = MAX_ROW_SIZE;
    self->total_row_size = 0;
    ret = 0;
out:
    return ret;
}

static PyMemberDef Table_members[] = {
    {"db_filename", T_OBJECT_EX, offsetof(Table, db_filename), READONLY, "db_filename"},
    {"data_filename", T_OBJECT_EX, offsetof(Table, data_filename), READONLY, "data_filename"},
    {"cache_size", T_ULONGLONG, offsetof(Table, cache_size), READONLY, "cache_size"},
    {"num_rows", T_ULONGLONG, offsetof(Table, num_rows), READONLY, "num_rows"},
    {"total_row_size", T_ULONGLONG, offsetof(Table, total_row_size), READONLY, "total_row_size"},
    {"min_row_size", T_UINT, offsetof(Table, min_row_size), READONLY, "min_row_size"},
    {"max_row_size", T_UINT, offsetof(Table, max_row_size), READONLY, "max_row_size"},
    {"fixed_region_size", T_UINT, offsetof(Table, fixed_region_size), READONLY,
            "fixed_region_size"},
    {NULL}  /* Sentinel */
};


/*
 * Returns 0 if the column is value. Otherwise
 * -1 is returned with the appropriate Python exception set.
 */
static int
Table_check_column_index(Table *self, int col_index)
{
    int ret = -1;
    if (col_index < 0 || col_index >= self->num_columns) {
        PyErr_Format(WormtableError, "Column index out of range.");
        goto out;
    }
    ret = 0;
out:
    return ret;
}

/*
 * Returns 0 if the table is opened in write mode. Otherwise
 * -1 is returned with the appropriate Python exception set.
 */
static int
Table_check_write_mode(Table *self)
{
    int ret = -1;
    int db_ret;
    uint32_t flags;
    if (self->db == NULL) {
        PyErr_Format(WormtableError, "Table closed.");
        goto out;
    }
    db_ret = self->db->get_open_flags(self->db, &flags);
    if (db_ret != 0) {
        handle_bdb_error(flags);
        goto out;
    }
    if ((flags & DB_RDONLY) != 0) {
        PyErr_Format(WormtableError, "Table must be opened WT_WRITE.");
        goto out;
    }
    ret = 0;
out:
    return ret;
}


/*
 * Returns 0 if the table is opened in read mode. Otherwise
 * -1 is returned with the appropriate Python exception set.
 */
static int
Table_check_read_mode(Table *self)
{
    int ret = -1;
    int db_ret;
    uint32_t flags;
    if (self == NULL) {
        PyErr_Format(PyExc_SystemError, "Null table.");
        goto out;
    }
    if (self->db == NULL) {
        PyErr_Format(WormtableError, "Table closed.");
        goto out;
    }
    db_ret = self->db->get_open_flags(self->db, &flags);
    if (db_ret != 0) {
        handle_bdb_error(flags);
        goto out;
    }
    if ((flags & DB_RDONLY) == 0) {
        PyErr_Format(WormtableError, "Table must be opened WT_READ.");
        goto out;
    }
    ret = 0;
out:
    return ret;
}

static PyObject *
Table_open(Table* self, PyObject *args)
{
    PyObject *ret = NULL;
    char *db_name = NULL;
    char *data_name = NULL;
    char *data_mode = NULL;
    uint32_t flags = 0;
    Py_ssize_t gigabyte = 1024 * 1024 * 1024;
    uint32_t gigs, bytes;
    int db_ret, mode;
    if (!PyArg_ParseTuple(args, "i", &mode)) {
        goto out;
    }
    if (mode == WT_WRITE) {
        flags = DB_CREATE|DB_TRUNCATE;
        data_mode = "wb";
    } else if (mode == WT_READ) {
        flags = DB_RDONLY|DB_NOMMAP;
        data_mode = "rb";
    } else {
        PyErr_Format(PyExc_ValueError, "mode must be WT_READ or WT_WRITE.");
        goto out;
    }
    if (self->db != NULL) {
        PyErr_Format(WormtableError, "Table already open.");
        goto out;
    }
    db_name = PyBytes_AsString(self->db_filename);
    data_name = PyBytes_AsString(self->data_filename);
    if (db_name == NULL || data_name == NULL) {
        goto out;
    }
    /* Now we create the DB handle */
    db_ret = db_create(&self->db, NULL, 0);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;
    }
    gigs = (uint32_t) (self->cache_size / gigabyte);
    bytes = (uint32_t) (self->cache_size % gigabyte);
    db_ret = self->db->set_cachesize(self->db, gigs, bytes, 1);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;
    }
    /* Disable DB error messages */
    self->db->set_errcall(self->db, NULL);
    db_ret = self->db->open(self->db, NULL, db_name, NULL, DB_BTREE, flags,
            WT_DB_FILE_PERMS);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        self->db->close(self->db, 0);
        self->db = NULL;
        goto out;
    }
    /* Now open the data file */
    self->data_file = fopen(data_name, data_mode);
    if (self->data_file == NULL) {
        handle_io_error();
        goto out;
    }
    if (setvbuf(self->data_file, NULL, _IOFBF, 1024 * 1024) != 0) {
        handle_io_error();
        goto out;
    }

    Py_INCREF(Py_None);
    ret = Py_None;
out:
    return ret;
}


static PyObject *
Table_close(Table* self)
{
    PyObject *ret = NULL;
    int db_ret, io_ret;
    DB *db = self->db;
    if (db == NULL) {
        PyErr_SetString(WormtableError, "table closed");
        goto out;
    }
    db_ret = db->close(db, 0);
    self->db = NULL;
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;
    }
    if (self->data_file != NULL) {
        io_ret = fclose(self->data_file);
        self->data_file = NULL;
        if (io_ret != 0) {
            handle_io_error();
            goto out;
        }
    }
    Py_INCREF(Py_None);
    ret = Py_None;
out:
    return ret;
}


static PyObject *
Table_insert_elements(Table* self, PyObject *args)
{
    PyObject *ret = NULL;
    Column *col = NULL;
    PyObject *elements = NULL;
    int m, col_index, wt_ret;
    if (!PyArg_ParseTuple(args, "iO", &col_index, &elements)) {
        goto out;
    }
    if (Table_check_column_index(self, col_index) != 0) {
        goto out;
    }
    if (col_index == 0) {
        PyErr_Format(WormtableError, "Cannot update ID col.");
        goto out;
    }
    if (Table_check_write_mode(self) != 0) {
        goto out;
    }
    col = self->columns[col_index];
    wt_ret = col->python_to_native(col, elements);
    if (wt_ret < 0) {
        goto out;
    }
    if (wt_ret != WT_MISSING_VALUE) {
        m = Column_update_row(col, self->row_buffer, self->current_row_size);
        if (m < 0) {
            goto out;
        }
        self->current_row_size += m;
    }
    Py_INCREF(Py_None);
    ret = Py_None;
out:
    return ret;
}

static PyObject *
Table_insert_encoded_elements(Table* self, PyObject *args)
{
    PyObject *ret = NULL;
    Column *column = NULL;
    PyBytesObject *value = NULL;
    char *v;
    int  m, col_index;
    if (!PyArg_ParseTuple(args, "iO!", &col_index, &PyBytes_Type,
            &value)) {
        goto out;
    }
    if (Table_check_column_index(self, col_index) != 0) {
        goto out;
    }
    if (col_index == 0) {
        PyErr_Format(WormtableError, "Cannot update ID column.");
        goto out;
    }
    if (Table_check_write_mode(self) != 0) {
        goto out;
    }
    column = self->columns[col_index];
    v = PyBytes_AsString((PyObject *) value);
    if (column->string_to_native(column, v) < 0) {
        goto out;
    }
    m = Column_update_row(column, self->row_buffer, self->current_row_size);
    if (m < 0) {
        goto out;
    }
    self->current_row_size += m;
    Py_INCREF(Py_None);
    ret = Py_None;
out:
    return ret;
}

static void
Table_update_row_stats(Table *self, uint32_t row_size)
{
    self->total_row_size += row_size;
    if (row_size < self->min_row_size) {
        self->min_row_size = row_size;
    }
    if (row_size > self->max_row_size) {
        self->max_row_size = row_size;
    }
}

/* Retrieves the row from the data file identified by data into the
 * row buffer such that it is ready for reading. Also copy the specified
 * key into the buffer so that we can read the col_id column also.
 */
static int
Table_retrieve_row(Table *self, DBT *key, DBT *data)
{
    int ret = -1;
    char *v;
    char *rb = (char *) self->row_buffer;
    Column *id_col = self->columns[0];
    uint32_t key_size = id_col->element_size;
    uint64_t offset = 0;
    uint16_t len = 0;

    if (key->size != key_size) {
        PyErr_Format(PyExc_SystemError, "table key record size mismatch");
        goto out;
    }
    if (data->size != OFFSET_LEN_RECORD_SIZE) {
        PyErr_Format(PyExc_SystemError, "offset/len record size mismatch");
        goto out;
    }
    memcpy(self->row_buffer, key->data, key->size);
    v = (char *) data->data;
    offset = unpack_uint(v, sizeof(offset));
    v += sizeof(offset);
    len = unpack_uint(v, sizeof(len));
    /* Now read this record from the file and put it in the row buffer */
    if (fseeko(self->data_file, (off_t) offset, SEEK_SET) != 0) {
        handle_io_error();
        goto out;
    }
    v = rb + key_size;
    if (fread(v, len, 1, self->data_file) != 1) {
        handle_io_error();
        goto out;
    }
    ret = 0;
out:
    return ret;
}

static int
Table_retrieve_row_by_id(Table *self, uint64_t row_id)
{
    int ret = -1;
    int db_ret;
    unsigned char key_buffer[sizeof(row_id)];
    Column *id_col = self->columns[0];
    DBT key, data;

    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
    key.data = key_buffer;
    key.size = id_col->element_size;
    if (Column_set_row_id(id_col, row_id) != 0) {
        goto out;
    }
    if (Column_update_row(id_col, key_buffer, 0) != 0) {
        goto out;
    }
    db_ret = self->db->get(self->db, NULL, &key, &data, 0);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;
    }
    ret = Table_retrieve_row(self, &key, &data);
out:
    return ret;
}

static PyObject *
Table_commit_row(Table* self)
{
    PyObject *ret = NULL;
    size_t io_ret;
    int db_ret;
    char *v;
    char *rb = (char *) self->row_buffer;
    uint64_t offset;
    uint16_t len;
    char record[OFFSET_LEN_RECORD_SIZE];
    void *row = NULL;
    DBT key, data;
    Column *id_col = self->columns[0];
    uint32_t key_size = id_col->element_size;
    if (Table_check_write_mode(self) != 0) {
        goto out;
    }
    if (Column_set_row_id(id_col, (uint64_t) self->num_rows) != 0) {
        goto out;
    }
    if (Column_update_row(id_col, self->row_buffer, self->current_row_size)
            != 0) {
        goto out;
    }
    /* write the data row */
    offset = (uint64_t) ftello(self->data_file);
    len = self->current_row_size - key_size;
    row = rb + key_size;
    io_ret = fwrite(row, len, 1, self->data_file);
    if (io_ret != 1) {
        handle_io_error();
        goto out;
    }
    /* pack offset|length into record */
    v = record;
    pack_uint(offset, v, sizeof(offset));
    v += sizeof(offset);
    pack_uint(len, v, sizeof(len));
    /* Now store the offset+length in the DB */
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
    key.data = self->row_buffer;
    key.size = key_size;
    data.data = record;
    data.size = OFFSET_LEN_RECORD_SIZE;
    db_ret = self->db->put(self->db, NULL, &key, &data, 0);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;
    }
    memset(self->row_buffer, 0, self->current_row_size);
    self->current_row_size = self->fixed_region_size;
    self->num_rows++;
    Table_update_row_stats(self, len);
    Py_INCREF(Py_None);
    ret = Py_None;
out:
    return ret;
}

static PyObject *
Table_get_num_rows(Table* self)
{
    int db_ret;
    int wt_ret;
    Column *id_col = self->columns[0];
    uint64_t max_key = 0;
    PyObject *ret = NULL;
    DBC *cursor = NULL;
    DBT key, data;
    if (Table_check_read_mode(self) != 0) {
        goto out;
    }
    db_ret = self->db->cursor(self->db, NULL, &cursor, 0);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;
    }
    /* retrieve the last key from the DB */
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
    db_ret = cursor->get(cursor, &key, &data, DB_LAST);
    if (db_ret == 0) {
        if (key.size != id_col->element_size) {
            PyErr_Format(PyExc_SystemError, "key size mismatch");
            goto out;
        }
        wt_ret = Column_extract_elements(id_col, key.data);
        if (wt_ret > 0) {
            PyErr_Format(PyExc_SystemError, "Missing value in id column.");
            goto out;
        }
        if (wt_ret < 0) {
            goto out;
        }
        if (Column_get_row_id(id_col, &max_key) != 0) {
            goto out;
        }
        max_key++;
    } else if (db_ret != DB_NOTFOUND) {
        handle_bdb_error(db_ret);
        goto out;
    }
    /* Free the cursor */
    db_ret = cursor->close(cursor);
    cursor = NULL;
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;
    }
    ret = PyLong_FromUnsignedLongLong(max_key);
out:
    if (cursor != NULL) {
        cursor->close(cursor);
    }
    return ret;
}

static PyObject *
Table_get_row(Table* self, PyObject *args)
{
    PyObject *ret = NULL;
    PyObject *t = NULL;
    Column *col = NULL;
    PyObject *value = NULL;
    int wt_ret;
    unsigned long long row_id = 0;
    uint32_t j;
    if (!PyArg_ParseTuple(args, "K", &row_id)) {
        goto out;
    }
    if (Table_check_read_mode(self) != 0) {
        goto out;
    }
    if (Table_retrieve_row_by_id(self, (uint64_t) row_id) != 0) {
        goto out;
    }
    t = PyTuple_New(self->num_columns);
    if (t == NULL) {
        PyErr_NoMemory();
        goto out;
    }
    for (j = 0; j < self->num_columns; j++) {
        col = self->columns[j];
        wt_ret = Column_extract_elements(col, self->row_buffer);
        if (wt_ret < 0) {
            Py_DECREF(t);
            goto out;
        }
        value = Column_get_python_elements(col, wt_ret == WT_MISSING_VALUE);
        if (value == NULL) {
            Py_DECREF(t);
            goto out;
        }
        PyTuple_SET_ITEM(t, j, value);
    }
    ret = t;
out:
    return ret;
}




static PyMethodDef Table_methods[] = {
    {"get_num_rows", (PyCFunction) Table_get_num_rows, METH_NOARGS,
            "Returns the number of rows in the table" },
    {"get_row", (PyCFunction) Table_get_row, METH_VARARGS,
            "Return the jth row as a tuple" },
    {"open", (PyCFunction) Table_open, METH_VARARGS, "Open the table" },
    {"close", (PyCFunction) Table_close, METH_NOARGS, "Close the table" },
    {"commit_row", (PyCFunction) Table_commit_row, METH_NOARGS,
            "Commit a row to the table in write mode." },
    {"insert_elements", (PyCFunction) Table_insert_elements, METH_VARARGS,
            "insert element values encoded as native Python objects." },
    {"insert_encoded_elements", (PyCFunction) Table_insert_encoded_elements,
            METH_VARARGS,
            "insert element values encoded as comma seperated byte values." },
    {NULL}  /* Sentinel */
};

static PyTypeObject TableType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_wormtable.Table",             /* tp_name */
    sizeof(Table),             /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)Table_dealloc, /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_reserved */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash  */
    0,                         /* tp_call */
    0,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
    "Table objects",           /* tp_doc */
    0,                     /* tp_traverse */
    0,                     /* tp_clear */
    0,                     /* tp_richcompare */
    0,                     /* tp_weaklistoffset */
    0,                     /* tp_iter */
    0,                     /* tp_iternext */
    Table_methods,             /* tp_methods */
    Table_members,             /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)Table_init,      /* tp_init */
};




/*==========================================================
 * Index object
 *==========================================================
 */

static void
Index_dealloc(Index* self)
{
    Py_XDECREF(self->table);
    Py_XDECREF(self->db_filename);
    /* make sure that the DB handles are closed. We can ignore errors here. */
    if (self->db != NULL) {
        self->db->close(self->db, 0);
    }
    if (self->columns != NULL) {
        PyMem_Free(self->columns);
    }
    if (self->bin_widths != NULL) {
        PyMem_Free(self->bin_widths);
    }
    if (self->key_buffer != NULL) {
        PyMem_Free(self->key_buffer);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static int
Index_init(Index *self, PyObject *args, PyObject *kwds)
{
    int j;
    long k;
    int ret = -1;
    static char *kwlist[] = {"table", "db_filename", "columns", "cache_size", NULL};
    PyObject *v;
    Column *col;
    PyObject *db_filename = NULL;
    PyObject *columns = NULL;
    Table *table = NULL;
    uint32_t n;

    self->db = NULL;
    self->table = NULL;
    self->db_filename = NULL;
    self->bin_widths = NULL;
    self->key_buffer = NULL;
    self->columns = NULL;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!O!O!K", kwlist,
            &TableType, &table,
            &PyBytes_Type, &db_filename,
            &PyList_Type,  &columns,
            &self->cache_size)) {
        goto out;
    }
    self->table = table;
    Py_INCREF(self->table);
    self->db_filename = db_filename;
    Py_INCREF(self->db_filename);
    if (Table_check_read_mode(self->table) != 0) {
        goto out;
    }
    self->num_columns = PyList_GET_SIZE(columns);
    if (self->num_columns < 1) {
        PyErr_SetString(PyExc_ValueError, "Must be 1 or more columns index.");
        goto out;
    }
    self->columns = PyMem_Malloc(self->num_columns * sizeof(uint32_t));
    if (self->columns == NULL) {
        PyErr_NoMemory();
        goto out;
    }
    self->bin_widths = PyMem_Malloc(self->num_columns * sizeof(double));
    if (self->bin_widths == NULL) {
        PyErr_NoMemory();
        goto out;
    }
    memset(self->bin_widths, 0, self->num_columns * sizeof(double));
    self->key_buffer_size = 0;
    for (j = 0; j < self->num_columns; j++) {
        v = PyList_GET_ITEM(columns, j);
        if (!PyNumber_Check(v)) {
            PyErr_SetString(PyExc_ValueError, "Column indexes must be int");
            goto out;
        }
        k = PyLong_AsLong(v);
        if (k < 0 || k >= self->table->num_columns) {
            PyErr_SetString(PyExc_ValueError, "Column indexes out of bounds");
            goto out;
        }
        self->columns[j] = (uint32_t) k;
        col = self->table->columns[k];
        n = col->num_elements;
        if (Column_is_variable(col)) {
            /* allow space for the sentinel */
            n = Column_get_max_num_elements(col) + 1;
            /* allow space for the missing value indicator */
            self->key_buffer_size += 1;
        }
        self->key_buffer_size += n * col->element_size;
    }
    self->key_buffer = PyMem_Malloc(self->key_buffer_size);
    if (self->key_buffer == NULL) {
        PyErr_NoMemory();
        goto out;
    }
    ret = 0;
out:

    return ret;
}


static PyMemberDef Index_members[] = {
    {"table", T_OBJECT_EX, offsetof(Index, table), READONLY, "table"},
    {"db_filename", T_OBJECT_EX, offsetof(Index, db_filename), READONLY, "db_filename"},
    {"cache_size", T_ULONGLONG, offsetof(Index, cache_size), READONLY, "cache_size"},
    {NULL}  /* Sentinel */
};


/*
 * Returns 0 if the table is opened in write mode. Otherwise
 * -1 is returned with the appropriate Python exception set.
 */
static int
Index_check_write_mode(Index *self)
{
    int ret = -1;
    int db_ret;
    uint32_t flags;
    if (Table_check_read_mode(self->table) != 0) {
        goto out;
    }
    if (self->db == NULL) {
        PyErr_Format(WormtableError, "Index closed.");
        goto out;
    }
    db_ret = self->db->get_open_flags(self->db, &flags);
    if (db_ret != 0) {
        handle_bdb_error(flags);
        goto out;
    }
    if ((flags & DB_RDONLY) != 0) {
        PyErr_Format(WormtableError, "Index must be opened WT_WRITE.");
        goto out;
    }
    ret = 0;
out:
    return ret;
}


/*
 * Returns 0 if the table is opened in read mode. Otherwise
 * -1 is returned with the appropriate Python exception set.
 */
static int
Index_check_read_mode(Index *self)
{
    int ret = -1;
    int db_ret;
    uint32_t flags;
    if (Table_check_read_mode(self->table) != 0) {
        goto out;
    }
    if (self->db == NULL) {
        PyErr_Format(WormtableError, "Index closed.");
        goto out;
    }
    db_ret = self->db->get_open_flags(self->db, &flags);
    if (db_ret != 0) {
        handle_bdb_error(flags);
        goto out;
    }
    if ((flags & DB_RDONLY) == 0) {
        PyErr_Format(WormtableError, "Index must be opened WT_READ.");
        goto out;
    }
    ret = 0;
out:
    return ret;
}

/* extract values from the specified row and push them into the specified
 * secondary key. This has valid memory associated with it.
 */
static int
Index_fill_key(Index *self, void *row, DBT *skey)
{
    int ret = -1;
    int wt_ret;
    Column *col;
    uint32_t j, k;
    int len;
    unsigned char *v = skey->data;
    skey->size = 0;
    for (j = 0; j < self->num_columns; j++) {
        col = self->table->columns[self->columns[j]];
        len = 0;
        wt_ret = Column_extract_elements(col, row);
        if (wt_ret < 0) {
            ret = wt_ret;
            goto out;
        }
        if (Column_is_variable(col)) {
            /* insert the missing value prefix */
            *v = wt_ret == WT_MISSING_VALUE ? 0 : 1;
            v++;
            skey->size++;
        }
        if (self->bin_widths[j] != 0.0) {
            if (col->truncate_elements(col, self->bin_widths[j]) < 0) {
                goto out;
            }
        }
        if (col->pack_elements(col, v) < 0) {
            goto out;
        }
        len = col->num_buffered_elements * col->element_size;
        if (len < 0) {
            goto out;
        }
        v += len;
        skey->size += len;
        if (Column_is_variable(col)) {
            /* insert the sentinel */
            for (k = 0; k < col->element_size; k++) {
                *v = 0;
                v++;
                skey->size++;
            }
        }
    }
    ret = 0;
out:
    return ret;
}

/*
 * Reads the arguments and sets a key in the specified buffer, returning
 * its length.
 */
static int
Index_set_key(Index *self, PyObject *args, void *buffer)
{
    int ret = -1;
    int j, m, k, wt_ret, overhead;
    int key_size = 0;
    Py_ssize_t n;
    Column *col = NULL;
    PyObject *elements = NULL;
    PyObject *v = NULL;
    char *key_buffer = (char *) buffer;
    if (!PyArg_ParseTuple(args, "O!", &PyTuple_Type, &elements)) {
        goto out;
    }
    if (Index_check_read_mode(self) != 0) {
        goto out;
    }
    n = PyTuple_GET_SIZE(elements);
    if (n > self->num_columns) {
        PyErr_Format(PyExc_ValueError, "More key values than columns.");
        goto out;
    }
    for (j = 0; j < n; j++) {
        col = self->table->columns[self->columns[j]];
        v = PyTuple_GetItem(elements, j);
        wt_ret = col->python_to_native(col, v);
        if (wt_ret < 0) {
            goto out;
        }
        if (col->verify_elements(col) < 0) {
            goto out;
        }
        m = col->num_buffered_elements * col->element_size;
        overhead = Column_is_variable(col) ? 1 + col->element_size: 0;
        if (key_size + m + overhead > self->key_buffer_size) {
            PyErr_Format(PyExc_SystemError, "Max key_size exceeded.");
            goto out;
        }
        if (Column_is_variable(col)) {
            /* insert the missing value prefix */
            *key_buffer = wt_ret == WT_MISSING_VALUE? 0 : 1;
            key_buffer++;
            key_size++;
        }
        col->pack_elements(col, key_buffer);
        key_buffer += m;
        key_size += m;
        if (Column_is_variable(col)) {
            /* insert the sentinel */
            for (k = 0; k < col->element_size; k++) {
                *key_buffer = 0;
                key_buffer++;
                key_size++;
            }
        }
    }
    ret = key_size;
out:
    return ret;
}

/*
 * Increment the specified key. This increments the least significant
 * byte, and then carries the result up the more signficant bytes as
 * required. Returns 1 if the key overflows, and the key is zero as
 * a result.
 */
static int
Index_increment_key(Index *self, void *buffer, uint32_t key_size)
{
    int j;
    int overflow = 0;
    unsigned char *key_buffer = (unsigned char *) buffer;
    j = key_size - 1;
    /* add one to the buffer, making sure to carry base 256 */
    while (j > 0 && key_buffer[j] == 255) {
        key_buffer[j] = 0;
        j--;
    }
    key_buffer[j] += 1;
    if (j == 0 && key_buffer[0] == 0) {
        overflow = 1;
    }
    return overflow;
}



static PyObject *
Index_key_to_python(Index *self, void *key_buffer, uint32_t key_size)
{
    PyObject *ret = NULL;
    PyObject *value;
    uint32_t j, offset;
    Column *col;
    char *v;
    int n, missing_value, wt_ret;
    PyObject *t = PyTuple_New(self->num_columns);
    if (t == NULL) {
        PyErr_NoMemory();
        goto out;
    }
    if (Index_check_read_mode(self) != 0) {
        goto out;
    }
    offset = 0;
    for (j = 0; j < self->num_columns; j++) {
        missing_value = 0;
        col = self->table->columns[self->columns[j]];
        n = 0;
        if (Column_is_variable(col)) {
            v = (char *) key_buffer;
            v += offset;
            missing_value = *v == 0;
            offset++;
            wt_ret = Column_extract_key(col, key_buffer, offset, key_size);
            if (wt_ret < 0) {
                Py_DECREF(t);
                goto out;
            }
            /* skip the sentinel */
            n = 1;
        } else {
            wt_ret = Column_extract_key(col, key_buffer, offset, key_size);
            if (wt_ret < 0) {
                Py_DECREF(t);
                goto out;
            }
            missing_value = wt_ret == WT_MISSING_VALUE;
        }
        n += col->num_buffered_elements;
        offset += n * col->element_size;
        value = Column_get_python_elements(col, missing_value);
        if (value == NULL) {
            Py_DECREF(t);
            goto out;
        }
        PyTuple_SET_ITEM(t, j, value);
    }
    ret = t;
out:
    return ret;
}

static PyObject *
Index_get_num_rows(Index *self, PyObject *args)
{
    PyObject *ret = NULL;
    int db_ret, key_size;
    db_recno_t count = 0;
    DB *db = NULL;
    DBC *cursor = NULL;
    DBT key, data;
    key_size = Index_set_key(self, args, self->key_buffer);
    if (key_size < 0) {
        goto out;
    }
    if (Index_check_read_mode(self) != 0) {
        goto out;
    }
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
    db = self->db;
    if (db == NULL) {
        PyErr_SetString(WormtableError, "table closed");
        goto out;
    }
    db_ret = db->cursor(db, NULL, &cursor, 0);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;
    }
    key.data = self->key_buffer;
    key.size = (uint32_t) key_size;
    db_ret = cursor->get(cursor, &key, &data, DB_SET);
    if (db_ret == 0) {
        db_ret = cursor->count(cursor, &count, 0);
        if (db_ret != 0) {
            handle_bdb_error(db_ret);
            goto out;
        }
    } else if (db_ret != DB_NOTFOUND) {
        handle_bdb_error(db_ret);
        goto out;
    }
    ret = PyLong_FromUnsignedLongLong((unsigned long long) count);
out:
    if (cursor != NULL) {
        cursor->close(cursor);
    }

    return ret;
}

static PyObject *
Index_get_min(Index* self, PyObject *args)
{
    PyObject *ret = NULL;
    int db_ret;
    DBC *cursor = NULL;
    DBT key, data;
    int key_size = Index_set_key(self, args, self->key_buffer);
    if (key_size < 0) {
        goto out;
    }
    if (Index_check_read_mode(self) != 0) {
        goto out;
    }
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
    db_ret = self->db->cursor(self->db, NULL, &cursor, 0);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;
    }
    key.data = self->key_buffer;
    key.size = (uint32_t) key_size;
    db_ret = cursor->get(cursor, &key, &data, DB_SET_RANGE);
    if (db_ret == 0) {
        if (key_size > key.size) {
            PyErr_Format(PyExc_SystemError, "key size comparison error.");
            goto out;
        }
        if (memcmp(self->key_buffer, key.data, key_size) != 0) {
            PyErr_SetObject(PyExc_KeyError, args);
            goto out;
        }
        ret = Index_key_to_python(self, key.data, key.size);
        if (ret == NULL) {
            goto out;
        }
    } else {
        handle_bdb_error(db_ret);
        goto out;
    }
out:
    if (cursor != NULL) {
        cursor->close(cursor);
    }
    return ret;
}


static PyObject *
Index_get_max(Index* self, PyObject *args)
{
    PyObject *ret = NULL;
    int db_ret, key_size, cmp, overflow;
    unsigned char *search_key = NULL;
    DBC *cursor = NULL;
    DBT key, data;
    key_size = Index_set_key(self, args, self->key_buffer);
    if (key_size < 0) {
        goto out;
    }
    if (Index_check_read_mode(self) != 0) {
        goto out;
    }
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
    db_ret = self->db->cursor(self->db, NULL, &cursor, 0);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;
    }
    key.data = self->key_buffer;
    key.size = (uint32_t) key_size;
    if (key_size == 0) {
        /* An empty list has been passed so we want the last value */
        db_ret = cursor->get(cursor, &key, &data, DB_LAST);
        if (db_ret != 0) {
            handle_bdb_error(db_ret);
            goto out;
        }
    } else {
        search_key = PyMem_Malloc(key_size + 1);
        if (search_key == NULL) {
            PyErr_NoMemory();
            goto out;
        }
        memcpy(search_key, self->key_buffer, key_size);
        overflow = Index_increment_key(self, self->key_buffer, key_size);
        /* Seek to the first key prefix >= to this */
        db_ret = cursor->get(cursor, &key, &data, DB_SET_RANGE);
        /* If this is not found, we want the last key in the index */
        if (db_ret == DB_NOTFOUND || overflow) {
            db_ret = cursor->get(cursor, &key, &data, DB_LAST);
            if (db_ret != 0) {
                handle_bdb_error(db_ret);
                goto out;
            }
        } else if (db_ret == 0) {
           /* We need to seek backwards from here until the prefix matches
            * the provided key.
            */
            do {
                db_ret = cursor->get(cursor, &key, &data, DB_PREV);
                if (db_ret != 0) {
                    handle_bdb_error(db_ret);
                    goto out;
                }
                cmp = memcmp(search_key, key.data, key_size);
                if (cmp > 0) {
                    PyErr_SetObject(PyExc_KeyError, args);
                    goto out;
                }
            } while (cmp != 0);
        } else {
            handle_bdb_error(db_ret);
            goto out;
        }
    }
    ret = Index_key_to_python(self, key.data, key.size);
out:
    if (cursor != NULL) {
        cursor->close(cursor);
    }
    if (search_key != NULL) {
        PyMem_Free(search_key);
    }
    return ret;
}

static PyObject *
Index_set_bin_widths(Index* self, PyObject *args)
{
    Column* col = NULL;
    PyObject *ret = NULL;
    PyObject *w = NULL;
    unsigned int j;
    PyObject *bin_widths = NULL;
    if (!PyArg_ParseTuple(args, "O!", &PyList_Type, &bin_widths)) {
        goto out;
    }
    if (Table_check_read_mode(self->table) != 0) {
        goto out;
    }
    if (self->db != NULL) {
        PyErr_Format(WormtableError, "Cannot set bin_widths after open()");
        goto out;
    }
    if (PyList_GET_SIZE(bin_widths) != self->num_columns) {
        PyErr_Format(PyExc_ValueError,
                "Number of bins must equal to the number of columns");
        goto out;
    }
    for (j = 0; j < PyList_GET_SIZE(bin_widths); j++) {
        w = PyList_GET_ITEM(bin_widths, j);
        col = self->table->columns[self->columns[j]];
        if (!PyNumber_Check(w)) {
            PyErr_Format(PyExc_TypeError,
                    "Bad bin width for '%s': bin widths must be numeric",
                    PyBytes_AsString(col->name));
            goto out;
        }
        self->bin_widths[j] = PyFloat_AsDouble(w);
        if (PyErr_Occurred()) {
            goto out;
        }
        if (self->bin_widths[j] < 0.0) {
            PyErr_Format(PyExc_ValueError,
                    "Bad bin width for '%s': bin widths must be nonnegative",
                    PyBytes_AsString(col->name));
            goto out;
        }
        if (col->element_type == WT_CHAR
                && self->bin_widths[j] != 0.0) {
            PyErr_Format(PyExc_ValueError,
                    "Bad bin width for '%s': char columns do not support bins",
                    PyBytes_AsString(col->name));
            goto out;
        }
        if (col->element_type == WT_INT) {
            if (fmod(self->bin_widths[j], 1.0) != 0.0) {
                PyErr_Format(PyExc_ValueError,
                        "Bad bin width for '%s': "
                        "integer column bins must be integers",
                        PyBytes_AsString(col->name));
                goto out;
            }
        }
    }
    Py_INCREF(Py_None);
    ret = Py_None;
out:
    return ret;
}

static PyObject *
Index_build(Index* self, PyObject *args)
{
    int db_ret;
    PyObject *ret = NULL;
    PyObject *arglist, *result;
    PyObject *progress_callback = NULL;
    Column *id_col;
    uint32_t primary_key_size;
    DBC *cursor = NULL;
    DB *pdb = NULL;
    DB *sdb = NULL;
    DBT pkey, pdata, skey, sdata;
    uint32_t truncate_count;
    uint64_t callback_interval = 1000;
    uint64_t records_processed = 0;

    if (!PyArg_ParseTuple(args, "|OK", &progress_callback,
            &callback_interval)) {
        progress_callback = NULL;
        goto out;
    }
    Py_XINCREF(progress_callback);
    if (Index_check_write_mode(self) != 0) {
        goto out;
    }
    if (progress_callback != NULL) {
        if (!PyCallable_Check(progress_callback)) {
            PyErr_SetString(PyExc_TypeError, "progress_callback must be callable");
            goto out;
        }
    }
    if (callback_interval == 0) {
        PyErr_SetString(PyExc_ValueError, "callback interval cannot be 0");
        goto out;
    }
    id_col = self->table->columns[0];
    primary_key_size = id_col->element_size;
    pdb = self->table->db;
    sdb = self->db;
    db_ret = pdb->cursor(pdb, NULL, &cursor, 0);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        cursor = NULL;
        goto out;
    }
    memset(&pkey, 0, sizeof(DBT));
    memset(&pdata, 0, sizeof(DBT));
    memset(&skey, 0, sizeof(DBT));
    memset(&sdata, 0, sizeof(DBT));
    skey.data = self->key_buffer;
    sdata.data = self->table->row_buffer;
    sdata.size = primary_key_size;
    while ((db_ret = cursor->get(cursor, &pkey, &pdata, DB_NEXT)) == 0) {
        if (Table_retrieve_row(self->table, &pkey, &pdata) != 0) {
            goto out;
        }
        if (Index_fill_key(self, self->table->row_buffer, &skey) < 0 ) {
            goto out;
        }
        db_ret = sdb->put(sdb, NULL, &skey, &sdata, 0);
        if (db_ret != 0) {
            handle_bdb_error(db_ret);
            goto out;
        }
        /* Invoke the callback if necessary */
        records_processed++;
        if (records_processed % callback_interval == 0) {
            if (progress_callback != NULL) {
                arglist = Py_BuildValue("(K)", records_processed);
                if (arglist == NULL) {
                    goto out;
                }
                result = PyObject_CallObject(progress_callback, arglist);
                Py_DECREF(arglist);
                if (result == NULL) {
                    goto out;
                }
                Py_DECREF(result);
                /* Anything might have happened in the mean time, so
                 * check the state of the DBs again!
                 */
                if (Index_check_write_mode(self) != 0) {
                    goto out;
                }
            }
        }
    }
    if (db_ret != DB_NOTFOUND) {
        handle_bdb_error(db_ret);
        goto out;
    }
    db_ret = cursor->close(cursor);
    cursor = NULL;
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;
    }
    Py_INCREF(Py_None);
    ret = Py_None;
out:
    Py_XDECREF(progress_callback);
    if (cursor != NULL) {
        /* ignore errors in this case, as we're already handling one */
        if (self->table != NULL) {
            if (self->table->db != NULL) {
                cursor->close(cursor);
            }
        }
        if (self->db != NULL) {
            sdb = self->db;
            db_ret = sdb->truncate(sdb, NULL, &truncate_count, 0);
        }
    }
    return ret;
}

static PyObject *
Index_open(Index* self, PyObject *args)
{
    PyObject *ret = NULL;
    char *db_name = NULL;
    uint32_t flags = 0;
    Py_ssize_t gigabyte = 1024 * 1024 * 1024;
    uint32_t gigs, bytes;
    int db_ret, mode;
    DB *pdb;
    if (!PyArg_ParseTuple(args, "i", &mode)) {
        goto out;
    }
    if (Table_check_read_mode(self->table) != 0) {
        goto out;
    }
    pdb = self->table->db;
    if (mode == WT_WRITE) {
        flags = DB_CREATE|DB_TRUNCATE;
    } else if (mode == WT_READ) {
        flags = DB_RDONLY|DB_NOMMAP;
    } else {
        PyErr_Format(PyExc_ValueError, "mode must be WT_READ or WT_WRITE.");
        goto out;
    }
    if (self->db != NULL) {
        PyErr_Format(WormtableError, "Index already open.");
        goto out;
    }
    db_name = PyBytes_AsString(self->db_filename);
    if (db_name == NULL) {
        goto out;
    }
    /* Now we create the DB handle */
    db_ret = db_create(&self->db, NULL, 0);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;
    }
    gigs = (uint32_t) (self->cache_size / gigabyte);
    bytes = (uint32_t) (self->cache_size % gigabyte);
    db_ret = self->db->set_cachesize(self->db, gigs, bytes, 1);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;
    }
    db_ret = self->db->set_flags(self->db, DB_DUPSORT);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;
    }
    db_ret = self->db->set_bt_compress(self->db, NULL, NULL);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;
    }
    /* Disable DB error messages */
    self->db->set_errcall(self->db, NULL);
    db_ret = self->db->open(self->db, NULL, db_name, NULL, DB_BTREE, flags,
            WT_DB_FILE_PERMS);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        self->db->close(self->db, 0);
        self->db = NULL;
        goto out;
    }
    if (mode == WT_READ) {
        db_ret = pdb->associate(pdb, NULL, self->db, NULL, 0);
        if (db_ret != 0) {
            handle_bdb_error(db_ret);
            goto out;
        }
    }
    Py_INCREF(Py_None);
    ret = Py_None;
out:
    return ret;
}

static PyObject *
Index_close(Index* self)
{
    PyObject *ret = NULL;
    int db_ret;
    DB *db = self->db;
    if (db == NULL) {
        PyErr_SetString(WormtableError, "index closed");
        goto out;
    }
    db_ret = db->close(db, 0);
    self->db = NULL;
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;
    }
    Py_INCREF(Py_None);
    ret = Py_None;
out:
    return ret;
}


static PyMethodDef Index_methods[] = {
    {"build", (PyCFunction) Index_build, METH_VARARGS, "Build the index" },
    {"set_bin_widths", (PyCFunction) Index_set_bin_widths, METH_VARARGS,
        "Sets the bin widths for the columns" },
    {"get_min", (PyCFunction) Index_get_min, METH_VARARGS,
        "Returns the minumum key value in this index" },
    {"get_max", (PyCFunction) Index_get_max, METH_VARARGS,
        "Returns the maxumum key value in this index" },
    {"get_num_rows", (PyCFunction) Index_get_num_rows, METH_VARARGS,
        "Returns the number of rows in the index with the specified key." },
    {"open", (PyCFunction) Index_open, METH_VARARGS, "Open the index" },
    {"close", (PyCFunction) Index_close, METH_NOARGS, "Close the index" },
    {NULL}  /* Sentinel */
};


static PyTypeObject IndexType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_wormtable.Index",             /* tp_name */
    sizeof(Index),             /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)Index_dealloc, /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_reserved */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash  */
    0,                         /* tp_call */
    0,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
    "Index objects",           /* tp_doc */
    0,                     /* tp_traverse */
    0,                     /* tp_clear */
    0,                     /* tp_richcompare */
    0,                     /* tp_weaklistoffset */
    0,                     /* tp_iter */
    0,                     /* tp_iternext */
    Index_methods,             /* tp_methods */
    Index_members,             /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)Index_init,      /* tp_init */
};




/*==========================================================
 * TableRowIterator object
 *==========================================================
 */

static void
TableRowIterator_dealloc(TableRowIterator* self)
{
    if (self->cursor != NULL) {
        if (self->table != NULL) {
            if (self->table->db != NULL) {
                self->cursor->close(self->cursor);
            }
        }
    }
    Py_XDECREF(self->table);
    if (self->min_key != NULL) {
        PyMem_Free(self->min_key);
    }
    if (self->max_key != NULL) {
        PyMem_Free(self->max_key);
    }
    if (self->read_columns != NULL) {
        PyMem_Free(self->read_columns);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static int
TableRowIterator_init(TableRowIterator *self, PyObject *args, PyObject *kwds)
{
    int j;
    int ret = -1;
    long k;
    static char *kwlist[] = {"table", "columns", NULL};
    PyObject *v = NULL;
    PyObject *columns = NULL;
    Table *table = NULL;
    Column *id_col = NULL;

    self->completed = 0;
    self->read_columns = NULL;
    self->table = NULL;
    self->min_key = NULL;
    self->max_key = NULL;
    self->cursor = NULL;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!O!", kwlist,
            &TableType, &table,
            &PyList_Type, &columns)) {
        goto out;
    }
    self->table = table;
    Py_INCREF(self->table);
    if (Table_check_read_mode(self->table) != 0) {
        goto out;
    }
    self->num_read_columns = PyList_GET_SIZE(columns);
    if (self->num_read_columns < 1) {
        PyErr_SetString(PyExc_ValueError, "At least one read column required");
        goto out;
    }
    self->read_columns = PyMem_Malloc(self->num_read_columns
            * sizeof(uint32_t));
    if (self->read_columns == NULL) {
        PyErr_NoMemory();
        goto out;
    }
    for (j = 0; j < self->num_read_columns; j++) {
        v = PyList_GET_ITEM(columns, j);
        if (!PyNumber_Check(v)) {
            PyErr_SetString(PyExc_ValueError, "Column positions must be int");
            goto out;
        }
        k = PyLong_AsLong(v);
        if (k < 0 || k >= self->table->num_columns) {
            PyErr_SetString(PyExc_ValueError, "Column positions out of bounds");
            goto out;
        }
        self->read_columns[j] = (uint32_t) k;
    }
    id_col = self->table->columns[0];
    self->min_key = PyMem_Malloc(id_col->element_size);
    self->max_key = PyMem_Malloc(id_col->element_size);
    if (self->min_key == NULL || self->max_key == NULL) {
        PyErr_NoMemory();
        goto out;
    }
    ret = 0;
out:
    return ret;
}



static PyMemberDef TableRowIterator_members[] = {
    {NULL}  /* Sentinel */
};


static PyObject *
TableRowIterator_next_iter(TableRowIterator *self)
{
    PyObject *ret = NULL;
    PyObject *t = NULL;
    PyObject *value;
    Column *col;
    int db_ret, j, wt_ret;
    DB *db;
    DBT key, data;
    uint32_t flags;
    int max_exceeded = 0;
    if (Table_check_read_mode(self->table) != 0) {
        goto out;
    }
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
    flags = DB_NEXT;
    if (self->cursor == NULL) {
        /* it's the first time through the loop, so set up the cursor */
        db = self->table->db;
        db_ret = db->cursor(db, NULL, &self->cursor, 0);
        if (db_ret != 0) {
            handle_bdb_error(db_ret);
            goto out;
        }
        if (self->min_key_size != 0) {
            key.data = self->min_key;
            key.size = self->min_key_size;
            flags = DB_SET_RANGE;
        }
    }
    db_ret = self->cursor->get(self->cursor, &key, &data, flags);
    if (db_ret == 0) {
        if (Table_retrieve_row(self->table, &key, &data) != 0) {
            goto out;
        }
        /* Now, check if we've hit or gone past max_key */
        if (self->max_key_size > 0) {
            if (key.size != self->max_key_size) {
                PyErr_Format(PyExc_SystemError, "key size mismatch.");
                goto out;
            }
            max_exceeded = memcmp(self->max_key, key.data, key.size) <= 0;
        }
        if (!max_exceeded) {
            t = PyTuple_New(self->num_read_columns);
            if (t == NULL) {
                PyErr_NoMemory();
                goto out;
            }
            for (j = 0; j < self->num_read_columns; j++) {
                col = self->table->columns[self->read_columns[j]];
                wt_ret = Column_extract_elements(col, self->table->row_buffer);
                if (wt_ret < 0) {
                    Py_DECREF(t);
                    goto out;
                }
                value = Column_get_python_elements(col,
                        wt_ret == WT_MISSING_VALUE);
                if (value == NULL) {
                    Py_DECREF(t);
                    goto out;
                }
                PyTuple_SET_ITEM(t, j, value);
            }
            ret = t;
        }
    } else if (db_ret != DB_NOTFOUND) {
        handle_bdb_error(db_ret);
        goto out;
    }
    if (ret == NULL) {
        /* Iteration is finished - free the cursor */
        self->cursor->close(self->cursor);
        self->cursor = NULL;
        self->completed = 1;
    }
out:
    return ret;
}

static PyObject *
TableRowIterator_next(TableRowIterator *self)
{
    PyObject *ret = NULL;
    if (!self->completed) {
        ret = TableRowIterator_next_iter(self);
    }
    return ret;
}

static PyObject *
TableRowIterator_set_min(TableRowIterator *self, PyObject *args)
{
    PyObject *ret = NULL;
    Column *id_col = NULL;
    unsigned PY_LONG_LONG row_id = 0;
    /* TODO: This is unsatisfactory as it doesn't check for overflow;
     * -1 is accepted as a valid index value.
     */
    if (!PyArg_ParseTuple(args, "K", &row_id)) {
        goto out;
    }
    if (Table_check_read_mode(self->table) != 0) {
        goto out;
    }
    id_col = self->table->columns[0];
    if (Column_set_row_id(id_col, (uint64_t) row_id) != 0) {
        goto out;
    }
    /* this is safe because this column must be at offset 0 */
    if (Column_update_row(id_col, self->min_key, 0) != 0) {
        goto out;
    }
    self->min_key_size = id_col->element_size;
    Py_INCREF(Py_None);
    ret = Py_None;
out:
    return ret;
}

static PyObject *
TableRowIterator_set_max(TableRowIterator *self, PyObject *args)
{
    PyObject *ret = NULL;
    Column *id_col = NULL;
    unsigned PY_LONG_LONG row_id = 0;
    /* TODO: This is unsatisfactory as it doesn't check for overflow;
     * -1 is accepted as a valid index value.
     */
    if (!PyArg_ParseTuple(args, "K", &row_id)) {
        goto out;
    }
    if (Table_check_read_mode(self->table) != 0) {
        goto out;
    }
    id_col = self->table->columns[0];
    if (Column_set_row_id(id_col, (uint64_t) row_id) != 0) {
        goto out;
    }
    /* this is safe because this column must be at offset 0 */
    if (Column_update_row(id_col, self->max_key, 0) != 0) {
        goto out;
    }
    self->max_key_size = id_col->element_size;
    Py_INCREF(Py_None);
    ret = Py_None;
out:
    return ret;
}


static PyMethodDef TableRowIterator_methods[] = {
    {"set_min", (PyCFunction) TableRowIterator_set_min, METH_VARARGS, "Set the minimum key" },
    {"set_max", (PyCFunction) TableRowIterator_set_max, METH_VARARGS, "Set the maximum key" },
    {NULL}  /* Sentinel */
};


static PyTypeObject TableRowIteratorType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_wormtable.TableRowIterator",             /* tp_name */
    sizeof(TableRowIterator),             /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)TableRowIterator_dealloc, /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_reserved */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash  */
    0,                         /* tp_call */
    0,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
    "TableRowIterator objects",           /* tp_doc */
    0,                     /* tp_traverse */
    0,                     /* tp_clear */
    0,                     /* tp_richcompare */
    0,                     /* tp_weaklistoffset */
    PyObject_SelfIter,               /* tp_iter */
    (iternextfunc) TableRowIterator_next, /* tp_iternext */
    TableRowIterator_methods,             /* tp_methods */
    TableRowIterator_members,             /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)TableRowIterator_init,      /* tp_init */
};



/*==========================================================
 * IndexRowIterator object
 *==========================================================
 */

static void
IndexRowIterator_dealloc(IndexRowIterator* self)
{
    if (self->cursor != NULL) {
        if (self->index != NULL) {
            if (self->index->db != NULL) {
                self->cursor->close(self->cursor);
            }
        }
    }
    Py_XDECREF(self->index);
    if (self->min_key != NULL) {
        PyMem_Free(self->min_key);
    }
    if (self->max_key != NULL) {
        PyMem_Free(self->max_key);
    }
    if (self->read_columns != NULL) {
        PyMem_Free(self->read_columns);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);

}

static int
IndexRowIterator_init(IndexRowIterator *self, PyObject *args, PyObject *kwds)
{
    int j;
    int ret = -1;
    long k;
    static char *kwlist[] = {"index", "columns", NULL};
    PyObject *v = NULL;
    PyObject *columns = NULL;
    Index *index = NULL;

    self->completed = 0;
    self->read_columns = NULL;
    self->index = NULL;
    self->cursor = NULL;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!O!", kwlist,
            &IndexType, &index,
            &PyList_Type, &columns)) {
        goto out;
    }
    self->index = index;
    Py_INCREF(self->index);
    if (Index_check_read_mode(self->index) != 0) {
        goto out;
    }
    self->num_read_columns = PyList_GET_SIZE(columns);
    if (self->num_read_columns < 1) {
        PyErr_SetString(PyExc_ValueError, "At least one read column required");
        goto out;
    }
    self->read_columns = PyMem_Malloc(self->num_read_columns
            * sizeof(uint32_t));
    if (self->read_columns == NULL) {
        PyErr_NoMemory();
        goto out;
    }
    for (j = 0; j < self->num_read_columns; j++) {
        v = PyList_GET_ITEM(columns, j);
        if (!PyNumber_Check(v)) {
            PyErr_SetString(PyExc_ValueError, "Column indexes must be int");
            goto out;
        }
        k = PyLong_AsLong(v);
        if (k < 0 || k >= self->index->table->num_columns) {
            PyErr_SetString(PyExc_ValueError, "Column indexes out of bounds");
            goto out;
        }
        self->read_columns[j] = (uint32_t) k;
    }
    self->min_key = PyMem_Malloc(self->index->key_buffer_size);
    self->max_key = PyMem_Malloc(self->index->key_buffer_size);
    if (self->min_key == NULL || self->max_key == NULL) {
        PyErr_NoMemory();
        goto out;
    }
    self->min_key_size = 0;
    self->max_key_size = 0;
    ret = 0;
out:

    return ret;
}



static PyMemberDef IndexRowIterator_members[] = {
    {NULL}  /* Sentinel */
};


static PyObject *
IndexRowIterator_next_iter(IndexRowIterator *self)
{
    PyObject *ret = NULL;
    PyObject *t = NULL;
    PyObject *value;
    Column *col;
    int db_ret, j, cmp, wt_ret;
    DB *db;
    DBT primary_key, primary_data, secondary_key;
    uint32_t flags, cmp_size;
    int max_exceeded = 0;

    if (Index_check_read_mode(self->index) != 0) {
        goto out;
    }
    memset(&primary_key, 0, sizeof(DBT));
    memset(&primary_data, 0, sizeof(DBT));
    memset(&secondary_key, 0, sizeof(DBT));
    flags = DB_NEXT;
    if (self->cursor == NULL) {
        /* it's the first time through the loop, so set up the cursor */
        db = self->index->db;
        db_ret = db->cursor(db, NULL, &self->cursor, 0);
        if (db_ret != 0) {
            handle_bdb_error(db_ret);
            goto out;
        }
        if (self->min_key_size != 0) {
            secondary_key.data = self->min_key;
            secondary_key.size = self->min_key_size;
            flags = DB_SET_RANGE;
        }
    }
    db_ret = self->cursor->pget(self->cursor, &secondary_key, &primary_key,
            &primary_data, flags);
    if (db_ret == 0) {
        if (Table_retrieve_row(self->index->table, &primary_key,
                    &primary_data) != 0) {
            goto out;
        }
        /* Now, check if we've hit or gone past max_key */
        if (self->max_key_size > 0) {
            cmp_size = self->max_key_size;
            if (secondary_key.size < cmp_size) {
                cmp_size = secondary_key.size;
            }
            cmp = memcmp(self->max_key, secondary_key.data, cmp_size);
            max_exceeded = cmp <= 0;
            if (secondary_key.size < self->max_key_size) {
                max_exceeded = cmp < 0;
            }
        }
        if (!max_exceeded) {
            t = PyTuple_New(self->num_read_columns);
            if (t == NULL) {
                PyErr_NoMemory();
                goto out;
            }
            for (j = 0; j < self->num_read_columns; j++) {
                col = self->index->table->columns[self->read_columns[j]];
                wt_ret = Column_extract_elements(col,
                        self->index->table->row_buffer);
                if (wt_ret < 0) {
                    Py_DECREF(t);
                    goto out;
                }
                value = Column_get_python_elements(col,
                        wt_ret == WT_MISSING_VALUE);
                if (value == NULL) {
                    Py_DECREF(t);
                    goto out;
                }
                PyTuple_SET_ITEM(t, j, value);
            }
            ret = t;
        }
    } else if (db_ret != DB_NOTFOUND) {
        handle_bdb_error(db_ret);
        goto out;
    }
    if (ret == NULL) {
        /* Iteration is finished - free the cursor */
        self->cursor->close(self->cursor);
        self->cursor = NULL;
        self->completed = 1;
    }
out:
    return ret;
}

static PyObject *
IndexRowIterator_next(IndexRowIterator *self)
{
    PyObject *ret = NULL;
    if (!self->completed) {
        ret = IndexRowIterator_next_iter(self);
    }
    return ret;
}


static PyObject *
IndexRowIterator_set_min(IndexRowIterator *self, PyObject *args)
{
    PyObject *ret = NULL;
    int size = Index_set_key(self->index, args, self->min_key);
    if (size < 0) {
        goto out;
    }
    self->min_key_size = size;
    ret = Py_BuildValue("");
out:
    return ret;
}

static PyObject *
IndexRowIterator_set_max(IndexRowIterator *self, PyObject *args)
{
    PyObject *ret = NULL;
    int size = Index_set_key(self->index, args, self->max_key);
    if (size < 0) {
        goto out;
    }
    self->max_key_size = size;
    ret = Py_BuildValue("");
out:
    return ret;
}


static PyMethodDef IndexRowIterator_methods[] = {
    {"set_min", (PyCFunction) IndexRowIterator_set_min, METH_VARARGS, "Set the minimum key" },
    {"set_max", (PyCFunction) IndexRowIterator_set_max, METH_VARARGS, "Set the maximum key" },
    {NULL}  /* Sentinel */
};


static PyTypeObject IndexRowIteratorType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_wormtable.IndexRowIterator",             /* tp_name */
    sizeof(IndexRowIterator),             /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)IndexRowIterator_dealloc, /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_reserved */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash  */
    0,                         /* tp_call */
    0,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
    "IndexRowIterator objects",           /* tp_doc */
    0,                     /* tp_traverse */
    0,                     /* tp_clear */
    0,                     /* tp_richcompare */
    0,                     /* tp_weaklistoffset */
    PyObject_SelfIter,               /* tp_iter */
    (iternextfunc) IndexRowIterator_next, /* tp_iternext */
    IndexRowIterator_methods,             /* tp_methods */
    IndexRowIterator_members,             /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)IndexRowIterator_init,      /* tp_init */
};

/*==========================================================
 * IndexKeyIterator object
 *==========================================================
 */

static void
IndexKeyIterator_dealloc(IndexKeyIterator* self)
{
    if (self->cursor != NULL) {
        if (self->index != NULL) {
            if (self->index->db != NULL) {
                self->cursor->close(self->cursor);
            }
        }
    }
    Py_XDECREF(self->index);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static int
IndexKeyIterator_init(IndexKeyIterator *self, PyObject *args, PyObject *kwds)
{
    int ret = -1;
    static char *kwlist[] = {"index", NULL};
    Index *index = NULL;
    self->index = NULL;
    self->cursor = NULL;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!", kwlist,
            &IndexType, &index)) {
        goto out;
    }
    self->index = index;
    Py_INCREF(self->index);
    if (Index_check_read_mode(self->index) != 0) {
        goto out;
    }
    ret = 0;
out:
    return ret;
}

static PyMemberDef IndexKeyIterator_members[] = {
    {NULL}  /* Sentinel */
};


static PyObject *
IndexKeyIterator_next(IndexKeyIterator *self)
{
    PyObject *ret = NULL;
    int db_ret;
    DB *db;
    DBT key, data;
    if (Index_check_read_mode(self->index) != 0) {
        goto out;
    }
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
    if (self->cursor == NULL) {
        /* it's the first time through the loop, so set up the cursor */
        db = self->index->db;
        db_ret = db->cursor(db, NULL, &self->cursor, 0);
        if (db_ret != 0) {
            handle_bdb_error(db_ret);
            goto out;
        }
    }
    db_ret = self->cursor->get(self->cursor, &key, &data, DB_NEXT_NODUP);
    if (db_ret == 0) {
        ret = Index_key_to_python(self->index, key.data, key.size);
        if (ret == NULL) {
            goto out;
        }
    } else if (db_ret != DB_NOTFOUND) {
        handle_bdb_error(db_ret);
        goto out;
    }
    if (ret == NULL) {
        /* Iteration is finished - free the cursor */
        self->cursor->close(self->cursor);
        self->cursor = NULL;
    }
out:
    return ret;
}

static PyMethodDef IndexKeyIterator_methods[] = {
    {NULL}  /* Sentinel */
};


static PyTypeObject IndexKeyIteratorType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_wormtable.IndexKeyIterator",             /* tp_name */
    sizeof(IndexKeyIterator),             /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)IndexKeyIterator_dealloc, /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_reserved */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash  */
    0,                         /* tp_call */
    0,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
    "IndexKeyIterator objects",           /* tp_doc */
    0,                     /* tp_traverse */
    0,                     /* tp_clear */
    0,                     /* tp_richcompare */
    0,                     /* tp_weaklistoffset */
    PyObject_SelfIter,               /* tp_iter */
    (iternextfunc) IndexKeyIterator_next, /* tp_iternext */
    IndexKeyIterator_methods,             /* tp_methods */
    IndexKeyIterator_members,             /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)IndexKeyIterator_init,      /* tp_init */
};

/*==========================================================
 * Module level functions
 *==========================================================
 */

PyDoc_STRVAR(wormtable_get_db_version_doc,
"Returns Berkeley DB version information.\n\
This returns a tuple (compiled_version, runtime_version).\n\
Each of these is a tuple (major, minor, patch, version_string).\n");

static PyObject *
wormtable_get_db_version(PyObject *self)
{
    PyObject *ret = NULL;
    int major, minor, patch;
    char *str;

    str = db_version(&major, &minor, &patch);
    ret = Py_BuildValue("((i,i,i,s), (i,i,i,s))",
        DB_VERSION_MAJOR, DB_VERSION_MINOR, DB_VERSION_PATCH,
        DB_VERSION_STRING, major, minor, patch, str);
    return ret;
}

static PyMethodDef wormtable_methods[] = {
    {"get_db_version", (PyCFunction) wormtable_get_db_version, METH_NOARGS,
        wormtable_get_db_version_doc},
    {NULL}        /* Sentinel */
};


/* Initialisation code supports Python 2.x and 3.x. The framework uses the
 * recommended structure from http://docs.python.org/howto/cporting.html.
 * I've ignored the point about storing state in globals, as the examples
 * from the Python documentation still use this idiom.
 */

#if PY_MAJOR_VERSION >= 3

static struct PyModuleDef wormtablemodule = {
    PyModuleDef_HEAD_INIT,
    "_wormtable",   /* name of module */
    MODULE_DOC, /* module documentation, may be NULL */
    -1,
    wormtable_methods,
    NULL, NULL, NULL, NULL
};

#define INITERROR return NULL

PyObject *
PyInit__wormtable(void)

#else
#define INITERROR return

void
init_wormtable(void)
#endif
{
    char *db_version_str;
    int db_major, db_minor;
#if PY_MAJOR_VERSION >= 3
    PyObject *module = PyModule_Create(&wormtablemodule);
#else
    PyObject *module = Py_InitModule3("_wormtable", wormtable_methods,
            MODULE_DOC);
#endif
    if (module == NULL) {
        INITERROR;
    }
    /* Column */
    ColumnType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&ColumnType) < 0) {
        INITERROR;
    }
    Py_INCREF(&ColumnType);
    PyModule_AddObject(module, "Column", (PyObject *) &ColumnType);
    /* Table */
    TableType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&TableType) < 0) {
        INITERROR;
    }
    Py_INCREF(&TableType);
    PyModule_AddObject(module, "Table", (PyObject *) &TableType);
    /* Index */
    IndexType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&IndexType) < 0) {
        INITERROR;
    }
    Py_INCREF(&IndexType);
    PyModule_AddObject(module, "Index", (PyObject *) &IndexType);
    /* TableRowIterator */
    TableRowIteratorType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&TableRowIteratorType) < 0) {
        INITERROR;
    }
    Py_INCREF(&TableRowIteratorType);
    PyModule_AddObject(module, "TableRowIterator",
            (PyObject *) &TableRowIteratorType);
    /* TableRowIterator */
    IndexRowIteratorType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&IndexRowIteratorType) < 0) {
        INITERROR;
    }
    Py_INCREF(&IndexRowIteratorType);
    PyModule_AddObject(module, "IndexRowIterator",
            (PyObject *) &IndexRowIteratorType);
    /* IndexKeyIterator */
    IndexKeyIteratorType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&IndexKeyIteratorType) < 0) {
        INITERROR;
    }
    Py_INCREF(&IndexKeyIteratorType);
    PyModule_AddObject(module, "IndexKeyIterator",
            (PyObject *) &IndexKeyIteratorType);

    WormtableError = PyErr_NewException("_wormtable.WormtableError",
            NULL, NULL);
    Py_INCREF(WormtableError);
    PyModule_AddObject(module, "WormtableError", WormtableError);

    PyModule_AddIntConstant(module, "WT_VAR_1", WT_VAR_1);
    PyModule_AddIntConstant(module, "WT_VAR_2", WT_VAR_2);
    PyModule_AddIntConstant(module, "WT_CHAR", WT_CHAR);
    PyModule_AddIntConstant(module, "WT_UINT", WT_UINT);
    PyModule_AddIntConstant(module, "WT_INT", WT_INT);
    PyModule_AddIntConstant(module, "WT_FLOAT", WT_FLOAT);

    PyModule_AddIntConstant(module, "WT_READ", WT_READ);
    PyModule_AddIntConstant(module, "WT_WRITE", WT_WRITE);

    PyModule_AddIntConstant(module, "WT_VAR_1_MAX_ELEMENTS",
            WT_VAR_1_MAX_ELEMENTS);
    PyModule_AddIntConstant(module, "WT_VAR_2_MAX_ELEMENTS",
            WT_VAR_2_MAX_ELEMENTS);
    PyModule_AddIntConstant(module, "MAX_ROW_SIZE", MAX_ROW_SIZE);
    /* test for minimum supported version of DB at run time */
    db_version_str = db_version(&db_major, &db_minor, NULL);
    if (db_major < 4 || (db_major == 4 && db_minor < 8)) {
        PyErr_Format(PyExc_RuntimeError,
                "Run time Berkeley DB version must >= 4.8:'%s' found",
                db_version_str);
        INITERROR;
    }
#if PY_MAJOR_VERSION >= 3
    return module;
#endif
}


