#include <Python.h>
#include <structmember.h>
#include <db.h>


#if PY_MAJOR_VERSION >= 3
#define IS_PY3K
#endif

#define ELEMENT_TYPE_CHAR 0
#define ELEMENT_TYPE_INT 1 
#define ELEMENT_TYPE_FLOAT 2
#define ELEMENT_TYPE_ENUM 3

#define NUM_ELEMENTS_VARIABLE 0 
#define MAX_NUM_ELEMENTS 255
#define MAX_ROW_SIZE 65536

#define MODULE_DOC \
"Low level Berkeley DB interface for vcfdb"

static PyObject *BerkeleyDatabaseError;

/* TODO:
 * 1) We need much better error reporting for the parsers. These should have a
 * top level variadic function that takes arguments and set the Python exception
 * appropriately.
 * 2) It seems we have a memory leak somewhere. If we run the test cases in a 
 * loop the memory usage keeps increasing. We need to track this down.
 * 3) The numerical types need a little cleaning up and thought put into. We
 * now have single and double floating point sizes and should export constants
 * to tell what they are. There should also be some constants telling range
 * of integers and so on.
 * 4) Integer limits are not correct and need to be fixed, and then tested 
 * properly.
 * 5) Insert check for duplicate column names and columns: this is a nasty bug!
 */

typedef struct Column_t {
    PyObject_HEAD
    PyObject *name;
    PyObject *description;
    PyObject *enum_values;
    int element_type;
    int element_size;
    int num_elements;
    int fixed_region_offset;
    void **input_elements; /* pointer to each elements in input format */
    void *element_buffer; /* parsed input elements in native CPU format */
    int num_buffered_elements;
    int (*string_to_native)(struct Column_t*, char *);  /* DEPRECATED */
    int (*python_to_native)(struct Column_t*, PyObject *);
    int (*verify_elements)(struct Column_t*);
    int (*pack_elements)(struct Column_t*, void *);
    int (*unpack_elements)(struct Column_t*, void *);
    PyObject *(*native_to_python)(struct Column_t *, int);
} Column;


typedef struct {
    PyObject_HEAD
    DB *primary_db;
    PyObject *filename;
    PyObject *columns;
    Py_ssize_t cache_size;
    int fixed_region_size;
    int key_size;
} BerkeleyDatabase;

typedef struct {
    PyObject_HEAD
    BerkeleyDatabase *database;
    /* arrays for double buffering */
    void *key_buffer;
    void *data_buffer;
    int num_records;
    int current_data_offset;
    int current_key_offset;
    int current_record_size;
    DBT *key_dbts; 
    DBT *data_dbts; 
    /* end of arrays */
    int max_num_records;
    int data_buffer_size;
    int key_buffer_size;
    u_int64_t record_id; 
} WriteBuffer;

typedef struct {
    PyObject_HEAD
    DB *secondary_db;
    PyObject *filename;
    Py_ssize_t cache_size;
} Index;


static void 
handle_bdb_error(int err)
{
    PyErr_SetString(BerkeleyDatabaseError, db_strerror(err));
}

/* 
 * Copies n bytes of source into destination, swapping the order of the 
 * bytes if necessary.
 *
 */
static void
bigendian_copy(void* dest, void *source, size_t n)
{
#ifdef WORDS_BIGENDIAN
    printf("bigendian systems not supported\n");
    abort();
#else
    size_t j = 0;
    unsigned char *dest_c = (unsigned char *) dest;
    unsigned char *source_c = (unsigned char *) source;
    for (j = 0; j < n; j++) {
        dest_c[j] = source_c[n - j - 1];
    }
#endif
}


/*==========================================================
 * Column object 
 *==========================================================
 */

/**************************************
 *
 * Native values to Python conversion. 
 *
 *************************************/

static PyObject *
Column_native_to_python_int(Column *self, int index)
{
    PyObject *ret = NULL;
    int64_t *elements = (int64_t *) self->element_buffer;
    ret = PyLong_FromLongLong((long long) elements[index]);
    if (ret == NULL) {
        PyErr_NoMemory();
    }
    return ret;
}

static PyObject *
Column_native_to_python_float(Column *self, int index)
{
    PyObject *ret = NULL;
    double *elements = (double *) self->element_buffer;
    ret = PyFloat_FromDouble(elements[index]);
    if (ret == NULL) {
        PyErr_NoMemory();
    }
    return ret;
}

static PyObject *
Column_native_to_python_char(Column *self, int index)
{
    PyObject *ret = NULL;
    int j = self->num_buffered_elements - 1;
    char *str = (char *) self->element_buffer;
    if (self->num_elements != NUM_ELEMENTS_VARIABLE) {
        /* check for shortened fixed-length strings, which will be padded 
         * with NULLs */
        while (str[j] == '\0' && j >= 0) {
            j--;
        }
    }
    ret = PyBytes_FromStringAndSize(str, j + 1); 
    if (ret == NULL) {
        PyErr_NoMemory();
    }
    return ret;
}

/**************************************
 *
 * Floating point packing and unpacking. This is based on the implementation 
 * of SortedFloat and SortedDouble from Berkeley DB Java edition. See 
 * com.sleepycat.bind.tuple.TupleInput for the source of the bit
 * manipulations below.
 *
 *************************************/

static void 
pack_float(float value, void *dest)
{
    int32_t float_bits;
    memcpy(&float_bits, &value, sizeof(float));
    float_bits ^= (float_bits < 0) ? 0xffffffff: 0x80000000;
    bigendian_copy(dest, &float_bits, sizeof(float)); 
}

static double  
unpack_float(void *src)
{
    int32_t float_bits;
    float value;
    bigendian_copy(&float_bits, src, sizeof(float));
    float_bits ^= (float_bits < 0) ? 0x80000000: 0xffffffff;
    memcpy(&value, &float_bits, sizeof(float));
    return (double) value;
}

static void 
pack_double(double value, void *dest)
{
    int64_t double_bits;
    memcpy(&double_bits, &value, sizeof(double));
    double_bits ^= (double_bits < 0) ? 0xffffffffffffffffL: 0x8000000000000000L;
    bigendian_copy(dest, &double_bits, sizeof(double)); 
}

static double  
unpack_double(void *src)
{
    int64_t double_bits;
    double value;
    bigendian_copy(&double_bits, src, sizeof(double));
    double_bits ^= (double_bits < 0) ? 0x8000000000000000L: 0xffffffffffffffffL;
    memcpy(&value, &double_bits, sizeof(double));
    return (double) value;
}


/**************************************
 *
 * Unpacking from a row to the element buffer. 
 *
 *************************************/

static int 
Column_unpack_elements_int(Column *self, void *source)
{
    int j;
    int ret = -1;
    void *v = source;
    int64_t *elements = (int64_t *) self->element_buffer;
    int64_t tmp;
    for (j = 0; j < self->num_buffered_elements; j++) {
        bigendian_copy(&tmp, v, self->element_size);
        v += self->element_size;
        /* TODO fix this to work for all int sizes */
        switch (self->element_size) {
            case 1:
                elements[j] = (int8_t) tmp;
                break;
            case 2:
                elements[j] = (int16_t) tmp;
                break;
            case 4:
                elements[j] = (int32_t) tmp;
                break;
            case 8:
                elements[j] = (int64_t) tmp;
                break;
            default:
                Py_FatalError("Complete int sizes not yet supported");
        }
    }
    ret = 0;
    return ret; 
}

static int 
Column_unpack_elements_float(Column *self, void *source)
{
    int j;
    int ret = -1;
    void *v = source;
    double *elements = (double *) self->element_buffer;
    /* TODO Tidy this up and make it consistent with the pack definition */
    for (j = 0; j < self->num_buffered_elements; j++) {
        if (self->element_size == 4) {
            elements[j] = unpack_float(v); 
        } else {
            elements[j] = unpack_double(v); 
        }
        v += self->element_size;
    }
    ret = 0;
    return ret; 
}

static int 
Column_unpack_elements_char(Column *self, void *source)
{
    /*
    char v[1024];
    memcpy(v, source, self->num_buffered_elements); 
    v[self->num_buffered_elements] = 0;
    printf("unpacked: '%s': %d\n", v, self->num_buffered_elements);
    */
    memcpy(self->element_buffer, source, self->num_buffered_elements); 
    return  0; 
}




/**************************************
 *
 * Packing native values from the element_buffer to a row.
 *
 *************************************/

static int 
Column_pack_elements_int(Column *self, void *dest)
{
    int j;
    int ret = -1;
    void *v = dest;
    int64_t *elements = (int64_t *) self->element_buffer;
    for (j = 0; j < self->num_buffered_elements; j++) {
        //printf("\npacking :%ld\n", elements[j]); 
        bigendian_copy(v, &elements[j], self->element_size);
        v += self->element_size;
    }
    ret = 0;
    return ret; 
}

static int 
Column_pack_elements_float(Column *self, void *dest)
{
    int j;
    int ret = -1;
    void *v = dest;
    double *elements = (double *) self->element_buffer;
    /* TODO tidy this up */
    for (j = 0; j < self->num_buffered_elements; j++) {
        if (self->element_size == 4) {
            pack_float((float) elements[j], v);
        } else if (self->element_size == 8) {
            pack_double(elements[j], v);
        } else {
            assert(0);
        }
        
        
        v += self->element_size;
    }
    ret = 0;
    return ret; 
}

static int 
Column_pack_elements_char(Column *self, void *dest)
{
    int ret = -1;
    /*
    char v[1024];
    memcpy(v, self->element_buffer, self->num_buffered_elements); 
    v[self->num_buffered_elements] = 0;
    printf("packed: '%s': %d\n", v, self->num_buffered_elements);
    */
    memcpy(dest, self->element_buffer, self->num_buffered_elements); 
    ret = 0;
    return ret; 
}



/**************************************
 *
 * Verify elements in the buffer. 
 *
 *************************************/
static int 
Column_verify_elements_int(Column *self)
{
    int j;
    int ret = -1;
    int64_t *elements = (int64_t *) self->element_buffer;
    /* TODO check this - probably not totally right */
    /* This seems to be wrong - must get the correct formula */
    int64_t min_value = (-1) * (1ll << (8 * self->element_size - 1));
    int64_t max_value = (1ll << (8 * self->element_size - 1)) - 1;
    for (j = 0; j < self->num_buffered_elements; j++) {
        if (elements[j] < min_value || elements[j] > max_value) {
            PyErr_SetString(PyExc_ValueError, "Value out of bounds");
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
        if (self->num_elements == NUM_ELEMENTS_VARIABLE) {
            if (num_elements > MAX_NUM_ELEMENTS) {
                PyErr_SetString(PyExc_ValueError, "too many elements");
                goto out;
            }
        } else {    
            if (num_elements != self->num_elements) {
                PyErr_SetString(PyExc_ValueError, "incorrect number of elements");
                goto out;
            }
        }
        for (j = 0; j < num_elements; j++) {
            v = PySequence_Fast_GET_ITEM(seq, j);
            self->input_elements[j] = v; 
        }
    }
    if (self->num_elements != NUM_ELEMENTS_VARIABLE) {
        if (num_elements != self->num_elements) {
            PyErr_SetString(PyExc_ValueError, "incorrect number of elements");
            goto out;
        }
    }
    self->num_buffered_elements = num_elements;
    ret = 0;
out:
    Py_XDECREF(seq);
    return ret;
}

static int 
Column_python_to_native_int(Column *self, PyObject *elements)
{
    int ret = -1;
    int64_t *native= (int64_t *) self->element_buffer; 
    PyObject *v;
    int j;
    if (Column_parse_python_sequence(self, elements) < 0) {
        goto out;
    }
    for (j = 0; j < self->num_buffered_elements; j++) {
        v = (PyObject *) self->input_elements[j];
        /* 
         * This should be the only place that we _have_ to work around
         * the difference between integers in Python 2 and 3.
         */
#if PY_MAJOR_VERSION >= 3
        if (!PyLong_Check(v)) {
            PyErr_SetString(PyExc_TypeError, "Must be integer");
            goto out;
        }
        native[j] = (int64_t) PyLong_AsLongLong(v);
#else
        if (!PyInt_Check(v)) {
            PyErr_SetString(PyExc_TypeError, "Must be integer");
            goto out;
        }
        native[j] = (int64_t) PyInt_AsLong(v);
#endif
    }
    ret = 0;
out:
    return ret;
}

static int 
Column_python_to_native_float(Column *self, PyObject *elements)
{
    int ret = -1;
    double *native = (double *) self->element_buffer; 
    PyObject *v;
    int j;
    if (Column_parse_python_sequence(self, elements) < 0) {
        goto out;
    }
    for (j = 0; j < self->num_buffered_elements; j++) {
        v = (PyObject *) self->input_elements[j];
        if (!PyFloat_Check(v)) {
            PyErr_SetString(PyExc_TypeError, "Must be float");
            goto out;
        }
        native[j] = (double) PyFloat_AsDouble(v);
    }
    ret = 0;
out:
    return ret;
}

static int 
Column_python_to_native_char(Column *self, PyObject *elements)
{
    int ret = -1;
    char *s;
    Py_ssize_t max_length = self->num_elements == NUM_ELEMENTS_VARIABLE?
            MAX_NUM_ELEMENTS: self->num_elements;
    Py_ssize_t length;
    /* Elements must be a single Python bytes object */
    if (!PyBytes_Check(elements)) {
        PyErr_SetString(PyExc_TypeError, "Must be bytes");
        goto out;
    }
    if (PyBytes_AsStringAndSize(elements, &s, &length) < 0) {
        PyErr_SetString(PyExc_ValueError, "Error in string conversion");
        goto out;
    }
    if (length > max_length) {
        PyErr_SetString(PyExc_ValueError, "String too long");
        goto out;
        
    }
    memcpy(self->element_buffer, s, length);
    self->num_buffered_elements = length;
    ret = 0; 
out:
    return ret;
}



/**************************************
 *
 * String input element parsing.
 *
 *************************************/

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
    self->num_buffered_elements = 0;
    if (self->num_elements == 1) {
        self->input_elements[0] = s;
        num_elements = 1;
    } else {
        j = 0;
        num_elements = 0;
        delimiter = -1;
        if (s[0] == '\0') {
            PyErr_SetString(PyExc_ValueError, "Empty value");
            goto out;
        }
        /* TODO this needs lots of error checking! */
        while (s[j] != '\0') {
            if (s[j] == ',' || s[j] == ';') {
                delimiter = j; 
            }
            if (j == delimiter + 1) {
                /* this is the start of a new element */
                self->input_elements[num_elements] = &s[j];
                num_elements++;
            }
            j++;
        }
    }
    if (self->num_elements != NUM_ELEMENTS_VARIABLE) {
        if (num_elements != self->num_elements) {
            PyErr_SetString(PyExc_ValueError, "incorrect number of elements");
            goto out;
        }
    }
    self->num_buffered_elements = num_elements;
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
            PyErr_SetString(PyExc_ValueError, "Element overflow");
            goto out;
        }
        if (v == tail) {
            PyErr_SetString(PyExc_ValueError, "Element parse error");
            goto out;
        }
        if (*tail != '\0') {
            if (!(isspace(*tail) || *tail == ',' || *tail == ';')) {
                PyErr_SetString(PyExc_ValueError, "Element parse error");
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
            PyErr_SetString(PyExc_ValueError, "Element overflow");
            goto out;
        }
        if (v == tail) {
            PyErr_SetString(PyExc_ValueError, "Element parse error");
            goto out;
        }
        if (*tail != '\0') {
            if (!(isspace(*tail) || *tail == ',' || *tail == ';')) {
                PyErr_SetString(PyExc_ValueError, "Element parse error");
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
    size_t n = strlen(string);
    memcpy(self->element_buffer, string, n); 
    return n;
}

/*
 * TODO We need to support lists of enumeration values - this will need to 
 * copy the parsing code above.
 */
static int 
Column_string_to_native_enum(Column *self, char *string)
{
    int ret = -1;
    unsigned long value; 
    unsigned long max_value = 1l << (8 * self->element_size);
    PyObject *v = PyDict_GetItemString(self->enum_values, string);
    if (v == NULL) {
        value = PyDict_Size(self->enum_values) + 1;
        if (value > max_value) {
            PyErr_SetString(PyExc_ValueError, "Enum value too large");
            goto out;
        }
        v = PyLong_FromUnsignedLong(value);
        if (v == NULL) {
            PyErr_NoMemory();
            goto out; 
        }
        if (PyDict_SetItemString(self->enum_values, string, v) < 0) {
            Py_DECREF(v); 
            goto out;
        }
        Py_DECREF(v);    
    } else {
        if (!PyLong_Check(v)) {
            PyErr_SetString(PyExc_ValueError, "Enum value not a long");
            goto out;
        }
        value = PyLong_AsUnsignedLong(v); 
    }
    bigendian_copy(self->element_buffer, &value, self->element_size);
    //printf("%s -> %ld\n", string, value);
    ret = 1;
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
Column_update_row(Column *self, void *row, u_int32_t row_size)
{
    int ret = -1;
    void *dest;
    int bytes_added = 0;
    int num_elements = self->num_buffered_elements;
    int data_size = num_elements * self->element_size;
    if (self->verify_elements(self) < 0) {
        goto out;
    }
    dest = row + self->fixed_region_offset; 
    if (self->num_elements == NUM_ELEMENTS_VARIABLE) {
        bytes_added = data_size;
        if (row_size + bytes_added > MAX_ROW_SIZE) {
            PyErr_SetString(PyExc_ValueError, "Row overflow");
            goto out;
        }
        bigendian_copy(dest, &row_size, 2); // FIXME
        dest += 2; // FIXME
        bigendian_copy(dest, &num_elements, 1); // FIXME
        //printf("set to offset %d, with %d bytes\n", row_size, num_elements);
        dest = row + row_size; 
    }
    self->pack_elements(self, dest);
    ret = bytes_added;
out:
    return ret;
}

/*
 * Extracts elements from the specified row and inserts them into the 
 * element buffer.
 */
static int 
Column_extract_elements(Column *self, void *row)
{
    int ret = -1;
    void *dest;
    int offset, num_elements;
    dest = row + self->fixed_region_offset; 
    num_elements = self->num_elements;
    if (self->num_elements == NUM_ELEMENTS_VARIABLE) {
        offset = 0;
        num_elements = 0;
        bigendian_copy(&offset, dest, 2); // FIXME
        dest += 2; // FIXME
        bigendian_copy(&num_elements, dest, 1); // FIXME
        dest = row + offset;
        //printf("read %d * %d @ %d\n", offset, num_elements, self->fixed_region_offset);
        /* These should really be considered to be internal 
         * fatal errors, as they should only happen on database
         * corruption
         */
        if (offset < 0 || offset >= MAX_ROW_SIZE) {
            PyErr_SetString(PyExc_ValueError, "Row overflow");
            goto out;
        }
        if (num_elements > MAX_NUM_ELEMENTS) {
            PyErr_SetString(PyExc_ValueError, "too many elements");
            goto out;
        }
    }
    self->num_buffered_elements = num_elements;
    ret = self->unpack_elements(self, dest);
out:
    return ret;
}

/*
 * Converts the native values in the element buffer to the appropriate 
 * Python types, and returns the result.
 */
static PyObject *
Column_get_python_elements(Column *self)
{
    PyObject *ret = NULL;
    PyObject *u, *t;
    Py_ssize_t j;
    if (self->num_elements == 1 || self->element_type == ELEMENT_TYPE_CHAR) {
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
    if (self->num_elements == NUM_ELEMENTS_VARIABLE) {
        /* two byte offset + one byte count */
        ret = 3;
    }
    return ret;
}

static void
Column_dealloc(Column* self)
{
    Py_XDECREF(self->name); 
    Py_XDECREF(self->description); 
    Py_XDECREF(self->enum_values); 
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
    self->element_buffer = NULL;
    self->input_elements = NULL;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OOiii", kwlist, 
            &name, &description, 
            &self->element_type, &self->element_size, 
            &self->num_elements)) {  
        goto out;
    }
    self->name = name;
    self->description = description;
    Py_INCREF(self->name);
    Py_INCREF(self->description);
    if (self->element_type == ELEMENT_TYPE_INT) {
        if (self->element_size < 1 || self->element_size > 8) {
            PyErr_SetString(PyExc_ValueError, "bad element size");
            goto out;
        }
        self->python_to_native = Column_python_to_native_int;
        self->string_to_native = Column_string_to_native_int;
        self->verify_elements = Column_verify_elements_int;
        self->pack_elements = Column_pack_elements_int;
        self->unpack_elements = Column_unpack_elements_int;
        self->native_to_python = Column_native_to_python_int; 
        native_element_size = sizeof(int64_t);
    } else if (self->element_type == ELEMENT_TYPE_FLOAT) {
        if (self->element_size != sizeof(float)
                && self->element_size != sizeof(double)) {
            PyErr_SetString(PyExc_ValueError, "bad element size");
            goto out;
        }
        self->python_to_native = Column_python_to_native_float;
        self->string_to_native = Column_string_to_native_float;
        self->verify_elements = Column_verify_elements_float;
        self->pack_elements = Column_pack_elements_float;
        self->unpack_elements = Column_unpack_elements_float;
        self->native_to_python = Column_native_to_python_float; 
        native_element_size = sizeof(double);
    } else if (self->element_type == ELEMENT_TYPE_CHAR) {
        if (self->element_size != 1) {
            PyErr_SetString(PyExc_ValueError, "bad element size");
            goto out;
        }
        self->python_to_native = Column_python_to_native_char;
        self->string_to_native = Column_string_to_native_char;
        self->verify_elements = Column_verify_elements_char;
        self->pack_elements = Column_pack_elements_char;
        self->unpack_elements = Column_unpack_elements_char;
        self->native_to_python = Column_native_to_python_char; 
        native_element_size = sizeof(char);
    } else if (self->element_type == ELEMENT_TYPE_ENUM) {
        if (self->element_size < 1 || self->element_size > 2) {
            PyErr_SetString(PyExc_ValueError, "bad element size");
            goto out;
        }
        self->string_to_native = Column_string_to_native_enum;
        native_element_size = sizeof(char);
        Py_FatalError("Column type not supported yet");
    } else {    
        PyErr_SetString(PyExc_ValueError, "Unknown element type");
        goto out;
    }
    if (self->num_elements > MAX_NUM_ELEMENTS) {
        PyErr_SetString(PyExc_ValueError, "Too many elements");
        goto out;
    }
    if (self->num_elements < 0) {
        PyErr_SetString(PyExc_ValueError, "negative num elements");
        goto out;
    }
    self->enum_values = PyDict_New();
    if (self->enum_values == NULL) {
        goto out;
    }
    Py_INCREF(self->enum_values);
    max_num_elements = self->num_elements;
    if (self->num_elements == NUM_ELEMENTS_VARIABLE) {
        max_num_elements = MAX_NUM_ELEMENTS;    
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
    {"enum_values", T_OBJECT_EX, offsetof(Column, enum_values), 0, "enum_values"},
    {"element_type", T_INT, offsetof(Column, element_type), READONLY, "element_type"},
    {"element_size", T_INT, offsetof(Column, element_size), READONLY, "element_size"},
    {"num_elements", T_INT, offsetof(Column, num_elements), READONLY, "num_elements"},
    {"fixed_region_offset", T_INT, offsetof(Column, fixed_region_offset), 
        READONLY, "fixed_region_offset"},
    {NULL}  /* Sentinel */
};


static PyMethodDef Column_methods[] = {
    {NULL}  /* Sentinel */
};

static PyTypeObject ColumnType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_vcfdb.Column",             /* tp_name */
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
 * BerkeleyDatabase object 
 *==========================================================
 */

static void
BerkeleyDatabase_dealloc(BerkeleyDatabase* self)
{
    Py_XDECREF(self->filename);
    Py_XDECREF(self->columns);
    /* make sure that the DB handles are closed. We can ignore errors here. */ 
    if (self->primary_db != NULL) {
        self->primary_db->close(self->primary_db, 0);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static int
BerkeleyDatabase_init(BerkeleyDatabase *self, PyObject *args, PyObject *kwds)
{
    int j;
    Column *col;
    int ret = -1;
    static char *kwlist[] = {"filename", "columns", "cache_size", NULL}; 
    PyObject *filename = NULL;
    PyObject *columns = NULL;
    self->primary_db = NULL;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!O!n", kwlist, 
            &PyBytes_Type, &filename, 
            &PyList_Type,  &columns, 
            &self->cache_size)) {
        goto out;
    }
    self->filename = filename;
    Py_INCREF(self->filename);
    self->columns = columns;
    Py_INCREF(self->columns);
    /* TODO make this part of kwargs */
    self->key_size = 5; 
    /* calculate the variable region offset by summing up the fixed 
     * region size from each column
     */
    for (j = 0; j < PyList_GET_SIZE(self->columns); j++) {
        col = (Column *) PyList_GET_ITEM(self->columns, j);
        if (!PyObject_TypeCheck(col, &ColumnType)) {
            PyErr_SetString(PyExc_ValueError, "Must be Column objects");
            goto out;
        }
        col->fixed_region_offset = self->fixed_region_size;
        self->fixed_region_size += Column_get_fixed_region_size(col);
        if (self->fixed_region_size > MAX_ROW_SIZE) {
            PyErr_SetString(PyExc_ValueError, "Columns exceed max row size");
            goto out;
        }
    }
    ret = 0;
out:

    return ret;
}


static PyMemberDef BerkeleyDatabase_members[] = {
    {"filename", T_OBJECT_EX, offsetof(BerkeleyDatabase, filename), READONLY, "filename"},
    {"columns", T_OBJECT_EX, offsetof(BerkeleyDatabase, columns), READONLY, "columns"},
    {"cache_size", T_PYSSIZET, offsetof(BerkeleyDatabase, cache_size), READONLY, "cache_size"},
    {"fixed_region_size", T_INT, offsetof(BerkeleyDatabase, fixed_region_size), READONLY, "fixed_region_size"},
    {NULL}  /* Sentinel */
};



static PyObject *
BerkeleyDatabase_open_helper(BerkeleyDatabase* self, u_int32_t flags)
{
    PyObject *ret = NULL;
    int db_ret;
    char *db_name = NULL;
    Py_ssize_t gigabyte = 1024 * 1024 * 1024;
    u_int32_t gigs, bytes;
    DB *db;
    if (self->primary_db != NULL) {
        PyErr_SetString(BerkeleyDatabaseError, "database already open");
        goto out;
    }
    db_ret = db_create(&db, NULL, 0);
    self->primary_db = db;
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;    
    }
    gigs = (u_int32_t) (self->cache_size / gigabyte);
    bytes = (u_int32_t) (self->cache_size % gigabyte);
    db_ret = db->set_cachesize(db, gigs, bytes, 1); 
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;    
    }
    db_name = PyBytes_AsString(self->filename);
    db_ret = db->open(db, NULL, db_name, NULL, DB_BTREE,  flags,  0);         
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;    
    }
    ret = Py_BuildValue("");
out:
    return ret;
}

static PyObject *
BerkeleyDatabase_create(BerkeleyDatabase* self)
{
    u_int32_t flags = DB_CREATE|DB_TRUNCATE;
    return BerkeleyDatabase_open_helper(self, flags);
}

static PyObject *
BerkeleyDatabase_open(BerkeleyDatabase* self)
{
    u_int32_t flags = DB_RDONLY|DB_NOMMAP;
    return BerkeleyDatabase_open_helper(self, flags);
}

static PyObject *
BerkeleyDatabase_get_num_rows(BerkeleyDatabase* self)
{
    int db_ret;
    unsigned long long max_key = 0;
    PyObject *ret = NULL;
    DBC *cursor = NULL;
    DB *db = self->primary_db;
    DBT key, data;
    if (db == NULL) {
        PyErr_SetString(BerkeleyDatabaseError, "database closed");
        goto out;
    }
    db_ret = db->cursor(db, NULL, &cursor, 0);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;    
    }
    /* retrieve the last key from the DB */
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));  
    db_ret = cursor->get(cursor, &key, &data, DB_PREV);
    if (db_ret == 0) {
        bigendian_copy(&max_key, key.data, self->key_size);
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
BerkeleyDatabase_get_row(BerkeleyDatabase* self, PyObject *args)
{
    int db_ret;
    PyObject *ret = NULL;
    PyObject *row = NULL;
    Column *col = NULL;
    PyObject *value = NULL;
    Py_ssize_t j;
    Py_ssize_t num_columns = 0;
    unsigned long long record_id = 0;
    unsigned char key_buff[sizeof(unsigned long long)];
    DB *db = self->primary_db;
    DBT key, data;
    if (!PyArg_ParseTuple(args, "K", &record_id)) {
        goto out;
    }
    if (db == NULL) {
        PyErr_SetString(BerkeleyDatabaseError, "database closed");
        goto out;
    }
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));  
    bigendian_copy(key_buff, &record_id, self->key_size);
    key.size = self->key_size;
    key.data = key_buff;
    db_ret = db->get(db, NULL, &key, &data, 0);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;    
    }
    num_columns = PyList_Size(self->columns);
    row = PyDict_New();
    if (row == NULL) {
        PyErr_NoMemory();
        goto out;
    }
    for (j = 0; j < num_columns; j++) {
        col = (Column *) PyList_GET_ITEM(self->columns, j);
        if (!PyObject_TypeCheck(col, &ColumnType)) {
            PyErr_SetString(PyExc_ValueError, "Must be Column objects");
            goto out;
        }
        if (Column_extract_elements(col, data.data) < 0) {
            goto out;
        }
        value = Column_get_python_elements(col); 
        if (value == NULL) {
            goto out;
        }
        if (PyDict_SetItem(row, col->name, value) < 0) {
            goto out;
        }
    }
    ret = row;
out:
    return ret; 
}



static PyObject *
BerkeleyDatabase_close(BerkeleyDatabase* self)
{
    PyObject *ret = NULL;
    int db_ret;
    DB *db = self->primary_db;
    if (db == NULL) {
        PyErr_SetString(BerkeleyDatabaseError, "database closed");
        goto out;
    }
    db_ret = db->close(db, 0); 
    self->primary_db = NULL;
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;    
    }
    ret = Py_BuildValue("");
out:
    self->primary_db = NULL;
    return ret; 
}


static PyMethodDef BerkeleyDatabase_methods[] = {
    {"create", (PyCFunction) BerkeleyDatabase_create, METH_NOARGS, "Create the database" },
    {"open", (PyCFunction) BerkeleyDatabase_open, METH_NOARGS, "Open the database for reading" },
    {"get_num_rows", (PyCFunction) BerkeleyDatabase_get_num_rows, METH_NOARGS, "return the number of rows." },
    {"get_row", (PyCFunction) BerkeleyDatabase_get_row, METH_VARARGS, "returns the row at a specific index." },
    {"close", (PyCFunction) BerkeleyDatabase_close, METH_NOARGS, "Close the database" },
    {NULL}  /* Sentinel */
};


static PyTypeObject BerkeleyDatabaseType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_vcfdb.BerkeleyDatabase",             /* tp_name */
    sizeof(BerkeleyDatabase),             /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)BerkeleyDatabase_dealloc, /* tp_dealloc */
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
    "BerkeleyDatabase objects",           /* tp_doc */
    0,                     /* tp_traverse */
    0,                     /* tp_clear */
    0,                     /* tp_richcompare */
    0,                     /* tp_weaklistoffset */
    0,                     /* tp_iter */
    0,                     /* tp_iternext */
    BerkeleyDatabase_methods,             /* tp_methods */
    BerkeleyDatabase_members,             /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)BerkeleyDatabase_init,      /* tp_init */
};
   


/*==========================================================
 * WriteBuffer object 
 *==========================================================
 */

static void
WriteBuffer_clear_buffers(WriteBuffer* self)
{
    memset(self->data_buffer, 0, self->data_buffer_size);
    memset(self->key_buffer, 0, self->key_buffer_size);
    self->num_records = 0;
    self->current_data_offset = 0;
    self->current_key_offset = 0;
    self->current_record_size = self->database->fixed_region_size;
}
static void
WriteBuffer_dealloc(WriteBuffer* self)
{
    PyMem_Free(self->data_buffer);
    PyMem_Free(self->key_buffer);
    PyMem_Free(self->data_dbts);
    PyMem_Free(self->key_dbts);
    Py_XDECREF(self->database);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static int
WriteBuffer_init(WriteBuffer *self, PyObject *args, PyObject *kwds)
{
    int ret = -1; 
    static char *kwlist[] = {"database", "data_buffer_size", 
        "max_num_records", NULL};
    BerkeleyDatabase *database = NULL;
    self->database = NULL;
    self->key_buffer = NULL; 
    self->data_buffer = NULL; 
    self->key_dbts = NULL; 
    self->data_dbts = NULL; 
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!ii", kwlist, 
            &BerkeleyDatabaseType, &database, &self->data_buffer_size,
            &self->max_num_records)) {
        goto out;
    }
    self->database = database;
    Py_INCREF(self->database);
    if (self->data_buffer_size < MAX_ROW_SIZE) {
        PyErr_SetString(PyExc_ValueError, "data buffer size too small");
        goto out;
    }
    if (self->max_num_records < 1) {
        PyErr_SetString(PyExc_ValueError, "must have >= 1 records in buffer.");
        goto out;
    }
    self->key_buffer_size = self->max_num_records * self->database->key_size;
    /* Alloc some memory */
    self->key_buffer = PyMem_Malloc(self->key_buffer_size);
    if (self->key_buffer == NULL) {
        PyErr_NoMemory();
        goto out;
    }
    self->data_buffer = PyMem_Malloc(self->data_buffer_size);
    if (self->data_buffer == NULL) {
        PyErr_NoMemory();
        goto out;
    }
    self->key_dbts = PyMem_Malloc(self->max_num_records * sizeof(DBT));
    if (self->key_dbts == NULL) {
        PyErr_NoMemory();
        goto out;
    }
    self->data_dbts = PyMem_Malloc(self->max_num_records * sizeof(DBT));
    if (self->data_dbts == NULL) {
        PyErr_NoMemory();
        goto out;
    }
    /* zero the DBTs before use */
    memset(self->key_dbts, 0, self->max_num_records * sizeof(DBT));
    memset(self->data_dbts, 0, self->max_num_records * sizeof(DBT));
    self->record_id = 0;
    WriteBuffer_clear_buffers(self);
    
    ret = 0;
out:
    return ret;
}

static PyMemberDef WriteBuffer_members[] = {
    {"database", T_OBJECT_EX, offsetof(WriteBuffer, database), READONLY, "database"},
    {"max_num_records", T_INT, offsetof(WriteBuffer, max_num_records), READONLY, "max_num_records"},
    {"data_buffer_size", T_INT, offsetof(WriteBuffer, data_buffer_size), READONLY, "data_buffer_size"},
    {"key_buffer_size", T_INT, offsetof(WriteBuffer, key_buffer_size), READONLY, "key_buffer_size"},
    {NULL}  /* Sentinel */
};

static PyObject *
WriteBuffer_flush(WriteBuffer* self)
{
    PyObject *ret = NULL;
    int db_ret = 0;
    u_int32_t j;
    DB *db;
    DBT *key, *data;
    db = self->database->primary_db;
    if (db == NULL) {
        PyErr_SetString(BerkeleyDatabaseError, "database closed");
        goto out;
    }
    
    //printf("Flushing buffer: %d records in %dKiB\n", self->num_records, 
    //        self->current_data_offset / 1024);
    for (j = 0; j < self->num_records; j++) {
        key = &self->key_dbts[j];
        data = &self->data_dbts[j];
        db_ret = db->put(db, NULL, key, data, 0);
        if (db_ret != 0) {
            handle_bdb_error(db_ret); 
            goto out;
        } 
    }
    //printf("done\n");
    WriteBuffer_clear_buffers(self);

    ret = Py_BuildValue("");
out:
    return ret; 
}

static PyObject *
WriteBuffer_insert_encoded_elements(WriteBuffer* self, PyObject *args)
{
    PyObject *ret = NULL;
    Column *column = NULL;
    PyBytesObject *value = NULL;
    void *dest = self->data_buffer + self->current_data_offset;
    char *v;
    int  m;
    PyErr_SetString(BerkeleyDatabaseError, "encoded elements not supported: use"
           " insert_elements instead. This method is being removed. ");
    goto out;
    
    if (!PyArg_ParseTuple(args, "O!O!", &ColumnType, &column, &PyBytes_Type,
            &value)) {
        goto out;
    }
    v = PyBytes_AsString((PyObject *) value);
    if (column->string_to_native(column, v) < 0) {
        goto out;   
    }
    m = Column_update_row(column, dest, self->current_record_size); 
    if (m < 0) {
        goto out;
    }
    self->current_record_size += m;
    ret = Py_BuildValue("");
out:
    return ret; 
}

static PyObject *
WriteBuffer_insert_elements(WriteBuffer* self, PyObject *args)
{
    int m;
    PyObject *ret = NULL;
    Column *column = NULL;
    PyObject *elements = NULL;
    void *dest = self->data_buffer + self->current_data_offset;
    if (!PyArg_ParseTuple(args, "O!O", &ColumnType, &column, &elements)) { 
        goto out;
    }
    if (column->python_to_native(column, elements) < 0) {
        goto out;   
    }
    m = Column_update_row(column, dest, self->current_record_size); 
    if (m < 0) {
        goto out;
    }
    self->current_record_size += m;
    ret = Py_BuildValue("");
out:
    return ret; 
}




static PyObject *
WriteBuffer_commit_row(WriteBuffer* self, PyObject *args)
{
    DBT *key, *data;
    PyObject *ret = NULL;
    void *dest;
    int barrier = self->data_buffer_size - MAX_ROW_SIZE;
    dest = self->key_buffer + self->current_key_offset;
    bigendian_copy(dest, &self->record_id, self->database->key_size);
    /*
    int j;
    printf("Commit:%llu %d \n", key_val, self->current_record_size);
    for (j = 0; j < 1024; j++) {
        printf("%03d ", ((unsigned char *) self->data_buffer)[j]);
    }
    printf("\n");
    */
    key = &self->key_dbts[self->num_records];
    data = &self->data_dbts[self->num_records];
    key->size = self->database->key_size;
    key->data = self->key_buffer + self->current_key_offset;
    data->size = self->current_record_size;
    data->data = self->data_buffer + self->current_data_offset; 
    /* We are done with this record, so increment the counters */ 
    self->record_id++;
    self->current_key_offset += self->database->key_size;
    self->current_data_offset += self->current_record_size;
    self->current_record_size = self->database->fixed_region_size;
    self->num_records++;
    if (self->current_data_offset >= barrier 
            || self->num_records == self->max_num_records) {
        ret = WriteBuffer_flush(self);
        if (ret == NULL) {
            goto out;
        }
    }

    ret = Py_BuildValue("");
out:
    return ret; 
}




static PyMethodDef WriteBuffer_methods[] = {
    {"insert_encoded_elements", (PyCFunction) WriteBuffer_insert_encoded_elements, 
        METH_VARARGS, "insert element values encoded as a string." },
    {"insert_elements", (PyCFunction) WriteBuffer_insert_elements, 
        METH_VARARGS, "insert element values encoded native Python objects." },
    {"commit_row", (PyCFunction) WriteBuffer_commit_row, METH_VARARGS, "commit row" },
    {"flush", (PyCFunction) WriteBuffer_flush, METH_NOARGS, "flush" },
    {NULL}  /* Sentinel */
};


static PyTypeObject WriteBufferType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_vcfdb.WriteBuffer",             /* tp_name */
    sizeof(WriteBuffer),             /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)WriteBuffer_dealloc, /* tp_dealloc */
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
    "WriteBuffer objects",           /* tp_doc */
    0,                     /* tp_traverse */
    0,                     /* tp_clear */
    0,                     /* tp_richcompare */
    0,                     /* tp_weaklistoffset */
    0,                     /* tp_iter */
    0,                     /* tp_iternext */
    WriteBuffer_methods,             /* tp_methods */
    WriteBuffer_members,             /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)WriteBuffer_init,      /* tp_init */
};
 


/* Initialisation code supports Python 2.x and 3.x. The framework uses the 
 * recommended structure from http://docs.python.org/howto/cporting.html. 
 * I've ignored the point about storing state in globals, as the examples 
 * from the Python documentation still use this idiom. 
 */

#if PY_MAJOR_VERSION >= 3

static struct PyModuleDef vcfdbmodule = {
    PyModuleDef_HEAD_INIT,
    "_vcfdb",   /* name of module */
    MODULE_DOC, /* module documentation, may be NULL */
    -1,    
    NULL, NULL, NULL, NULL, NULL 
};

#define INITERROR return NULL

PyObject * 
PyInit__vcfdb(void)

#else
#define INITERROR return

void
init_vcfdb(void)
#endif
{
#if PY_MAJOR_VERSION >= 3
    PyObject *module = PyModule_Create(&vcfdbmodule);
#else
    PyObject *module = Py_InitModule3("_vcfdb", NULL, MODULE_DOC);
#endif
    if (module == NULL) {
        INITERROR;
    }
    /* BerkeleyDatabase */
    BerkeleyDatabaseType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&BerkeleyDatabaseType) < 0) {
        INITERROR;
    }
    Py_INCREF(&BerkeleyDatabaseType);
    PyModule_AddObject(module, "BerkeleyDatabase", 
            (PyObject *) &BerkeleyDatabaseType);
    /* Column */
    ColumnType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&ColumnType) < 0) {
        INITERROR;
    }
    Py_INCREF(&ColumnType);
    PyModule_AddObject(module, "Column", (PyObject *) &ColumnType);
    
    /* WriteBuffer */
    WriteBufferType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&WriteBufferType) < 0) {
        INITERROR;
    }
    Py_INCREF(&WriteBufferType);
    PyModule_AddObject(module, "WriteBuffer", (PyObject *) &WriteBufferType);
    
    BerkeleyDatabaseError = PyErr_NewException("_vcfdb.BerkeleyDatabaseError", 
            NULL, NULL);
    Py_INCREF(BerkeleyDatabaseError);
    PyModule_AddObject(module, "BerkeleyDatabaseError", BerkeleyDatabaseError);
    
    PyModule_AddIntConstant(module, "NUM_ELEMENTS_VARIABLE", 
            NUM_ELEMENTS_VARIABLE);
    PyModule_AddIntConstant(module, "NUM_ELEMENTS_VARIABLE_OVERHEAD", 3); /* FIXME */
    PyModule_AddIntConstant(module, "ELEMENT_TYPE_CHAR", ELEMENT_TYPE_CHAR);
    PyModule_AddIntConstant(module, "ELEMENT_TYPE_INT", ELEMENT_TYPE_INT);
    PyModule_AddIntConstant(module, "ELEMENT_TYPE_FLOAT", ELEMENT_TYPE_FLOAT);
    PyModule_AddIntConstant(module, "ELEMENT_TYPE_ENUM", ELEMENT_TYPE_ENUM);
    
    PyModule_AddIntConstant(module, "MAX_ROW_SIZE", MAX_ROW_SIZE);
    PyModule_AddIntConstant(module, "MAX_NUM_ELEMENTS", MAX_NUM_ELEMENTS);

#if PY_MAJOR_VERSION >= 3
    return module;
#endif
}


