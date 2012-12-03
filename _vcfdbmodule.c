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

#define NUM_ELEMENTS_ANY 0 
#define MAX_ELEMENTS 256
#define MAX_RECORD_SIZE 65536

#define MODULE_DOC \
"Low level Berkeley DB interface for vcfdb"

static PyObject *BerkeleyDatabaseError;


typedef struct Column_t {
    PyObject_HEAD
    PyObject *name;
    PyObject *description;
    PyObject *enum_values;
    int offset;
    int element_type;
    int element_size;
    int num_elements;
    /* possibly better names for this... */
    int (*convert_string)(struct Column_t*, char *, void *);
} Column;


typedef struct {
    PyObject_HEAD
    DB *primary_db;
    PyObject *filename;
    Py_ssize_t cache_size;
} BerkeleyDatabase;

typedef struct {
    PyObject_HEAD
    BerkeleyDatabase *database;
    /* Record storage */
    u_int32_t max_record_size;
    unsigned char *record_buffer;
    u_int32_t record_buffer_size;
    int current_record_size;
    u_int32_t current_record_offset;
    /* Key storage */
    unsigned char *key_buffer;
    u_int32_t key_size;
    /* DBT management */
    u_int32_t num_records;
    u_int32_t max_num_records;
    DBT *keys; 
    DBT *records; 
} WriteBuffer;

static void 
handle_bdb_error(int err)
{
    PyErr_SetString(BerkeleyDatabaseError, db_strerror(err));
}

/* 
 * Copies n bytes of source into destination, swapping the order of the 
 * bytes if necessary.
 *
 * TODO We need to put in #defs to detect endianness and adapt accordingly.
 * Currently this is little-endian only.
 */
static void
bigendian_copy(void* dest, void *source, size_t n)
{
    size_t j = 0;
    unsigned char *dest_c = (unsigned char *) dest;
    unsigned char *source_c = (unsigned char *) source;
    for (j = 0; j < n; j++) {
        dest_c[j] = source_c[n - j - 1];
    }
    /*
    for (j = 0; j < n; j++) {
        printf("%03d ", source_c[j]); 
    }
    printf("\t -> \t"); 
    for (j = 0; j < n; j++) {
        printf("%03d ", dest_c[j]); 
    }
    printf("\n");
    */
}




/*==========================================================
 * Column object 
 *==========================================================
 */


static int 
Column_convert_string_int(Column *self, char *string, void *destination)
{
    int ret = -1;
    char *v = string;
    char *tail;
    long long element;
    int num_elements = 1;
    int not_done = 1; 
    void *dest = destination;
    /* TODO check this - probably not totally right */
    long long max_element_value = (1l << (8 * self->element_size - 1)) - 1;
    while (not_done) {
        if (*v == ',') {
            v++;
            num_elements++;
            if (num_elements >= MAX_ELEMENTS) {
                PyErr_SetString(PyExc_ValueError, "Too many elements");
                goto out;
            }
        }
        if (*v == 0) {
            not_done = 0; 
        } else {
            errno = 0;
            element = strtoll(v, &tail, 0);
            if (errno) {
                PyErr_SetString(PyExc_ValueError, "Element overflow");
                goto out;
            }
            if (abs(element) >= max_element_value) {
                PyErr_SetString(PyExc_ValueError, "Value too large");
                goto out;
            }
            
            bigendian_copy(dest, &element, self->element_size);
            dest += self->element_size;
            if (v == tail) {
                PyErr_SetString(PyExc_ValueError, "Element parse error");
                goto out;
            }
            v = tail;
        }
    }
    if (self->num_elements != NUM_ELEMENTS_ANY) {
        if (num_elements != self->num_elements) {
            PyErr_SetString(PyExc_ValueError, "incorrect number of elements");
            goto out;
        }
    }
        
    ret = 0;
out:
    
    return ret;
}

static int 
Column_convert_string_float(Column *self, char *string, void *destination)
{
    int ret = -1;
    char *v = string;
    char *tail;
    float element;
    int num_elements = 1;
    int not_done = 1; 
    void *dest = destination;
    while (not_done) {
        if (*v == ',') {
            v++;
            num_elements++;
            if (num_elements >= MAX_ELEMENTS) {
                PyErr_SetString(PyExc_ValueError, "Too many elements");
                goto out;
            }
        }
        if (*v == 0) {
            not_done = 0; 
        } else {
            errno = 0;
            element = (float) strtod(v, &tail);
            if (errno) {
                PyErr_SetString(PyExc_ValueError, "Element overflow");
                goto out;
            }
            bigendian_copy(dest, &element, self->element_size);
            dest += self->element_size;
            if (v == tail) {
                PyErr_SetString(PyExc_ValueError, "Element parse error");
                goto out;
            }
            v = tail;
        }
    }
    if (self->num_elements != NUM_ELEMENTS_ANY) {
        if (num_elements != self->num_elements) {
            PyErr_SetString(PyExc_ValueError, "incorrect number of elements");
            goto out;
        }
    }
        
    ret = 0;
out:
    
    return ret;
}
 
static int 
Column_convert_string_char(Column *self, char *string, void *destination)
{
    //printf("Convert char %d:%s\n", self->num_elements, string);    
    size_t n = strlen(string);
    memcpy(destination, string, n); 
    return 0;
}

static int 
Column_convert_string_enum(Column *self, char *string, void *destination)
{
    //printf("Convert enum %d:%s\n", self->num_elements, string);    
    return 0;
}
 

static void
Column_dealloc(Column* self)
{
    Py_XDECREF(self->name); 
    Py_XDECREF(self->description); 
    //Py_XDECREF(self->enum_values); 
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static int
Column_init(Column *self, PyObject *args, PyObject *kwds)
{
    int ret = -1;
    static char *kwlist[] = {"name", "description",  "element_type", 
        "element_size", "num_elements", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OOiii", kwlist, 
            &self->name, &self->description, 
            &self->element_type, &self->element_size, 
            &self->num_elements)) {  
        goto out;
    }
    Py_INCREF(self->name);
    Py_INCREF(self->description);
    if (self->element_type == ELEMENT_TYPE_INT) {
        if (self->element_size < 1 || self->element_size > 8) {
            PyErr_SetString(PyExc_ValueError, "bad element size");
            goto out;
        }
        self->convert_string = Column_convert_string_int;
    } else if (self->element_type == ELEMENT_TYPE_FLOAT) {
        if (self->element_size != sizeof(float)) {
            PyErr_SetString(PyExc_ValueError, "bad element size");
            goto out;
        }
        self->convert_string = Column_convert_string_float;
    } else if (self->element_type == ELEMENT_TYPE_CHAR) {
        if (self->element_size != 1) {
            PyErr_SetString(PyExc_ValueError, "bad element size");
            goto out;
        }
        self->convert_string = Column_convert_string_char;
    } else if (self->element_type == ELEMENT_TYPE_ENUM) {
        if (self->element_size < 1 || self->element_size > 2) {
            PyErr_SetString(PyExc_ValueError, "bad element size");
            goto out;
        }
        self->convert_string = Column_convert_string_enum;
    } else {    
        PyErr_SetString(PyExc_ValueError, "Unknown element type");
        goto out;
    }
    
    /*
    self->enum_values = PyDict_New();
    if (self->enum_values == NULL) {
        goto out;
    }
    Py_INCREF(self->enum_values);
    */
    /*TODO Check the values for sanity*/

    ret = 0;
out:
    return ret;
}

static PyMemberDef Column_members[] = {
    {"name", T_OBJECT_EX, offsetof(Column, name), READONLY, "name"},
    {"description", T_OBJECT_EX, offsetof(Column, description), READONLY, "description"},
    {"enum_values", T_OBJECT_EX, offsetof(Column, enum_values), READONLY, "enum_values"},
    {"element_type", T_INT, offsetof(Column, element_type), READONLY, "element_type"},
    {"element_size", T_INT, offsetof(Column, element_size), READONLY, "element_size"},
    {"num_elements", T_INT, offsetof(Column, num_elements), READONLY, "num_elements"},
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
    /* make sure that the DB handles are closed. We can ignore errors here. */ 
    if (self->primary_db != NULL) {
        self->primary_db->close(self->primary_db, 0); 
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static int
BerkeleyDatabase_init(BerkeleyDatabase *self, PyObject *args, PyObject *kwds)
{
    DB *db;
    int ret = -1;
    int db_ret = 0;
    u_int32_t gigs, bytes;
    Py_ssize_t gigabyte = 1024 * 1024 * 1024;
    static char *kwlist[] = {"filename", "cache_size", NULL}; 
    self->primary_db = NULL;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "On", kwlist, 
            &self->filename, &self->cache_size)) {
        goto out;
    }
    Py_INCREF(self->filename);
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
    ret = 0;
out:

    return ret;
}


static PyMemberDef BerkeleyDatabase_members[] = {
    {"filename", T_OBJECT_EX, offsetof(BerkeleyDatabase, filename), READONLY, "filename"},
    {"cache_size", T_PYSSIZET, offsetof(BerkeleyDatabase, cache_size), READONLY, "cache_size"},
    {NULL}  /* Sentinel */
};



static PyObject *
BerkeleyDatabase_create(BerkeleyDatabase* self)
{
    PyObject *ret = NULL;
    int db_ret;
    u_int32_t flags = DB_CREATE|DB_TRUNCATE;
    char *db_name = NULL;
    if (!PyBytes_Check(self->filename)) {
        PyErr_SetString(PyExc_ValueError, "filename must be bytes");
        goto out;
    }
    db_name = PyBytes_AsString(self->filename);
    db_ret = self->primary_db->open(self->primary_db, NULL, db_name, NULL, 
            DB_BTREE,  flags,  0);         
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;    
    }

    ret = Py_BuildValue("");
out:
    return ret; 
}

static PyObject *
BerkeleyDatabase_close(BerkeleyDatabase* self)
{
    PyObject *ret = NULL;
    int db_ret;
    if (self->primary_db != NULL) {
        db_ret = self->primary_db->close(self->primary_db, 0); 
        if (db_ret != 0) {
            handle_bdb_error(db_ret);
            goto out;    
        }
    }
    ret = Py_BuildValue("");
out:
    self->primary_db = NULL;
    return ret; 
}


static PyMethodDef BerkeleyDatabase_methods[] = {
    {"create", (PyCFunction) BerkeleyDatabase_create, METH_NOARGS, "Create the database" },
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
WriteBuffer_dealloc(WriteBuffer* self)
{
    /* Is this safe?? */
    PyMem_Free(self->record_buffer);
    PyMem_Free(self->key_buffer);
    PyMem_Free(self->keys);
    PyMem_Free(self->records);
    Py_XDECREF(self->database);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static int
WriteBuffer_init(WriteBuffer *self, PyObject *args, PyObject *kwds)
{
    int ret = 0; 
    static char *kwlist[] = {"database", NULL};
    BerkeleyDatabase *database = NULL;
    u_int32_t n; 
    if (! PyArg_ParseTupleAndKeywords(args, kwds, "O!", kwlist, 
            &BerkeleyDatabaseType, &database)) {
        ret = -1;
        goto out;
    }
    Py_INCREF(database);
    self->database = database;
       
    /* TODO read params from args and error check */
    n = 1024 * 1024;
    self->key_size = 8;
    self->max_num_records = n;
    self->max_record_size = 64 * 1024; /* TEMP 64K */
    self->record_buffer_size = 32 * 1024 * 1024; /* TEMP 32 MiB*/
    self->record_buffer = PyMem_Malloc(self->record_buffer_size);
    self->key_buffer = PyMem_Malloc(n * self->key_size);
    self->keys = PyMem_Malloc(n * sizeof(DBT));
    self->records = PyMem_Malloc(n * sizeof(DBT));
    memset(self->keys, 0, n * sizeof(DBT));
    memset(self->records, 0, n * sizeof(DBT));
    self->num_records = 0;

out:
    return ret;
}

static PyMemberDef WriteBuffer_members[] = {
    {"database", T_OBJECT_EX, offsetof(WriteBuffer, database), READONLY, "database"},
    {NULL}  /* Sentinel */
};

static PyObject *
WriteBuffer_flush(WriteBuffer* self)
{
    PyObject *ret = NULL;
    
#if 0
    int db_ret = 0;
    u_int32_t flags = 0;
    u_int32_t j;
    DB *db;
    
    db = self->database->primary_db;
    /* TODO error check or verify this can't be null */
    //printf("\nFlushing buffer: %d records in %dKiB\n", self->num_records, self->current_record_offset / 1024);
    for (j = 0; j < self->num_records; j++) {
        db_ret = db->put(db, NULL, &self->keys[j], &self->records[j], flags);
        if (db_ret != 0) {
            handle_bdb_error(db_ret); 
            goto out;
        } 
    }
    //printf("done\n");
    WriteBuffer_clear(self);
#endif

    ret = Py_BuildValue("");
//out:
    return ret; 
}

static PyObject *
WriteBuffer_set_record_value(WriteBuffer* self, PyObject *args)
{
    PyObject *ret = NULL;
    Column *column = NULL;
    PyBytesObject *value = NULL;
    Py_ssize_t v_len;
    char temp[1024];
    char *v;
    if (! PyArg_ParseTuple(args, "O!O!", &ColumnType, &column, &PyBytes_Type,
            &value)) {
        goto out;
    }
    Py_INCREF(column);
    Py_INCREF(value);
    if (PyBytes_AsStringAndSize((PyObject *) value, &v, &v_len) < 0){
        goto out;
    }
    
    
    if (column->convert_string(column, v, temp) < 0) {
        goto out;
    }

#if 0
    if (column->num_elements == 1) {
        num_elements = 1;
        elements[0] = v; 
    } else {
        char *string = v;
        num_elements = 0;
        while (1) {
            char *tail;
            double next;

            /* Skip whitespace by hand, to detect the end.  */
            if (ispunct(*string)) {
                string++;
                num_elements++;
            }
            if (*string == NULL) {
                break;
            }
            errno = 0;
            /* Parse it.  */
            next = strtod(string, &tail);
            /* Add it in, if not overflow.  */
            if (errno)
                printf ("Overflow\n");
            else
                printf("next = %f\n", next);
            if (string == tail) {
                printf("PARSE ERROR!!\n");
                break;
            }
            
            /* Advance past it.  */
            printf("string = %s, tail = %s\n", string, tail);
            string = tail;
        }

        
        /*
        printf("%s\n", v);
        for (j = 0; j < v_len; j++) {
            if (v[j] == ',') {
                printf("\tbreak at %d\n", j);
            }
        }
        */
            
    }
    /*
    for (j = 0; j < num_elements; j++) {
        printf("element %d: %s\n", j, elements[j]); 
    }
    */
#endif
    ret = Py_BuildValue("");
out:
    Py_XDECREF(column);
    Py_XDECREF(value);
    return ret; 
}



static PyObject *
WriteBuffer_commit_record(WriteBuffer* self, PyObject *args)
{
    PyObject *ret = NULL;
    printf("Commit\n");
#if 0
    Py_buffer key_buff;
    unsigned char *key = &self->key_buffer[self->key_size * self->num_records];
    unsigned char *record = &self->record_buffer[self->current_record_offset];
    u_int32_t record_size = (u_int32_t) self->current_record_size;
    u_int32_t barrier = self->record_buffer_size - self->max_record_size;
    if (!PyArg_ParseTuple(args, "y*", &key_buff)) { 
        goto out; 
    }
    if (key_buff.len != self->key_size) {
        PyErr_SetString(PyExc_ValueError, "bad key size");
        PyBuffer_Release(&key_buff);
        goto out;
    }
    /* copy the key */
    memcpy(key, key_buff.buf, self->key_size);
    PyBuffer_Release(&key_buff);
    if (record_size > self->max_record_size) {
        PyErr_SetString(PyExc_ValueError, "record size too large");
        goto out;
    }
    self->keys[self->num_records].size = self->key_size;
    self->keys[self->num_records].data = key;
    self->records[self->num_records].size = record_size; 
    self->records[self->num_records].data = record;
    /* We are done setting up the record, so we can increment counters */ 
    self->current_record_offset += record_size;
    self->num_records++;
    if (self->current_record_offset >= barrier 
            || self->num_records == self->max_num_records) {
        ret = WriteBuffer_flush(self);
        if (ret == NULL) {
            goto out;
        }
    }

#endif
    ret = Py_BuildValue("");
//out:
    return ret; 
}




static PyMethodDef WriteBuffer_methods[] = {
    {"set_record_value", (PyCFunction) WriteBuffer_set_record_value, METH_VARARGS, "set value" },
    {"commit_record", (PyCFunction) WriteBuffer_commit_record, METH_VARARGS, "commit record" },
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
    
    PyModule_AddIntConstant(module, "ELEMENT_TYPE_CHAR", ELEMENT_TYPE_CHAR);
    PyModule_AddIntConstant(module, "ELEMENT_TYPE_INT", ELEMENT_TYPE_INT);
    PyModule_AddIntConstant(module, "ELEMENT_TYPE_FLOAT", ELEMENT_TYPE_FLOAT);
    PyModule_AddIntConstant(module, "ELEMENT_TYPE_ENUM", ELEMENT_TYPE_ENUM);

#if PY_MAJOR_VERSION >= 3
    return module;
#endif
}


