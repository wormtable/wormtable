#include <Python.h>
#include <structmember.h>

#include <db.h>


#if PY_MAJOR_VERSION >= 3
#define IS_PY3K
#endif

#define MODULE_DOC \
"Low level Berkeley DB interface for vcfdb"

static PyObject *BerkeleyDatabaseError;

typedef struct {
    PyObject_HEAD
    PyObject *record_buffer;
    PyObject *key_buffer;
    int record_length;
    int key_length;
    DB *primary_db;
    DB **secondary_dbs;
    unsigned int num_secondary_dbs;
} BerkeleyDatabase;

typedef struct {
    PyObject_HEAD
    BerkeleyDatabase *bdb;
    unsigned char *buffer;
    size_t max_record_size;
    size_t buffer_size;
    int current_record_size;
    size_t current_record_offset;
    int num_views;
} RecordBuffer;

/*==========================================================
 * RecordBuffer object 
 *==========================================================
 */

static void
RecordBuffer_dealloc(RecordBuffer* self)
{
    /* Is this safe?? */
    PyMem_Free(self->buffer);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject *
RecordBuffer_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    RecordBuffer *self;

    self = (RecordBuffer *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->max_record_size = 64 * 1024; /* TEMP 64K */
        self->buffer_size = 1024 * 1024; /* TEMP 1MiB */
        self->buffer = (unsigned char *) PyMem_Malloc(self->buffer_size);
         
        /* TODO Handle memory error */
        self->num_views = 0; 
        self->current_record_size = 0;
        self->current_record_offset = 0;
        memset(self->buffer, 0, self->buffer_size);

    }
    return (PyObject *)self;
}

static int
RecordBuffer_init(RecordBuffer *self, PyObject *args, PyObject *kwds)
{
    return 0;
}

static PyMemberDef RecordBuffer_members[] = {
    {"current_record_size", T_INT, offsetof(RecordBuffer, current_record_size), 0, "size of current record"},
    {NULL}  /* Sentinel */
};

static PyObject *
RecordBuffer_commit_record(RecordBuffer* self, PyObject *args)
{
    PyObject *ret = NULL;
    Py_buffer key_buff;
    size_t j;
    size_t record_size = (size_t) self->current_record_size;
    size_t barrier = self->buffer_size - self->max_record_size;
    if (!PyArg_ParseTuple(args, "y*", &key_buff)) { 
        goto out; 
    }
    /* copy the key */
    for (j = 0; j < key_buff.len; j++) {
        printf("%d ", ((unsigned char *) key_buff.buf)[j]);
    }
    PyBuffer_Release(&key_buff);
    
    printf("\t commiting %lu\n", record_size);
    if (record_size > self->max_record_size) {
        PyErr_SetString(PyExc_ValueError, "record size too large");
        goto out;
    }
    self->current_record_offset += record_size;
    if (self->current_record_offset >= barrier) {
        printf("FLUSHING RECORDS!!\n");
        memset(self->buffer, 0, self->buffer_size);
        self->current_record_offset = 0;
    }

    ret = Py_BuildValue("");
out:
    return ret; 
}

/* Implementation of the Buffer Protocol so that we can write values 
 * directly into our buffer memory from Python. This implementation 
 * copies the bytearray (Objects/bytearrayobject.c), and doesn't
 * strictly need to have the self->num_views, as the memory is always
 * valid until the object is freed.
 */
static int
RecordBuffer_getbuffer(RecordBuffer *self, Py_buffer *view, int flags)
{
    int ret = 0;
    void *ptr;
    if (view == NULL) {
        self->num_views++;
        goto out; 
    }
    ptr = &self->buffer[self->current_record_offset];
    ret = PyBuffer_FillInfo(view, (PyObject*) self, ptr, self->max_record_size,
            0, flags);
    if (ret >= 0) {
        self->num_views++;
    }
out:
    return ret;
}
static void
RecordBuffer_releasebuffer(RecordBuffer *self, Py_buffer *view)
{
    self->num_views--;
}

static PyMethodDef RecordBuffer_methods[] = {
    {"commit_record", (PyCFunction) RecordBuffer_commit_record, METH_VARARGS, "commit record" },
    {NULL}  /* Sentinel */
};

static PyBufferProcs RecordBuffer_as_buffer = {
    (getbufferproc)RecordBuffer_getbuffer,
    (releasebufferproc)RecordBuffer_releasebuffer,
};


static PyTypeObject RecordBufferType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_vcfdb.RecordBuffer",             /* tp_name */
    sizeof(RecordBuffer),             /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)RecordBuffer_dealloc, /* tp_dealloc */
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
    &RecordBuffer_as_buffer,   /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT |
        Py_TPFLAGS_BASETYPE,   /* tp_flags */
    "RecordBuffer objects",           /* tp_doc */
    0,                     /* tp_traverse */
    0,                     /* tp_clear */
    0,                     /* tp_richcompare */
    0,                     /* tp_weaklistoffset */
    0,                     /* tp_iter */
    0,                     /* tp_iternext */
    RecordBuffer_methods,             /* tp_methods */
    RecordBuffer_members,             /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)RecordBuffer_init,      /* tp_init */
    0,                         /* tp_alloc */
    RecordBuffer_new,                 /* tp_new */
};
 
/*==========================================================
 * BerkeleyDatabase object 
 *==========================================================
 */

static void 
handle_bdb_error(int err)
{
    PyErr_SetString(BerkeleyDatabaseError, db_strerror(err));
}

static void
BerkeleyDatabase_dealloc(BerkeleyDatabase* self)
{
    Py_XDECREF(self->record_buffer);
    Py_XDECREF(self->key_buffer);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject *
BerkeleyDatabase_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    BerkeleyDatabase *self;

    self = (BerkeleyDatabase *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->record_buffer = PyUnicode_FromString("");
        if (self->record_buffer == NULL) {
            Py_DECREF(self);
            return NULL;
        }
        self->key_buffer = PyUnicode_FromString("");
        if (self->key_buffer == NULL) {
            Py_DECREF(self->record_buffer);
            Py_DECREF(self);
            return NULL;
        }
        self->record_length = 0;
        self->key_length = 0;
        
        self->primary_db = NULL;
        self->secondary_dbs = NULL;
        self->num_secondary_dbs = 0;
    }
    return (PyObject *)self;
}

static int
BerkeleyDatabase_init(BerkeleyDatabase *self, PyObject *args, PyObject *kwds)
{
    return 0;
}


static PyMemberDef BerkeleyDatabase_members[] = {
    {"key_buffer", T_OBJECT_EX, offsetof(BerkeleyDatabase, key_buffer), 0, "key"},
    {"key_length", T_INT, offsetof(BerkeleyDatabase, key_length), 0, "key length"},
    {"record_buffer", T_OBJECT_EX, offsetof(BerkeleyDatabase, record_buffer), 0, "record"},
    {"record_length", T_INT, offsetof(BerkeleyDatabase, record_length), 0, "record length"},
    {NULL}  /* Sentinel */
};


static PyObject *
BerkeleyDatabase_create(BerkeleyDatabase* self)
{
    PyObject *ret = NULL;
    int db_ret;
    u_int32_t flags;
    char *db_name = "tmp/primary.db";
    /* TODO This should be done in the constructor */
    db_ret = db_create(&self->primary_db, NULL, 0);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;    
    }
    db_ret = self->primary_db->set_cachesize(self->primary_db, 0, 
            32 * 1024 * 1024, 1);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;    
    }
    flags = DB_CREATE|DB_TRUNCATE;
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
BerkeleyDatabase_open(BerkeleyDatabase* self)
{
    int db_ret;
    PyObject *ret = NULL;
    u_int32_t flags = DB_RDONLY;
    char *db_name = "tmp/primary.db";
    /* TODO This should be done in the constructor */
    db_ret = db_create(&self->primary_db, NULL, 0);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;    
    }
    db_ret = self->primary_db->open(self->primary_db, NULL, db_name, NULL, 
            DB_UNKNOWN,  flags,  0);         
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
    return ret; 
}

static PyObject *
BerkeleyDatabase_store_record(BerkeleyDatabase* self)
{
    int db_ret;
    PyObject *ret = NULL;
    Py_buffer key_view, data_view;
    u_int32_t flags = 0;
    DBT key, data;
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
    
    if (!PyByteArray_Check(self->key_buffer)) {
        printf("Not a byte array!\n");
        goto out;
    }
    if (PyObject_GetBuffer(self->key_buffer, &key_view, PyBUF_SIMPLE) != 0) {
        printf("Cannot get buffer!!\n");
        goto out;
    }
    key.size = (u_int32_t) self->key_length;
    key.data = key_view.buf;
    /*
    printf("storing key:");
    for (ret = 0; ret < key.size; ret++) {
        printf("%d ", ((unsigned char *) key.data)[ret]);
    }
    printf("\n");
    */
    if (!PyByteArray_Check(self->record_buffer)) {
        printf("Not a byte array!\n");
        goto out;
    }
    if (PyObject_GetBuffer(self->record_buffer, &data_view, PyBUF_SIMPLE) != 0) {
        printf("Cannot get buffer!!\n");
        goto out;
    }
    data.size = (u_int32_t) self->record_length;
    data.data = data_view.buf;

    db_ret = self->primary_db->put(self->primary_db, NULL, &key, &data, flags);
    if (db_ret != 0) {
        handle_bdb_error(db_ret); 
    }
    
    /* what about error conditions?? */
    PyBuffer_Release(&key_view);
    PyBuffer_Release(&data_view);
    
    ret = Py_BuildValue("");
out:
    return ret; 
}



static PyMethodDef BerkeleyDatabase_methods[] = {
    {"open", (PyCFunction) BerkeleyDatabase_open, METH_NOARGS, "Open the table" },
    {"create", (PyCFunction) BerkeleyDatabase_create, METH_NOARGS, "Open the table" },
    {"store_record", (PyCFunction) BerkeleyDatabase_store_record, METH_NOARGS, "store a record" },
    {"close", (PyCFunction) BerkeleyDatabase_close, METH_NOARGS, "Close the table" },
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
    0,                         /* tp_alloc */
    BerkeleyDatabase_new,                 /* tp_new */
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
    if (PyType_Ready(&BerkeleyDatabaseType) < 0) {
        INITERROR;
    }
    Py_INCREF(&BerkeleyDatabaseType);
    PyModule_AddObject(module, "BerkeleyDatabase", 
            (PyObject *) &BerkeleyDatabaseType);
    if (PyType_Ready(&RecordBufferType) < 0) {
        INITERROR;
    }
    Py_INCREF(&RecordBufferType);
    PyModule_AddObject(module, "RecordBuffer", 
            (PyObject *) &RecordBufferType);
    
    BerkeleyDatabaseError = PyErr_NewException("_vcfdb.BerkeleyDatabaseError", 
            NULL, NULL);
    Py_INCREF(BerkeleyDatabaseError);
    PyModule_AddObject(module, "BerkeleyDatabaseError", BerkeleyDatabaseError);


#if PY_MAJOR_VERSION >= 3
    return module;
#endif
}


