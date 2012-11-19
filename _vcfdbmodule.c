#include <Python.h>
#include <structmember.h>

#include <db.h>


#if PY_MAJOR_VERSION >= 3
#define IS_PY3K
#endif


#define MODULE_DOC \
"Low level Berkeley DB interface for vcfdb"

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
BerkeleyDatabase_open(BerkeleyDatabase* self)
{
    int ret;
    u_int32_t flags;
    ret = db_create(&self->primary_db, NULL, 0);
    if (ret != 0) {
        printf("Error creading DB handle\n");
        /* Error handling goes here */
    }
    ret = self->primary_db->set_cachesize(self->primary_db, 0, 
            32 * 1024 * 1024, 1);
    if (ret != 0) {
        printf("Error setting cache"); 
    }
    
    
    flags = DB_CREATE|DB_TRUNCATE;
    
    ret = self->primary_db->open(self->primary_db,        /* DB structure pointer */
            NULL,       /* Transaction pointer */
            "tmp/primary.db", /* On-disk file that holds the database. */
            NULL,       /* Optional logical database name */
            DB_BTREE,   /* Database access method */
            flags,      /* Open flags */
            0);         /* File mode (using defaults) */
    if (ret != 0) {
        printf("Error opening DB"); 
    }
    


    return Py_BuildValue("");
}

static PyObject *
BerkeleyDatabase_close(BerkeleyDatabase* self)
{
    int ret;
    if (self->primary_db != NULL) {
        ret = self->primary_db->close(self->primary_db, 0); 
        if (ret != 0) {
            printf("error closing table\n");
        }
    }
    return Py_BuildValue("");
}

static PyObject *
BerkeleyDatabase_store_record(BerkeleyDatabase* self)
{
    int ret;
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
    /*
    printf("C:");
    for (ret = 0; ret <= len; ret++) {
        printf("%d", ((unsigned char *) view.buf)[ret]);
        
    }
    */
    //printf("\n");
    //fflush(stdout);
    data.size = (u_int32_t) self->record_length;
    data.data = data_view.buf;

    ret = self->primary_db->put(self->primary_db, NULL, &key, &data, flags);
    if (ret != 0) {
        printf("ERROR!! %d\n", ret);
    }
    
    /* what about error conditions?? */
    PyBuffer_Release(&key_view);
    PyBuffer_Release(&data_view);
    
    //printf("stored record %d with len = %d\n", recno, len);
out:

    return Py_BuildValue("");
}



static PyMethodDef BerkeleyDatabase_methods[] = {
    {"open", (PyCFunction) BerkeleyDatabase_open, METH_NOARGS, "Open the table" },
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
    PyModule_AddObject(module, "BerkeleyDatabase", (PyObject *) &BerkeleyDatabaseType);


#if PY_MAJOR_VERSION >= 3
    return module;
#endif
}


