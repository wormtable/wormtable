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
    int record_length;
    int record_number;
    DB *primary_db;
    DB **secondary_dbs;
    unsigned int num_secondary_dbs;
} Table;

static void
Table_dealloc(Table* self)
{
    Py_XDECREF(self->record_buffer);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject *
Table_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    Table *self;

    self = (Table *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->record_buffer = PyUnicode_FromString("");
        if (self->record_buffer == NULL) {
            Py_DECREF(self);
            return NULL;
        }
        self->record_length = 0;
        self->record_number = 0;
        
        self->primary_db = NULL;
        self->secondary_dbs = NULL;
        self->num_secondary_dbs = 0;
    }
    return (PyObject *)self;
}

static int
Table_init(Table *self, PyObject *args, PyObject *kwds)
{
    printf("Table contstructor\n");
    return 0;
}


static PyMemberDef Table_members[] = {
    {"record_buffer", T_OBJECT_EX, offsetof(Table, record_buffer), 0, "first name"},
    {"record_length", T_INT, offsetof(Table, record_length), 0, "record length"},
    {"record_number", T_INT, offsetof(Table, record_number), 0, "record number"},
    {NULL}  /* Sentinel */
};


static PyObject *
Table_open(Table* self)
{
    int ret;
    u_int32_t flags;
    ret = db_create(&self->primary_db, NULL, 0);
    if (ret != 0) {
        printf("Error creading DB handle\n");
        /* Error handling goes here */
    }
    flags = DB_CREATE;
    
    ret = self->primary_db->open(self->primary_db,        /* DB structure pointer */
            NULL,       /* Transaction pointer */
            "tmp/primary.db", /* On-disk file that holds the database. */
            NULL,       /* Optional logical database name */
            DB_RECNO,   /* Database access method */
            flags,      /* Open flags */
            0);         /* File mode (using defaults) */
    if (ret != 0) {
        printf("Error opening DB"); 
    }
    printf("Table open\n");
    
    return Py_BuildValue("");
}

static PyObject *
Table_close(Table* self)
{
    int ret;
    if (self->primary_db != NULL) {
        ret = self->primary_db->close(self->primary_db, 0); 
        if (ret != 0) {
            printf("error closing table\n");
        }
    }
    printf("Table close\n");
    return Py_BuildValue("");
}

static PyObject *
Table_store_record(Table* self)
{
    int ret;
    Py_buffer buff;
    
    u_int32_t flags = DB_APPEND;
    u_int32_t len = self->record_length;
    //db_recno_t recno = self->record_number;
    DBT key, data;
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
    if (!PyByteArray_Check(self->record_buffer)) {
        printf("Not a byte array!\n");
        goto out;
    }
    if (PyObject_GetBuffer(self->record_buffer, &buff, PyBUF_SIMPLE) != 0) {
        printf("Cannot get buffer!!\n");
        goto out;
    }
    /*
    printf("C:");
    for (ret = 0; ret <= len; ret++) {
        printf("%d", ((unsigned char *) buff.buf)[ret]);
        
    }
    */
    //printf("\n");
    //fflush(stdout);

    data.size = len;
    data.data = buff.buf;

    ret = self->primary_db->put(self->primary_db, NULL, &key, &data, flags);
    if (ret != 0) {
        printf("ERROR!! %d\n", ret);
    }
    
    //recno = (db_recno_t) key.data;
    //printf("store record %d with len = %d\n", recno, len);
out:

    return Py_BuildValue("");
}



static PyMethodDef Table_methods[] = {
    {"open", (PyCFunction) Table_open, METH_NOARGS, "Open the table" },
    {"store_record", (PyCFunction) Table_store_record, METH_NOARGS, "store a record" },
    {"close", (PyCFunction) Table_close, METH_NOARGS, "Close the table" },
    {NULL}  /* Sentinel */
};


static PyTypeObject TableType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_vcfdb.Table",             /* tp_name */
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
    Py_TPFLAGS_DEFAULT |
        Py_TPFLAGS_BASETYPE,   /* tp_flags */
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
    0,                         /* tp_alloc */
    Table_new,                 /* tp_new */
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
    if (PyType_Ready(&TableType) < 0) {
        INITERROR;
    }
    Py_INCREF(&TableType);
    PyModule_AddObject(module, "Table", (PyObject *) &TableType);


#if PY_MAJOR_VERSION >= 3
    return module;
#endif
}


