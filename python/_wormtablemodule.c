#include <Python.h>
#include <structmember.h>

#include "wormtable.h"

#if PY_MAJOR_VERSION >= 3
#define IS_PY3K
#endif

#define MODULE_DOC \
"Low level interface for wormtable"

static PyObject *WormtableError;

typedef struct {
    PyObject_HEAD
    wt_table_t *table;
} Table;


static void 
handle_wt_error(int err)
{
    PyErr_SetString(WormtableError, wt_strerror(err));
}



/*==========================================================
 * Table object 
 *==========================================================
 */

static void
Table_dealloc(Table* self)
{
    if (self->table != NULL) {
        self->table->close(self->table);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static int
Table_init(Table *self, PyObject *args, PyObject *kwds)
{
    int ret = -1;
    int wt_ret;
    self->table = NULL;
    wt_ret = wt_table_create(&self->table);
    if (wt_ret != 0) {
        handle_wt_error(wt_ret); 
        goto out;
    }
    ret = 0;
out:
    return ret;
}

    
static PyObject *
Table_open(Table* self, PyObject *args)
{
    int wt_ret;
    PyObject *ret = NULL;
    unsigned int flags = 0;
    const char *homedir;
    if (!PyArg_ParseTuple(args, "sI", &homedir, &flags)) {
        goto out;
    }
    
    if (self->table == NULL) {
        PyErr_SetString(WormtableError, "table closed");
        goto out;
    }
    wt_ret = self->table->open(self->table, homedir, (u_int32_t) flags);
    if (wt_ret != 0) {
        handle_wt_error(wt_ret);
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
    int wt_ret;
    if (self->table == NULL) {
        PyErr_SetString(WormtableError, "table closed");
        goto out;
    }
    wt_ret = self->table->close(self->table);
    self->table = NULL;
    if (wt_ret != 0) {
        handle_wt_error(wt_ret);
        goto out;    
    }
    Py_INCREF(Py_None);
    ret = Py_None;
out:
    return ret; 
}

static PyMethodDef Table_methods[] = {
    {"open", (PyCFunction) Table_open, METH_VARARGS, "Opens the table." },
    {"close", (PyCFunction) Table_close, METH_NOARGS, "Close the table" },
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
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)Table_init,      /* tp_init */
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
    NULL, NULL, NULL, NULL, NULL 
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
#if PY_MAJOR_VERSION >= 3
    PyObject *module = PyModule_Create(&wormtablemodule);
#else
    PyObject *module = Py_InitModule3("_wormtable", NULL, MODULE_DOC);
#endif
    if (module == NULL) {
        INITERROR;
    }
    /* Table */
    TableType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&TableType) < 0) {
        INITERROR;
    }
    Py_INCREF(&TableType);
    PyModule_AddObject(module, "Table", (PyObject *) &TableType);
       
    WormtableError = PyErr_NewException("_wormtable.WormtableError", NULL, 
            NULL);
    Py_INCREF(WormtableError);
    PyModule_AddObject(module, "WormtableError", WormtableError);
    
    PyModule_AddIntConstant(module, "WT_VARIABLE", WT_VARIABLE);
    PyModule_AddIntConstant(module, "WT_WRITE", WT_WRITE);
    PyModule_AddIntConstant(module, "WT_READ", WT_READ);
#if PY_MAJOR_VERSION >= 3
    return module;
#endif
}


