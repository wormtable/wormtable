#include "Python.h"

#if PY_MAJOR_VERSION >= 3
#define IS_PY3K
#endif


#define MODULE_DOC \
"Low level Berkeley DB interface for vcfdb"

typedef struct {
    PyObject_HEAD
    /* Type-specific fields go here. */
} vcfdb_TableObject;

static PyTypeObject vcfdb_TableType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_vcfdb.Table",             /* tp_name */
    sizeof(vcfdb_TableObject), /* tp_basicsize */
    0,                         /* tp_itemsize */
    0,                         /* tp_dealloc */
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

    vcfdb_TableType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&vcfdb_TableType) < 0) {
        INITERROR;
    }
    Py_INCREF(&vcfdb_TableType);
    PyModule_AddObject(module, "Table", (PyObject *)&vcfdb_TableType);


#if PY_MAJOR_VERSION >= 3
    return module;
#endif
}


