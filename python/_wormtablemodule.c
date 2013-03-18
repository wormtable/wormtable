#include <Python.h>
#include <structmember.h>

#include "wormtable.h"

#if PY_MAJOR_VERSION >= 3
#define IS_PY3K
#endif

#define MODULE_DOC \
"Low level interface for wormtable"

static PyObject *WormtableError;

/* forward declaration */
static PyTypeObject TableType;

typedef struct {
    PyObject_HEAD
    wt_table_t *table;
} Table;

typedef struct {
    PyObject_HEAD
    wt_column_t *column;
} Column;

typedef struct {
    PyObject_HEAD
    wt_row_t *row;
    u_int32_t conversion_buffer_size;
    void *conversion_buffer;
} Row;

static void 
handle_wt_error(int err)
{
    PyErr_SetString(WormtableError, wt_strerror(err));
}

/*==========================================================
 * Python to native conversion. 
 *==========================================================
 */


static int 
python_to_native_uint(PyObject *element, void *output_buffer, 
        u_int32_t output_buffer_size)
{
    int ret = -1;
    u_int64_t native = 0;
    if (!PyNumber_Check(element)) {
        PyErr_SetString(PyExc_TypeError, "Must be numeric");
        goto out;
    }
    native = (u_int64_t) PyLong_AsUnsignedLongLong(element);
    if (native == (u_int64_t) -1) {
        /* PyLong_AsUnsignedLongLong return -1 and raises OverFlowError if 
         * the value cannot be represented as an unsigned long long 
         */
        if (PyErr_Occurred()) {
            goto out;
        }
    } 
    memcpy(output_buffer, &native, sizeof(native));
    ret = 0;
out:
    return ret;
}

static int 
python_to_native_int(PyObject *element, void *output_buffer, 
        u_int32_t output_buffer_size)
{
    int ret = -1;
    int64_t native = 0;
    if (!PyNumber_Check(element)) {
        PyErr_SetString(PyExc_TypeError, "Must be numeric");
        goto out;
    }
    native = (int64_t) PyLong_AsLongLong(element);
    if (native == -1) {
        /* PyLong_AsLongLong return -1 and raises OverFlowError if 
         * the value cannot be represented as a long long 
         */
        if (PyErr_Occurred()) {
            goto out;
        }
    }
    memcpy(output_buffer, &native, sizeof(native));
    ret = 0;
out:
    return ret;
}


/* 
 * Converts a single element to the native type.
 */
static int 
python_to_native_element(wt_column_t *column, PyObject *element, void *output_buffer,
        u_int32_t output_buffer_size)
{
    int ret = 0;
    if (column->element_type == WT_UINT) {
        ret = python_to_native_uint(element, output_buffer, output_buffer_size);
    } else if (column->element_type == WT_INT) {
        ret = python_to_native_int(element, output_buffer, output_buffer_size);
    } else {
        printf("error!");
    }
    return ret;
}

/* 
 * Converts the elements in the specified python object into native values
 * stored in the specified output_buffer, and return the number of elements 
 * converted.
 */
static int 
python_to_native(wt_column_t *column, PyObject *elements, void *output_buffer,
        u_int32_t output_buffer_size)
{
    int ret = 0;
    if (column->num_elements == 1) {
        /* this error and return protocol is shite - need to fix it! */
        ret = python_to_native_element(column, elements, output_buffer, 
                output_buffer_size);
        if (ret != 0) {
            ret = 0;
            goto out;
        }
        ret = 1;
    }

out:
    return ret;
}

/*==========================================================
 * Column object 
 *==========================================================
 */

static void
Column_dealloc(Column* self)
{
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static int
Column_init(Column *self, PyObject *args, PyObject *kwds)
{
    int ret = -1;
    self->column = NULL;
    ret = 0;
    return ret;
}

static PyObject * 
Column_get_name(Column *self)
{
    PyObject *ret = NULL;
    const char *name;
    int wt_ret;
    if (self->column != NULL) {
        wt_ret = self->column->get_name(self->column, &name);     
        if (wt_ret != 0) {
            handle_wt_error(wt_ret); 
            goto out;
        }
        ret = Py_BuildValue("s", name);
    } else {
        PyErr_SetString(WormtableError, "Null Column");
    }
out:
    return ret;
}

static PyObject * 
Column_get_description(Column *self)
{
    PyObject *ret = NULL;
    const char *description;
    int wt_ret;
    if (self->column != NULL) {
        wt_ret = self->column->get_description(self->column, &description);     
        if (wt_ret != 0) {
            handle_wt_error(wt_ret); 
            goto out;
        }
        ret = Py_BuildValue("s", description);
    } else {
        PyErr_SetString(WormtableError, "Null Column");
    }
out:
    return ret;
}

static PyObject * 
Column_get_element_type(Column *self)
{
    PyObject *ret = NULL;
    u_int32_t element_type;
    int wt_ret;
    if (self->column != NULL) {
        wt_ret = self->column->get_element_type(self->column, &element_type);     
        if (wt_ret != 0) {
            handle_wt_error(wt_ret); 
            goto out;
        }
        ret = Py_BuildValue("i", (int) element_type);
    } else {
        PyErr_SetString(WormtableError, "Null Column");
    }
out:
    return ret;
}

static PyObject * 
Column_get_element_size(Column *self)
{
    PyObject *ret = NULL;
    u_int32_t element_size;
    int wt_ret;
    if (self->column != NULL) {
        wt_ret = self->column->get_element_size(self->column, &element_size);     
        if (wt_ret != 0) {
            handle_wt_error(wt_ret); 
            goto out;
        }
        ret = Py_BuildValue("i", (int) element_size);
    } else {
        PyErr_SetString(WormtableError, "Null Column");
    }
out:
    return ret;
}
    
static PyObject * 
Column_get_num_elements(Column *self)
{
    PyObject *ret = NULL;
    u_int32_t num_elements;
    int wt_ret;
    if (self->column != NULL) {
        wt_ret = self->column->get_num_elements(self->column, &num_elements);     
        if (wt_ret != 0) {
            handle_wt_error(wt_ret); 
            goto out;
        }
        ret = Py_BuildValue("i", (int) num_elements);
    } else {
        PyErr_SetString(WormtableError, "Null Column");
    }
out:
    return ret;
}


static PyMethodDef Column_methods[] = {
    {"get_name", (PyCFunction) Column_get_name, METH_NOARGS, "Return the name." },
    {"get_description", (PyCFunction) Column_get_description, METH_NOARGS, "Return the description." },
    {"get_element_type", (PyCFunction) Column_get_element_type, METH_NOARGS, "Return the element_type." },
    {"get_element_size", (PyCFunction) Column_get_element_size, METH_NOARGS, "Return the element_size." },
    {"get_num_elements", (PyCFunction) Column_get_num_elements, METH_NOARGS, "Return the num_elements." },
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
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
    "Column objects",           /* tp_doc */
    0,                     /* tp_traverse */
    0,                     /* tp_clear */
    0,                     /* tp_richcompare */
    0,                     /* tp_weaklistoffset */
    0,                     /* tp_iter */
    0,                     /* tp_iternext */
    Column_methods,             /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)Column_init,      /* tp_init */
};
   

/*==========================================================
 * Row object 
 *==========================================================
 */

static void
Row_dealloc(Row* self)
{
    if (self->row != NULL) {
        self->row->free(self->row);
    }
    if (self->conversion_buffer != NULL) {
        PyMem_Free(self->conversion_buffer);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static int
Row_init(Row *self, PyObject *args, PyObject *kwds)
{
    int ret = -1;
    int wt_ret = 0;
    wt_table_t *table;
    PyObject *py_table;
    static char *kwlist[] = {"table", NULL}; 
    self->conversion_buffer = NULL;
    self->row = NULL;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!", kwlist, 
            &TableType, &py_table)) {
        goto out;
    }
    table = ((Table *) py_table)->table;
    wt_ret = wt_row_alloc(&self->row, table, WT_MAX_ROW_SIZE);
    if (wt_ret != 0) {
        handle_wt_error(wt_ret);
        self->row = NULL;
        goto out;
    }
    self->conversion_buffer = PyMem_Malloc(WT_MAX_ROW_SIZE);
    self->conversion_buffer_size = WT_MAX_ROW_SIZE;
    ret = 0;
out:
    return ret;
}

static PyObject *
Row_set_value(Row *self, PyObject *args)
{
    PyObject *ret = NULL;
    PyObject *column;
    PyObject *elements;
    wt_column_t *wt_col;
    u_int32_t num_elements;
    int wt_ret;
    if (!PyArg_ParseTuple(args, "O!O", &ColumnType, &column, &elements)) {
        goto out;
    }
    if (self->row == NULL) {
        PyErr_SetString(WormtableError, "Null row");
        goto out;
    }
    wt_col = ((Column *) column)->column;
    num_elements = python_to_native(wt_col, elements, self->conversion_buffer,
            self->conversion_buffer_size);
    if (num_elements == 0) {
        goto out;
    }
    wt_ret = self->row->set_value(self->row, wt_col, self->conversion_buffer,
            num_elements);
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
Row_clear(Row *self)
{
    PyObject *ret = NULL;
    int wt_ret;
    if (self->row == NULL) {
        PyErr_SetString(WormtableError, "Null Column");
        goto out;
    }
    wt_ret = self->row->clear(self->row);     
    if (wt_ret != 0) {
        handle_wt_error(wt_ret); 
        goto out;
    }
    Py_INCREF(Py_None);
    ret = Py_None;
out:
    return ret;
}



static PyMethodDef Row_methods[] = {
    
    {"clear", (PyCFunction) Row_clear, METH_NOARGS, "Clears the row." },
    {"set_value", (PyCFunction) Row_set_value, METH_VARARGS, "Sets values in the row." },
    {NULL}  /* Sentinel */
};


static PyTypeObject RowType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_wormtable.Row",             /* tp_name */
    sizeof(Row),             /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)Row_dealloc, /* tp_dealloc */
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
    "Row objects",           /* tp_doc */
    0,                     /* tp_traverse */
    0,                     /* tp_clear */
    0,                     /* tp_richcompare */
    0,                     /* tp_weaklistoffset */
    0,                     /* tp_iter */
    0,                     /* tp_iternext */
    Row_methods,             /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)Row_init,      /* tp_init */
};






/*==========================================================
 * Table object 
 *==========================================================
 */

static void
Table_dealloc(Table* self)
{
    if (self->table != NULL) {
        self->table->free(self->table);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static int
Table_init(Table *self, PyObject *args, PyObject *kwds)
{
    int ret = -1;
    int wt_ret;
    self->table = NULL;
    wt_ret = wt_table_alloc(&self->table);
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

static PyObject *
Table_get_num_rows(Table* self)
{
    PyObject *ret = NULL;
    int wt_ret;
    u_int64_t num_rows;
    if (self->table == NULL) {
        PyErr_SetString(WormtableError, "Null table");
        goto out;
    }
    wt_ret = self->table->get_num_rows(self->table, &num_rows);
    if (wt_ret != 0) {
        handle_wt_error(wt_ret);
        goto out;    
    }
    ret = Py_BuildValue("K", (unsigned long long) num_rows);
out:
    return ret; 
}

static PyObject *
Table_get_num_columns(Table* self)
{
    PyObject *ret = NULL;
    int wt_ret;
    u_int32_t num_columns;
    if (self->table == NULL) {
        PyErr_SetString(WormtableError, "Null table");
        goto out;
    }
    wt_ret = self->table->get_num_columns(self->table, &num_columns);
    if (wt_ret != 0) {
        handle_wt_error(wt_ret);
        goto out;    
    }
    ret = Py_BuildValue("i", (int) num_columns);
out:
    return ret; 
}




static PyObject *
Table_get_column_by_index(Table* self, PyObject *args)
{
    int wt_ret;
    PyObject *ret = NULL;
    unsigned int index; 
    wt_column_t *wt_col;
    Column *col = NULL;
    if (!PyArg_ParseTuple(args, "I", &index)) {
        goto out;
    }
    if (self->table == NULL) {
        PyErr_SetString(WormtableError, "table closed");
        goto out;
    }
    wt_ret = self->table->get_column_by_index(self->table, index, &wt_col);
    if (wt_ret != 0) {
        handle_wt_error(wt_ret);
        goto out;    
    }
    col = PyObject_New(Column, &ColumnType);
    col->column = wt_col;
    ret = (PyObject *) col;
out:
    return ret; 
}



static PyObject *
Table_add_column(Table* self, PyObject *args)
{
    int wt_ret;
    PyObject *ret = NULL;
    unsigned int element_type, element_size, num_elements; 
    const char *name, *description;
    wt_column_t *col = NULL;
    if (!PyArg_ParseTuple(args, "ssIII", &name, &description, &element_type,
            &element_size, &num_elements)) {
        goto out;
    }
    if (self->table == NULL) {
        PyErr_SetString(WormtableError, "table closed");
        goto out;
    }
    wt_ret = wt_column_alloc(&col,  name, description, 
            (u_int32_t) element_type, (u_int32_t) element_size, 
            (u_int32_t) num_elements);
    if (wt_ret != 0) {
        handle_wt_error(wt_ret);
        goto out;
    }
    wt_ret = self->table->add_column(self->table, col);
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
Table_add_row(Table* self, PyObject *args)
{
    int wt_ret;
    PyObject *ret = NULL;
    Row *row = NULL;
    if (!PyArg_ParseTuple(args, "O!", &RowType, &row)) {
        goto out;
    }
    if (self->table == NULL) {
        PyErr_SetString(WormtableError, "table closed");
        goto out;
    }
    if (row->row == NULL) {
        PyErr_SetString(WormtableError, "Bad row");
        goto out;
    }
    wt_ret = self->table->add_row(self->table, row->row);
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
    {"get_num_rows", (PyCFunction) Table_get_num_rows, METH_NOARGS, 
            "Get the number of rows" },
    {"get_num_columns", (PyCFunction) Table_get_num_columns, METH_NOARGS, 
            "Get the number of columns" },
    {"get_column_by_index", (PyCFunction) Table_get_column_by_index, 
            METH_VARARGS, "Gets a column by its index.." },
    {"add_column", (PyCFunction) Table_add_column, METH_VARARGS, 
            "Add a column." },
    {"add_row", (PyCFunction) Table_add_row, METH_VARARGS, 
            "Add a row." },
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

#ifdef IS_PY3K

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
#ifdef IS_PY3K 
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
    /* Column */
    ColumnType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&ColumnType) < 0) {
        INITERROR;
    }
    Py_INCREF(&ColumnType);
    PyModule_AddObject(module, "Column", (PyObject *) &ColumnType);
    /* Row */
    RowType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&RowType) < 0) {
        INITERROR;
    }
    Py_INCREF(&RowType);
    PyModule_AddObject(module, "Row", (PyObject *) &RowType);
        
    
    WormtableError = PyErr_NewException("_wormtable.WormtableError", NULL, 
            NULL);
    Py_INCREF(WormtableError);
    PyModule_AddObject(module, "WormtableError", WormtableError);
    
    PyModule_AddIntConstant(module, "WT_VARIABLE", WT_VARIABLE);
    PyModule_AddIntConstant(module, "WT_WRITE", WT_WRITE);
    PyModule_AddIntConstant(module, "WT_READ", WT_READ);
    PyModule_AddIntConstant(module, "WT_UINT", WT_UINT);
    PyModule_AddIntConstant(module, "WT_INT", WT_INT);
    PyModule_AddIntConstant(module, "WT_FLOAT", WT_FLOAT);
    PyModule_AddIntConstant(module, "WT_CHAR", WT_CHAR);


#ifdef IS_PY3K 
    return module;
#endif
}


