#include <Python.h>
#include <structmember.h>

#include <db.h>


#if PY_MAJOR_VERSION >= 3
#define IS_PY3K
#endif

#define COLUMN_TYPE_FIXED 0
#define COLUMN_TYPE_VARIABLE 1 
    
#define ELEMENT_TYPE_CHAR  0
#define ELEMENT_TYPE_INT_1  1
#define ELEMENT_TYPE_INT_2  2
#define ELEMENT_TYPE_INT_4  3
#define ELEMENT_TYPE_INT_8  4
#define ELEMENT_TYPE_FLOAT  5
 
static int element_type_size_map[] = {1, 1, 2, 4, 8, 4};

#define MODULE_DOC \
"Low level Berkeley DB interface for vcfdb"

static PyObject *BerkeleyDatabaseError;


typedef struct Column_t {
    PyObject_HEAD
    PyObject *name;
    PyObject *description;
    int offset;
    int type; //  DEPRECATED
    int element_type;
    int element_size;
    int num_elements;
    int (*compare)(struct Column_t *, unsigned char *, unsigned char *);
} Column;

typedef struct {
    Column *columns;
    u_int32_t num_columns;
    DB *secondary_db;
    char *filename;
} db_index_t;


typedef struct {
    PyObject_HEAD
    DB *primary_db;
    DBC *cursor;
    db_index_t *indexes;
    u_int32_t num_indexes;
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
    /* Reading management */
    u_int32_t read_records;
    int reading_done;
    /* Used for Python buffer protocal compliance */
    int num_views; 
} RecordBuffer;

static void 
handle_bdb_error(int err)
{
    PyErr_SetString(BerkeleyDatabaseError, db_strerror(err));
}


/* 
 * Copies n bytes of source into destination, swapping the order of the 
 * bytes.
 */
static void
byte_swap(unsigned char *source, unsigned char *destination, size_t n)
{
    size_t j = 0;
    for (j = 0; j < n; j++) {
        printf("%03d ", source[j]); 
    }
    for (j = 0; j < n; j++) {
        destination[j] = source[n - j - 1];
    }
    printf("\t -> \t"); 
    for (j = 0; j < n; j++) {
        printf("%03d ", destination[j]); 
    }
    printf("\n");

}



static int
db_column_float_compare(Column *self, unsigned char *key1, 
        unsigned char *key2)
{
    int ret;
    float v1, v2;
    memcpy(&v1, key1, sizeof(float));
    memcpy(&v2, key2, sizeof(float));
    ret = 0;
    if (v1 < v2) {
        ret = -1;
    } else if (v2 < v1) {
        ret = 1;
    }
    //printf("compare float: %f %f = %d\n", v1, v2, ret);
    return ret; 
     
}

static int
db_column_memory_compare(Column *self, unsigned char *key1, 
        unsigned char *key2)
{
    int ret = memcmp(key1, key2, self->element_size);
    //printf("compare mem: %d bytes = %d\n", self->element_size, ret);
    
    return ret; 
     
}



static void
db_index_free(db_index_t *self)
{
    if (self->num_columns != 0) {
        PyMem_Free(self->columns);
    }
    if (self->secondary_db != NULL) {
        self->secondary_db->close(self->secondary_db, 0); 
    }
    if (self->filename != NULL) {
        PyMem_Free(self->filename);
    }
}

static int 
db_index_callback(DB* secondary, const DBT *key, const DBT *record, DBT *result)
{   
    unsigned int j;
    db_index_t *self = (db_index_t *) secondary->app_private;
    Column *col;
    u_int16_t size;
    unsigned char *source, *dest;
    /* copy the bytes into result for now */
    result->flags = DB_DBT_APPMALLOC;
    result->size = 0;
    for (j = 0; j < self->num_columns; j++) {
        col = &self->columns[j];
        if (col->type == COLUMN_TYPE_FIXED) {
            result->size += col->element_size;
                
        } else {
            printf("NOT SUPPORTED!!\n");
        }
    }
    dest = malloc(result->size); 
    source = (unsigned char *) record->data;
    result->data = dest;
    for (j = 0; j < self->num_columns; j++) {
        col = &self->columns[j];
        if (col->type == COLUMN_TYPE_FIXED) {
            size = col->element_size; 
            memcpy(dest, &source[col->offset], size);
            dest += size;
        } else {

        }
    }
    /*
    printf("callback for index %p: num columns = %d, type = %d element_type = %d\n", 
            self, self->num_columns, self->columns[0].type, self->columns[0].element_type);
    for (j = 0; j < key->size; j++) {
        printf("%d ", ((unsigned char *) key->data)[j]);
    }

    printf("key %d\trecord %d\t", key->size, record->size);
    for (j = 0; j < result->size; j++) {
        printf("%d ", ((unsigned char *) result->data)[j]);
    }
    printf(": size = %d\n", result->size);
    */
    return 0;
}
/*
 * Generic comparison function called from DB. 
 */
static int 
db_index_compare(DB *secondary, const DBT *dbt1, const DBT *dbt2)
{
    int ret = 0;
    int j = 0;
    Column *col;
    db_index_t *self = (db_index_t *) secondary->app_private;
    unsigned char *key1 = (unsigned char *) dbt1->data;
    unsigned char *key2 = (unsigned char *) dbt2->data;

    while (ret == 0 && j < self->num_columns) {
        col = &self->columns[j];
        ret = col->compare(col, key1, key2);
        key1 += col->element_size;
        key2 += col->element_size;
        j++;
    }
    //printf("compare: %p %p = %d\n", dbt1, dbt2, ret);
    return ret; 
        
}



static void 
db_index_initialise(db_index_t *self, PyObject *index_description)
{
    int db_ret;
    unsigned int j;
    PyObject *column_description, *number, *filename;
    self->num_columns = 0; 
    self->columns = NULL; 
    self->secondary_db = NULL;
    self->filename = NULL;
    if (!PyList_Check(index_description)) {
        PyErr_SetString(PyExc_ValueError, "must be a list");
        goto out;
    }
    self->num_columns = PyList_Size(index_description) - 1;
    filename = PyList_GetItem(index_description, 0);
    if (!PyBytes_Check(filename)) {
        PyErr_SetString(PyExc_ValueError, "must be bytes");
        goto out;
    }
    self->filename = PyMem_Malloc(PyBytes_Size(filename) + 1);
    strcpy(self->filename, PyBytes_AsString(filename));
    printf("initalising index %p: %s\n", self, self->filename);
    
    self->columns = PyMem_Malloc(self->num_columns * sizeof(Column));
    for (j = 0; j < self->num_columns; j++) {
        column_description = PyList_GetItem(index_description, j + 1);
        if (!PyList_Check(column_description)) {
            PyErr_SetString(PyExc_ValueError, "must be a list");
            goto out;
        }
        if (PyList_Size(column_description) != 3) {
            PyErr_SetString(PyExc_ValueError, "must be length 2");
            goto out;
        }
        number = PyList_GetItem(column_description, 0);
        if (!PyLong_Check(number)) {
            PyErr_SetString(PyExc_ValueError, "values must be numbers");
            goto out;
        }
        self->columns[j].type= (u_int32_t) PyLong_AsLong(number);
        number = PyList_GetItem(column_description, 1);
        if (!PyLong_Check(number)) {
            PyErr_SetString(PyExc_ValueError, "values must be numbers");
            goto out;
        }
        self->columns[j].element_type= (u_int32_t) PyLong_AsLong(number);
        number = PyList_GetItem(column_description, 2);
        if (!PyLong_Check(number)) {
            PyErr_SetString(PyExc_ValueError, "values must be numbers");
            goto out;
        }
        self->columns[j].offset = (u_int32_t) PyLong_AsLong(number);
        printf("\tallocated column type = %d, element_type = %d offset = %d\n", 
                self->columns[j].type, self->columns[j].element_type, 
                self->columns[j].offset);
        
        self->columns[j].compare = db_column_memory_compare;
        if (self->columns[j].element_type == ELEMENT_TYPE_FLOAT) {
            self->columns[j].compare = db_column_float_compare;
        }
        self->columns[j].element_size = element_type_size_map[
                self->columns[j].element_type];

    }
    db_ret = db_create(&self->secondary_db, NULL, 0);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;    
    }
    self->secondary_db->app_private = self;
    db_ret = self->secondary_db->set_flags(self->secondary_db, DB_DUPSORT);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;    
    }
    
    db_ret = self->secondary_db->set_bt_compare(self->secondary_db, db_index_compare);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;    
    }
    /* This may be useful - check it out */
    db_ret = self->secondary_db->set_bt_compress(self->secondary_db, NULL, NULL); 
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;    
    }
    
    

    db_ret = self->secondary_db->open(self->secondary_db, NULL, self->filename, 
            NULL, DB_BTREE, DB_CREATE|DB_TRUNCATE, 0);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;    
    }

out:
    return;
}

/*==========================================================
 * Column object 
 *==========================================================
 */
static void
Column_dealloc(Column* self)
{
    Py_XDECREF(self->name); 
    Py_XDECREF(self->description); 
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static int
Column_init(Column *self, PyObject *args, PyObject *kwds)
{
    int ret = -1;
    static char *kwlist[] = {"name", "description", "offset", 
        "element_type", "element_size", "num_elements", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OOiiii", kwlist, 
            &self->name, &self->description, 
            &self->offset, &self->element_type, &self->element_size, 
            &self->num_elements)) {  
        goto out;
    }
    Py_INCREF(self->name);
    Py_INCREF(self->description);
    /*TODO Check the values for sanity*/
    
    ret = 0;
out:
    return ret;
}

static PyMemberDef Column_members[] = {
    {"name", T_OBJECT_EX, offsetof(Column, name), READONLY, "name"},
    {"description", T_OBJECT_EX, offsetof(Column, description), READONLY, "description"},
    {"offset", T_INT, offsetof(Column, offset), READONLY, "offset"},
    {"element_type", T_INT, offsetof(Column, element_type), READONLY, "element_type"},
    {"element_size", T_INT, offsetof(Column, element_size), READONLY, "element_size"},
    {"num_elements", T_INT, offsetof(Column, num_elements), READONLY, "num_elements"},
    {NULL}  /* Sentinel */
};


static PyObject *
Column_set_record_value(Column* self, PyObject *args)
{
    PyObject *ret = NULL;
    PyObject *values = NULL;
    long v;
    float tmp;
    unsigned char buff[64];
    if (! PyArg_ParseTuple(args, "O", &values)) {
        goto out;
    }
    if (self->num_elements == 1) {
        if (!PyLong_Check(values)) {
            PyErr_SetString(PyExc_ValueError, "Long expected");
            goto out;
        }
        v = PyLong_AsLong(values);
        byte_swap((unsigned char *) &v, buff, self->element_size);
        byte_swap(buff, (unsigned char *) &v, self->element_size);
        printf("v = %ld\n", v); 
        
        for (tmp = 1.0; tmp < 10.0; tmp += 0.1) {
            byte_swap((unsigned char *) &tmp, buff, sizeof(float));
        }
    }
    
 
    ret = Py_BuildValue("");
out:
    return ret; 
}




static PyMethodDef Column_methods[] = {
    {"set_record_value", (PyCFunction) Column_set_record_value, METH_VARARGS, 
            "Sets the value of the column in a record" },
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
    int j;
    /* make sure that the DB handles are closed. We can ignore errors here. */ 
    for (j = 0; j < self->num_indexes; j++) {
        db_index_free(&self->indexes[j]);        
    }
    if (self->num_indexes != 0) { 
        PyMem_Free(self->indexes);
    }
    if (self->primary_db != NULL) {
        self->primary_db->close(self->primary_db, 0); 
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static int
BerkeleyDatabase_init(BerkeleyDatabase *self, PyObject *args, PyObject *kwds)
{
    int ret = 0;
    int db_ret = 0;
    self->primary_db = NULL;
    self->indexes = NULL;
    self->cursor = NULL;
    self->num_indexes = 0;
    db_ret = db_create(&self->primary_db, NULL, 0);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        ret = -1;
        goto out;    
    }
    /* 
     * Initial examination indicates that it's good for secondary indexes
     * but not for the main DB. This should be tested with the real 
     * key sizes and a proper dataset though.

    db_ret = self->primary_db->set_bt_compress(self->primary_db, NULL, NULL);
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        ret = -1;
        goto out;    
    }
     */
    

out:

    return ret;
}


static PyMemberDef BerkeleyDatabase_members[] = {
    {NULL}  /* Sentinel */
};


static PyObject *
BerkeleyDatabase_add_index(BerkeleyDatabase* self, PyObject *args)
{
    int db_ret;
    unsigned int j;
    PyObject *ret = NULL;
    PyObject *index_list = NULL;
    if (! PyArg_ParseTuple(args, "O!", &PyList_Type, &index_list)) {
        goto out;
    }
    self->num_indexes = PyList_Size(index_list);
    self->indexes = PyMem_Malloc(self->num_indexes * sizeof(db_index_t));
    printf("adding %d indexes\n", self->num_indexes);
    for (j = 0; j < self->num_indexes; j++) {
        db_index_initialise(&self->indexes[j], PyList_GetItem(index_list, j));
        /* TODO this is not the corect way to go about this */
        if (PyErr_Occurred() != NULL) {
            goto out;
        }
    }
    /* do this here for now */
    for (j = 0; j < self->num_indexes; j++) {
        db_ret = self->primary_db->associate(self->primary_db, NULL, 
                self->indexes[j].secondary_db, 
                db_index_callback, 0); 
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
BerkeleyDatabase_create(BerkeleyDatabase* self)
{
    PyObject *ret = NULL;
    int db_ret;
    u_int32_t flags;
    char *db_name = "db_NOBACKUP_/primary.db";
    
    db_ret = self->primary_db->set_cachesize(self->primary_db, 0, 
            512 * 1024 * 1024, 1);
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
    char *db_name = "db_NOBACKUP_/primary.db";
    db_ret = self->primary_db->open(self->primary_db, NULL, db_name, NULL, 
            DB_UNKNOWN,  flags,  0);         
    if (db_ret != 0) {
        handle_bdb_error(db_ret);
        goto out;    
    }



    db_ret = self->primary_db->cursor(self->primary_db, NULL, &self->cursor, DB_CURSOR_BULK);
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
    if (self->cursor != NULL) {  
        db_ret = self->cursor->close(self->cursor);
        if (db_ret != 0) {
            handle_bdb_error(db_ret); 
            goto out;
        } 
    }
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
    {"add_index", (PyCFunction) BerkeleyDatabase_add_index, METH_VARARGS, "Adds an index" },
    {"open", (PyCFunction) BerkeleyDatabase_open, METH_NOARGS, "Open the table" },
    {"create", (PyCFunction) BerkeleyDatabase_create, METH_NOARGS, "Open the table" },
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
};
   


/*==========================================================
 * RecordBuffer object 
 *==========================================================
 */

static void
RecordBuffer_dealloc(RecordBuffer* self)
{
    /* Is this safe?? */
    PyMem_Free(self->record_buffer);
    PyMem_Free(self->key_buffer);
    PyMem_Free(self->keys);
    PyMem_Free(self->records);
    Py_XDECREF(self->database);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

/* 
 * Resets this buffer so that it is ready to receive more records.
 */
static void
RecordBuffer_clear(RecordBuffer *self)
{
    memset(self->record_buffer, 0, self->current_record_offset);
    self->current_record_size = 0;
    self->current_record_offset = 0;
    self->num_records = 0;
    self->read_records = 0;
    self->reading_done = 0;

}

static int
RecordBuffer_init(RecordBuffer *self, PyObject *args, PyObject *kwds)
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
    self->read_records = 0;
    self->reading_done = 0;
    self->num_views = 0; 


out:
    return ret;
}

static PyMemberDef RecordBuffer_members[] = {
    {"database", T_OBJECT_EX, offsetof(RecordBuffer, database), READONLY, "database"},
    {"current_record_size", T_INT, offsetof(RecordBuffer, current_record_size), 0, "size of current record"},
    {NULL}  /* Sentinel */
};

static PyObject *
RecordBuffer_flush(RecordBuffer* self)
{
    int db_ret = 0;
    PyObject *ret = NULL;
    u_int32_t flags = 0;
    u_int32_t j;
    DB *db;
    
    db = self->database->primary_db;
    /* TODO error check or verify this can't be null */
    printf("\nFlushing buffer: %d records in %dKiB\n", self->num_records, self->current_record_offset / 1024);
    for (j = 0; j < self->num_records; j++) {
        db_ret = db->put(db, NULL, &self->keys[j], &self->records[j], flags);
        if (db_ret != 0) {
            handle_bdb_error(db_ret); 
            goto out;
        } 
    }
    printf("done\n");
    RecordBuffer_clear(self);
    
    ret = Py_BuildValue("");
out:
    return ret; 
}


static PyObject *
RecordBuffer_commit_record(RecordBuffer* self, PyObject *args)
{
    PyObject *ret = NULL;
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
        ret = RecordBuffer_flush(self);
        if (ret == NULL) {
            goto out;
        }
    }

    ret = Py_BuildValue("");
out:
    return ret; 
}

static PyObject *
RecordBuffer_fill(RecordBuffer* self)
{
    int db_ret = 0;
    PyObject *ret = NULL;
    DBC *cursor = self->database->cursor; 
    DBT *key, *record;
    u_int32_t barrier = self->record_buffer_size - self->max_record_size;
    RecordBuffer_clear(self);
    
    while (db_ret == 0 && self->current_record_offset <= barrier 
            && self->num_records < self->max_num_records) {
        /* Some of these flags can be set an intialisation time */
        key = &self->keys[self->num_records];
        key->ulen = self->key_size;
        key->data = &self->key_buffer[self->key_size * self->num_records];
        key->flags = DB_DBT_USERMEM;
        record = &self->records[self->num_records];
        record->ulen = self->max_record_size;
        record->data = &self->record_buffer[self->current_record_offset];
        record->flags = DB_DBT_USERMEM;
        db_ret = cursor->get(cursor, key, record, DB_NEXT); 
        if (db_ret == 0) {
            self->num_records++; 
            self->current_record_offset += record->size;
        } else if (db_ret == DB_NOTFOUND) {
            self->reading_done = 1;
        } else {
            handle_bdb_error(db_ret); 
            goto out;
        } 
        
    }
    
    printf("filled buffer to offset %d : %d records\n",self->current_record_offset, self->num_records);
   
    ret = Py_BuildValue("");
out:
    return ret; 
}




static PyObject *
RecordBuffer_retrieve_record(RecordBuffer* self, PyObject *args)
{
    PyObject *ret = NULL;
    unsigned int n = self->read_records;
    /* this is dreadful */
    if (n == self->num_records && self->reading_done) {
        ret = Py_BuildValue("");
    } else {
        if (n == self->num_records) {
            RecordBuffer_fill(self);
        }
        n = self->read_records;
        if (n < self->num_records) {
            /* set the current_record_offset so that it points to the next record.
             * Using pointer arithmmetic for now.
             */
            self->current_record_offset = ((unsigned char *) self->records[n].data)
                    - self->record_buffer;
            /* build a bytes object for the key */ 
            ret = Py_BuildValue("y#", self->keys[n].data, self->key_size);
            self->read_records++;
        } else {
            ret = Py_BuildValue("");
        }

    }
//out:
    return ret; 
}






/* Implementation of the Buffer Protocol so that we can write values 
 * directly into our record buffer memory from Python. This implementation 
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
    ptr = &self->record_buffer[self->current_record_offset];
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
    {"retrieve_record", (PyCFunction) RecordBuffer_retrieve_record, METH_NOARGS, "retrieve record" },
    {"commit_record", (PyCFunction) RecordBuffer_commit_record, METH_VARARGS, "commit record" },
    {"flush", (PyCFunction) RecordBuffer_flush, METH_NOARGS, "flush" },
    {"fill", (PyCFunction) RecordBuffer_fill, METH_NOARGS, "fill the buffer with records." },
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
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
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
    
    /* RecordBuffer */
    RecordBufferType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&RecordBufferType) < 0) {
        INITERROR;
    }
    Py_INCREF(&RecordBufferType);
    PyModule_AddObject(module, "RecordBuffer", (PyObject *) &RecordBufferType);
    
    BerkeleyDatabaseError = PyErr_NewException("_vcfdb.BerkeleyDatabaseError", 
            NULL, NULL);
    Py_INCREF(BerkeleyDatabaseError);
    PyModule_AddObject(module, "BerkeleyDatabaseError", BerkeleyDatabaseError);

    PyModule_AddIntConstant(module, "COLUMN_TYPE_FIXED", COLUMN_TYPE_FIXED);
    PyModule_AddIntConstant(module, "COLUMN_TYPE_VARIABLE", COLUMN_TYPE_VARIABLE);
    
    PyModule_AddIntConstant(module, "ELEMENT_TYPE_CHAR", ELEMENT_TYPE_CHAR);
    PyModule_AddIntConstant(module, "ELEMENT_TYPE_INT_1", ELEMENT_TYPE_INT_1);
    PyModule_AddIntConstant(module, "ELEMENT_TYPE_INT_2", ELEMENT_TYPE_INT_2);
    PyModule_AddIntConstant(module, "ELEMENT_TYPE_INT_4", ELEMENT_TYPE_INT_4);
    PyModule_AddIntConstant(module, "ELEMENT_TYPE_INT_8", ELEMENT_TYPE_INT_8);
    PyModule_AddIntConstant(module, "ELEMENT_TYPE_FLOAT", ELEMENT_TYPE_FLOAT);

#if PY_MAJOR_VERSION >= 3
    return module;
#endif
}


