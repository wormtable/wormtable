#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "wormtable.h"


static int
wt_table_add_column(wt_table_t *self, const char *name, 
    const char *description, u_int32_t element_type, u_int32_t element_size,
    u_int32_t num_elements)
{
    int ret = 0;
    wt_column_t *col;
    if (self->mode != WT_WRITE) {
        ret = EINVAL;
        goto out;
    }
    /* make some space for the new column */
    self->num_columns++;
    col = realloc(self->columns, self->num_columns * sizeof(wt_column_t));
    if (col == NULL) {
        ret = ENOMEM;
        goto out;
    }
    self->columns = col;
    /* Now fill it in */
    col = &self->columns[self->num_columns - 1];
    col->name = name;
    col->description = description;
    col->element_type = element_type;
    col->element_size = element_size;
    col->num_elements = num_elements;
out:
    return ret;
}
static int
wt_table_open_writer(wt_table_t *self)
{
    int ret = 0;
    printf("opening table %s for writing\n", self->homedir);   
    self->mode = WT_WRITE;
    ret = mkdir(self->homedir, S_IRWXU);
    if (ret == -1) {
        ret = errno;
        errno = 0;
        goto out;
    }
    ret = self->db->open(self->db, NULL, self->db_filename, NULL, 
            DB_BTREE, DB_CREATE|DB_EXCL, 0);
    if (ret != 0) {
        goto out;
    }
out:
    return ret;
}

/* TODO add a flag to specify a build file so that we 
 * cannot read from a currently-building DB
 */
static int
wt_table_set_db_filename(wt_table_t *self)
{
    int ret = 0;
    size_t length = strlen(self->homedir); 
    self->db_filename = malloc(1 + length + strlen(WT_PRIMARY_DB_FILE));
    if (self->db_filename == NULL) {
        ret = ENOMEM;
    }
    strncpy(self->db_filename, self->homedir, length);
    strcpy(self->db_filename + length, WT_PRIMARY_DB_FILE);
    return ret;
}

static int 
wt_table_open(wt_table_t *self, const char *homedir, u_int32_t flags)
{
    int ret = 0;
    self->homedir = homedir;
    ret = wt_table_set_db_filename(self);
    if (ret != 0) {
        goto out;
    }
    if (flags == WT_WRITE) {
        ret = wt_table_open_writer(self); 
    } else if (flags == WT_READ) {

    } else {
        ret = EINVAL; 
    }
out:
    return ret;
}

static int 
wt_table_set_cachesize(wt_table_t *self, u_int64_t bytes)
{
    int ret = 0;
    u_int64_t gb = 1ULL << 30;
    ret = self->db->set_cachesize(self->db, bytes / gb, bytes % gb, 1);
    return ret;
}



static void
wt_table_free(wt_table_t *self)
{
    if (self->db_filename != NULL) {
        free(self->db_filename);
    }
    if (self->db != NULL) {
        self->db->close(self->db, 0);
    }
    if (self->columns != NULL) {
        free(self->columns);
    }
    free(self);
}



static int 
wt_table_close(wt_table_t *self)
{
    int ret = 0;
   
    ret = self->db->close(self->db, 0);
    self->db = NULL;
    printf("closing table %s\n", self->homedir);   
    wt_table_free(self);
    
    return ret;
}

int 
wt_table_create(wt_table_t **wtp)
{
    int ret = 0;
    wt_table_t *self = malloc(sizeof(wt_table_t));
    if (self == NULL) {
        ret = ENOMEM;
        goto out;
    }
    memset(self, 0, sizeof(wt_table_t));
    ret = db_create(&self->db, NULL, 0);
    if (ret != 0) {
        goto out;
    }
    self->open = wt_table_open;
    self->add_column = wt_table_add_column;
    self->set_cachesize = wt_table_set_cachesize;
    self->close = wt_table_close;
    *wtp = self;
out:
    if (ret != 0) {
        wt_table_free(self);
    }

    return ret;
}

char *
wt_strerror(int err)
{
    return db_strerror(err);
}
