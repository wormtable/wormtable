#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include <libxml/parser.h>
#include <libxml/tree.h>

#include "wormtable.h"

/* 
 * Returns a new string with the specified prefix and suffix 
 * concatenated together and copied to the new buffer.
 */
static char * 
strconcat(const char *prefix, const char *suffix)
{
    char *ret = NULL;
    size_t length = strlen(prefix);
    ret = malloc(1 + length + strlen(suffix));
    if (ret == NULL) {
        goto out; 
    }
    strncpy(ret, prefix, length);
    strcpy(ret + length, suffix);
out:
    return ret;
}

/*
 * Returns a new copy of the specified string.
 */
static char *
copy_string(const char *string)
{
    size_t length = strlen(string);
    char *ret = malloc(1 + length);
    if (ret == NULL) {
        goto out;
    }
    strcpy(ret, string);
out:
    return ret;
}

static const char *element_type_strings[] = { "uint", "int", "float", "char" };

static int 
element_type_to_str(u_int32_t element_type, const char **type_string)
{
    int ret = -1;
    if (element_type < sizeof(element_type_strings) / sizeof(char *)) {
        *type_string = element_type_strings[element_type];
        ret = 0;
    }
    return ret;
}

static int 
str_to_element_type(const char *type_string, u_int32_t *element_type)
{
    int ret = -1;
    u_int32_t num_types = sizeof(element_type_strings) / sizeof(char *);
    u_int32_t j;
    for (j = 0; j < num_types; j++) {
        if (strcmp(type_string, element_type_strings[j]) == 0) {
            *element_type = j; 
            ret = 0;
        }
    }
    return ret;
}




static int
wt_table_add_column(wt_table_t *self, const char *name, 
    const char *description, u_int32_t element_type, u_int32_t element_size,
    u_int32_t num_elements)
{
    int ret = 0;
    wt_column_t *col;
    /* make some space for the new column */
    self->num_columns++;
    col = realloc(self->columns, self->num_columns * sizeof(wt_column_t));
    if (col == NULL) {
        ret = ENOMEM;
        goto out;
    }
    printf("adding column: '%s' :%d %d %d\n", name,
            element_type, element_size, num_elements);
    self->columns = col;
    /* Now fill it in */
    col = &self->columns[self->num_columns - 1];
    col->name = copy_string(name);
    col->description = copy_string(description);
    if (col->name == NULL || col->description == NULL) {
        ret = ENOMEM;
        goto out;
    }
    col->element_type = element_type;
    col->element_size = element_size;
    col->num_elements = num_elements;
    if (element_type == WT_UINT) {

    } else if (element_type == WT_INT) {

    } else if (element_type == WT_FLOAT) {

    } else if (element_type == WT_CHAR) {

    } else {
        ret = EINVAL;
        goto out;
    }
out:
    return ret;
}

static int
wt_table_add_column_write_mode(wt_table_t *self, const char *name, 
    const char *description, u_int32_t element_type, u_int32_t element_size,
    u_int32_t num_elements)
{
    int ret = 0;
    if (self->mode != WT_WRITE) {
        ret = EINVAL;
        goto out;
    }
    ret = wt_table_add_column(self, name, description, element_type, 
            element_size, num_elements);
out:
    return ret;
}
    
static int
wt_table_open_writer(wt_table_t *self)
{
    int ret = 0;
    char *db_filename = strconcat(self->homedir, WT_BUILD_PRIMARY_DB_FILE); 
    if (db_filename == NULL) {
        ret = ENOMEM;
        goto out;
    }
    printf("opening table %s for writing\n", self->homedir);   
    self->mode = WT_WRITE;
    ret = mkdir(self->homedir, S_IRWXU);
    if (ret == -1) {
        ret = errno;
        errno = 0;
        goto out;
    }
    ret = self->db->open(self->db, NULL, db_filename, NULL, 
            DB_BTREE, DB_CREATE|DB_EXCL, 0);
    if (ret != 0) {
        goto out;
    }
    /* add the key column */
    wt_table_add_column(self, "row_id", "key column", WT_UINT, self->keysize, 1);

out:
    if (db_filename != NULL) {
        free(db_filename);
    }
    return ret;
}

static int 
wt_table_write_schema(wt_table_t *self)
{
    int ret = 0;
    unsigned int j;
    const char *type_str = NULL;
    wt_column_t *col;
    FILE *f;
    char *schema_file = strconcat(self->homedir, WT_SCHEMA_FILE);
    f = fopen(schema_file, "w");
    if (f == NULL) {
        ret = errno;
        errno = 0;
        goto out;
    }
    fprintf(f, "<?xml version=\"1.0\" ?>\n");
    fprintf(f, "<schema version=\"%s\">\n", WT_SCHEMA_VERSION);
    fprintf(f, "\t<columns>\n");

    for (j = 0; j < self->num_columns; j++) {
        col = &self->columns[j];
        if (element_type_to_str(col->element_type, &type_str)) {
            ret = WT_ERR_FATAL;
        } 
        fprintf(f, "\t\t<column ");
        fprintf(f, "name=\"%s\" ", col->name); 
        fprintf(f, "element_type=\"%s\" ", type_str); 
        fprintf(f, "element_size=\"%d\" ", col->element_size); 
        fprintf(f, "num_elements=\"%d\" ", col->num_elements); 
        fprintf(f, "description =\"%s\" ", col->description); 
        fprintf(f, "/>\n");
    }
    fprintf(f, "\t</columns>\n");
    fprintf(f, "</schema>\n");

out:
    if (f != NULL) {
        if (fclose(f) != 0) {
            ret = errno;
            errno = 0;
        }
    } 
    if (schema_file != NULL) {
        free(schema_file);
    }

    return ret;
}

static int 
wt_table_add_xml_column(wt_table_t *self, xmlNode *node)
{
    int ret = EINVAL;
    xmlAttr *attr;
    xmlChar *xml_name = NULL;
    xmlChar *xml_description = NULL;
    xmlChar *xml_num_elements = NULL;
    xmlChar *xml_element_type = NULL;
    xmlChar *xml_element_size = NULL;
    const char *name, *description;
    u_int32_t element_type;
    int num_elements, element_size;
    for (attr = node->properties; attr != NULL; attr = attr->next) {
        if (xmlStrEqual(attr->name, (const xmlChar *) "name")) {
            xml_name = attr->children->content;
        } else if (xmlStrEqual(attr->name, (const xmlChar *) "description")) {
            xml_description = attr->children->content;
        } else if (xmlStrEqual(attr->name, (const xmlChar *) "num_elements")) {
            xml_num_elements = attr->children->content;
        } else if (xmlStrEqual(attr->name, (const xmlChar *) "element_type")) {
            xml_element_type = attr->children->content;
        } else if (xmlStrEqual(attr->name, (const xmlChar *) "element_size")) {
            xml_element_size = attr->children->content;
        } else {
            goto out;
        }
    }
    if (xml_name == NULL || xml_description == NULL 
            || xml_num_elements == NULL || xml_element_type == NULL
            || xml_element_size == NULL) {
        goto out;
    }
    /* TODO: must do some error checking - atoi is useless. */
    num_elements = atoi((const char *) xml_num_elements);
    element_size = atoi((const char *) xml_element_size);
    ret = str_to_element_type((const char *) xml_element_type, &element_type);
    if (ret != 0) {
        goto out;
    }
    name = (const char *) xml_name;
    description = (const char *) xml_description;
    ret = wt_table_add_column(self, name, description, element_type, 
            element_size, num_elements);
out:
    return ret;
}

static int 
wt_table_read_schema(wt_table_t *self)
{
    int ret = 0;
    const xmlChar *version;
    xmlAttr *attr;
    xmlDocPtr doc = NULL; 
    xmlNode *schema,  *columns, *node;
    char *schema_file = NULL;
    schema_file = strconcat(self->homedir, "schema.xml");
    if (schema_file == NULL) {
        ret = ENOMEM;
        goto out;
    }
    printf("reading schema from %s\n", schema_file);
    doc = xmlReadFile(schema_file, NULL, 0); 
    if (doc == NULL) {
        ret = EINVAL;
        goto out;
    }
    schema = xmlDocGetRootElement(doc);
    if (schema == NULL) {
        printf("parse error");
        ret = EINVAL;
        goto out;
    }
    if (xmlStrcmp(schema->name, (const xmlChar *) "schema")) {
        printf("parse error");
        ret = EINVAL;
        goto out;
    }
    attr = schema->properties;
    while (attr != NULL) {
        //printf("attr:%s = %s\n", attr->name, attr->children->content);
        if (xmlStrEqual(attr->name, (const xmlChar *) "version")) {
            version = attr->children->content;
        }
        attr = attr->next;
    }
    if (version == NULL) {
        printf("parse error: version required");
        ret = EINVAL;
        goto out;
    }
    node = schema->xmlChildrenNode;
    for (node = schema->xmlChildrenNode; node != NULL; node = node->next) {
        if (node->type == XML_ELEMENT_NODE) {
            if (!xmlStrEqual(node->name, (const xmlChar *) "columns")) {
                printf("parse error");
                ret = EINVAL;
                goto out;
            }
            columns = node;
        }
    }
    for (node = columns->xmlChildrenNode; node != NULL; node = node->next) {
        if (node->type == XML_ELEMENT_NODE) {
            if (!xmlStrEqual(node->name, (const xmlChar *) "column")) {
                printf("parse error");
                ret = EINVAL;
                goto out;
            }
            ret = wt_table_add_xml_column(self, node);
            if (ret != 0) {
                goto out;
            }
        }
   }
    
out:
    if (schema_file != NULL) {
        free(schema_file);
    }
    if (doc != NULL) {
        xmlFreeDoc(doc);
    }
    xmlCleanupParser();

    return ret;
}


static int
wt_table_close_writer(wt_table_t *self)
{
    DB *tmp;
    int ret = 0;
    char *old_name = strconcat(self->homedir, WT_BUILD_PRIMARY_DB_FILE);
    char *new_name = strconcat(self->homedir, WT_PRIMARY_DB_FILE);
    if (old_name == NULL || new_name == NULL) {
        ret = ENOMEM;
        goto out;
    }
    ret = wt_table_write_schema(self);
    if (ret != 0) {
        goto out;
    }
    assert(self->db == NULL);
    ret = db_create(&tmp, NULL, 0);
    if (ret != 0) {
        goto out;
    }
    ret = tmp->rename(tmp, old_name, NULL, new_name, 0);
    if (ret != 0) {
        goto out;
    }
    printf("renamed %s to %s\n", old_name, new_name);
out:
    if (old_name != NULL) {
        free(old_name);
    }
    if (new_name != NULL) {
        free(new_name);
    }
    
    return ret;
}


static int
wt_table_open_reader(wt_table_t *self)
{
    int ret;
    char *db_filename = NULL;
    db_filename = strconcat(self->homedir, WT_PRIMARY_DB_FILE);
    if (db_filename == NULL) {
        ret = ENOMEM;
        goto out;
    }
    printf("opening table %s for reading\n", self->homedir);   
    self->mode = WT_READ;
    ret = self->db->open(self->db, NULL, db_filename, NULL, 
            DB_BTREE, DB_RDONLY, 0);
    if (ret != 0) {
        goto out;
    }
    ret = wt_table_read_schema(self);
    if (ret != 0) {
        goto out;
    }
out:
    if (db_filename != NULL) {
        free(db_filename);
    }
    return ret;
}

static int 
wt_table_open(wt_table_t *self, const char *homedir, u_int32_t flags)
{
    int ret = 0;
    self->homedir = homedir;
    if (flags == WT_WRITE) {
        ret = wt_table_open_writer(self); 
    } else if (flags == WT_READ) {
        ret = wt_table_open_reader(self); 
    } else {
        ret = EINVAL; 
    }
    return ret;
}

/* TODO these functions should check the state to make sure we 
 * are in the just-opened state. Check the DB source code to 
 * see how they manage this. We should have a simple state 
 * machine for the table.
 */

static int 
wt_table_set_cachesize(wt_table_t *self, u_int64_t bytes)
{
    int ret = 0;
    u_int64_t gb = 1ULL << 30;
    ret = self->db->set_cachesize(self->db, bytes / gb, bytes % gb, 1);
    return ret;
}

static int 
wt_table_set_keysize(wt_table_t *self, u_int32_t keysize)
{
    int ret = 0;
    if (keysize < 1 || keysize > 8) {
        ret = EINVAL;
        goto out;
    }
    self->keysize = keysize;
out:
    return ret;
}


static void
wt_table_free(wt_table_t *self)
{
    unsigned int j;
    wt_column_t *col;
    if (self->db != NULL) {
        self->db->close(self->db, 0);
    }
    if (self->columns != NULL) {
        for (j = 0; j < self->num_columns; j++) {
            col = &self->columns[j];
            if (col->name != NULL) {
                free(col->name);
            }
            if (col->description != NULL) {
                free(col->description);
            }
        }
        
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
    if (self->mode == WT_WRITE) {
        ret = wt_table_close_writer(self);
    }
    
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
    self->keysize = WT_DEFAULT_KEYSIZE;
    self->open = wt_table_open;
    self->add_column = wt_table_add_column_write_mode;
    self->set_cachesize = wt_table_set_cachesize;
    self->set_keysize = wt_table_set_keysize;
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

