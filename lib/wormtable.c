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

#ifndef WORDS_BIGENDIAN
/* 
 * Copies n bytes of source into destination, swapping the order of the 
 * bytes.
 */
static void
byteswap_copy(void* dest, void *source, size_t n)
{
    size_t j = 0;
    unsigned char *dest_c = (unsigned char *) dest;
    unsigned char *source_c = (unsigned char *) source;
    for (j = 0; j < n; j++) {
        dest_c[j] = source_c[n - j - 1];
    }
}
#endif

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

/* element packing */

static int 
pack_uint(void *dest, u_int64_t element, u_int8_t size) 
{
    int ret = 0;
    void *src = &element;
#ifdef WORDS_BIGENDIAN
    memcpy(dest, src + 8 - size, size);
#else
    byteswap_copy(dest, src, size);
#endif
    return ret;
}

static int 
pack_int(void *dest, int64_t element, u_int8_t size) 
{
    int ret = 0;
    int64_t u = element; 
    void *src = &u;
    /* flip the sign bit */
    u ^= 1LL << (size * 8 - 1);
#ifdef WORDS_BIGENDIAN
    memcpy(dest, src + 8 - size, size);
#else
    byteswap_copy(dest, src, size);
#endif
    return ret;
}

static int 
pack_float4(void *dest, double element, u_int8_t size) 
{
    int ret = 0;
    int32_t bits;
    float v = (float) element;
    memcpy(&bits, &v, sizeof(float));
    bits ^= (bits < 0) ? 0xffffffff: 0x80000000;
#ifdef WORDS_BIGENDIAN
    memcpy(dest, &bits, sizeof(float)); 
#else    
    byteswap_copy(dest, &bits, sizeof(float)); 
#endif
    return ret;
}

static int 
pack_float8(void *dest, double element, u_int8_t size) 
{
    int ret = 0;
    int64_t bits;
    memcpy(&bits, &element, sizeof(double));
    bits ^= (bits < 0) ? 0xffffffffffffffffLL: 0x8000000000000000LL;
#ifdef WORDS_BIGENDIAN
    memcpy(dest, &bits, sizeof(double)); 
#else
    byteswap_copy(dest, &bits, sizeof(double)); 
#endif
    return ret;
}

/* element unpacking */

static int 
unpack_uint(u_int64_t *element, void *src, u_int8_t size) 
{
    u_int64_t dest = 0;
#ifdef WORDS_BIGENDIAN
    memcpy(&dest + 8 - size, src, size);
#else
    byteswap_copy(&dest, src, size);
#endif
    *element = dest;
    return 0;
}

static int 
unpack_int(int64_t *element, void *src, u_int8_t size) 
{
    int64_t dest = 0;
    const int64_t m = 1LL << (size * 8 - 1);
#ifdef WORDS_BIGENDIAN
    memcpy(&dest + 8 - size, src, size);
#else
    byteswap_copy(&dest, src, size);
#endif
    /* flip the sign bit */
    dest ^= m;
    /* sign extend and return */
    *element = (dest ^ m) - m;
    return 0;
}

static int 
unpack_float4(double *element, void *src, u_int8_t size) 
{
    int32_t float_bits;
    float dest;
#ifdef WORDS_BIGENDIAN
    memcpy(&float_bits, src, sizeof(float));
#else
    byteswap_copy(&float_bits, src, sizeof(float));
#endif
    float_bits ^= (float_bits < 0) ? 0x80000000: 0xffffffff;
    memcpy(&dest, &float_bits, sizeof(float));
    *element = (double) dest;
    return 0;
}
   
static int 
unpack_float8(double *element, void *src, u_int8_t size) 
{
    int64_t double_bits;
    double dest;
#ifdef WORDS_BIGENDIAN
    memcpy(&double_bits, src, sizeof(double));
#else
    byteswap_copy(&double_bits, src, sizeof(double));
#endif
    double_bits ^= (double_bits < 0) ? 0x8000000000000000LL: 0xffffffffffffffffLL;
    memcpy(&dest, &double_bits, sizeof(double));
    *element = dest;
    return 0;
}
 
/*==========================================================
 * Column object 
 *==========================================================
 */

/* value packing */

static int
wt_column_pack_elements_uint(wt_column_t *self, void *dest, void *elements,
        u_int32_t num_elements)
{
    int ret = 0;
    void *p = dest;
    u_int32_t j;
    u_int64_t *e = (u_int64_t *) elements;
    for (j = 0; j < num_elements; j++) {
        ret = pack_uint(p, e[j], self->element_size);
        p += sizeof(u_int64_t);
    }
    return ret;
}

static int
wt_column_pack_elements_int(wt_column_t *self, void *dest, void *elements,
        u_int32_t num_elements)
{
    int ret = 0;
    void *p = dest;
    u_int32_t j;
    int64_t *e = (int64_t *) elements;
    for (j = 0; j < num_elements; j++) {
        ret = pack_int(p, e[j], self->element_size);
        p += sizeof(int64_t);
    }
    return ret;
}

static int
wt_column_pack_elements_float4(wt_column_t *self, void *dest, void *elements,
        u_int32_t num_elements)
{
    int ret = 0;
    void *p = dest;
    u_int32_t j;
    double *e = (double *) elements;
    for (j = 0; j < num_elements; j++) {
        ret = pack_float4(p, e[j], self->element_size);
        p += sizeof(double);
    }
    return ret;
}

static int
wt_column_pack_elements_float8(wt_column_t *self, void *dest, void *elements,
        u_int32_t num_elements)
{
    int ret = 0;
    void *p = dest;
    u_int32_t j;
    double *e = (double *) elements;
    for (j = 0; j < num_elements; j++) {
        ret = pack_float8(p, e[j], self->element_size);
        p += sizeof(double);
    }
    return ret;
}

static int
wt_column_pack_elements_char(wt_column_t *self, void *dest, void *elements,
        u_int32_t num_elements)
{
    int ret = 0;
    memcpy(dest, elements, num_elements);
    return ret;
}

/* value unpacking */

static int
wt_column_unpack_elements_uint(wt_column_t *self, void *elements, void *src, 
        u_int32_t num_elements)
{
    int ret = 0;
    void *p = src;
    u_int32_t j;
    u_int64_t *e = (u_int64_t *) elements;
    for (j = 0; j < num_elements; j++) {
        ret = unpack_uint(&e[j], p, self->element_size);
        p += self->element_size; 
    }
    return ret;
}

static int
wt_column_unpack_elements_int(wt_column_t *self, void *elements, void *src, 
        u_int32_t num_elements)
{
    int ret = 0;
    void *p = src;
    u_int32_t j;
    int64_t *e = (int64_t *) elements;
    for (j = 0; j < num_elements; j++) {
        ret = unpack_int(&e[j], p, self->element_size);
        p += self->element_size; 
    }
    return ret;
}

static int
wt_column_unpack_elements_float4(wt_column_t *self, void *elements, void *src, 
        u_int32_t num_elements)
{
    int ret = 0;
    void *p = src;
    u_int32_t j;
    double *e = (double *) elements;
    for (j = 0; j < num_elements; j++) {
        ret = unpack_float4(&e[j], p, self->element_size);
        p += self->element_size; 
    }
    return ret;
}

static int
wt_column_unpack_elements_float8(wt_column_t *self, void *elements, void *src, 
        u_int32_t num_elements)
{
    int ret = 0;
    void *p = src;
    u_int32_t j;
    double *e = (double *) elements;
    for (j = 0; j < num_elements; j++) {
        ret = unpack_float8(&e[j], p, self->element_size);
        p += self->element_size; 
    }
    return ret;
}

static int
wt_column_unpack_elements_char(wt_column_t *self, void *elements, void *src,
        u_int32_t num_elements)
{
    int ret = 0;
    memcpy(elements, src, num_elements);
    return ret;
}

static int 
wt_column_get_name(wt_column_t *self, const char **name)
{
    *name = self->name;
    return 0;
}

static int 
wt_column_get_description(wt_column_t *self, const char **description)
{
    *description = self->description;
    return 0;
}

static int 
wt_column_get_element_type(wt_column_t *self, u_int32_t *element_type)
{
    *element_type = self->element_type;
    return 0;
}

static int 
wt_column_get_num_elements(wt_column_t *self, u_int32_t *num_elements)
{
    *num_elements = self->num_elements;
    return 0;
}

static int 
wt_column_get_element_size(wt_column_t *self, u_int32_t *element_size)
{
    *element_size = self->element_size;
    return 0;
}






/*==========================================================
 * Row object 
 *==========================================================
 */

/* 
 * Packs the address for a variable column for the specified number of elements
 * at the end of this row, and increase the row size accordingly.
 */
static int 
wt_row_pack_address(wt_row_t *self, wt_column_t *col, u_int32_t num_elements)
{
    int ret = 0;
    void *p = self->data + col->fixed_region_offset;
    u_int32_t new_size = self->size + num_elements * col->element_size;
    if (new_size > self->max_size) {
        ret = EINVAL;
        goto out;
    }
    ret = pack_uint(p, (u_int64_t) self->size, WT_VARIABLE_OFFSET_SIZE);
    if (ret != 0) {
        goto out;
    }
    p += WT_VARIABLE_OFFSET_SIZE;
    ret = pack_uint(p, (u_int64_t) num_elements, WT_VARIABLE_COUNT_SIZE);
    if (ret != 0) {
        goto out;
    }
    self->size = new_size;
out:
    return ret;
}

static int 
wt_row_unpack_address(wt_row_t *self, wt_column_t *col, u_int32_t *offset, 
        u_int32_t *num_elements)
{
    int ret = 0;
    void *p = self->data + col->fixed_region_offset;
    u_int64_t o = 0; 
    u_int64_t n = 0; 
    ret = unpack_uint(&o, p, WT_VARIABLE_OFFSET_SIZE);
    if (ret != 0) {
        goto out;
    }
    p += WT_VARIABLE_OFFSET_SIZE;
    ret = unpack_uint(&n, p, WT_VARIABLE_COUNT_SIZE);
    if (ret != 0) {
        goto out;
    }
    *offset = (u_int32_t) o;
    *num_elements = (u_int32_t) n;
out:
    return ret;
}


static int 
wt_row_set_value(wt_row_t *self, wt_column_t *col, void *elements, 
        u_int32_t num_elements)
{
    int ret = 0;
    void *p;
    if (self->size == 0) {
        ret = EINVAL;
        goto out;
    }
    if (col->num_elements == WT_VARIABLE) {
        p = self->data + self->size;
        ret = wt_row_pack_address(self, col, num_elements);
        if (ret != 0) {
            goto out;
        }   
    } else {
        p = self->data + col->fixed_region_offset;
        if (num_elements != col->num_elements) {
            ret = EINVAL;
            goto out;
        }
    }
    col->pack_elements(col, p, elements, num_elements);

    //printf("Setting elements for column '%s'@%p\n", col->name, p);
out:
    return ret;
}

static int 
wt_row_get_value(wt_row_t *self, wt_column_t *col, void *elements, 
        u_int32_t *num_elements)
{
    int ret = 0;
    void *p;
    u_int32_t offset;
    u_int32_t n;
    if (col->num_elements == WT_VARIABLE) {
        ret = wt_row_unpack_address(self, col, &offset, &n);
        if (ret != 0) {
            goto out;
        }  
        if (offset >= self->max_size) {
            ret = EINVAL;
            goto out;
        }
        p = self->data + offset;
    } else {
        p = self->data + col->fixed_region_offset;
        n = col->num_elements;
    }
    col->unpack_elements(col, elements, p, n);
    *num_elements = n;
    //printf("Getting elements for column '%s'@%p\n", col->name, p);
out:
    return ret;
}

static int 
wt_row_clear(wt_row_t *self)
{
    memset(self->data, 0, self->size);
    self->size = self->table->fixed_region_size;
    return 0;
}

static int 
wt_row_free(wt_row_t *self)
{
    if (self != NULL) {
        if (self->data != NULL) {
            free(self->data);
        }
        free(self);
    }
    return 0;
}

int 
wt_row_alloc(wt_row_t **wtrp, wt_table_t *table, u_int32_t size)
{
    int ret = 0;
    wt_row_t *self = calloc(1, sizeof(wt_row_t));
    void *p = calloc(1, size);
    if (self == NULL || p == NULL) {
        ret = ENOMEM;
        goto out;
    }
    self->table = table;
    self->max_size = size;
    self->data = p;
    self->size = 0; 
    self->set_value = wt_row_set_value;
    self->get_value = wt_row_get_value;
    self->clear = wt_row_clear;
    self->free = wt_row_free;
    *wtrp = self;
out:
    if (ret != 0) {
        wt_row_free(self);
    }
    return ret;
}

int 
wt_column_free(wt_column_t *self)
{
    if (self != NULL) {
        if (self->name != NULL) {
            free(self->name);
        }
        if (self->description != NULL) {
            free(self->description);
        }
        free(self);
    }
    return 0;
}

int
wt_column_alloc(wt_column_t **wtcp, const char *name, const char *description, 
        u_int32_t element_type, u_int32_t element_size,
        u_int32_t num_elements)
{
    int ret = 0;
    wt_column_t *self = malloc(sizeof(wt_column_t));
    if (self == NULL) {
        ret = ENOMEM;
        goto out;
    }
    self->name = copy_string(name);
    self->description = copy_string(description);
    if (self->name == NULL || self->description == NULL) {
        ret = ENOMEM;
        goto out;
    }
    self->element_type = element_type;
    self->element_size = element_size;
    self->num_elements = num_elements;
    if (element_type == WT_UINT) {
        self->pack_elements = wt_column_pack_elements_uint;
        self->unpack_elements = wt_column_unpack_elements_uint;
    } else if (element_type == WT_INT) {
        self->pack_elements = wt_column_pack_elements_int;
        self->unpack_elements = wt_column_unpack_elements_int;
    } else if (element_type == WT_FLOAT) {
        if (self->element_size == 4) {
            self->pack_elements = wt_column_pack_elements_float4;   
            self->unpack_elements = wt_column_unpack_elements_float4;
        } else {
            self->pack_elements = wt_column_pack_elements_float8;   
            self->unpack_elements = wt_column_unpack_elements_float8;
        }
    } else if (element_type == WT_CHAR) {
        self->pack_elements = wt_column_pack_elements_char;
        self->unpack_elements = wt_column_unpack_elements_char;
    } else {
        ret = EINVAL;
        goto out;
    }
    self->fixed_region_size = self->num_elements * self->element_size;
    if (self->num_elements == WT_VARIABLE) {
        self->fixed_region_size = WT_VARIABLE_OFFSET_SIZE 
            + WT_VARIABLE_COUNT_SIZE;
    }
    self->get_name = wt_column_get_name;
    self->get_description = wt_column_get_description;
    self->get_element_type = wt_column_get_element_type;
    self->get_element_size = wt_column_get_element_size;
    self->get_num_elements = wt_column_get_num_elements;
    self->free = wt_column_free; 
    *wtcp = self;
out:
    return ret;

}

/*==========================================================
 * Table object 
 *==========================================================
 */

static int
wt_table_add_column(wt_table_t *self, wt_column_t *col)
{
    int ret = 0;
    wt_column_t *last_col, **cols, *search;
    /* duplicate names are not permitted */
    if (self->get_column_by_name(self, col->name, &search) == 0) {
        ret = EINVAL;
        goto out;
    }
    /* make some space for the new column */
    self->num_columns++;
    cols = realloc(self->columns, self->num_columns * sizeof(wt_column_t *));
    if (cols == NULL) {
        ret = ENOMEM;
        goto out;
    }
    self->columns = cols;
    self->columns[self->num_columns - 1] = col;
    /* There is a slight blurring of the object boundaries here */ 
    if (self->num_columns == 1) {
        col->fixed_region_offset = 0;
    } else {
        last_col = self->columns[self->num_columns - 2];
        col->fixed_region_offset = last_col->fixed_region_offset 
                + last_col->fixed_region_size;
    }
    self->fixed_region_size += col->fixed_region_size;
out:
    return ret;
}

static int
wt_table_add_column_write_mode(wt_table_t *self, wt_column_t *col) 
{
    int ret = 0;
    if (self->mode != WT_WRITE) {
        ret = EINVAL;
        goto out;
    }
    ret = wt_table_add_column(self, col);
out:
    return ret;
}
    
static int
wt_table_open_writer(wt_table_t *self)
{
    int ret = 0;
    char *db_filename = strconcat(self->homedir, WT_BUILD_PRIMARY_DB_FILE); 
    wt_column_t *col;
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
    self->num_rows = 0;
    /* add the key column */
    // TODO add #defs for these constants.
    ret = wt_column_alloc(&col, WT_KEY_COL_NAME, WT_KEY_COL_DESCRIPTION, 
            WT_UINT, self->keysize, 1);
    wt_table_add_column(self, col); 

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
        col = self->columns[j];
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
    wt_column_t *col;
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
    ret = wt_column_alloc(&col, name, description, element_type, 
            element_size, num_elements);
    if (ret != 0) {
        goto out;
    }
    ret = wt_table_add_column(self, col);
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
    version = NULL;
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
    columns = NULL;
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
wt_table_read_num_rows(wt_table_t *self)
{
    int ret = 0;
    wt_column_t *id_col = self->columns[0];
    u_int64_t max_key = 0;
    DBC *cursor = NULL;
    DBT key, data;
    ret = self->db->cursor(self->db, NULL, &cursor, 0);
    if (ret != 0) {
        goto out;    
    }
    /* retrieve the last key from the DB */
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));  
    ret = cursor->get(cursor, &key, &data, DB_PREV);
    if (ret == 0) {
        id_col->unpack_elements(id_col, &max_key, key.data, 1);
        self->num_rows = max_key + 1;
    } else if (ret == DB_NOTFOUND) {
        self->num_rows = 0; 
    } 
out:
    /* Free the cursor */
    cursor->close(cursor);
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
    u_int32_t flags = DB_RDONLY|DB_NOMMAP;
    db_filename = strconcat(self->homedir, WT_PRIMARY_DB_FILE);
    if (db_filename == NULL) {
        ret = ENOMEM;
        goto out;
    }
    printf("opening table %s for reading\n", self->homedir);   
    self->mode = WT_READ;
    ret = self->db->open(self->db, NULL, db_filename, NULL, 
            DB_BTREE, flags, 0);
    if (ret != 0) {
        goto out;
    }
    ret = wt_table_read_schema(self);
    if (ret != 0) {
        goto out;
    }
    ret = wt_table_read_num_rows(self);
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

static int
wt_table_get_column_by_index(wt_table_t *self, u_int32_t index, 
        wt_column_t **col) 
{
    int ret = EINVAL;
    if (index < self->num_columns) {
        *col = self->columns[index];
        ret = 0;
    }
    return ret;
}

static int
wt_table_get_column_by_name(wt_table_t *self, const char *name, 
        wt_column_t **col) 
{
    int ret = EINVAL;
    u_int32_t index;
    for (index = 0; index < self->num_columns && ret != 0; index++) {
        if (strcmp(name, self->columns[index]->name) == 0) {
            *col = self->columns[index];
            ret = 0;
        }
    }
    return ret;
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
    
     
    return ret;
}

static int 
wt_table_add_row(wt_table_t *self, wt_row_t *row)
{
    int ret = 0;
    wt_column_t *id_col = self->columns[0];
    DBT key, data;
    ret = row->set_value(row, id_col, &self->num_rows, 1);
    if (ret != 0) {
        goto out;
    }
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
    key.data = row->data;
    key.size = self->keysize;
    data.data = row->data + self->keysize;
    data.size = row->size - self->keysize;
    
    /*
    printf("data size = %d\n", data.size);
    {unsigned int j;
        for (j = 0; j < row->size; j++) {
            printf("%d \n", ((char *) row->data)[j]);
        }   
    }
    */
    ret = self->db->put(self->db, NULL, &key, &data, 0);
    if (ret != 0) {
        goto out;
    }
    self->num_rows++;
    printf("added row\n");
out:
    return ret;
}

static int
wt_table_get_row(wt_table_t *self, u_int64_t row_id, wt_row_t *row)
{
    int ret = 0;
    DBT key, data;
    wt_column_t *id_col = self->columns[0];
    row->clear(row);
    ret = row->set_value(row, id_col, &row_id, 1);
    if (ret != 0) {
        goto out;
    }
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));  
    key.size = self->keysize;
    key.data = row->data;
    data.data = row->data + self->keysize;
    data.ulen = row->max_size - self->keysize;
    data.flags = DB_DBT_USERMEM;
    ret = self->db->get(self->db, NULL, &key, &data, 0);
out:
    return ret;
}

static int 
wt_table_get_num_rows(wt_table_t *self, u_int64_t *num_rows)
{
    *num_rows = self->num_rows;
    return 0;
}

static int 
wt_table_free(wt_table_t *self)
{
    unsigned int j;
    if (self != NULL) {
        if (self->db != NULL) {
            self->db->close(self->db, 0);
        }
        if (self->columns != NULL) {
            for (j = 0; j < self->num_columns; j++) {
                if (self->columns[j] != NULL) {
                    self->columns[j]->free(self->columns[j]);
                }
            }
            free(self->columns);
        }
        free(self);
    }
    return 0;
}

int 
wt_table_alloc(wt_table_t **wtp)
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
    self->fixed_region_size = 0;
    self->keysize = WT_DEFAULT_KEYSIZE;
    self->open = wt_table_open;
    self->add_column = wt_table_add_column_write_mode;
    self->set_cachesize = wt_table_set_cachesize;
    self->set_keysize = wt_table_set_keysize;
    self->get_column_by_index = wt_table_get_column_by_index;
    self->get_column_by_name = wt_table_get_column_by_name;
    self->close = wt_table_close;
    self->add_row = wt_table_add_row;
    self->get_num_rows = wt_table_get_num_rows;
    self->get_row = wt_table_get_row;
    self->free = wt_table_free;
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

