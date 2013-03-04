#include <db.h>

#define WT_SCHEMA_VERSION           "0.5-dev"

#define WT_WRITE    0
#define WT_READ     1

#define WT_VARIABLE 0

#define WT_UINT    0
#define WT_INT     1
#define WT_FLOAT   2
#define WT_CHAR    3

#define WT_DEFAULT_KEYSIZE  4

#define WT_ERR_FATAL -1

/* These are linked to each other, and will probably change in favour 
 * of a more flexible addressing system in future versions.
 */
#define WT_MAX_ELEMENTS     256
#define WT_MAX_ROW_SIZE     65536
#define WT_VARIABLE_OFFSET_SIZE 2
#define WT_VARIABLE_COUNT_SIZE 1

#define WT_SCHEMA_FILE              "schema.xml"
#define WT_PRIMARY_DB_FILE          "primary.db"
#define WT_BUILD_PRIMARY_DB_FILE    "__build_primary.db"

#define WT_KEY_COL_NAME             "row_id"
#define WT_KEY_COL_DESCRIPTION      "key column"

typedef struct wt_column_t_t {
    char *name;
    char *description;
    u_int32_t element_type;
    u_int32_t element_size;
    u_int32_t num_elements;
    u_int32_t fixed_region_size;
    u_int32_t fixed_region_offset;
    int (*pack_elements)(struct wt_column_t_t *self, void *dest, void *elements,
            u_int32_t num_elements);
    int (*unpack_elements)(struct wt_column_t_t *self, void *elements, void *src,
            u_int32_t num_elements);
    /* Public API */
    int (*get_name)(struct wt_column_t_t *self, const char **name);
    int (*get_description)(struct wt_column_t_t *self, const char **description);
    int (*get_element_type)(struct wt_column_t_t *self, u_int32_t *element_type); 
    int (*get_element_size)(struct wt_column_t_t *self, u_int32_t *element_size); 
    int (*get_num_elements)(struct wt_column_t_t *self, u_int32_t *num_elements); 
    int (*free)(struct wt_column_t_t *self);
} wt_column_t;

typedef struct wt_schema_t_t {

} wt_schema_t;


typedef struct wt_row_t_t {
    void *data;
    u_int32_t size;
    u_int32_t max_size;
    struct wt_table_t_t *table;
    int (*set_value)(struct wt_row_t_t *wtr, wt_column_t *col, void *elements,
            u_int32_t num_elements);
    int (*get_value)(struct wt_row_t_t *wtr, wt_column_t *col, void *elements,
            u_int32_t *num_elements);
    int (*clear)(struct wt_row_t_t *wtr);
    int (*free)(struct wt_row_t_t *wtr);
} wt_row_t;

typedef struct wt_table_t_t {
    DB *db;
    const char *homedir;
    u_int32_t num_columns;
    u_int64_t num_rows;
    wt_column_t **columns;
    u_int32_t mode;
    u_int32_t keysize;
    u_int32_t fixed_region_size;
    int (*open)(struct wt_table_t_t *wtt, const char *homedir, u_int32_t flags);
    int (*close)(struct wt_table_t_t *wtt);
    int (*add_column)(struct wt_table_t_t *wtt, wt_column_t *col);
    int (*set_keysize)(struct wt_table_t_t *wtt, u_int32_t keysize); 
    int (*set_cachesize)(struct wt_table_t_t *wtt, u_int64_t bytes);
    int (*get_column_by_index)(struct wt_table_t_t *wtt, u_int32_t index, 
            wt_column_t **column);
    int (*get_column_by_name)(struct wt_table_t_t *wtt, const char *name, 
            wt_column_t **column);
    int (*add_row)(struct wt_table_t_t *wtt, wt_row_t *row);
    int (*get_num_rows)(struct wt_table_t_t *wtt, u_int64_t *num_rows);
    int (*get_num_columns)(struct wt_table_t_t *wtt, u_int32_t *num_columns);
    int (*get_row)(struct wt_table_t_t *wtt, u_int64_t row_id, wt_row_t *row);
    int (*free)(struct wt_table_t_t *wtt);
} wt_table_t;

typedef struct wt_index_t_t {
    DB *db;
    char *name;
    wt_table_t *table;
    wt_column_t **columns;
    u_int32_t num_columns;
    int (*open)(struct wt_index_t_t *wti, u_int32_t flags);
    int (*close)(struct wt_index_t_t *wti);
    int (*free)(struct wt_index_t_t *wti);
    int (*build)(struct wt_index_t_t *wti);
} wt_index_t;


typedef struct wt_cursor_t_t {
    wt_table_t *table;
    wt_index_t *index;
    DBC *cursor;
    int (*open)(struct wt_cursor_t_t *wtc, wt_index_t *index, u_int32_t flags);
    int (*close)(struct wt_cursor_t_t *wtc);
    int (*next)(struct wt_cursor_t_t *wtc, wt_row_t *row);
    int (*free)(struct wt_cursor_t_t *wtc);
} wt_cursor_t;

char * wt_strerror(int err);
int wt_table_alloc(wt_table_t **wttp);
int wt_index_alloc(wt_index_t **wtip, wt_table_t *wtt, wt_column_t **columns,
        u_int32_t num_columns);
int wt_column_alloc(wt_column_t **wtcp, const char *name, 
        const char *description, u_int32_t element_type, 
        u_int32_t element_size, u_int32_t num_elements);
int wt_row_alloc(wt_row_t **wtrp, wt_table_t *wtt, u_int32_t size);
int wt_cursor_alloc(wt_cursor_t **wtrp, wt_table_t *wtt);



