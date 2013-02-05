#include <db.h>

#define WT_SCHEMA_VERSION           "0.5-dev"

#define WT_WRITE    0
#define WT_READ     1

#define WT_VARIABLE 0

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

#define WT_UINT    0
#define WT_INT     1
#define WT_FLOAT   2
#define WT_CHAR    3

#define WT_DEFAULT_KEYSIZE  4

#define WT_ERR_FATAL -1


typedef struct wt_column_t_t {
    char *name;
    char *description;
    u_int32_t element_type;
    u_int32_t element_size;
    u_int32_t num_elements;
    u_int32_t fixed_region_offset;
    u_int32_t fixed_region_size;
    int (*pack_elements)(struct wt_column_t_t *self, void *dest, void *elements,
            u_int32_t num_elements);
} wt_column_t;

typedef struct wt_row_t_t {
    void *data;
    u_int32_t size;
    u_int32_t fixed_region_size;
    int (*set_value)(struct wt_row_t_t *wtr, wt_column_t *col, void *elements,
            u_int32_t num_elements);
    int (*clear)(struct wt_row_t_t *wtr);
    /* not implemented */
    //int (*get_value)(struct wt_row_t_t *wtr, wt_column_t *col, wt_value_t *val);
} wt_row_t;

typedef struct wt_table_t_t {
    const char *homedir;
    u_int32_t num_columns;
    u_int64_t num_rows;
    wt_column_t *columns;
    DB *db;
    u_int32_t mode;
    u_int32_t keysize;
    int (*open)(struct wt_table_t_t *wtt, const char *homedir, u_int32_t flags);
    int (*close)(struct wt_table_t_t *wtt);
    int (*add_column)(struct wt_table_t_t *wtt, const char *name, 
            const char *description, u_int32_t element_type, 
            u_int32_t element_size, u_int32_t num_elements);
    int (*set_keysize)(struct wt_table_t_t *wtt, u_int32_t keysize); 
    int (*set_cachesize)(struct wt_table_t_t *wtt, u_int64_t bytes);
    int (*get_column)(struct wt_table_t_t *wtt, u_int32_t col_id, 
            wt_column_t **column);
    int (*alloc_row)(struct wt_table_t_t *wtc, wt_row_t **row);
    int (*free_row)(struct wt_table_t_t *wtc, wt_row_t *row);
    int (*add_row)(struct wt_table_t_t *wtt, wt_row_t *row);
    /* not implemented */
    int (*get_num_rows)(struct wt_table_t_t *wtt, u_int64_t *num_rows);
    int (*get_row)(struct wt_table_t_t *wtt, u_int64_t row_id, wt_row_t *row);
} wt_table_t;

typedef struct {
    wt_table_t *table;
    wt_column_t *columns;
    DB *db;
} wt_index_t;


char * wt_strerror(int err);
int wt_table_create(wt_table_t **wttp);



