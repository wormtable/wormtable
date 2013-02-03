#include <db.h>

#define WT_SCHEMA_VERSION           "0.5-dev"

#define WT_WRITE    0
#define WT_READ     1

#define WT_SCHEMA_FILE              "schema.xml"
#define WT_PRIMARY_DB_FILE          "primary.db"
#define WT_BUILD_PRIMARY_DB_FILE    "__build_primary.db"

#define WT_UINT    0
#define WT_INT     1
#define WT_FLOAT   2
#define WT_CHAR    3

#define WT_DEFAULT_KEYSIZE  4

#define WT_ERR_FATAL -1


typedef struct {
    char *name;
    char *description;
    u_int32_t element_type;
    u_int32_t element_size;
    u_int32_t num_elements;
} wt_column_t;

typedef struct wt_row_t_t {
    void *data;
    u_int32_t size;
    /* not implemented */
    int (*get_elements)(struct wt_row_t_t *wtr, wt_column_t *column, 
            void *elements, int *num_elements);
} wt_row_t;

typedef struct wt_table_t_t {
    const char *homedir;
    u_int32_t num_columns;
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
    /* not implemented */
    int (*get_num_rows)(struct wt_table_t_t *wtt, u_int64_t *num_rows);
    int (*get_row)(struct wt_table_t_t *wtt, u_int64_t row_id, wt_row_t *row);
    int (*append_row)(struct wt_table_t_t *wtt, wt_row_t *row);
} wt_table_t;

typedef struct {
    wt_table_t *table;
    wt_column_t *columns;
    DB *db;
} wt_index_t;


char * wt_strerror(int err);
int wt_table_create(wt_table_t **wttp);



