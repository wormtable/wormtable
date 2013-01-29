#include <db.h>

#define WT_WRITE    0
#define WT_READ     1

#define WT_PRIMARY_DB_FILE          "primary.db"
#define WT_BUILD_PRIMARY_DB_FILE    "__build_primary.db"

#define WT_COL_TYPE_UINT    0
#define WT_COL_TYPE_INT     1
#define WT_COL_TYPE_FLOAT   2
#define WT_COL_TYPE_CHAR    3


typedef struct {
    char *name;
    char *description;
    u_int32_t element_type;
    u_int32_t element_size;
    u_int32_t num_elements;
} wt_column_t;

typedef struct wt_table_t_t {
    const char *homedir;
    char *db_filename;
    u_int32_t num_columns;
    wt_column_t *columns;
    DB *db;
    u_int32_t mode;
    int (*open)(struct wt_table_t_t *wtt, const char *homedir, u_int32_t flags);
    int (*add_column)(struct wt_table_t_t *wtt, const char *name, 
            const char *description, u_int32_t element_type, 
            u_int32_t element_size, u_int32_t num_elements);
    int (*set_cachesize)(struct wt_table_t_t *wtt, u_int64_t bytes); 
    int (*close)(struct wt_table_t_t *wtt);
} wt_table_t;

typedef struct {
    wt_table_t *table;
    wt_column_t *columns;
    DB *secondaty_db;
} wt_index_t;


char * wt_strerror(int err);
int wt_table_create(wt_table_t **wttp);



