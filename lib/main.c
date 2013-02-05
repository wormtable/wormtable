#include <string.h>
#include <stdlib.h>

#include "wormtable.h"

void 
handle_error(int err)
{
    printf("error: %d: '%s'\n", err, wt_strerror(err));
    exit(1);
}

int 
main(int argc, char **argv)
{
    int wt_ret = 0;
    int j;
    char *command;
    u_int64_t uint_val;
    int64_t int_val;
    double float_val;
    char *char_val;
    wt_table_t *wtt;
    wt_column_t *uint_col, *int_col, *float_col, *char_col;
    wt_row_t *row;
    //wt_value_t *val;
    if (argc < 2) {
        printf("argument required\n");
        return 1;
    }
    command = argv[1];
    wt_ret = wt_table_create(&wtt);
    if (wt_ret != 0) {
        return 1;
    }
    printf("allocated new table %p\n", wtt); 
    wt_ret = wtt->set_cachesize(wtt, 100);
    if (wt_ret != 0) {
        handle_error(wt_ret);
    }
    if (strstr(command, "read") != NULL) {
        wt_ret = wtt->open(wtt, "test_table/", WT_READ);
        if (wt_ret != 0) {
            handle_error(wt_ret);
        }
    } else if (strstr(command, "write") != NULL) {
        wt_ret = wtt->open(wtt, "test_table/", WT_WRITE);
        if (wt_ret != 0) {
            handle_error(wt_ret);
        }
        wtt->add_column(wtt, "test_uint", "testing", WT_UINT, 1, 1);
        wtt->add_column(wtt, "test_int", "testing", WT_INT, 1, 1);
        wtt->add_column(wtt, "test_float", "testing", WT_FLOAT, 4, 1);
        wtt->add_column(wtt, "test_str", "testing", WT_CHAR, 1, 0);
        wtt->alloc_row(wtt, &row);
        wtt->get_column(wtt, 1, &uint_col);
        wtt->get_column(wtt, 2, &int_col);
        wtt->get_column(wtt, 3, &float_col);
        wtt->get_column(wtt, 4, &char_col);
        for (j = 0; j < 10; j++) { 
            uint_val = 102;
            row->set_value(row, uint_col, &uint_val, 1);
            int_val = -101;
            row->set_value(row, int_col, &int_val, 1);
            float_val = 1.5; 
            row->set_value(row, float_col, &float_val, 1);
            char_val = "TESTING";
            row->set_value(row, char_col, char_val, strlen(char_val));
            
            wt_ret = wtt->add_row(wtt, row); 
            if (wt_ret != 0) {
                handle_error(wt_ret);
            }
            row->clear(row);
        }
        wtt->free_row(wtt, row);
            

    } else {
        printf("Unrecognised command");
        return 1;
    }
    wt_ret = wtt->close(wtt);
    if (wt_ret != 0) {
        handle_error(wt_ret);
    }
    return 0;
}
