#include <string.h>
#include <stdlib.h>

#include "wormtable.h"

void 
handle_error(int err)
{
    printf("error: %d: '%s'\n", err, wt_strerror(err));
    exit(1);
}

void 
generate_table(const char *table_name)
{
    int wt_ret = 0;
    int j;
    u_int64_t uint_val;
    int64_t int_val;
    double float_val;
    char *char_val;
    wt_table_t *wtt;
    wt_column_t *uint_col, *int_col, *float_col, *char_col;
    wt_row_t *row;
    wt_ret = wt_table_create(&wtt);
    if (wt_ret != 0) {
        handle_error(wt_ret); 
    }
    wt_ret = wtt->open(wtt, "test_table/", WT_WRITE);
    if (wt_ret != 0) {
        handle_error(wt_ret);
    }
    wtt->add_column(wtt, "test_uint", "testing", WT_UINT, 1, 1);
    wtt->add_column(wtt, "test_int", "testing", WT_INT, 4, 1);
    wtt->add_column(wtt, "test_float", "testing", WT_FLOAT, 4, 1);
    wtt->add_column(wtt, "test_str", "testing", WT_CHAR, 1, 0);
    wtt->alloc_row(wtt, &row);
    wtt->get_column(wtt, 1, &uint_col);
    wtt->get_column(wtt, 2, &int_col);
    wtt->get_column(wtt, 3, &float_col);
    wtt->get_column(wtt, 4, &char_col);
    for (j = 0; j < 500; j++) { 
        uint_val = j;
        row->set_value(row, uint_col, &uint_val, 1);
        int_val = -1 * j;
        row->set_value(row, int_col, &int_val, 1);
        float_val = 1.5; 
        row->set_value(row, float_col, &float_val, 1);
        char_val = "TESTING";
        row->set_value(row, char_col, char_val, strlen(char_val));
        {
            u_int32_t tmp;
            char buff[1024];
            row->get_value(row, uint_col, &uint_val, &tmp);
            printf("got uint value: %lu\n", uint_val);
            row->get_value(row, int_col, &int_val, &tmp);
            printf("got int value: %ld\n", int_val);
            row->get_value(row, float_col, &float_val, &tmp);
            printf("ERROR:: got float value: %f\n", float_val);
            row->get_value(row, char_col, buff, &tmp);
            buff[tmp] = '\0';
            printf("got char  value '%s'\n", buff);
        }
        wt_ret = wtt->add_row(wtt, row); 
        if (wt_ret != 0) {
            handle_error(wt_ret);
        }
        row->clear(row);
    }
    wtt->free_row(wtt, row);
    wt_ret = wtt->close(wtt);
    if (wt_ret != 0) {
        handle_error(wt_ret);
    }
}

void 
dump_table(const char *table_name)
{
    int wt_ret;
    u_int64_t num_rows;
    wt_table_t *wtt;
    wt_ret = wt_table_create(&wtt);
    if (wt_ret != 0) {
        handle_error(wt_ret); 
    }
    wt_ret = wtt->open(wtt, table_name, WT_READ);
    if (wt_ret != 0) {
        handle_error(wt_ret);
    }
    wt_ret = wtt->get_num_rows(wtt, &num_rows);
    if (wt_ret != 0) {
        handle_error(wt_ret);
    }
    printf("num rows = %ld\n", num_rows);


    wt_ret = wtt->close(wtt);
    if (wt_ret != 0) {
        handle_error(wt_ret);
    }
}

int 
main(int argc, char **argv)
{
    char *command;
    if (argc < 2) {
        printf("argument required\n");
        return 1;
    }
    command = argv[1];
    if (strstr(command, "read") != NULL) {
        dump_table("test_table/");
    } else if (strstr(command, "write") != NULL) {
        generate_table("test_table/");           
    } else {
        printf("Unrecognised command");
        return 1;
    }
    return 0;
}
