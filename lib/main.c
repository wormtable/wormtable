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
    double v[2];
    char *char_val;
    wt_table_t *wtt;
    wt_column_t *uint_col, *int_col, *float_col, *double_2_col, *char_col;
    wt_row_t *row;
    wt_ret = wt_table_alloc(&wtt);
    if (wt_ret != 0) {
        handle_error(wt_ret); 
    }
    wt_ret = wtt->open(wtt, "test_table/", WT_WRITE);
    if (wt_ret != 0) {
        handle_error(wt_ret);
    }
    wt_column_alloc(&uint_col, "uint_1_1", "testing", WT_UINT, 1, 1);
    wt_column_alloc(&int_col, "int_1_1", "testing", WT_INT, 1, 1);
    wt_column_alloc(&float_col, "float_4_1", "testing", WT_FLOAT, 4, 1);
    wt_column_alloc(&double_2_col, "float_8_2", "testing", WT_FLOAT, 8, 2);
    wt_column_alloc(&char_col, "str_1_0", "testing", WT_CHAR, 1, 0);
    wtt->add_column(wtt, uint_col);
    wtt->add_column(wtt, int_col);
    wtt->add_column(wtt, float_col);
    wtt->add_column(wtt, double_2_col);
    wtt->add_column(wtt, char_col);
    wt_row_alloc(&row, wtt, WT_MAX_ROW_SIZE);
    for (j = 0; j < 5; j++) { 
        row->clear(row);
        uint_val = j;
        wt_ret = row->set_value(row, 1, &uint_val, 1);
        if (wt_ret != 0) {
            handle_error(wt_ret);
        }
        int_val = -1 * j;
        row->set_value(row, 2, &int_val, 1);
        float_val = j; 
        row->set_value(row, 3, &float_val, 1);
        char_val = "TESTING";
        row->set_value(row, 5, char_val, strlen(char_val));
        v[0] = -j * 3.4;
        v[1] = j * 3.4;
        row->set_value(row, 4, v, 2);
        wt_ret = wtt->add_row(wtt, row); 
        if (wt_ret != 0) {
            handle_error(wt_ret);
        }
    }
    row->free(row);
    wt_ret = wtt->close(wtt);
    if (wt_ret != 0) {
        handle_error(wt_ret);
    }
    wtt->free(wtt);
}

void 
dump_table(const char *table_name)
{
    int wt_ret;
    u_int64_t j, num_rows;
    wt_table_t *wtt;
    wt_column_t *uint_col, *int_col, *float_col, *char_col, *double_2_col;
    wt_row_t *row;
    
    const char *description, *name;
    double float_val, v[2];
    u_int64_t uint_val;
    int64_t int_val;
    u_int32_t tmp;
    char buff[1024];
    
    wt_ret = wt_table_alloc(&wtt);
    if (wt_ret != 0) {
        handle_error(wt_ret); 
    }
    wt_ret = wtt->open(wtt, table_name, WT_READ);
    if (wt_ret != 0) {
        handle_error(wt_ret);
    }
    wt_row_alloc(&row, wtt, WT_MAX_ROW_SIZE);
    wtt->get_column_by_name(wtt, "uint_1_1", &uint_col);
    wtt->get_column_by_name(wtt, "int_1_1", &int_col);
    wtt->get_column_by_index(wtt, 3, &float_col);
    wtt->get_column_by_index(wtt, 4, &double_2_col);
    wtt->get_column_by_index(wtt, 5, &char_col);
    wt_ret = wtt->get_num_rows(wtt, &num_rows);
    uint_col->get_name(uint_col, &name);
    uint_col->get_description(uint_col, &description);
    printf("%s: %s\n", name, description); 
    
    if (wt_ret != 0) {
        handle_error(wt_ret);
    }
    printf("num rows = %ld\n", num_rows);
    for (j = 0; j < num_rows; j++) {
        wt_ret = wtt->get_row(wtt, j, row); 
        printf("got row %lu: ret = %d\n", j, wt_ret);
        if (wt_ret != 0) {
            handle_error(wt_ret);
        }
        row->get_value(row, 1, &uint_val, &tmp);
        printf("got uint value: %lu\n", uint_val);
        row->get_value(row, 2, &int_val, &tmp);
        printf("got int value: %ld\n", int_val);
        row->get_value(row, 3, &float_val, &tmp);
        printf("got float value: %f\n", float_val);
        row->get_value(row, 5, buff, &tmp);
        buff[tmp] = '\0';
        printf("got char  value '%s'\n", buff);
        row->get_value(row, 4, v, &tmp);
        printf("got double2 val: %f %f\n", v[0], v[1]);

    }
    wt_ret = wtt->close(wtt);
    if (wt_ret != 0) {
        handle_error(wt_ret);
    }
    row->free(row);
    wtt->free(wtt);
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
