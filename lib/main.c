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
    char *command;
    wt_table_t *wtt;
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
        wtt->add_column(wtt, "test_int", "testing", WT_INT, 1, 1);
        wtt->add_column(wtt, "test_float", "testing", WT_FLOAT, 4, 1);
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
