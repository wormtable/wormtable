#include <stdlib.h>

#include "wormtable.h"

void 
handle_error(int err)
{
    printf("error: %d: '%s'\n", err, wt_strerror(err));
    exit(1);
}

int 
main(void)
{
    int wt_ret = 0;
    printf("wormable main\n");
    wt_table_t *wtt;
    wt_ret = wt_table_create(&wtt);
    if (wt_ret != 0) {
        return 1;
    }
    printf("allocated new table %p\n", wtt); 
    wt_ret = wtt->set_cachesize(wtt, 100);
    if (wt_ret != 0) {
        handle_error(wt_ret);
    }
    
    wt_ret = wtt->open(wtt, "test_table/", WT_READ);
    if (wt_ret != 0) {
        handle_error(wt_ret);
    }
    //wtt->add_column(wtt, "test_int", "testing", 0, 0, 0);
    //wtt->add_column(wtt, "test_float", "testing", 0, 0, 0);
    wtt->close(wtt);
    return 0;
}
