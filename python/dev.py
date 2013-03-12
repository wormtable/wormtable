from __future__ import print_function

"""
Development code.
"""
import os
import sys
import shutil 

import _wormtable


def main():
    test_table = "../test_table/"
    shutil.rmtree(test_table)
    t = _wormtable.Table()
    #t.open(test_table, _wormtable.WT_READ)
    t.open(test_table, _wormtable.WT_WRITE)
    t.add_column("uint_1_1", "testing", _wormtable.WT_UINT, 1, 1)
    t.add_column("int_1_1", "testing", _wormtable.WT_INT, 1, 1)
    row = _wormtable.Row(t)
    for j in range(t.get_num_columns()):
        col = t.get_column_by_index(j)
        print("Column at index:", col.get_name())
        print("name:", col.get_name())
        print("description:", col.get_description())
        print("element_type:", col.get_element_type())
        print("element_size:", col.get_element_size())
        print("num_elements:", col.get_num_elements())
    
    col_uint_1_1 = t.get_column_by_index(1)
    col_int_1_1 = t.get_column_by_index(2)
    row.clear()
    row.set_value(col_uint_1_1, 2**65)
    row.set_value(col_int_1_1, 1)
    t.close()
           
       
if __name__ == "__main__":
    main()

