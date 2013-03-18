from __future__ import print_function

"""
Development code.
"""
import os
import sys
import shutil 

import _wormtable

def write_table(table_name):
    shutil.rmtree(table_name)
    t = _wormtable.Table()
    #t.open(test_table, _wormtable.WT_READ)
    t.open(table_name, _wormtable.WT_WRITE)
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
    for j in range(10):
        row.clear()
        row.set_value(col_uint_1_1, j)
        row.set_value(col_int_1_1, -j)
        t.add_row(row) 

    t.close()

def read_table(table_name):
    t = _wormtable.Table()
    t.open(table_name, _wormtable.WT_READ)
    cols = [t.get_column_by_index(j) for j in range(t.get_num_columns())]
    row = _wormtable.Row(t)
    num_rows = t.get_num_rows()
    print("num_rows = ",  num_rows)
    for j in range(num_rows):
        print("got row:", j)
        t.get_row(j, row)
        for c in cols:
            print("\t", c.get_name(), "->", row.get_value(c))
    
    t.close()

def main():
    test_table = "../test_table/"
    # write_table(test_table) 
    read_table(test_table)       
if __name__ == "__main__":
    main()

