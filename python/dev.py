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
    t.open(test_table, _wormtable.WT_WRITE)
    t.add_column("uint_1_1", "testing", _wormtable.WT_UINT, 1, 1)
    t.add_column("int_1_1", "testing", _wormtable.WT_INT, 2, 1)
    t.add_column("float_4_1", "testing", _wormtable.WT_FLOAT, 4, 1)
    t.add_column("char_1_0", "testing", _wormtable.WT_CHAR, 1, 
        _wormtable.WT_VARIABLE)
    col = t.get_column_by_index(0)
    print("Column at index:", col)
    t.close()
           
       
if __name__ == "__main__":
    main()

