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
    col = _wormtable.Column("uint_1_1", "testing", _wormtable.WT_UINT, 1, 1)
    t.add_column(col)
    col = t.get_column_by_index(0)
    print("Column at index:", col.get_name())
    t.close()
           
       
if __name__ == "__main__":
    main()

