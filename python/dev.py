from __future__ import print_function

"""
Development code.
"""
import os
import sys

import _wormtable


def main():
    t = _wormtable.Table()
    print(t)
    t.open("test_table", _wormtable.WT_WRITE)
    t.close()
           
       
if __name__ == "__main__":
    main()

