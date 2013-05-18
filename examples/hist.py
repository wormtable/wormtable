"""
Create a histogram for a numeric index.
Usage: hist.py homedir index
"""
from __future__ import print_function
from __future__ import division

import sys
import wormtable as wt

table = wt.Table(sys.argv[1]) 
index = wt.Index(table, [sys.argv[2]])
index.open()
for x in index.get_distinct_values():
    print(x,"\t",index.get_num_rows(x))
   
