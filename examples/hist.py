#!/usr/bin/python3

"""
Create a histogram for a numeric index.
Usage: hist.py homedir index
"""

import sys
import wormtable as wt

table = wt.Table(sys.argv[1]) 
index = wt.Index(table, [sys.argv[2]])
index.open()
[print(x,"\t",index.get_num_rows(x)) for x in index.get_distinct_values()]
   
