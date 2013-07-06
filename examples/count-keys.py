#!/usr/bin/env python

"""
Count the number of distinct values for a given index
Usage: count-distinct.py homedir index
"""
from __future__ import print_function
from __future__ import division

import sys
import wormtable as wt


def count_keys(homedir, index):
    with wt.open_table(homedir) as t, t.open_index(index) as i:
        table = [[k,v] for k,v in i.counter().items()]
        assert(len(t) == sum(r[1] for r in table))
    return(table)


def main():
    table = count_keys(sys.argv[1], sys.argv[2])
    for r in table:
        print("\t".join(map(str, r)))
        
        
if __name__ == "__main__":
    main()
