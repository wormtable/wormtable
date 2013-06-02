"""
This is an example file get started with building a wormtable for 
a VCF file, dumping out the values, and reading indexes.
"""
from __future__ import print_function
from __future__ import division 

import sys

import numpy as np
import wormtable as wt

def read_position_index(homedir, chromosome, position):
    """
    Read the index on CHROM and POS, which is assumed to already exist.
    """
    with wt.open_table(homedir) as t, t.open_index("CHROM+POS") as i:
        cursor = t.cursor(["CHROM", "POS", "FILTER"], i)
        cursor.set_min(chromosome, position)
        for row in cursor:
            print(row)

def read_filter_index(homedir):
    """
    Example demonstrating the use of the counter method on indexes
    """
    with wt.open_table(homedir) as t, t.open_index("FILTER") as i:
        c = i.counter()
        total = 0
        for k, v in c.items():
            print(v, "\t", k)
            total += v
        assert(len(t) == total)

def min_max_example(homedir):
    """
    A straightforward example of using index.get_min and get_max.
    """
    name = "CHROM+POS+QUAL[10]"
    with wt.open_table(homedir) as t, t.open_index(name) as i:
    
        print("whole index: ")
        print("\t", i.get_min(), "\t", i.get_max())
        # Now, let's get the min and max of the two chromosomes
        print("Individual chromosomes:")
        print("\t", i.get_min('1'), "\t", i.get_max('1'))
        print("\t", i.get_min('Y'), "\t", i.get_max('Y'))
        # This can be extended indefinitely, matching as many columns 
        # as are in the index
        print("\t", i.get_min('1', 3000055), "\t", i.get_max('1', 3000555))


def dump_table(homedir):
    """
    Dumps the wormtable in the specified directory to stdout row-by-row.
    
    This uses the get_row(j) function, which returns the jth row of the 
    table as a dictionary mapping column name to values.
    """
    with wt.open_table(homedir) as t:
        r = 0
        for row in t: 
            r += 1
            print(row)
        print("num rows = ", len(t), r)
    
def main():
    homedir = sys.argv[1]
    # Uncomment this is you want to just dump out the whole table in 
    # the native order.
    # dump_table(homedir)
    
    #read_position_index(homedir, b"1", 1328637)
    #read_filter_index(homedir)
    min_max_example(homedir)

if __name__ == "__main__":
    main()

