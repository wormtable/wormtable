"""
This is an example file get started with building a wormtable for 
a VCF file, dumping out the values, and reading indexes.
"""
from __future__ import print_function

import sys

import wormtable as wt

def read_position_index(homedir, chromosome, position):
    """
    Read the index on CHROM and POS, which is assumed to already exist.
    This proceeds by opening the table, and then the index. (The API is 
    clumsy here and will be changed). We then specifiy the rows we want 
    to read back from the index. An index is simply a sorting of the table 
    in the order defined by the columns, and so we read it back in sorted
    order. We specify the minimum values for the two columns that we 
    are using here as parameters and also retrieve the FILTER column, 
    as an example. Rows are returned as tuples, with the value corresponding 
    to the column provided in read_cols.
    """
    table = wt.Table(homedir) 
    indexed_columns = ['CHROM', 'POS']
    index = wt.Index(table, indexed_columns)
    index.open()
    min_val = (chromosome, position)
    read_cols = indexed_columns + ["FILTER"]
    for row in index.get_rows(read_cols, min_val): 
        print(row)
    index.close()
    table.close()

def dump_table(homedir):
    """
    Dumps the wormtable in the specified directory to stdout row-by-row.
    
    This uses the get_row(j) function, which returns the jth row of the 
    table as a dictionary mapping column name to values.
    """
    table = wt.Table(homedir)
    print("num rows = ", table.get_num_rows())
    for j in range(table.get_num_rows()):
        row = table.get_row(j)
        print("row", j)
        for k, v in row.items():
            print("\t", k, "->", v)
    table.close()

def main():
    homedir = sys.argv[1]
    # Uncomment this is you want to just dump out the whole table in 
    # the native order.
    #dump_table(homedir)
    
    # Print out some columns from the table using these values to 
    # 'seek' to the place you're interested in within the table.
    # NOTE: all strings must be specified as bytes literals at the 
    # moment (i.e. prefixed with a b). This will probably change 
    # at some point.
    read_position_index(homedir, b"1", 3220987)
       
if __name__ == "__main__":
    main()

