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

def read_filter_index(homedir):
    """
    Example demonstrating the use of get_distinct_values and get_num_rows 
    on the Index class. Requires the existence of and index on the FILTER
    column.
    """
    table = wt.Table(homedir) 
    index = wt.Index(table, ['FILTER'])
    index.open()
    total = 0
    for v in index.get_distinct_values():
        c = index.get_num_rows(v)
        total += c
        print(c, "\t->", v) 
    # verify that this count is the same as the total number of rows
    assert(total == table.get_num_rows())
    # See if we return 0 for a non-existent key correctly.
    v = b"NO SUCH VALUE" 
    c = index.get_num_rows(v)
    print(c, "\t->", v) 
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

def allele_frequency_example(homedir):
    table = wt.Table(homedir) 
    index = wt.Index(table, ["CHROM", "POS"]) 
    index.open()
    chromosome = b"2"
    min_val = (chromosome, 0)
    max_val = (chromosome, 2**32) # some big value
    
    # Two casese are covered in the code here; when we have INFO_AF and 
    # when we don't. Uncomment the code to try each one out.
    # If we have INFO_AF, we may as well use it
    # read_cols = ['INFO_AF'] 
    
    # Otherwise, we list the genotypes. We are only making an example, so 
    # it's totally fine to be specific to a given VCF. It would be good if
    # this VCF was publically available, so we can say 'wget http://...vcf'.
    genotypes = ["Fam_GT", "H12_GT", "H14_GT", "H15_GT", "H24_GT", "H26_GT",
            "H27_GT", "H28_GT", "H30_GT", "H34_GT", "H36_GT"]
    num_genotypes = len(genotypes)
    read_cols = ["ALT"] + genotypes 
    j = 0
    # Allocate a zeroed numpy array to store the allele frequencies for the 
    # whole chromosome. This is 100 million values, but still requires less
    # than a gig of memory. That's memory well spent!
    a = np.zeros(int(1e8)) 
    for row in index.get_rows(read_cols, min_val, max_val): 
        # this is the code for when we have INFO_AF
        #if row[0] != None:
        #    af = row[0][0]
    
        # And this is code for when we don't
        if row[0] != '':
            af = 0.0
            # This is wrong, but should be easy to correct...
            for g in row[1:]:
                if g != b"0/0":
                    af += 1
                    if g == b"1/1":
                        af += 1
            af /= 2 * num_genotypes
        # Now insert the allele frequency into our big array. 
        a[j] = af
        j += 1 
    index.close()
    table.close()
    # trim off the end of the array we didn't use
    a = a[:j]
    # now we have the full array of allele frequecy values, use numpy 
    # to do the stats on it. This would be by far the most efficient 
    # way to do the sliding window stuff.
    print("total rows = ", j)
    print("mean = ", np.mean(a))
    print("var = ", np.var(a))




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
    #read_position_index(homedir, b"1", 3220987)
    #read_filter_index(homedir)
    allele_frequency_example(homedir)


if __name__ == "__main__":
    main()

