"""
This is an example file get started with building a wormtable for 
a VCF file, dumping out the values, and making and reading 
indexes.
"""
from __future__ import print_function
import os
import sys

import vcfdb


import time

class ProgressMonitor(object):
    """
    Class representing a progress monitor for a terminal based interface.
    """
    def __init__(self, total):
        self.__total = total
        self.__progress_width = 40
        self.__bar_index = 0
        self.__bars = "/-\\|"
        self.__start_time = time.clock()

    def update(self, processed):
        """
        Updates this progress monitor to display the specified number 
        of processed items.
        """
        complete = processed / self.__total
        filled = int(complete * self.__progress_width)
        spaces = self.__progress_width - filled 
        bar = self.__bars[self.__bar_index]
        self.__bar_index = (self.__bar_index + 1) % len(self.__bars)
        elapsed = time.clock() - self.__start_time
        rate = processed / elapsed
        s = '\r[{0}{1}] {2:2.2f}% @{3:4.2G} rows/s {4}'.format('#' * filled, 
            ' ' * spaces, complete * 100, rate, bar)
        print(s, end="")
    
    def finish(self):
        """
        Completes the progress monitor.
        """
        print()

def build_index(homedir, column_names):
    print("building index on ", column_names)
    cache_size = 1 * 2**30
    table = vcfdb.Table(homedir, cache_size)
    schema = table.get_schema()
    columns = [schema.get_column(name) for name in column_names] 
    cache_size = 8 * 2**30
    index = vcfdb.Index(table, columns, cache_size)
    n = table.get_num_rows()
    monitor = ProgressMonitor(n)
    def progress(processed_rows):
        monitor.update(processed_rows)
    index.build(progress, int(n / 1000))
    monitor.finish()

def read_index(homedir, column_names):
    cache_size = 4 * 2**30
    table = vcfdb.Table(homedir, cache_size)
    schema = table.get_schema()
    columns = [schema.get_column(name) for name in column_names] 
    cache_size = 4 * 2**30
    index = vcfdb.Index(table, columns, cache_size)
    cols = [b"CHROM", b"POS", b"H15_GQ", b"H24_GQ"]
    read_cols = [schema.get_column(c) for c in cols] + columns
    index.open()
    min_val = (b"0/1", b"0/1") 
    max_val = (b"0/1", b"0/1") 
    min_qual = 50.0
    for row in index.get_rows(read_cols, min_val, max_val): 
        if row[2] >= min_qual and row[3] >= min_qual:
            print(row)
    index.close()

def build_vcf(homedir, vcf_file):
    input_schema = os.path.join(homedir, "input_schema.xml")
    schema = vcfdb.vcf_schema_factory(vcf_file)
    schema.write_xml(input_schema)
    # In the command line tool we'll optionally stop here 
    # and allow the user to edit the schema. This means 
    # we don't have to generate the 'perfect' vcf schema.
    dbb = vcfdb.VCFTableBuilder(homedir, input_schema)
    dbb.set_cache_size(8 * 2**30) # 8 gigs - bigger is better
    dbb.set_buffer_size(64 * 2**20) # 64 megs
    dbb.build(vcf_file)
    
def print_table(homedir):
    table = vcfdb.Table(homedir)
    print("num rows = ", table.get_num_rows())
    v = 0
    for j in range(table.get_num_rows()):
        row = table.get_row(j)
        print("row", j)
        for k, v in row.items():
            print("\t", k, "->", v)
    table.close()

def main():

    if len(sys.argv) == 3: 
        homedir = sys.argv[1] 
        vcf_file = sys.argv[2]
        build_vcf(homedir, vcf_file)
    elif len(sys.argv) == 2:
        homedir = sys.argv[1]
        #print_table(homedir)
        indexed_columns = [b'H15_GT', b'H24_GT']
        build_index(homedir, indexed_columns) 
        #read_index(homedir, indexed_columns)
       
if __name__ == "__main__":
    main()

