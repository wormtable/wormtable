"""
This is an example file get started with building a wormtable for 
a VCF file, dumping out the values, and making and reading 
indexes.
"""
from __future__ import print_function
import os
import sys

import vcfdb
import collections

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

class IndexReader(object):
    """
    Temporary class used to abstract away messy details of 
    the current implementation of index reading.
    """
    def __init__(self, homedir, column_names):
        cache_size = 6 * 2**30
        self.__table = vcfdb.Table(homedir, cache_size)
        schema = self.__table.get_schema()
        columns = [schema.get_column(name.encode()) for name in column_names] 
        cache_size = 2 * 2**30
        self.__index = vcfdb.Index(self.__table, columns, cache_size)
        self.__index.open()

    def get_rows(self, columns, min_val=None, max_val=None, row_type=tuple):
        """
        Iterates over the rows in the index starting at min_val
        and ending at max_val.
        """
        schema = self.__table.get_schema()
        read_cols = [schema.get_column(name.encode()) for name in columns] 
        nt = collections.namedtuple("Row", columns)
        for row in self.__index.get_rows(read_cols, min_val, max_val): 
            if row_type == tuple:
                yield row
            elif row_type == dict:
                yield dict(zip(columns, row))
            else:
                yield nt(*row)
                
    def close(self):
        self.__index.close()
        self.__table.close()

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
    index.build(progress, max(1, int(n / 1000)))
    monitor.finish()

def read_position_index(homedir, chromosome, position):
    indexed_columns = ['CHROM', 'POS']
    ir = IndexReader(homedir, indexed_columns)
    min_val = (chromosome, position)
    total_rows = 0
    read_cols = indexed_columns + ["FILTER"]
    before = time.time()
    #for row in ir.get_rows(read_cols, min_val, row_type=collections.namedtuple): 
    for row in ir.get_rows(read_cols, min_val, row_type=dict): 
    #for row in ir.get_rows(read_cols, min_val): 
        #print(row.FILTER)
        #print(row)
        total_rows += 1
        if total_rows > 1000000:
            break
    duration = time.time() - before
    if duration > 0:
        print("processed rows at ", total_rows / duration, " rows/sec")
    ir.close()

def read_genotype_index(homedir, genotypes):
    indexed_columns = [s + "_GT" for s in genotypes]
    ir = IndexReader(homedir, indexed_columns) 
    num_genotypes = len(genotypes)
    read_cols = [s + "_GQ" for s in genotypes] 
    min_val = tuple(b"0/1" for s in genotypes)
    max_val = min_val 
    min_qual = 50.0
    total_rows = 0
    filtered_rows = 0
    before = time.time()
    for row in ir.get_rows(read_cols + indexed_columns + ["CHROM", "POS"], min_val, max_val): 
        total_rows += 1
        #print(row[num_genotypes:]) 
        print(row) 
        if all(row[j] >= min_qual for j in range(num_genotypes)):
            filtered_rows += 1
    ir.close()
    duration = time.time() - before
    print("read rows = ", total_rows)
    print("filtered rows = ", filtered_rows)
    if duration > 0.0:
        print("processed rows at ", total_rows / duration, " rows/sec")
    

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
        # Standard CHROM/POS index example
        indexed_columns = [b'CHROM', b'POS']
        # build_index(homedir, indexed_columns) 
        # read_position_index(homedir, b"X", 0)
        # Genotype index example
        genotypes = ['Fam', 'H12', 'H14', 'H15', 'H24', 'H26', 'H27',
                'H28', 'H30', 'H34', 'H36']
        indexed_columns = [s.encode() + b"_GT" for s in genotypes]
        build_index(homedir, indexed_columns) 
        read_genotype_index(homedir, genotypes)
       
if __name__ == "__main__":
    main()

