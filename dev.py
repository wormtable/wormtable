from __future__ import print_function

"""
This is an example file get started with building a wormtable for 
a VCF file, dumping out the values, and making and reading 
indexes.
"""
import os
import sys

import vcfdb


def build_index(homedir, column_names):
    table = vcfdb.Table(homedir)
    schema = table.get_schema()
    columns = [schema.get_column(name) for name in column_names] 
    cache_size = 8 * 2**30
    index = vcfdb.Index(table, columns, cache_size)
    n = table.get_num_rows()
    # Something snazzier than this will be used for the admin tool
    # but this gives you an idea of how things are going anyway.
    def progress(processed_rows):
        print("index built: {0:.2f}%".format(100 * processed_rows / n));
    # emit progress as percentage
    index.build(progress, int(n / 100))
    
def read_index(homedir, column_names):
    table = vcfdb.Table(homedir)
    schema = table.get_schema()
    columns = [schema.get_column(name) for name in column_names] 
    cache_size = 1 * 2**30
    index = vcfdb.Index(table, columns, cache_size)
    read_cols = [schema.get_column(c) for c in [b"CHROM", b"POS"]] + columns
    index.open()
    min_val = (b"0/1", b"0/1", 10.0) 
    max_val = (b"0/1", b"0/1", 10000.0) 
    for row in index.get_rows(read_cols, min_val, max_val): 
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
        indexed_columns = [b'H15_GT', b'H24_GT', b"H15_GQ"]
        #build_index(homedir, indexed_columns) 
        read_index(homedir, indexed_columns)
       
if __name__ == "__main__":
    main()

