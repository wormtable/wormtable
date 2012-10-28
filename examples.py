"""
Examples of using the vcfdb module to quickly access particular
records in a VCF file.
"""
import sys
import time
import random 

import vcfdb

def print_record(r):
    print(r["record"][:50])
    #print(r[:50])

def min_max_example(vdb):
    """
    Prints out the min and max values for each column.
    """
    for c in vdb.get_indexed_columns():
        col_min = vdb.get_min(c)
        col_max = vdb.get_max(c)
        print("{0}:\tmin = {1}\tmax = {2}".format(c, col_min, col_max)) 

def compound_column_example(vdb):
    """
    Illustrates the compound column search.
    """
    column = ("H27_GT", "H27_GQ")
    min_value = ("0/1", 15)
    max_value = ("1/1", 0)
    q = (column, min_value, max_value) 
    #for r in vdb.get_records(query_range=q, max_records=20):
    #    #print_record(r)
    #    print(r[vcfdb.POS], ":", r["H27_GT"], ":", r["H27_GQ"]) 
    print("compound query:", q)
    c, s = time_and_count(vdb.get_records(query_range=q))
    print("\tfound = ", c, " in ", s, " seconds")
        

def get_record_example(vdb):
    """
    Simple example of the get_record interface.
    """
    min_record_id = vdb.get_min(vcfdb.RECORD_ID)
    print_record(vdb.get_record(min_record_id))
    max_record_id = vdb.get_max(vcfdb.RECORD_ID)
    print_record(vdb.get_record(max_record_id))
    try:
        print_record(vdb.get_record(max_record_id + 1))
    except Exception as e:
        print("Non existant record raised:", e)    

def range_query_example(vdb):
    """
    Shows an example of running range queries.
    """
    first_key = vdb.get_min(vcfdb.POS)
    last_key = vdb.get_max(vcfdb.POS)
    random_key = random.randint(first_key, last_key)
    
    q = (vcfdb.POS, random_key, random_key + 100)    
    j = 0 
    print("random key: ", q)
    for r in vdb.get_records(query_range=q):
        j += 1
        print_record(r)
    print(j, "records")
    
    q = (vcfdb.QUAL, 2000, 6001)    
    print("quality range query:", q)
    for r in vdb.get_records(query_range=q, max_records=20):
        print_record(r)
    
    q = (vcfdb.QUAL, 100000, 100001)    
    print("empty quality range query:", q)
    for r in vdb.get_records(query_range=q, max_records=20):
        print_record(r)

def key_query_example(vdb):
    """
    Shows an example of a simple key query.
    """
    print("all records")
    for r in vdb.get_records(max_records=20):
        print_record(r)
    keys = [(vcfdb.FILTER, "PASS")]
    print("records with filter=PASS; total = ", vdb.get_num_records(keys))
    for r in vdb.get_records(keys, max_records=20):
        print_record(r)
    keys = [(vcfdb.FILTER, "InDel;SnpCluster")]
    print("records with filter=InDel;SnpCluster: total = ", 
            vdb.get_num_records(keys))
    for r in vdb.get_records(keys, max_records=20):
        print_record(r)


def time_and_count(iterator):
    """
    Counts the items in the specified iterator and returns the count 
    and time elapsed.
    """
    before = time.time()
    c = 0
    print("\t", end="")
    for x in iterator:
        c += 1
        if c % 1000 == 0:
            print(".", end="")
            sys.stdout.flush()
    print()
    duration = time.time() - before
    return c, duration

def count_example(vdb):
    """
    Shows an example of counting records from the various indexes.
    """

    print("Counting records with ALT=A,T...")
    keys = [(vcfdb.ALT, "A,T")]
    print("\tcount value = ", vdb.get_num_records(keys))
    c, s = time_and_count(vdb.get_records(keys))
    print("\tfound = ", c, " in ", s, " seconds")
   
    # this is surprisingly slow when the cache is cold...

    print("Counting records with FILTER=HARD_TO_VALIDATE;InDel...:") 
    keys = [(vcfdb.FILTER, "HARD_TO_VALIDATE;InDel")]
    print("\tcount value = ", vdb.get_num_records(keys))
    c, s = time_and_count(vdb.get_records(keys))
    print("\tfound = ", c, " in ", s, " seconds")

    

def join_query_example(vdb):
    """
    An example of join queries, where we efficiently find records 
    with multiple equality constraints.
    """
    keys = [(vcfdb.FILTER, "PASS"), (vcfdb.REF, "G"), (vcfdb.ALT, "T")]
    s = "records with "
    for k, v in keys:
        s += " {0}={1} AND".format(k, v)
    print(s[:-4], ":: total = ", vdb.get_num_records(keys))
    for r in vdb.get_records(query_keys=keys, max_records=20):
        print_record(r)
    


def distinct_values_example(vdb):
    """
    Gets the distinct values for a few columns and prints their counts.
    """
    cols = [vcfdb.REF, vcfdb.ALT, vcfdb.FILTER]
    for col in cols:
        print("Distinct values for ", col)
        for v in vdb.get_distinct_values(col):
            c = vdb.get_num_records(query_keys=[(col, v)])
            print("\t{0:10d}".format(c), "\t\t", v)


def main():
    db_name = "mpileup_gatk_qcall_min2"
    #db_name = "subset"
    sep = "=" * 50
    with vcfdb.opendb(db_name, dbdir="tmp/vcfdb") as vdb:
        min_max_example(vdb)
        print(sep)
        get_record_example(vdb)
        print(sep)
        range_query_example(vdb)
        print(sep)
        compound_column_example(vdb)
        """
        print(sep)
        key_query_example(vdb)
        print(sep)
        join_query_example(vdb)
        print(sep)
        distinct_values_example(vdb)
        print(sep)
        count_example(vdb)
        """
if __name__ == "__main__":
    main() 
    
