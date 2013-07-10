#!/usr/bin/env python

"""
Get a list of high quality SNP calls (in order of lowest->highest quality).
"""
from __future__ import print_function
from __future__ import division

import sys
import wormtable as wt
import os.path
import argparse

def hq_snps(homedir, minq, cols):
    with wt.open_table(homedir) as t, t.open_index("QUAL[1]") as i:
        cursor = t.cursor(cols, i)
        cursor.set_min(minq)
        for row in cursor:
            yield row 

def main():
    parser = argparse.ArgumentParser(description=globals()['__doc__'])
    
    parser.add_argument('-q', default=1000, type=float,
        help='Minimum QUAL for site')
    
    parser.add_argument('homedir',
        help='home directory of database')

    parser.add_argument('-H', action='store_true',
        help='print header')
    
    args = vars(parser.parse_args())
    
    cols = ["CHROM", "POS", "REF", "ALT", "QUAL"]
    
    if(args['H']):
        print("\t".join(cols))
    
    for row in hq_snps(args['homedir'], args['q'], cols):
        print(row)

if __name__ == "__main__":
    main()

