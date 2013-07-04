#!/usr/bin/env python

"""
Get a list of high quality SNP calls (in order of lowest->highest quality).
"""

import sys
import wormtable as wt
import os.path
import argparse

def hq_snps(homedir, minq, cols):
    t =  wt.open_table(homedir)
    i = t.open_index("QUAL[5]")
    cursor = t.cursor(cols, i)
    cursor.set_min(minq)
    cursor.set_max(i.get_max())
    for row in cursor:
        print "\t".join([str(i) for i in row])

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
        print "\t".join(cols)
    
    hq_snps(args['homedir'], args['q'], cols)


if __name__ == "__main__":
    main()

