#!/usr/bin/env python

"""
Get a list of high quality genotypes of given type (e.g. heterozygotes) for a
specified sample (e.g. S1). This program builds the necessary index if missing 
(a compound index on genotype of the given sample and (binned) QUAL. The output 
is a tab separated list of CHROM, POS, REF, ALT and QUAL of SNPs that pass (in 
order of lowest->highest quality).
"""
from __future__ import print_function
from __future__ import division 

import sys
import wormtable as wt
import os.path
import argparse

def hq_snps_bygt(homedir, sample, gt, minq, cols):
    t =  wt.open_table(homedir)
    i = t.open_index("{0}.GT+QUAL[1]".format(sample))
    start = (gt, minq)
    stop = (gt, i.max_key(gt)[1] + 1)
    for row in i.cursor(cols, start=start, stop=stop):
        print("\t".join([str(i) for i in row]))

def main():
    parser = argparse.ArgumentParser(description=globals()['__doc__'])
    
    parser.add_argument('-s', default='S1',
        help='The sample name')
    
    parser.add_argument('-g', default='0/1',
        help='Genotype code to select')
    
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
    
    hq_snps_bygt(args['homedir'], args['s'], args['g'], args['q'], cols)


if __name__ == "__main__":
    main()

