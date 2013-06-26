#!/usr/bin/env python

"""
Get a list of high quality genotypes of given type (e.g. heterozygotes) for a
specified sample (e.g. S1). This program builds the necessary index if missing 
(a compound index on genotype of the given sample and (binned) QUAL. The output 
is a tab separated list of CHROM, POS, REF, ALT and QUAL of SNPs that pass (in 
order of lowest->highest quality).
"""

import sys
import wormtable as wt
import os.path
import argparse

def get_index(t, name) :
    if not os.path.isfile("%s/index_%s.db" %(t.get_homedir(), name)):
        sys.stderr.write("Building index %s\n" %(name)) 
        os.system("wtadmin add %s %s" %(t.get_homedir(), name))
    return t.open_index(name)


def hq_snps_bygt(homedir, sample, gt, minq, cols):
    t =  wt.open_table(homedir)
    i = get_index(t, "%s_GT+QUAL[1]" %(sample))
    cursor = t.cursor(cols, i)
    cursor.set_min(gt, minq)
    cursor.set_max(gt, i.get_max(gt)[1])
    for row in cursor:
        print "\t".join([str(i) for i in row])

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
        print "\t".join(cols)
    
    hq_snps_bygt(args['homedir'], args['s'], args['g'], args['q'], cols)


if __name__ == "__main__":
    main()

