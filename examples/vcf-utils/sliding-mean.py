#!/usr/bin/env python

"""
Perform a simple sliding window over chromosomes in a VCF. Within each
non-overlapping window we calculate the means of a specified numeric columns
(e.g. QUAL,INFO_DP). In the case that the requested value is a tuple (e.g.
allele frequency (AF) from GATK) just take the first value.
"""

from __future__ import print_function
from __future__ import division 

import re
import sys
import math
import wormtable as wt
import argparse

def increment(val, dat):
    if val is not None:
        dat['c'] += 1
        if isinstance(val, tuple):
            dat['t'] += val[0]
        else:
            dat['t'] += val


def getmean(tot, count):
    if count == 0:
        return 'NA'
    else:
        return tot/count


class SlidingWindow(object):
    """
    A class representing a sliding window over statistics in a VCF 
    wormtable.
    """
    def __init__(self, homedir, chrs, cols, wsize=10000, db_cache_size="256M"):
        self.__table = wt.open_table(homedir, db_cache_size=db_cache_size)
        self.__index = self.__table.open_index("CHROM+POS", db_cache_size=db_cache_size)
        self.__wsize = wsize
        self.__chrs = chrs
        self.__cols = cols
    
    def __iter__(self):
        for chrom in self.__chrs:
            for window in self.__chriter(chrom):
                yield window

    def __chriter(self, chrom):        
        cols = ["POS"] + self.__cols
        start = self.__index.min_key(chrom)[1]
        end = self.__index.max_key(chrom)[1]
        stop = chrom, end + 1
        n = 1 + math.ceil((end - start) / self.__wsize)
        dat = {n:{'c':0, 't':0} for n in self.__cols}
        j = 0
        for row in self.__index.cursor(cols, start=(chrom,start), stop=stop):
            pos = row[0]
            vals = row[1:]
            for i in range(len(self.__cols)):
                increment(vals[i], dat[self.__cols[i]])
            if pos >= start + j * self.__wsize:
                yield([chrom, start+j * self.__wsize] + 
                        [getmean(dat[n]['t'], dat[n]['c']) for n in dat])
                dat = {n:{'c':0, 't':0} for n in self.__cols}
                j += 1
    
    def close(self):
        """
        Closes the opened table and index.
        """
        self.__index.close()
        self.__table.close()


def main():
    parser = argparse.ArgumentParser(description=globals()['__doc__'])
    
    parser.add_argument('-w', default=1000, type=int,
        help='window size')
    
    parser.add_argument('-H', action='store_true',
        help='print header')

    parser.add_argument('cols',
        help='comma separated column names to calculate mean on')

    parser.add_argument('chrs',
        help='comma separated chromsomes to use')

    parser.add_argument('homedir',
        help='home directory of database')
        
    args = vars(parser.parse_args())
    
    cols = args['cols'].split(',')
    if(args['H']):
        print('CHROM\tPOS\t' + "\t".join(cols))
    chrs = args['chrs'].split(',')
    
    sw = SlidingWindow(args['homedir'], chrs=chrs, cols=cols, wsize=args['w'])
    for window in sw:
        print("\t".join([str(x) for x in window]))
  

if __name__ == "__main__":
    main()
