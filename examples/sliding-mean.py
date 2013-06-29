#!/usr/bin/env python

"""
Perform a simple sliding window over a chromosome in a VCF. Within each
non-overlapping window we calculate the means of a specified numeric columns
(e.g. QUAL,INFO_DP)
"""

from __future__ import print_function
from __future__ import division 

import re
import sys
import math
import wormtable as wt
import argparse


class SlidingWindow(object):
    """
    A class representing a sliding window over statistics in a VCF 
    wormtable.
    """
    def __init__(self, homedir, wsize=10000, cache_size="256M"):
        self.__table = wt.open_table(homedir, cache_size=cache_size)
        self.__index = self.__table.open_index("CHROM+POS", cache_size=cache_size)
        self.__wsize = wsize
    
    def run(self, chrom, cols, indels=None):
        cursor = self.__table.cursor(["POS"]+cols, self.__index)
        
        start = self.__index.get_min(chrom)[1]
        end = self.__index.get_max(chrom)[1]
        n = 1 + math.ceil((end - start) / self.__wsize)
        cursor.set_min(chrom, start)
        cursor.set_max(chrom, end + 1)
        
        dat = {n:{'c':0, 't':0} for n in cols}
        j = 0
        for row in cursor:
            for i in range(len(cols)):
                if row[i+1] is not None:
                    dat[cols[i]]['c'] += 1
                    dat[cols[i]]['t'] += row[i+1]
            if row[0] >= start + j * self.__wsize:
                for n in dat:
                    if dat[n]['c'] == 0:
                        dat[n]['c'] = 1
                print("%s\t%d\t" %(chrom, start+j * self.__wsize) + 
                      "\t".join([str(dat[n]['t']/dat[n]['c']) for n in dat]))
                for n in cols:
                    dat[n]['t'],dat[n]['c'] = 0,0
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
        print('chr\tpos\t' + "\t".join(cols))
    
    sw = SlidingWindow(args['homedir'], wsize=args['w'])
    for chr in args['chrs'].split(','):
        sw.run(chr, cols)
  

if __name__ == "__main__":
    main()
