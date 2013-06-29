#!/usr/bin/env python

"""
Perform a simple sliding window over chromosomes in a VCF. Within each window
we calculate the mean of any given numeric value from the wormtable
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
    
    def run(self, chrom, col, indels=None):
        cols = ['POS', col]
        cursor = self.__table.cursor(cols, self.__index)
        
        start = self.__index.get_min(chrom)[1]
        end = self.__index.get_max(chrom)[1]
        n = 1 + math.ceil((end - start) / self.__wsize)
        cursor.set_min(chrom, start)
        cursor.set_max(chrom, end + 1)
        j, count, tot = 0, 0, 0
        for pos, val in cursor:
            count += 1
            tot += val
            if pos >= start + j * self.__wsize:
                if count==0:
                    count = 1
                print("%s\t%d\t%f" %(chrom, start+j * self.__wsize, tot/count))
                count, tot = 0, 0
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

    parser.add_argument('col',
        help='wormtable column name')

    parser.add_argument('chr',
        help='chromsome to use')

    parser.add_argument('homedir',
        help='home directory of database')
        
    args = vars(parser.parse_args())
    
    if(args['H']):
        print('pos\t%s' %(args['col']))
    
    sw = SlidingWindow(args['homedir'], wsize=args['w'])
    sw.run(args['chr'], args['col'])
  

if __name__ == "__main__":
    main()

