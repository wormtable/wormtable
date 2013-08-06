#!/usr/bin/env python

"""
Filter variant calls and print selected columns
"""
from __future__ import print_function
from __future__ import division 

import re
import sys
import math
import wormtable as wt
import argparse
import operator

ops = {
    ">": operator.gt,
    "<": operator.lt,
    ">=": operator.ge,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne
}

class DictCursor(object):
    def __init__(self, index, cols):
        self.__index = index
        self.__cols = cols
        self.__start = None
        self.__stop = None
        
    def __iter__(self):
        self.__cursor = self.__index.cursor(self.__cols, start=self.__start,
               stop=self.__stop)
        for row in self.__cursor:
            yield {self.__cols[i]:row[i] for i in range(len(row))}

    def set_start(self, *args):
        self.__start = args 

    def set_stop(self, *args):
        self.__stop = args 


def make_filter(s):
    # make >, < , >= and <= comparisons numeric, else string comparisons
    if s[1] in ['>','<','>=','<=']:   
        return lambda x: ops[s[1]](x[s[0]], int(s[2]))
    else:
        return lambda x: ops[s[1]](str(x[s[0]]), s[2])


def parse_cf(cf):
    fns = []
    cols = []
    for f in cf.split(';'):
        s = re.split('(>=|<=|>|<|==|!=)', f)
        fns.append(make_filter(s))
        cols.append(s[0])
    return fns, cols


def isindel(row): 
    """
    Returns True if the specified ref and alt VCF entries represent 
    and indel. This is True if either contain two consecutive word characters
    """
    return re.search('\w\w', row['REF']) or re.search('\w\w', row['ALT'])


def snp_filter(t, i, args):
    pcols = args['cols'].split(',') # columns to print
    allcols = pcols # columns required for printing and all functions
    fns = [] # array of functions to test with each line

    # add function to find/exclude indels if required
    if 'imode' in args and args['imode'] in ['e','f']:  
        allcols = allcols+['REF','ALT']
        if args['imode'] == 'f':
            fns.append(isindel)
        else:
            fns.append(lambda row: not isindel(row))
            
    # parse user specified functions
    if 'f' in args and args['f'] is not None:
        fns, fcols = parse_cf(args['f'])
        allcols = allcols+fcols
    
    # open cursor and set region if defined
    # Note that end position cursor in wormtable is exclusive but we want to include
    dc = DictCursor(i, list(set(allcols)))
    if 'r' in args and args['r'] is not None:
        chrom,start,end = re.split('\W', args['r'])
        dc.set_start(chrom, int(start))
        dc.set_stop(chrom, int(end)+1) 
    
    for row in dc:
        if all([fn(row) for fn in fns]):
            yield [str(row[c]) for c in pcols]


def main():
    parser = argparse.ArgumentParser(description=globals()['__doc__'])
    
    parser.add_argument('cols', default="CHROM,POS",
        help='comma separated column names to print')

    parser.add_argument('-i', default='i', choices=['i','e','f'],
        help='indel mode: i=include, e=exclude, f=find [default=i]')

    parser.add_argument('-f',
        help='specify semicolon separated filters as COLUMN(>=|<=|>|<|==|!=)VALUE,\
              e.g. "QUAL>20;SAMPLE.GT==0/0"')

    parser.add_argument('-r',
        help='region, e.g. 1:300-500 (start and end inclusive)')

    parser.add_argument('homedir',
        help='home directory of database')
        
    args = vars(parser.parse_args())
        
    with wt.open_table(args['homedir']) as t, t.open_index("CHROM+POS") as i:
        for row in snp_filter(t, i, args):
            print('\t'.join([str(x) for x in row]))


if __name__ == "__main__":
    main()
