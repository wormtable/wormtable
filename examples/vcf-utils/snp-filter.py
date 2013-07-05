#!/usr/bin/env python

"""
Filter variant calls and print selected columns
"""

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

class dictcursor(object):
    def __init__(self, table, cols, index):
        self.__cursor = table.cursor(cols, index)
        self.__cols = cols
        
    def __iter__(self):
        for row in self.__cursor:
            yield {self.__cols[i]:row[i] for i in range(len(row))}

    def set_min(self, *args):
        self.__cursor.set_min(*args)

    def set_max(self, *args):
        self.__cursor.set_max(*args)


def make_filter(s):
    if s[1] in ['>','<','>=','<=']:   # make all of these comparisons numeric
        return lambda x: ops[s[1]](x[s[0]], int(s[2]))
    else:
        return lambda x: ops[s[1]](str(x[s[0]]), s[2])


def parse_cf(cf):
    fns = []
    cols = []
    for filter in cf.split(';'):
        s = re.split('(>=|<=|>|<|==)', filter)
        fns.append(make_filter(s))
        cols.append(s[0])
    return fns, cols


def filter(t, i, args):
    pcols = args['cols'].split(',')
    
    allcols = pcols
    if args['imode'] in ['e','f']:  
        allcols = allcols+['REF','ALT']
    
    # parse functions
    fns, fcols = parse_cf(args['cf'])
    allcols = allcols+fcols
    cols = list(set(allcols))
    dc = dictcursor(t, allcols, i)
    
    # set region if defined
    chr,start,end = re.split('\W', args['r'])
    dc.set_min(chr, int(start))
    dc.set_max(chr, int(end)+1)
    
    for row in dc:
        if all([fn(row) for fn in fns]):
            yield [str(row[c]) for c in pcols]


def main():
    parser = argparse.ArgumentParser(description=globals()['__doc__'])
    
    parser.add_argument('cols', default="CHROM,POS",
        help='comma separated column names to print')

    parser.add_argument('--imode', default='i', choices=['i','e','f'],
        help='indel mode: i=include, e=exclude, f=find')

    parser.add_argument('--f',
        help='specify semicolon separated filters, e.g. "QUAL>20;INFO_DP>5"')

    parser.add_argument('-r',
        help='region, e.g. 1:300-500 (start and end inclusive)')

    parser.add_argument('homedir',
        help='home directory of database')
        
    args = vars(parser.parse_args())
        
    with wt.open_table(args['homedir']) as t, t.open_index("CHROM+POS") as i:
        for row in filter(t, i, args):
            print('\t'.join([str(x) for x in row]))


if __name__ == "__main__":
    main()
