from __future__ import print_function
from __future__ import division 

import sys
import wormtable as wt


def count_rows(t, i, chr, start, stop):
    """ 
    return number of rows in specified region
    """
    cursor = t.cursor(["POS"], i)
    cursor.set_min(chr, start)
    cursor.set_max(chr, stop)
    tot = sum(1 for i in cursor)
    return(tot)

def mean_AF(t, i, chr, start, stop):
    """ 
    return mean allele frequency over specified region using 'AF' field in 
    INFO column
    """
    cursor = t.cursor(["INFO_AF"], i)
    cursor.set_min(chr, start)
    cursor.set_max(chr, stop)
    af = [row[0][0] for row in cursor]
    return(sum(af)/len(af))

def AFS(t, i, chr, start, stop):
    """ 
    return comma separated Allele Frequency Spectrum calculated over specified
    range using 'AF' field in info column and number of sampled alleles (ngt)
    """
    cursor = t.cursor(["INFO_AF"], i)
    cursor.set_min(chr, start)
    cursor.set_max(chr, stop)
    ngt=20
    afs = [0]*(ngt+1)
    for row in cursor:
        af = int(round(row[0][0]*ngt))
        afs[af] += 1
    afs = ','.join(map(str, afs))
    return(afs)


def sliding_window(homedir, chrs, fn, wsize=500, slide=500):
    """
    Apply a function over a sliding window on CHROM, POS. Assumes that
    a CHROM+POS index is already made...
    """
    with wt.open_table(homedir) as t, t.open_index('CHROM+POS') as i:
        for chr in chrs:
            min = i.get_min(chr)[1]
            max = i.get_max(chr)[1]-wsize
            for x in range(min, max, slide):
                print(chr, x, x+wsize, fn(t, i, chr, x, x+wsize))


sliding_window('sample.wt', ['1','2','3'], count_rows)
sliding_window('sample.wt', ['1','2','3'], mean_AF, wsize=1000, slide=1000)
sliding_window('sample.wt', ['1','2','3'], AFS, wsize=1000, slide=1000)
