from __future__ import print_function
from __future__ import division 

import sys
import wormtable as wt
import re


def isindel(ref,alt): 
    """
    Returns True if the specified ref and alt VCF entries represent 
    and indel. This is True if either contain two consecutive word characters
    """
    if re.search('\w\w', ref) or re.search('\w\w', alt): return(1)
    else : return(0)


def count_rows(t, i, chr, start, stop):
    """ 
    return number of rows in specified region
    """
    cursor = t.cursor(["POS"], i)
    cursor.set_min(chr, start)
    cursor.set_max(chr, stop)
    tot = sum(1 for i in cursor)
    return(tot)


def count_indels(t, i, chr, start, stop):
    """ 
    return number of rows in specified region
    """
    cursor = t.cursor(["REF","ALT"], i)
    cursor.set_min(chr, start)
    cursor.set_max(chr, stop)
    tot = sum(isindel(row[0],row[1]) for row in cursor)
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
    cursor = t.cursor(["REF","ALT","INFO_AF"], i)
    cursor.set_min(chr, start)
    cursor.set_max(chr, stop)
    ngt=20
    afs = [0]*(ngt+1)
    for row in cursor:
        if not isindel(row[0],row[1]):
            af = int(round(row[2][0]*ngt))
            afs[af] += 1
    afs = ','.join(map(str, afs))
    return(afs)

    
def AFS_gt(t, i, chr, start, stop):
    """ 
    return comma separated Allele Frequency Spectrum calculated over specified
    range using genotype calls rather than pre-calculated allele frequency.
    For simplicity although this ignores indels, it does not account for the
    possibility of triallelic sites.
    """
    cursor = t.cursor(["REF","ALT","S1_GT","S2_GT","S3_GT","S4_GT","S5_GT","S6_GT","S7_GT","S8_GT","S9_GT","S10_GT"], i)
    cursor.set_min(chr, start)
    cursor.set_max(chr, stop)
    ngt=20
    afs = [0]*(ngt+1)
    for row in cursor:
        if not isindel(row[0],row[1]):
            af = 0
            if row[1] != '':
                for g in row[2:]:
                    if g == b"0/1":
                        af += 1 
                    elif g == b"1/1":
                        af += 2
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
sliding_window('sample.wt', ['1','2','3'], count_indels)
sliding_window('sample.wt', ['1','2','3'], mean_AF, wsize=1000, slide=1000)
sliding_window('sample.wt', ['1','2','3'], AFS, wsize=10000, slide=10000)
sliding_window('sample.wt', ['1','2','3'], AFS_gt, wsize=10000, slide=10000)
