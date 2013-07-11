"""
Count numbers of transitions and transversions across an entire VCF using
pyvcf or wormtable
Usage: ts_tv.py method homedir/vcffile

method is one of: pyvcf, wtindex, wtcursor
"""

from __future__ import print_function
from __future__ import division

import sys
import vcf
from itertools import permutations
import wormtable as wt

bases = {b'A':b'purine', b'G':b'purine', b'C':b'pyrimidine', b'T':b'pyrimidine'}

def count_Ts_Tv_pyvcf(vcf_file):
    """ 
    Count number of transitions and transversions using pyVCF
    """
    Ts, Tv = 0, 0
    for r in vcf.Reader(filename=vcf_file):
        ref = bytes(r.REF)
        alt = bytes(r.ALT[0])
        if ref in bases and alt in bases and ref != alt: 
            if bases[ref] == bases[alt]:
                Ts +=1
            else:
                Tv +=1

    return Ts, Tv


def count_Ts_Tv_wtcursor(homedir):
    """ 
    Count number of transitions and transversions using wormtable and an 
    index on CHROM+POS, counting Ts and Tv row by row
    """
    with wt.open_table(homedir) as t:
        Ts, Tv = 0, 0
        for ref, alt in t.cursor(["REF", "ALT"]):
            if ref != alt and ref in bases and alt in bases:
                if bases[ref] == bases[alt]:
                    Ts +=1
                else:
                    Tv +=1
    return Ts, Tv


def count_Ts_Tv_wtindex(homedir):
    """ 
    Count number of of transitions and transversions using wormtable and 
    an index on REF+ALT
    """
    with wt.open_table(homedir) as t, t.open_index("REF+ALT") as i:
        Ts, Tv = 0, 0
        c = i.counter()
        for s in permutations(bases.keys(), 2):
            if bases[s[0]] == bases[s[1]]: 
                Ts += c[s] 
            else: 
                Tv += c[s] 
    return Ts, Tv


def main():
    if len(sys.argv) != 3:
        s = "usage: {0} [pyvcf|wtcursor|wtindex] homedir".format(sys.argv[0])
        sys.exit(s)
    method, homedir = sys.argv[1:]
    if method == 'pyvcf':
        Ts, Tv = count_Ts_Tv_pyvcf(homedir)
    elif method == 'wtcursor':
        Ts, Tv = count_Ts_Tv_wtcursor(homedir)
    elif method == 'wtindex':
        Ts, Tv = count_Ts_Tv_wtindex(homedir)
    else: 
        sys.exit("Method %s not recognised" %(method))
    
    print("ts: {0} tv: {1}".format(Ts, Tv))

    
if __name__ == "__main__":
    main()
