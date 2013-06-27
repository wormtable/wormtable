#!/usr/bin/env python

"""
Count numbers of transitions and transversions
Usage: ts_tv.py [OPTIONS] homedir
"""

from __future__ import print_function
from __future__ import division

import sys
from itertools import permutations
import wormtable as wt


def count_Ts_Tv(homedir):
    """ 
    Count number of of transitions and transversions using an index on REF+ALT
    """
    subs = [p for p in permutations([b'A',b'C',b'G',b'T'], 2)]
    bases = {b'A':'purine', b'G':'purine', b'C':'pyrimidine', b'T':'pyrimidine'}
    t = wt.open_table(homedir)
    i = t.open_index("REF+ALT")
    Ts, Tv = 0, 0
    c = i.counter()
    for s in subs:
        if bases[s[0]] == bases[s[1]]: 
            Ts += c[s] 
        else: 
            Tv += c[s] 
    i.close()
    t.close()
    return Ts, Tv


def main():
    Ts, Tv = count_Ts_Tv(sys.argv[1])
    print("ts: %d tv: %d" % (Ts, Tv))

    
if __name__ == "__main__":
    main()
