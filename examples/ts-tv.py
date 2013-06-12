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
    for val, count in i.counter().items():
        if(val in subs):
            if bases[val[0]] == bases[val[1]]: Ts += count
            else: Tv += count
    return(Ts, Tv)


def main():
    (Ts,Tv) = count_Ts_Tv(sys.argv[1])
    print("ts: %d tv: %d" % (Ts, Tv))

    
if __name__ == "__main__":
    main()