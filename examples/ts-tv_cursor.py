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
	bases = {'A':'purine', 'G':'purine', 'C':'pyrimidine', 'T':'pyrimidine'}
	t = wt.open_table(homedir)
	i = t.open_index("CHROM+POS")
	c = t.cursor(["REF","ALT"], i)
	Ts, Tv = 0, 0
	for row in c:
		if str(row[0]) in bases.keys() and \
		str(row[1]) in bases.keys():
			if bases[str(row[0])] == bases[str(row[1])]:
				Ts +=1
			else:
				Tv +=1
	i.close()
	t.close()
	return Ts, Tv


def main():
	Ts, Tv = count_Ts_Tv(sys.argv[1])
	print("ts: %d tv: %d" % (Ts, Tv))

	
if __name__ == "__main__":
	main()
