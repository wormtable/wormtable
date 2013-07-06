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

bases = {'A':'purine', 'G':'purine', 'C':'pyrimidine', 'T':'pyrimidine'}

def count_Ts_Tv_pyvcf(vcf_file):
	""" 
	Count number of transitions and transversions using pyVCF
	"""
	vcf_reader = vcf.Reader(filename=vcf_file)
	Ts, Tv = 0, 0
	for record in vcf_reader:
		if str(record.REF) in bases.keys() and \
		record.ALT[0] in bases.keys() \
		and len(record.ALT) == 1:
			if bases[record.REF] == bases[str(record.ALT[0])]:
				Ts +=1
			else:
				Tv +=1
	return Ts, Tv


def count_Ts_Tv_wtcursor(homedir):
	""" 
	Count number of transitions and transversions using wormtable and an 
	index on CHROM+POS, counting Ts and Tv row by row
	"""
	t = wt.open_table(homedir)
	i = t.open_index("CHROM+POS")
	c = t.cursor(["REF","ALT"], i)
	Ts, Tv = 0, 0
	for row in c:
		if str(row[0]) in bases.keys() and str(row[1]) in bases.keys():
			if bases[str(row[0])] == bases[str(row[1])]:
				Ts +=1
			else:
				Tv +=1
	i.close()
	t.close()
	return Ts, Tv


def count_Ts_Tv_wtindex(homedir):
    """ 
    Count number of of transitions and transversions using wormtable and 
    an index on REF+ALT
    """
    subs = [p for p in permutations([b'A',b'C',b'G',b'T'], 2)]
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
    method, homedir = sys.argv[1:]
    if method == 'pyvcf':
    	Ts, Tv = count_Ts_Tv_pyvcf(homedir)
    elif method == 'wtcursor':
    	Ts, Tv = count_Ts_Tv_wtcursor(homedir)
    elif method == 'wtindex':
    	Ts, Tv = count_Ts_Tv_wtindex(homedir)
    else: 
        sys.exit("Method %s not recognised" %(method))
    
    print("ts: %d tv: %d" % (Ts, Tv))

	
if __name__ == "__main__":
	main()
