# """
# Count numbers of transitions and transversions
# Usage: ts_tv.py [OPTIONS] homedir
# """
# 
# from __future__ import print_function
# from __future__ import division

import sys, vcf


def count_Ts_Tv(vcf_file):
	# """ 
	# Count number of of transitions and transversions across an entire VCF
	# """
	bases = {'A':'purine', 'G':'purine', 'C':'pyrimidine', 'T':'pyrimidine'}
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


def main():
	Ts, Tv = count_Ts_Tv(sys.argv[1])
	print("ts: %d tv: %d" % (Ts, Tv))

	
if __name__ == "__main__":
	main()
