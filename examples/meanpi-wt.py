import sys
import wormtable as wt
import re

class Record(object):
    """
    A basic class representing a single VCF record
    """
    def __init__(self, ref, alt, gts):
        self.REF = ref
        self.ALT = alt
        self.gts = gts
        
    def is_indel(self): 
        """
        Returns True if the specified ref and alt VCF entries represent 
        and indel. This is True if either contain two consecutive word characters
        """
        return re.search('\w\w', self.REF) or re.search('\w\w', self.ALT)
    
    def af_gt(self):
        """ 
        return allele frequency for a site given an array of genotypes. This only 
        counts the first (most frequent) alternative allele
        """
        hets,homs,count = 0,0,0
        for gt in self.gts:
            if gt != './.':
                count += 1
                if gt == "0/1":
                    hets += 1 
                elif gt == "1/0":
                    hets += 1 
                elif gt == "1/1":
                    homs += 1
        af = hets + 2*homs
        return(af,count)
    
    def nucl_diversity(self):
        """ 
        return nucleotide diversity for a site given an array of genotypes. 
        This only counts the first (most frequent) alternative allele
        """
        af, count = self.af_gt()
        p = af / float(count*2)
        q = 1.0 - p
        return float(count*2 / (count*2 - 1.0)) * (2.0 * p * q)



SAMPLES = ['DGRP-021','DGRP-026','DGRP-028','DGRP-031','DGRP-032','DGRP-038',
'DGRP-040','DGRP-041','DGRP-042','DGRP-045','DGRP-048','DGRP-049','DGRP-057',
'DGRP-059','DGRP-069','DGRP-073','DGRP-075','DGRP-083','DGRP-085','DGRP-088',
'DGRP-091','DGRP-093','DGRP-100','DGRP-101','DGRP-105','DGRP-109','DGRP-129',
'DGRP-136','DGRP-138','DGRP-142','DGRP-149','DGRP-153','DGRP-158','DGRP-161',
'DGRP-176','DGRP-177','DGRP-181','DGRP-189','DGRP-195','DGRP-208','DGRP-217',
'DGRP-223','DGRP-227','DGRP-228','DGRP-229','DGRP-233','DGRP-235','DGRP-237',
'DGRP-239','DGRP-256','DGRP-280','DGRP-287','DGRP-301','DGRP-303','DGRP-304',
'DGRP-306','DGRP-307','DGRP-309','DGRP-310','DGRP-313','DGRP-315','DGRP-317',
'DGRP-318','DGRP-319','DGRP-320','DGRP-321','DGRP-324','DGRP-325','DGRP-332',
'DGRP-335','DGRP-336','DGRP-338','DGRP-340','DGRP-348','DGRP-350','DGRP-352',
'DGRP-354','DGRP-355','DGRP-356','DGRP-357','DGRP-358','DGRP-359','DGRP-360',
'DGRP-361','DGRP-362','DGRP-365','DGRP-367','DGRP-370','DGRP-371','DGRP-373',
'DGRP-374','DGRP-375','DGRP-377','DGRP-379','DGRP-380','DGRP-381','DGRP-382',
'DGRP-383','DGRP-385','DGRP-386','DGRP-390','DGRP-391','DGRP-392','DGRP-395',
'DGRP-397','DGRP-399','DGRP-405','DGRP-406','DGRP-409','DGRP-426','DGRP-427',
'DGRP-437','DGRP-439','DGRP-440','DGRP-441','DGRP-443','DGRP-461','DGRP-486',
'DGRP-491','DGRP-492','DGRP-502','DGRP-505','DGRP-508','DGRP-509','DGRP-513',
'DGRP-517','DGRP-528','DGRP-530','DGRP-531','DGRP-535','DGRP-551','DGRP-555',
'DGRP-559','DGRP-563','DGRP-566','DGRP-584','DGRP-589','DGRP-595','DGRP-596',
'DGRP-627','DGRP-630','DGRP-634','DGRP-639','DGRP-642','DGRP-646','DGRP-703',
'DGRP-705','DGRP-707','DGRP-712','DGRP-714','DGRP-716','DGRP-721','DGRP-727',
'DGRP-730','DGRP-732','DGRP-737','DGRP-738','DGRP-748','DGRP-757','DGRP-761',
'DGRP-765','DGRP-774','DGRP-776','DGRP-783','DGRP-786','DGRP-787','DGRP-790',
'DGRP-796','DGRP-799','DGRP-801','DGRP-802','DGRP-804','DGRP-805','DGRP-808',
'DGRP-810','DGRP-812','DGRP-818','DGRP-819','DGRP-820','DGRP-821','DGRP-822',
'DGRP-832','DGRP-837','DGRP-843','DGRP-849','DGRP-850','DGRP-852','DGRP-853',
'DGRP-855','DGRP-857','DGRP-859','DGRP-861','DGRP-879','DGRP-882','DGRP-884',
'DGRP-887','DGRP-890','DGRP-892','DGRP-894','DGRP-897','DGRP-900','DGRP-907',
'DGRP-908','DGRP-911','DGRP-913']


def main():
    table = wt.open_table(sys.argv[1])
    index = table.open_index("INFO_IN+INFO_UT+INFO_SY+INFO_NS")
    
    keys = [x for x in index.keys()]
    keys.append((1,1,1,1))
    
    gts = [x+'_GT' for x in SAMPLES]
    num_chroms = len(gts)*2
    cursor = table.cursor(['REF','ALT']+gts, index)
    
    print("IN\tUT\tSY\tNS\tpi")
    for i in range(1, len(keys)-1):
        cursor.set_min(*keys[i])
        cursor.set_max(*keys[i+1])

        tot, count = 0,0
        for row in cursor:
            rec = Record(row[0], row[1], row[2:])
            if not rec.is_indel():
                count += 1
                tot += rec.nucl_diversity()
        mean = tot/count

        lst = [0 if x is None else 1 for x in keys[i]]
        for x in lst:
            if x is None: 
                lst[i] = 0
        print("%s\t%f" %('\t'.join([str(x) for x in lst]), mean))

if __name__ == "__main__":
    main()

