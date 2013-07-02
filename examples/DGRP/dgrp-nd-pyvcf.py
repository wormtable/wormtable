import sys
import vcf

def main():
    types = ['IN','UT','SY','NS']
    vcf_reader = vcf.Reader(filename=sys.argv[1])
    dat = {n:{'tot':0, 'count':0} for n in types}
    for record in vcf_reader:
        if record.is_indel is not True:
            for n in types:
                if n in record.INFO:
                    dat[n]['tot'] += record.nucl_diversity
                    dat[n]['count'] += 1
    
    print("%s\t%s" %("\t".join(types), "mean"))
    for n in types:
        lst = [1 if n==m else 0 for m in types]
        print("%s\t%f" %('\t'.join([str(x) for x in lst]), dat[n]['tot']/dat[n]['count']))

if __name__ == "__main__":
    main()
