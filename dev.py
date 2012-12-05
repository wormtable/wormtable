from __future__ import print_function

"""
Development code.
"""
import sys

import vcfdb


def main():
    import time 

    global last 
    last = time.time()
    def progress_monitor(progress, record_number):
        #print("=", end="")
        #sys.stdout.flush()
        global last
        now = time.time()
        #print(progress, "\t", record_number, now - last)
        last = now

    if len(sys.argv) == 2: 
        vcf_file = sys.argv[1]
        dbdir = "db_NOBACKUP_"
        # TODO: put back in open_vcf_file
        with open(vcf_file, "r") as f:
            sg = vcfdb.VCFSchemaFactory(f)
            schema = sg.generate_schema()
            schema.write_xml(dbdir)
        
        # Start again - read the schema back
        schema = vcfdb.Schema.read_xml(dbdir)
        #schema.show()
        
        dbw = vcfdb.VCFDatabaseWriter(schema, dbdir)
        dbw.open_database()
        with open(vcf_file, "r") as f:
            dbw.build(f)
        dbw.close_database()
        dbw.finalise() 
        schema.show()


        #dbw = VCFDatabaseWriter(dbdir, vcf_file)
        #dbw.process_header()
        #dbw.process_records(progress_monitor)
        #ib = IndexBuilder(dbdir, ["chrom", "pos"])
        #ib.build(progress_monitor)

    else:
        #dbr = DatabaseReader()
        """
        records = 0
        for r in dbr.get_records(["POS", "QUAL"]):
            print(r)
            #print(r.record_id, r.POS[0], r.QUAL[0], sep="\t")
            records += 1
        print("read ", records, "records")
        """
        #dbr.close()
        print(dir(vcfdb))

if __name__ == "__main__":
    # temp development code.
    main()

