from __future__ import print_function

"""
Development code.
"""
import sys

import vcfdb


def main():

    if len(sys.argv) == 2: 
        vcf_file = sys.argv[1]
        dbdir = "db_NOBACKUP_"
        # TODO: put back in open_vcf_file
        with open(vcf_file, "rb") as f:
            sg = vcfdb.VCFSchemaFactory(f)
            schema = sg.generate_schema()
            schema.write_xml(dbdir)
        
        # Start again - read the schema back
        schema = vcfdb.Schema.read_xml(dbdir)
        #schema.show()
        
        dbw = vcfdb.VCFDatabaseWriter(schema, dbdir)
        dbw.set_cache_size(8 * 2**30)
        dbw.set_buffer_size(64 * 2**20)
        dbw.open_database()
        with open(vcf_file, "rb") as f:
            dbw.build(f)
        dbw.close_database()
        dbw.finalise() 
        schema.show()


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
    main()

