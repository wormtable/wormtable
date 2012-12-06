from __future__ import print_function

"""
Development code.
"""
import os
import sys

import vcfdb

def main():

    if len(sys.argv) == 3: 
        homedir = sys.argv[1] 
        vcf_file = sys.argv[2]
        input_schema = os.path.join(homedir, "input.xml")
        schema= vcfdb.vcf_schema_factory(vcf_file)
        schema.write_xml(input_schema)
        
        # In the command line tool we'll optionally stop here 
        # and allow the user to edit the schema. This means 
        # we don't have to generate the 'perfect' vcf schema.
        # Start again - read the schema back
        
        schema = vcfdb.Schema.read_xml(input_schema)
        dbb = vcfdb.VCFTableBuilder(schema, homedir)
        dbb.set_cache_size(8 * 2**30) # 8 gigs - bigger is better
        dbb.set_buffer_size(64 * 2**20) # 64 megs
        dbb.build(vcf_file)
        
    elif len(sys.argv) == 2:
        table = vcfdb.Table(sys.argv[1])
        print(table)
        table.close()
    else:
        print("nothing for here")
       
if __name__ == "__main__":
    main()

