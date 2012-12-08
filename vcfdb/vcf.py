from __future__ import print_function
from __future__ import division 

"""
VCF processing for vcfdb. 

TODO:document.

Implementation Note: We use bytes throughout the parsing process here for
a few reasons. Mostly, this is because it's much easier to deal with bytes
values within the C module, as we'd have to decode Unicode objects before 
getting string. At the same time, it's probably quite a bit more efficient 
to work with bytes directly, so we win both ways. It's a bit tedious making 
sure that all the literals have a 'b' in front of them, but worth the 
effort.

"""
import _vcfdb
from . import core 

# VCF Fixed columns

CHROM = b"CHROM"
POS = b"POS"
ID = b"ID"
REF = b"REF"
ALT = b"ALT"
QUAL = b"QUAL"
FILTER = b"FILTER"
INFO = b"INFO"

# Special values in VCF
MISSING_VALUE = b"."

# Strings used in the header for identifiers
ID = b"ID"
DESCRIPTION = b"Description"
NUMBER = b"Number"
TYPE = b"Type"
INTEGER = b"Integer"
FLOAT = b"Float"
FLAG = b"Flag"
CHARACTER = b"Character"
STRING = b"String"

def vcf_column_factory(line):
    """
    Returns a column object suitable for encoding a vcf column described by the 
    specified meta-information line.
    """
    d = {}
    s = line[line.find(b"<") + 1: line.find(b">")]
    for j in range(3):
        k = s.find(b",")
        tokens = s[:k].split(b"=")
        s = s[k + 1:]
        d[tokens[0]] = tokens[1]
    tokens = s.split(b"=", 1)
    d[tokens[0]] = tokens[1]
    name = d[ID]
    description = d[DESCRIPTION].strip(b"\"")
    number = d[NUMBER]
    if number == b".":
        num_elements = _vcfdb.NUM_ELEMENTS_VARIABLE 
    else:
        num_elements = int(number) 
    st = d[TYPE]
    if st == INTEGER:
        element_type = _vcfdb.ELEMENT_TYPE_INT
        element_size = 2
    elif st == FLOAT: 
        element_type = _vcfdb.ELEMENT_TYPE_FLOAT
        element_size = 4
    elif st == FLAG: 
        element_type = _vcfdb.ELEMENT_TYPE_INT
        element_size = 1
    elif st == CHARACTER: 
        element_type = _vcfdb.ELEMENT_TYPE_CHAR
        element_size = 1
    elif st == STRING: 
        element_type = _vcfdb.ELEMENT_TYPE_ENUM
        element_size = 1
    else:
        raise ValueError("Unknown VFC type:", st)
    col = _vcfdb.Column(name, description, element_type, 
            element_size, num_elements)
    return col
   
def copy_column(col, prefix):
    """
    Returns a copy of the specified column with the specified prefix 
    appended to its name.
    """
    new_name = prefix + b"_" + col.name
    new_col = _vcfdb.Column(name=new_name, description=col.description, 
            element_type=col.element_type, element_size=col.element_size, 
            num_elements=col.num_elements)
    return new_col

class VCFSchemaGenerator(object):
    """
    Class that generates a database schema for a VCF file by parsing 
    the header.
    """
    def __init__(self, vcf_file):
        self._file = vcf_file
        self._info_columns = [] 
        self._genotype_columns = []
    
    def _parse_version(self, s):
        """
        Parse the VCF version number from the specified string.
        """
        self._version = -1.0
        tokens = s.split(b"v")
        if len(tokens) == 2:
            self._version = float(tokens[1])

    def _parse_meta_information(self, line):
        """
        Processes the specified meta information line to obtain the values 
        of the various columns and their types.
        """
        if line.startswith(b"##INFO"):
            col = vcf_column_factory(line)
            self._info_columns.append(col)
        elif line.startswith(b"##FORMAT"):
            col = vcf_column_factory(line)
            self._genotype_columns.append(col)
        else:
            pass
            # Should we parse the FILTER values and make a proper enum 
            # column? Probably.

    def _parse_header_line(self, s):
        """
        Processes the specified header string to get the genotype labels.
        """
        self._genotypes = s.split()[9:]

    def generate_schema(self):
        """
        Reads the header for this VCF file, constructing the database 
        schema and returning it. 
        """
        f = self._file 
        s = f.readline()
        self._parse_version(s)
        if self._version < 4.0:
            raise ValueError("VCF versions < 4.0 not supported")
        while s.startswith(b"##"):
            self._parse_meta_information(s)
            s = f.readline()
        self._parse_header_line(s)
        int_type = _vcfdb.ELEMENT_TYPE_INT
        char_type = _vcfdb.ELEMENT_TYPE_CHAR
        float_type = _vcfdb.ELEMENT_TYPE_FLOAT
        enum_type = _vcfdb.ELEMENT_TYPE_ENUM
        variable = _vcfdb.NUM_ELEMENTS_VARIABLE
        # Get the fixed columns
        # TODO Add string constants at the top of the file for the descriptions 
        # of these columns.
        columns = [
            _vcfdb.Column(CHROM, b"Chromosome", enum_type, 2, 1),
            _vcfdb.Column(POS, b"position", int_type, 5, 1),
            _vcfdb.Column(ID, b"ID", char_type, 1, variable),  
            _vcfdb.Column(REF, b"Reference allele", enum_type, 1, 1),
            _vcfdb.Column(ALT, b"Alternatve allele", enum_type, 1, variable),
            _vcfdb.Column(QUAL, b"Quality", float_type, 4, 1),
            _vcfdb.Column(FILTER, b"Filter", char_type, 1, variable),
        ]
        for col in self._info_columns:
            columns.append(copy_column(col, INFO))
        for genotype in self._genotypes:
            for col in self._genotype_columns:
                columns.append(copy_column(col, genotype))
        schema = core.Schema(columns)
        return schema

def vcf_schema_factory(vcf_file):
    """
    Returns a schema for the specified VCF file.
    """
    with open(vcf_file, "rb") as f:
        sg = VCFSchemaGenerator(f)
        schema = sg.generate_schema()
    return schema


class VCFTableBuilder(core.TableBuilder):
    """
    Class responsible for parsing a VCF file and creating a database. 
    """
    
    def build(self, vcf_file):
        """
        Builds the table.
        """
        self.open_database()
        # TODO add support for gzips and progress callbacks
        with open(vcf_file, "rb") as f:
            self._source_file = f 
            self._prepare()
            self._insert_rows()
        self.close_database()
        self.finalise()
        
    
    def _prepare(self):
        """
        Prepares for parsing records by getting the database columns 
        ready and skipping the file header.
        """
        f = self._source_file
        # Skip the header
        s = f.readline()
        while s.startswith(b"##"):
            s = f.readline()
        # Get the genotypes from the header
        genotypes = s.split()[9:] 
        # In the interest of efficiency, we want to split the columns 
        # up into the smallest possible lists so we don't have to 
        # put in as much effort searching for them.
        all_columns = dict((c.name, c) for c in self._schema.get_columns())
        all_fixed_columns = [CHROM, POS, ID, REF, ALT, QUAL, FILTER]
        self._fixed_columns = []
        for j in range(len(all_fixed_columns)):
            name = all_fixed_columns[j]
            if name in all_columns:
                self._fixed_columns.append((all_columns[name], j))
        self._info_columns = {}
        self._genotype_columns = [{} for g in genotypes]
        for c in self._schema.get_columns():
            if b"_" in c.name:
                split = c.name.split(b"_")
                if split[0] == INFO:
                    name = split[1]
                    self._info_columns[name] = c
                else:
                    index = genotypes.index(split[0])
                    self._genotype_columns[index][split[1]] = c
 

    def _insert_rows(self):
        """
        Builds the database in opened file.
        """
        fixed_columns = self._fixed_columns
        info_columns = self._info_columns
        genotype_columns = self._genotype_columns
        rb = self._row_buffer
        for s in self._source_file:
            l = s.split()
            # Read in the fixed columns
            for col, index in fixed_columns:
                if l[index] != MISSING_VALUE:
                    rb.insert_encoded_element(col, l[index])
            # Now process the info columns.
            for mapping in l[7].split(b";"):
                tokens = mapping.split(b"=")
                name = tokens[0]
                if name in info_columns:
                    col = info_columns[name]
                    if len(tokens) == 2:
                        rb.insert_encoded_element(col, tokens[1])
                    else:
                        # This is a Flag column.
                        rb.insert_encoded_element(col, b"1")
            # Process the genotype columns. 
            j = 0
            fmt = l[8].split(b":")
            for genotype_values in l[9:]:
                tokens = genotype_values.split(b":")
                if len(tokens) == len(fmt):
                    for k in range(len(fmt)):
                        if fmt[k] in genotype_columns[j]:
                            col = genotype_columns[j][fmt[k]]
                            rb.insert_encoded_element(col, tokens[k])
                elif len(tokens) > 1:
                    # We can treat a genotype value on its own as missing values.
                    # We can have skipped columns at the end though, which we 
                    # should deal with properly. So, put in a loud complaint 
                    # here and fix later.
                    print("PARSING CORNER CASE NOT HANDLED!!! FIXME!!!!")
                j += 1
            # Finally, commit the record.
            rb.commit_row()
 
