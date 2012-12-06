
from __future__ import print_function
from __future__ import division 

import _vcfdb
from . import core 

class VCFFileColumn(object):
    """
    Class representing a column in a VCF file.
    
    TODO This class should be deprecated - it does very little except 
    provde the column factory method. This should be done directly
    """
    NUMBER_ANY = 0 
    TYPE_INTEGER = 0
    TYPE_FLOAT = 1
    TYPE_FLAG = 2
    TYPE_STRING = 3
    TYPE_CHAR = 4
    
    TYPE_MAP = {
        TYPE_INTEGER: "Integer",
        TYPE_FLOAT: "Float",
        TYPE_FLAG: "Flag", 
        TYPE_STRING: "String",
        TYPE_CHAR: "Character"}
  
    PARSER_MAP = {
        TYPE_INTEGER: int, 
        TYPE_FLOAT: float, 
        TYPE_FLAG: int, 
        TYPE_STRING: str,
        TYPE_CHAR: str} 
    
    def parse(self, s):
        """
        Parses the specified value into the appropriate Python types.
        """
        p = self.PARSER_MAP[self.type]
        v = []
        for tok in s.split(","):
            v.append(p(tok))
        if self.number != self.NUMBER_ANY:
            assert(len(v) == self.number)
        return v 
    
    def get_db_column(self, prefix):
        """
        Returns a Column instance suitable to represent this VCFFileColumn.
        """ 
        name = prefix + "_" + self.name
        element_type = _vcfdb.ELEMENT_TYPE_INT
        element_size = 2
        num_elements = self.number 
        if self.type == self.TYPE_INTEGER:
            element_type = _vcfdb.ELEMENT_TYPE_INT
            element_size = 2
        elif self.type == self.TYPE_FLOAT:
            element_type = _vcfdb.ELEMENT_TYPE_FLOAT
            element_size = 4
        elif self.type == self.TYPE_FLAG:
            element_type = _vcfdb.ELEMENT_TYPE_INT
            element_size = 1 
        elif self.type in [self.TYPE_STRING, self.TYPE_CHAR]:
            element_type = _vcfdb.ELEMENT_TYPE_ENUM
            element_size = 1
        dbc = _vcfdb.Column(name, self.description, element_type, 
            element_size, num_elements)
        return dbc 

    def __str__(self):
        t = self.TYPE_MAP[self.type]
        s = "{0}:type={1}:number={2}:desc={3}".format(self.name, t,
                self.number, self.description)
        return s

def vcf_file_column_factory(line):
    """
    Constructs a VCFFileColumn object from the specified line from a VCF file.
    """
    d = {}
    s = line[line.find("<") + 1: line.find(">")]
    for j in range(3):
        k = s.find(",")
        tokens = s[:k].split("=")
        s = s[k + 1:]
        d[tokens[0]] = tokens[1]
    tokens = s.split("=", 1)
    d[tokens[0]] = tokens[1]
    col = VCFFileColumn()
    col.description = d["Description"].strip("\"")
    col.name = d["ID"]
    st = d["Type"]
    if st == "Integer":
        t = VCFFileColumn.TYPE_INTEGER
    elif st == "Float":
        t = VCFFileColumn.TYPE_FLOAT
    elif st == "Flag":
        t = VCFFileColumn.TYPE_FLAG
    elif st == "Character":
        t = VCFFileColumn.TYPE_CHAR
    elif st == "String":
        t = VCFFileColumn.TYPE_STRING
    else:
        raise ValueError("Unknown type:", st)
        
    col.type = t 
    number = d["Number"]
    if number == ".":
        col.number = VCFFileColumn.NUMBER_ANY
    else:
        col.number = int(number) 
    return col 
    

class VCFSchemaFactory(object):
    """
    Class that generates a database schema for a VCF file by parsing 
    the header.
    """
    def __init__(self, vcf_file):
        self._file = vcf_file
        self._info_columns = {}
        self._genotype_columns = {}
    
    def _parse_version(self, s):
        """
        Parse the VCF version number from the specified string.
        """
        self._version = -1.0
        tokens = s.split("v")
        if len(tokens) == 2:
            self._version = float(tokens[1])

    def _parse_meta_information(self, line):
        """
        Processes the specified meta information line to obtain the values 
        of the various columns and their types.
        """
        if line.startswith("##INFO"):
            col = vcf_file_column_factory(line)
            self._info_columns[col.name] = col
        elif line.startswith("##FORMAT"):
            col = vcf_file_column_factory(line)
            self._genotype_columns[col.name] = col
        else:
            #print("NOT PARSED:", line)
            pass
            # TODO insert another case here to deal with the FILTER column
            # and add enumeration values to it.

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
        while s.startswith("##"):
            self._parse_meta_information(s)
            s = f.readline()
        self._parse_header_line(s)
        int_type = _vcfdb.ELEMENT_TYPE_INT
        char_type = _vcfdb.ELEMENT_TYPE_CHAR
        float_type = _vcfdb.ELEMENT_TYPE_FLOAT
        enum_type = _vcfdb.ELEMENT_TYPE_ENUM
        # Get the fixed columns
        columns = [
            _vcfdb.Column("CHROM", "Chromosome", enum_type, 2, 1),
            _vcfdb.Column("POS", "position", int_type, 5, 1),
            _vcfdb.Column("ID", "ID", char_type, 1, 0),
            _vcfdb.Column("REF", "Reference allele", enum_type, 1, 1),
            _vcfdb.Column("ALT", "Alternatve allele", enum_type, 1, 0),
            _vcfdb.Column("QUAL", "Quality", float_type, 4, 1),
            _vcfdb.Column("FILTER", "Filter", char_type, 1, 0),
        ]

        for name, col in self._info_columns.items():
            columns.append(col.get_db_column("INFO"))
        for genotype in self._genotypes:
            for name, col in self._genotype_columns.items():
                columns.append(col.get_db_column(genotype))
        schema = core.Schema(columns)
        return schema

def vcf_schema_factory(vcf_file):
    """
    Returns a schema for the specified VCF file.
    """
    with open(vcf_file, "rb") as f:
        sg = VCFSchemaFactory(f)
        schema = sg.generate_schema()
    return schema



class VCFTableBuilder(core.TableBuilder):
    """
    Class responsible for parsing a VCF file and creating a database. 
    """
    
    def build(self, vcf_file, progress_callback=None):
        """
        Builds the table.
        """
        # TODO handle gzipped files.
        self.open_database()
        with open(vcf_file, "rb") as f:
            self._prepare(f)
            self._insert_records(f)
        self.close_database()
        self.finalise()
        
    
    def _prepare(self, f):
        """
        Prepares for parsing records by getting the database columns 
        ready and skipping the file header.
        """
        # Skip the header
        # TODO: set this up to work in binary mode.
        s = f.readline()
        while s.startswith("##"):
            s = f.readline()
        # Get the genotypes from the header
        genotypes = s.split()[9:] 
        # TODO make this more elegant...
        all_columns = dict((c.name, c) for c in self._schema.get_columns())
        all_fixed_columns = [("CHROM", 0), ("POS", 1),  ("ID", 2),
            ("REF", 3), ("ALT", 4), ("QUAL", 5), ("FILTER", 6)
        ]
        self._fixed_columns = [(all_columns[name], index) 
                for name, index in all_fixed_columns if name in all_columns]
        self._info_columns = {}
        self._genotype_columns = [{} for g in genotypes]
        for c in self._schema.get_columns():
            if "_" in c.name:
                split = c.name.split("_")
                if split[0] == "INFO":
                    name= c.name.split("_")[1]
                    self._info_columns[name] = c
                else:
                    index = genotypes.index(split[0])
                    # TEMP - we should be dealing with all of these as 
                    # bytes literals.
                    self._genotype_columns[index][split[1].encode()] = c
 

    def _insert_records(self, f):
        """
        Builds the database in opened file.
        """
        fixed_columns = self._fixed_columns
        info_columns = self._info_columns
        genotype_columns = self._genotype_columns
        rb = self._record_buffer
        # TODO: this doesn't handle missing values properly. We should 
        # test each value to see if it is "." before adding.
        for s in f:
            l = s.encode().split()
            # Read in the fixed columns
            for col, index in fixed_columns:
                rb.set_record_value(col, l[index])
            # Now process the info columns.
            for mapping in l[7].split(b";"):
                tokens = mapping.split(b"=")
                name = tokens[0]
                if name in info_columns:
                    col = info_columns[name]
                    if len(tokens) == 2:
                        rb.set_record_value(col, tokens[1])
                    else:
                        # This is a Flag column.
                        rb.set_record_value(col, b"1")
            # Process the genotype columns. 
            j = 0
            fmt = l[8].split(b":")
            for genotype_values in l[9:]:
                tokens = genotype_values.split(b":")
                if len(tokens) == len(fmt):
                    for k in range(len(fmt)):
                        if fmt[k] in genotype_columns[j]:
                            col = genotype_columns[j][fmt[k]]
                            rb.set_record_value(col, tokens[k])
                elif len(tokens) > 1:
                    # We can treat a genotype value on its own as missing values.
                    # We can have skipped columns at the end though, which we 
                    # should deal with properly. So, put in a loud complaint 
                    # here and fix later.
                    print("PARSING CORNER CASE NOT HANDLED!!! FIXME!!!!")
                j += 1
            # Finally, commit the record.
            rb.commit_record()
 
