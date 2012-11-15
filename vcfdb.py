"""
Prototype implementation of the Berkeley DB VCF record store.
"""
import os
import sys
import gzip
import bz2
import pickle
import struct 
import tempfile
from contextlib import contextmanager

import bsddb3.db as bdb 

__version__ = '0.0.1-dev'

RECORD_ID = "RECORD_ID"
CHROM = "CHROM"
POS = "POS"
ID = "ID"
REF = "REF"
ALT = "ALT"
QUAL = "QUAL"
FILTER = "FILTER"
INFO = "INFO"

DEFAULT_DB_DIR="/var/lib/vcfdb"
MAX_RECORD_SIZE = 65536 # 64KiB 


#class VCFColumn(object):
#    """
#    Class representing a column in a VCF database. This encapsulates information 
#    about the type, desciption, and so on, and provides encoders and decoders
#    appropriate to the column.
#    """
#    def __init__(self):
#        self._description = None 
#        self._id = None
#
#    def set_description(self, description):
#        """
#        Sets the column description to the specified value.
#        """
#        self._description = description
#
#    def set_id(self, cid):
#        """
#        Sets the ID for this column to the specified value.
#        """
#        self._id = cid
#    
#    def get_id(self):
#        """
#        Returns the ID of this column.
#        """
#        return self._id
#
#    def encode(self, value):
#        """
#        Encodes the specified value as bytes object.
#        """
#        return value.encode()
#    
#    def decode(self, b):
#        """
#        Decodes the specified bytes into a python value of the 
#        appropriate type.
#        """
#        return b.decode()
#    
#    def default_value(self):
#        return "."
#
#    def __str__(self):
#        return "ID={0}; DESC={1}".format(self._id, self._description)
#
#
#class CompoundColumn(VCFColumn):
#    """
#    Class representing a compound column in the VCF database. Compound
#    columns are obtained by concatenating two or more columns 
#    together.
#    """
#    def __init__(self, colid, columns):
#        self._id = colid
#        self._columns = columns
#
#    def encode(self, value):
#        """
#        Encodes the specified tuple of values as a concatenated list 
#        of bytes.
#        """
#        v = b""
#        for j in range(len(self._columns)):
#            col = self._columns[j]
#            v += col.encode(value[j]) + b"_"
#        return v[:-1] 
#    
#    def decode(self, b):
#        """
#        Decodes the specified _ delimited value into a tuple of values.
#        """ 
#        values = b.split(b"_")
#        t = list(values) 
#        for j in range(len(self._columns)):
#            t[j] = self._columns[j].decode(values[j])
#        return tuple(t)
#
#
#class NumberColumn(VCFColumn):
#    """
#    Class representing columns with numeric values.
#    """
#    def __init__(self, width):
#        super().__init__()
#        self._width = width
#    
#    def __str__(self):
#        return "width={0};".format(self._width) + super().__str__() 
#    
#    def default_value(self):
#        return -1 
#
#class LongIntegerColumn(NumberColumn):
#    """
#    Class representing columns with large integer values.
#    """
#    def __init__(self):
#        super().__init__(12)
#
#    def __str__(self):
#        return "Integer:" + super().__str__()
#        
#    def encode(self, value):
#        """
#        Encodes the specified integer value as bytes object.
#        """
#        fmt = "{0:0" +  str(self._width) + "d}" 
#        s = fmt.format(value)
#        return s.encode()
#    
#    def decode(self, string):
#        """
#        Decodes the specified string into an integer value.
#        """
#        return int(string)
#
#
#class ShortIntegerColumn(NumberColumn):
#    """
#    Class representing columns with small integer values.
#    """
#    def __init__(self):
#        super().__init__(6)
#
#    def __str__(self):
#        return "Integer:" + super().__str__()
#        
#    def encode(self, value):
#        """
#        Encodes the specified integer value as bytes object.
#        """
#        fmt = "{0:0" +  str(self._width) + "d}" 
#        s = fmt.format(value)
#        return s.encode()
#    
#    def decode(self, string):
#        """
#        Decodes the specified string into an integer value.
#        """
#        return int(string)
#
#         
#         
#class FloatColumn(NumberColumn):
#    """
#    Class representing columns with floating point values.
#    """
#    def __str__(self):
#        return "Float:" + super().__str__()
#
#    def encode(self, value):
#        """
#        Encodes the specified float value as bytes object.
#        """
#        fmt = "{0:0" +  str(self._width) + ".2f}" 
#        s = fmt.format(value)
#        return s.encode()
#
#    def decode(self, string):
#        """
#        Decodes the specified string into a float value.
#        """
#        return float(string)
#     
#
#
#class StringColumn(VCFColumn):
#    """
#    Class representing a column with arbitrary string values.
#    """
#    def __str__(self):
#        return "String:" + super().__str__()
#   
#
#def column_type_factory(ctype):
#    """
#    Returns a column of the appropriate type given the specified type string.
#    """ 
#    if ctype == "Integer":
#        col = ShortIntegerColumn()
#    elif ctype == "Float":
#        col = FloatColumn(10)
#    elif ctype == "String":
#        col = StringColumn()
#    else:
#        raise ValueError("Unexpected column:", d)
#    return col
#
#def column_factory(cid, d):
#    """
#    Generates a column object representing a column descibed by the 
#    specified dictionary. 
#    """ 
#    #print("column factory:", d)
#    description = d["Description"]
#    ctype = d["Type"]
#    number = d["Number"]
#    #print("\t", cid, ":", description, ":", ctype, ":",  number)
#    columns = {}
#    if number == "1":
#        col = column_type_factory(ctype)
#        columns[cid] = col
#    elif number == "." or number == "0":
#        # For now treat as strings
#        col = StringColumn()
#        columns[cid] = col
#    else:
#        # number should be an integer
#        n = int(number)
#        for j in range(n):
#            cid_col = cid + "_{0}".format(j)
#            col = column_type_factory(ctype) 
#            columns[cid_col] = col
#    for col_id, col in columns.items():
#        col.set_description(description)
#        col.set_id(col_id)
#    return columns 
#
#class VCFSchema(object):
#    """
#    Class representing the columns of a VCF database, including information 
#    on the id, type and width of each column.
#    """
#    def __init__(self):
#        self.__genotypes = []
#        self.__genotype_columns = []
#        self.__columns = {}
#        self.__add_fixed_columns()
#
#    def __add_fixed_columns(self):
#        """
#        Sets up the fixed column schema.
#        """
#        string_cols = [CHROM, ID, REF, ALT, FILTER]
#        for c in string_cols:
#            col = StringColumn()
#            col.set_id(c)
#            self.__columns[c] = col
#        c = RECORD_ID 
#        col = LongIntegerColumn()
#        col.set_id(c)
#        self.__columns[c] = col
#        c = POS
#        col = LongIntegerColumn()
#        col.set_id(c)
#        self.__columns[c] = col
#        c = QUAL
#        col = FloatColumn(10)
#        col.set_id(c)
#        self.__columns[c] = col
#    
#    def add_genotype(self, genotype):
#        """
#        Adds the specified  genotype to this schema
#        """
#        self.__genotypes.append(genotype)
#        for d in self.__genotype_columns:
#            cid = genotype + "_" + d["ID"]
#            for col_id, col in column_factory(cid, d).items():
#                self.__columns[col_id] = col 
#
#    def add_info_column(self, d):
#        """
#        Adds an INFO column with the specified desrciption and type.
#        """
#        cid = "INFO_" + d["ID"]
#        for col_id, col in column_factory(cid, d).items():
#            self.__columns[col_id] = col 
#
#    def add_genotype_column(self, d):
#        """
#        Adds a genotype column with the specified description and type.
#        """
#        self.__genotype_columns.append(d)
#    
#    def add_compound_column(self, cid):
#        """
#        Adds a compound column with the specified id.
#        """
#        cols = [self.get_column(scid) for scid in cid] 
#        col = CompoundColumn(cid, cols)
#        self.__columns[cid] = col 
#
#    def get_columns(self):
#        """
#        Returns an iterator over all columns.
#        """
#        return self.__columns.values()
#   
#    def get_column_ids(self):
#        """
#        Returns the list of column ids.
#        """
#        return self.__columns.keys()
#    
#    def get_column(self, cid):
#        """
#        Returns the column with the specified id.
#        """
#        return self.__columns[cid]
#
#    def get_genotype(self, j):
#        """
#        Returns the jth genotype id.
#        """
#        return self.__genotypes[j]
#   
#
#    def generate_packing_format(self):
#        """
#        All data for the headers has been read in and we can now decide the 
#        canonical packing format for the records.
#        """
#        self.__short_integer_columns = [] 
#        self.__long_integer_columns = [] 
#        self.__float_columns = []
#        self.__string_columns = []
#        for k, v in self.__columns.items():
#            if isinstance(v, LongIntegerColumn):
#                self.__long_integer_columns.append(k)
#            elif isinstance(v, ShortIntegerColumn):
#                self.__short_integer_columns.append(k)
#            elif isinstance(v, FloatColumn):
#                self.__float_columns.append(k)
#            elif isinstance(v, StringColumn):
#                self.__string_columns.append(k)
#        self.__long_integer_columns.sort()
#        self.__short_integer_columns.sort()
#        self.__float_columns.sort() 
#        self.__string_columns.sort()
#         
#        print("long integers = ", self.__long_integer_columns)
#        print("short integers = ", self.__short_integer_columns)
#        print("floats = ", self.__float_columns)
#        print("strings = ", self.__string_columns)
#
#
#    def pack_record(self, d, record_id):
#        """
#        Packs the specified record into a binary form suitable for 
#        storage and retreival.
#        """
#        d[RECORD_ID] = record_id
#        b = bytearray(8192)
#        offset = 0
#        for col in self.__long_integer_columns:
#            value = d[col]
#            fmt = ">q"
#            struct.pack_into(fmt, b, offset, value)
#            offset += struct.calcsize(fmt) 
#            #print("\t", col, "->", value, ":", offset)
#        for col in self.__short_integer_columns:
#            value = -1
#            if col in d:
#                value = d[col]
#            fmt = ">h"
#            struct.pack_into(fmt, b, offset, value)
#            offset += struct.calcsize(fmt) 
#            #print("\t", col, "->", value, ":", offset)
#        for col in self.__float_columns:
#            value = -1.0
#            if col in d:
#                value = d[col]
#            fmt = ">f"
#            struct.pack_into(fmt, b, offset, value)
#            offset += struct.calcsize(fmt) 
#            #print("\t", col, "->", value, ":", offset)
#        for col in self.__string_columns:
#            value = "." 
#            if col in d:
#                value = d[col]
#            #print("\t", col, "->", value.encode())
#            v = value.encode()
#            b[offset:offset + len(v)] = v
#            offset += len(v)
#            b[offset] = 0
#            offset += 1
#        #print("b = ", b[:offset] )
#        print(offset, len(d["record"]))
#

class VCFFileColumn(object):
    """
    Class representing a column in a VCF file.
    """
    NUMBER_ANY = -1
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
        return v[0] if self.number == 1 else v
    
    def get_db_column(self):
        """
        Returns a DBColumn instance suitable to represent this VCFFileColumn.
        """ 
        if self.type == self.TYPE_INTEGER:
            dbc = IntegerColumn()
            # We default to 16 bit integers; this should probably be a 
            # configuration option.
            dbc.set_size(2) 
        elif self.type == self.TYPE_FLOAT:
            dbc = FloatColumn()
        elif self.type == self.TYPE_FLAG:
            dbc = IntegerColumn()
            dbc.set_size(1) 
        elif self.type in [self.TYPE_STRING, self.TYPE_CHAR]:
            dbc = StringColumn()
        dbc.set_description(self.description)
        dbc.set_num_values(1 if self.type == self.TYPE_FLAG else self.number)
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
    col.description = d["Description"]
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
    


class VCFFileParser(object):
    """
    Class responsible for parsing a VCF file. First we contruct the database
    schema by parsing the header and ...
    
    TODO: finish docs once the process is sorted. The basic idea is that 
    this class is solely responsible for parsing the VCF file and contructing 
    the schema.
    """
    def __init__(self, vcf_file, progress_callback=None):
        """
        Allocates a new VCFFileParser for the specified file. This can be 
        either plain text, gzipped or bzipped.
        """
        self._file_path = vcf_file
        self._progress_callback = progress_callback
        self._version = -1.0
        self._genotypes = []
        self._info_columns = {}
        self._genotype_columns = {}
        self._schema = None

    def _define_schema(self):
        """
        After the file headers have been read we can allocate the 
        schema, which defines the method of packing and unpacking 
        data from database records.
        """
        self._schema = DBSchema()
        # TODO Get the text of these descriptions from the file format
        # definition and put them in here as constants.
        self._schema.add_integer_column(POS, 8, 1, "Chromosome position")
        self._schema.add_string_column(CHROM, 1, "Chromosome") #enum
        self._schema.add_string_column(ID, 1, "Identifiers")
        self._schema.add_string_column(REF, 1, "Reference allele")
        self._schema.add_string_column(ALT, 1, "Alternative allele")
        self._schema.add_float_column(QUAL, 1, "Quality")
        self._schema.add_string_column(FILTER, 1, "Filter") #enum
        for name, col in self._info_columns.items():
            self._schema.add_vcf_column(INFO + "_" + name, col)
        for genotype in self._genotypes:
            for name, col in self._genotype_columns.items():
                self._schema.add_vcf_column(genotype + "_" + name, col)
        self._schema.finalise()


    def _open_file(self):
        """
        Opens the source file so that it can be read by the parsing code. 
        Throws an error if it is not one of the supported formats:
        .vcf, .vcf.gz or .vcf.bz2
        """
        path = self._file_path
        if path.endswith(".vcf.gz"):
            f = gzip.open(path, 'rb') 
        elif path.endswith(".vcf.bz2"):
            f = bz2.BZ2File(path, 'rb') 
        elif path.endswith(".vcf"):
            f = open(path, 'rb') 
        else:
            raise ValueError("Unsupported file format")
        return f
    
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

    def _parse_record(self, s):
        """
        Parses the specified vcf record.
        """
        l = s.split()
        self._schema.set_value(CHROM, l[0])
        self._schema.set_value(POS, int(l[1]))
        self._schema.set_value(ID, l[2])
        self._schema.set_value(REF, l[3])
        self._schema.set_value(ALT, l[4])
        self._schema.set_value(QUAL, float(l[5]))
        self._schema.set_value(FILTER, l[6])
        for mapping in l[7].split(";"):
            tokens = mapping.split("=")
            col = self._info_columns[tokens[0]]
            db_name = INFO + "_" + col.name 
            if len(tokens) == 2:
                self._schema.set_value(db_name, col.parse(tokens[1]))
            else:
                assert(col.type == VCFFileColumn.TYPE_FLAG)
                self._schema.set_value(db_name, 1)
        j = 0
        fmt  = l[8].split(":")
        for genotype_values in l[9:]:
            tokens = genotype_values.split(":")
            if len(tokens) == len(fmt):
                for k in range(len(fmt)):
                    col = self._genotype_columns[fmt[k]]
                    db_name = self._genotypes[j] + "_" + fmt[k]
                    self._schema.set_value(db_name, col.parse(tokens[k]))
            elif len(tokens) > 1:
                # We can treat a genotype value on its own as missing values.
                # We can have skipped columns at the end though, which we 
                # should deal with properly. So, put in a loud complaint 
                # here and fix later.
                print("PARSING CORNER CASE NOT HANDLED!!! FIXME!!!!")
            j += 1


    def _read_header(self, f):
        """
        Reads the header for this VCF file, constructing the database
        schema and getting the parser prepared for processing records.
        """
        # Read the header
        s = (f.readline()).decode()
        self._parse_version(s)
        if self._version < 4.0:
            raise ValueError("VCF versions < 4.0 not supported")
        while s.startswith("##"):
            self._parse_meta_information(s)
            s = (f.readline()).decode()
        self._parse_header_line(s)
    
    def _read_records(self, f):
        """
        Reads the records from the VCF file, presenting records ready
        for storage to the database.
        """
        for line in f:
            #print("record size = ", len(line))
            self._schema.clear_record()
            self._parse_record(line.decode())
            # callback into the database and store the record?
    
    def parse(self):
        """
        Parses this vcf file.
        """
        with self._open_file() as f:
            self._read_header(f)
            self._define_schema()
            self._read_records(f)



class DBColumn(object):
    """
    Class representing a single column in a DBSchema. Each column has a 
    fixed type, and a methods to encode and decode values to and from 
    db format. Columns represent arrays of values, and can be either 
    single values, fixed length arrays or variable length arrays.
    """
    
    _byte_order = ">" # big-endian for now so we can sort integers.
    
    def __init__(self):
        self._description = "" 
        self._name = "" 
        self._num_values = 0
        self._storage_format = ""
        self._offset = 0

    def set_offset(self, offset):
        """
        Sets the offset at which we read the values in this column to the specified 
        value.
        """
        self._offset = offset

    def get_offset(self):
        """
        Returns the storage offset for this column.
        """
        return self._offset

    def is_fixed_size(self):
        """
        Returns true if this column can be packed into a fixed number of bytes.
        """
        return self._num_values >= 1

    def get_pack_format(self):
        """
        Returns a string representing the pack format used by the struct 
        module.
        """
        if self._num_values > 0:
            s = self._byte_order + self._storage_format * self._num_values
        else:
            s = self._byte_order + self._storage_format 
            
        return s

    def __str__(self):
        return "name={0}; num={1}; desc={2}".format(self._name, self._num_values, 
                self._description)
    
    def set_description(self, description):
        """
        Sets the column description to the specified value.
        """
        self._description = description

    def set_name(self, cname):
        """
        Sets the ID for this column to the specified value.
        """
        self._name = cname
   
    def set_num_values(self, num_values):
        """
        Sets the number of values in this column to the specified value.
        """
        self._num_values = num_values

    def get_name(self):
        """
        Returns the name of this column.
        """
        return self._name

    def get_num_values(self):
        """
        Returns the number of values in this column.
        """
        return self._num_values

    def encode(self, py_value):
        """
        Encodes the specified python value as a binary db format and 
        returns the resulting bytes object. 
        """
        ret = None
        return ret

    def decode(self, db_value):
        """
        Decodes the specified bytes object representing a database 
        value for this column and returns the resulting Python
        value.
        """
        ret = None
        return ret


class IntegerColumn(DBColumn):
    """
    Class representing signed integer columns.
    """
    storage_format_map = {
        1: "b",
        2: "h",
        4: "i",
        8: "q"
    }

    def set_size(self, size):
        """
        Sets the size of this integer column to the specified column in 
        bytes.
        """ 
        self._storage_format = self.storage_format_map[size] 
        

class FloatColumn(DBColumn):
    """
    Class representing IEEE floating point columns. 
    """
    def __init__(self):
        self._storage_format = "f"


class StringColumn(DBColumn):
    """
    Class representing String columns.
    """
    def is_fixed_size(self):
        return False

class DBSchema(object):
    """
    Class representing a schema for database records. In this schema 
    we have a set of columns supporting storage for arrays of 
    integer, float and string data.
    """
    def __init__(self):
        self.__columns = {} 
        self.__current_record = bytearray(MAX_RECORD_SIZE)
        self.__free_region = 0
        
    def add_integer_column(self, name, size, num_values, description):
        """
        Adds a column with the specified name to hold the specified 
        number of integer values of the specified size in bytes. 
        """
        col = IntegerColumn()
        col.set_name(name)
        col.set_size(size)
        col.set_num_values(num_values)
        col.set_description(description)
        self.__columns[name] = col 
    
    def add_float_column(self, name, num_values, description):
        """
        Adds a column with the specified name to hold the specified 
        number of float values.
        """
        col = FloatColumn()
        col.set_name(name)
        col.set_num_values(num_values)
        col.set_description(description)
        self.__columns[name] = col 

    def add_string_column(self, name, num_values, description):
        """
        Adds a column with the specified name to hold the specified 
        number of string values.
        """
        col = StringColumn()
        col.set_name(name)
        col.set_num_values(num_values)
        col.set_description(description)
        self.__columns[name] = col 


    def add_vcf_column(self, name, vcf_column):
        """
        Adds a column corresponding to the specified vcf column to this 
        schema.
        """
        col = vcf_column.get_db_column()
        col.set_name(name)
        self.__columns[name] = col 

    def finalise(self):
        """
        Finalises this schema, by taking all the columns that have 
        been added, and calculating and storing their record offsets.
        """
        fixed_columns = [col.get_name() for col in self.__columns.values()
                if col.is_fixed_size()]
        non_fixed_columns = [col.get_name() for col in self.__columns.values()
                if not col.is_fixed_size()]
        # Give them a canonical order.
        fixed_columns.sort()
        non_fixed_columns.sort()
        offset = 0
        for col_name in fixed_columns:
            col = self.__columns[col_name]
            col.set_offset(offset)
            fmt = col.get_pack_format()
            #print(col.get_name(), "->", repr(col), ":", col.get_num_values())
            packed_size = struct.calcsize(fmt)
            offset += packed_size
            #print("\tformat = ", fmt, ":", packed_size)
        print("end of fixed region = ", offset)
        for col_name in non_fixed_columns:
            col = self.__columns[col_name]
            col.set_offset(offset)
            # for each non-fixed column we allocate it 2 bytes so it can
            # record the offset at which it starts
            fmt = "=HH"
            packed_size = struct.calcsize(fmt)
            print(col_name, "->", repr(col), ":", col.get_num_values())
            offset += packed_size
        self.__free_region_start = offset    
        

    def clear_record(self):
        """
        Clears the current record, making it ready for values to 
        be set.
        """
        #print(self.__current_record)
        #print("size = ", self.__free_region)
        # We should really zero this out up to where it was last used.
        self.__current_record = bytearray(MAX_RECORD_SIZE)
        # TODO: Set default values for fixed values.
        self.__free_region = self.__free_region_start

    def set_value(self, col_name, value):
        """
        Sets the value for the specified column to the specified 
        value.
        """
        col = self.__columns[col_name]
        #col.set_store_record(value, self.__current_record)

        # TODO: delegate these to the Column class
        if col.is_fixed_size():
            fmt = col.get_pack_format()
            offset = col.get_offset()
            if col.get_num_values() == 1:
                struct.pack_into(fmt, self.__current_record, offset, value)
            else:
                struct.pack_into(fmt, self.__current_record, offset, *value)
            #v = self.get_value(col_name)
            #print("\t to offset ", offset, "->", v)
        else:
            # This is all a total mess, and needs to be moved up into the 
            # column classes so we can share functionality and so on.
            # The Column class should have a store_record function which 
            # takes the binary buffer as its argument.
            # The basic ideas here work well enough though.
            
            #print("set ", col_name, "->", value)
            if isinstance(col, StringColumn):
                # Lists of strings must be encoded - this is a very 
                # poor implementation. FIXME
                # This should be done at the encoded level using NULL bytes.
                if col.get_num_values() == 1:
                    v = value
                else:   
                    v = ""
                    for s in value:
                        v += s + ","
                    #print("mapped", value, "to ", v)
                fmt = "=HH"
                offset = col.get_offset()
                start = self.__free_region
                end = start + len(v)
                self.__free_region = end 
                #print("\trecording", start, end, "at offset ", offset)
                struct.pack_into(fmt, self.__current_record, offset, start, end)
                self.__current_record[start:end] = v.encode()
                #print("\tfree region at", self.__free_region) 
                #print("\tretrieved:", self.get_value(col_name))
            else:
                n = len(value)
                #print("non string length ", n)
                fmt = "=HH"
                offset = col.get_offset()
                start = self.__free_region
                end = start + n * struct.calcsize(col.get_pack_format())
                self.__free_region = end 
                #print("\tpack format = ", col.get_pack_format())
                #print("\trecording", start, end, "at offset ", offset)
                struct.pack_into(fmt, self.__current_record, offset, start, end)
                fmt = col.get_pack_format()
                offset = start
                for j in range(n):
                    struct.pack_into(fmt, self.__current_record, offset, value[j])
                    offset +=  struct.calcsize(col.get_pack_format())
                #print("\tretrieved:", self.get_value(col_name))
                



    def get_value(self, col_name):
        """
        Returns the value of the specified column in the current record.
        """
        col = self.__columns[col_name]
        ret = None
        if col.is_fixed_size():
            fmt = col.get_pack_format()
            offset = col.get_offset()
            t = struct.unpack_from(fmt, self.__current_record, offset)
            assert len(t) == col.get_num_values()
            ret = t
            if len(t) == 1:
                ret = t[0]
        else:
            if isinstance(col, StringColumn):
                fmt = "=HH"
                offset = col.get_offset()
                start, end = struct.unpack_from(fmt, self.__current_record, offset)
                b = self.__current_record[start:end] 
                ret = b.decode()
            else:
                fmt = "=HH"
                offset = col.get_offset()
                start, end = struct.unpack_from(fmt, self.__current_record, offset)
                b = self.__current_record[start:end] 
                fmt = col.get_pack_format()
                offset = start
                ret = []
                while offset < end:
                    t = struct.unpack_from(fmt, self.__current_record, offset)
                    ret.append(t[0])
                    offset +=  struct.calcsize(col.get_pack_format())

        return ret


class VCFDatabase(object):
    """
    Class representing a database of VCF records using Berkeley DB.
    """

    def __init__(self, directory):
        """
        Creates a new VCFDatabase using the specified database directory.
        """
        self.__directory = directory
        self.__environment = None
        self.__primary_db = None
        self.__dbs = {} 
        self.__indexed_columns_file = os.path.join(self.__directory, 
                "indexed_columns.pickle")
        self.__schema_file = os.path.join(self.__directory, "schema.pickle")
        self.__indexed_columns = set() 
        self.__schema = VCFSchema()
   
        
    def __decode_record(self, b):
        """
        Decodes the record in the specified bytes object.
        """
        d = self.__parse_record(b)
        d["record"] = b.decode() 
        return d 

    def __generate_callback(self, label):
        """
        Generates a callback function for the specified column label.
        """
        column = self.__schema.get_column(label)
        if isinstance(label, tuple): 
            columns = [self.__schema.get_column(cid) for cid in label]
            def callback(key, data):
                values = [] 
                j = 0
                for cid in label: 
                    if cid in self.__current_record:
                        values.append(self.__current_record[cid]) 
                    else:
                        values.append(columns[j].default_value())
                    j += 1
                return column.encode(values) 
        else:
            def callback(key, data):
                if label in self.__current_record: 
                    v = column.encode(self.__current_record[label])
                else:
                    v = column.encode(column.default_value())
                return v
        return callback

    def __setup_environment(self):
        """
        Opens the database environment and sets up the required caching 
        and page size parameter.
        """
        self.__environment = bdb.DBEnv()
        self.__environment.set_tmp_dir(tempfile.gettempdir())
        if False:
            # Standard filesystem based cache.
            self.__environment.set_cachesize(1, 0, 1) 
            flags = bdb.DB_CREATE|bdb.DB_INIT_MPOOL
        else:
            # Messing around with shm cache. I can't seem to allocate more than 
            # 512 MiB of shared memory, even though the system limits should 
            # allow much more than this. Important files that should be consulted
            # are /proc/sys/kernel/shm*. See 
            # http://www.idevelopment.info/data/Oracle/DBA_tips/Linux/LINUX_8.shtml#Configuring Shared Memory
            # for a good summary of system V shared memory.
            segment_size = 32 * 1024 * 1024
            cache_size = 512 * 1024 * 1024
            #print("allocating ", cache_size, " in ", cache_size / segment_size, " chunks")
            self.__environment.set_cachesize(0, cache_size, cache_size // segment_size) 
            #print("stored cache size: ", self.__environment.get_cachesize())
            flags = bdb.DB_CREATE|bdb.DB_INIT_MPOOL|bdb.DB_SYSTEM_MEM
            self.__environment.set_shm_key(0xbeefdead)
            #self.__environment.set_shm_key(0xdeadbeef)
        self.__environment.open(self.__directory, flags)


    def __close_environment(self):
        """
        Closes the database environment.
        """
        self.__environment.close()

    def __open_databases(self, open_flags):
        """
        Opens the primary database and secondary databases 
        
        TODO: This contains code that only makes a difference at 
        creation time. This should probably be seperated to avoid
        confusion. 
        """
        page_size = 64 * 1024 # maximum 64K
        file_pattern = "{0}.db"
        dbfile = file_pattern.format("primary")
        self.__primary_db = bdb.DB(self.__environment)
        self.__primary_db.set_pagesize(page_size)
        self.__primary_db.open(dbfile, dbtype=bdb.DB_BTREE, flags=open_flags)
        self.__dbs[RECORD_ID] = self.__primary_db
        for label in self.__indexed_columns:
            if isinstance(label, tuple):
                s = ""
                for col in label:
                    s += "+" + col 
                dbfile = file_pattern.format(s)
            else:
                dbfile = file_pattern.format(label)
            sdb = bdb.DB(self.__environment)        
            sdb.set_flags(bdb.DB_DUP|bdb.DB_DUPSORT)
            sdb.set_pagesize(page_size)
            sdb.open(dbfile, dbtype=bdb.DB_BTREE, flags=open_flags)
            self.__primary_db.associate(sdb, self.__generate_callback(label)) 
            self.__dbs[label] = sdb
  
    def __get_secondary_dbs(self):
        """
        Returns the list of secondary DBS.
        """
        return [db for db in self.__dbs.values() if db is not self.__primary_db]

    def __close_databases(self):
        """
        Closes all open database
        """
        for sdb in self.__get_secondary_dbs():
            sdb.close()
        self.__primary_db.close()
        self.__primary_db = None
        self.__dbs = {}

    def __parse_record(self, s):
        """
        Parses the specified bytes object representing a VCF record and returns a dictionary
        of key-value pairs corresponding to the columns.
        """
        l = s.split()
        d = {}
        d[CHROM] = l[0]
        d[POS] = l[1]
        d[ID] = l[2]
        d[REF] = l[3]
        d[ALT] = l[4]
        d[QUAL] = l[5]
        d[FILTER] = l[6]
        for mapping in l[7].split(b";"):
            tokens = mapping.split(b"=")
            col = INFO + "_" + tokens[0].decode()
            if len(tokens) == 2:
                d[col] = tokens[1]
            else:
                # FIXME!
                # Handle Flag column.
                d[col] = b"true" 

        fmt  = l[8].split(b":")
        j = 0
        for genotype_values in l[9:]:
            tokens = genotype_values.split(b":")
            # if the number of tokens does not match the format, treat as 
            # missing values.
            if len(tokens) == len(fmt):
                for k in range(len(fmt)):
                    column = self.__schema.get_genotype(j) + "_" + fmt[k].decode()
                    value = tokens[k]
                    # this is HORRIBLE!!
                    # README: There are major problems here. The main issue is that 
                    # abstraction doesn't really work. We need to distinguish between 
                    # parsing columns, where must be extract several values of the 
                    # same type from a particular string and DatabaseColumns which 
                    # only ever correpond to one value. So, what we really 
                    # need are two different types of column, which can be derived 
                    # from each other fairly easily.
                    mulicolumn = False
                    try:
                        self.__schema.get_column(column)
                        multicolumn = True
                    except KeyError:
                        pass
                    print(column, "->", value, "->", multicolumn)
                    if not multicolumn: 
                        d[column] = value
                    else:
                        # this is a number > 1 column
                        subtokens = value.split(b",")
                        for l in range(len(subtokens)):
                            col = column + "_{0}".format(l)
                            print(col, "->", subtokens[l])
                            d[col] = value
                        
            j += 1
        decoded = {}
        for k, v in d.items():
            col = self.__schema.get_column(k)
            decoded[k] = col.decode(v)
            #print(k, "::", v, "->", decoded[k], "\t", col.encode(decoded[k]))
        return decoded 

    def __parse_column_description(self, line):
        """
        Parses the column description in the specified line and 
        returns a dictionary of the key-value pairs within it.
        """
        d = {}
        s = line[line.find("<") + 1: line.find(">")]
        for j in range(3):
            k = s.find(",")
            tokens = s[:k].split("=")
            s = s[k + 1:]
            d[tokens[0]] = tokens[1]
        tokens = s.split("=")
        d[tokens[0]] = tokens[1]
        return d
        
    
    def __process_header(self, s):
        """
        Processes the specified header string to get the genotype labels.
        """
        for genotype in s.split()[9:]:
            self.__schema.add_genotype(genotype)

    def __process_meta_information(self, line):
        """
        Processes the specified meta information line to obtain the values 
        of the various columns and their types.
        """
        if line.startswith("##INFO"):
            d = self.__parse_column_description(line)
            self.__schema.add_info_column(d)
        elif line.startswith("##FORMAT"):
            d = self.__parse_column_description(line)
            self.__schema.add_genotype_column(d)
    
    def __setup_indexed_columns(self):
        """
        Sets up the set of columns to be indexed.
        """
        """
        print("potential_columns = ")
        cols = sorted(self.__schema.get_column_ids())
        for cid in cols:
            col = self.__schema.get_column(cid)
            print("\t", col.get_id(), "->", col)
        """
        # This is clearly bollocks - but fill in the columns you're 
        # interested in indexing here.
        self.__indexed_columns = {(CHROM, POS) }
        """
        # INFO columns
        "INFO_DP", "INFO_AF",
        # Genotype columns
        "H27_GT", "H27_GQ", 
        # Compound columns are specified with tuples.
        ("H27_GT", "H27_GQ")}
        """
        # setup the compound columns.
        for col in self.__indexed_columns:
            if isinstance(col, tuple):
                self.__schema.add_compound_column(col)

        with open(self.__indexed_columns_file, "wb") as f:
            pickle.dump(self.__indexed_columns, f) 

    def __parse_file(self, filename):
        """
        Parses the specified gzipped VCF file, and yields each record in turn.
        """
        with gzip.open(filename, 'rb') as f:
            # Read the header
            s = (f.readline()).decode()
            while s.startswith("##"):
                self.__process_meta_information(s)
                s = (f.readline()).decode()
            self.__process_header(s)
            self.__schema.generate_packing_format()
            self.__setup_indexed_columns()
            # save the schema 
            with open(self.__schema_file, "wb") as fs:
                pickle.dump(self.__schema, fs)
            # Now process the records
            for l in f:
                d = self.__parse_record(l)
                yield l.strip(), d
    
    def parse(self, vcf_file):
        """
        Parses the specified VCF file and inserts records into the database. 
        """
        self.__setup_environment()
        processed = 0
        open_flags = bdb.DB_CREATE|bdb.DB_TRUNCATE
        col = self.__schema.get_column(RECORD_ID)
        self.__primary_db = None
        for data, record in self.__parse_file(vcf_file):
            record['record'] = data 
            self.__schema.pack_record(record, processed)
            
            self.__current_record = record
            # Awful - the header processing code is hidden away in here
            if self.__primary_db == None:
                self.__open_databases(open_flags) 
            
            primary_key = col.encode(processed)
            self.__primary_db.put(primary_key, data)
            processed += 1
            if processed % 1000000 == 0:
                print("processed", processed, "records")
        self.__close_databases()
        self.__close_environment()

    def open(self):
        """
        Opens this VCFDatabase for reading.
        """
        self.__setup_environment()
        # load the indexed columns and the schema
        with open(self.__indexed_columns_file, "rb") as f:
            self.__indexed_columns = pickle.load(f)
        with open(self.__schema_file, "rb") as f:
            self.__schema = pickle.load(f)


        self.__open_databases(bdb.DB_RDONLY)


    def close(self):
        """
        Closes all open handles.
        """
        self.__close_databases()
        self.__close_environment()

    def get_indexed_columns(self):
        """
        Returns the list of indexed columns in the database. 
        """
        return self.__indexed_columns
    
    def get_columns(self):
        """
        Returns the list of all possible columns in this database.
        """
        return self.__schema.get_column_ids() 

    def get_record(self, record_id):
        """
        Returns the record at the specified position.
        """
        col = self.__schema.get_column(RECORD_ID)
        r = self.__primary_db.get(col.encode(record_id))
        if r is None:
            # Probably a more appropriate error to raise here...
            raise KeyError("record {0} not found".format(record_id))
        return self.__decode_record(r)

    def get_min(self, column):
        """
        Returns the smallest column value in the DB.
        """
        cursor = self.__dbs[column].cursor()
        k, v = cursor.first()
        cursor.close()
        return self.__schema.get_column(column).decode(k)
    
    def get_max(self, column):
        """
        Returns the largest column value in the DB.
        """
        cursor = self.__dbs[column].cursor()
        k, v = cursor.last()
        cursor.close() 
        return self.__schema.get_column(column).decode(k)
   

    def __run_range_query(self, query_range, max_records):
        """
        Runs a range query, yielding records in turn.
        """
        col, dec_min, dec_max = query_range
        cursor = self.__dbs[col].cursor()
        column = self.__schema.get_column(col)
        enc_min = column.encode(dec_min)
        enc_max = column.encode(dec_max)
        ret = cursor.set_range(enc_min)
        j = 0
        while ret is not None:
            k, v = ret 
            if k < enc_max and j < max_records:
                yield self.__decode_record(v)
                ret = cursor.next() 
                j += 1
            else:
                ret = None
        cursor.close()
    
    def __run_simple_query(self, query_keys, max_records):
        """
        Runs the simple query iterating over either all records in the 
        database or all records specified by a single equality constraint.
        """
        if query_keys == []:
            cursor = self.__primary_db.cursor()
            ret = cursor.first()
            next_function = cursor.next
        else:
            col, key = query_keys[0] 
            cursor = self.__dbs[col].cursor()
            enc_key = self.__schema.get_column(col).encode(key)
            next_function = cursor.next_dup
            ret = cursor.set(enc_key)
        j = 0
        while ret is not None and j < max_records:
            k, v = ret 
            yield self.__decode_record(v) 
            ret = next_function() 
            j += 1 
        cursor.close()

    def __get_join_cursor(self, query_keys):
        """
        Returns a join cursor for the specified set of query keys.
        """
        join_cursors = []
        for col, key in query_keys:
            cursor = self.__dbs[col].cursor()
            enc_key = self.__schema.get_column(col).encode(key)
            ret = cursor.set(enc_key)
            # Should we check this??
            join_cursors.append(cursor)
        cursor = self.__primary_db.join(join_cursors)
        return cursor, join_cursors


    def __run_join_query(self, query_keys, max_records):
        """
        Runs a join query where we have multiple equality constraints.
        """
        cursor, join_cursors = self.__get_join_cursor(query_keys)
        # we have to have this flags here or we segfault - weird.
        ret = cursor.get(flags=0)
        j = 0
        while ret is not None and j < max_records:
            k, v = ret
            yield self.__decode_record(v) 
            ret = cursor.get(flags=0)
            j += 1 
        cursor.close()
        for cursor in join_cursors:
            cursor.close()


    def get_records(self, query_keys=[], query_range=None, 
            max_records=sys.maxsize):
        """
        Iterate over the records defined by the specified list of query keys 
        or query range. The query_keys parameter must be a list of 
        (column, value) pairs, specifying the value required in a particular
        column; query range is a tuple (column, min, max) specifying the range 
        of values required in a particular column. Range and key queries 
        cannot combined.
        """
        g = None
        if query_range is not None:
            g = self.__run_range_query(query_range, max_records)
        else:
            num_queries = len(query_keys)
            if num_queries < 2:
                g = self.__run_simple_query(query_keys, max_records)
            else:
                g = self.__run_join_query(query_keys, max_records)
        return [] if g is None else (x for x in g)
    
    def get_num_records(self, query_keys=[]):
        """
        Returns the number of records in which the specified column has the specified 
        key value.
        """
        count = 0
        n = len(query_keys) 
        if n == 0: 
            # finding the total number of records in the DB can require a full 
            # traversal. Better to store some metadata in a database file of 
            # our own which we make at build time.
            raise ValueError("Empty query keys currently not supported")
        elif n == 1: 
            column, key = query_keys[0]
            cursor = self.__dbs[column].cursor()
            enc_key = self.__schema.get_column(column).encode(key)
            ret = cursor.set(enc_key)
            if ret is not None:
                count = cursor.count()
            cursor.close()
        else:
            cursor, join_cursors = self.__get_join_cursor(query_keys)
            # This segfaults if we run it - annoying!
            #count = cursor.count() 
            count = -1
            cursor.close()
            for cursor in join_cursors:
                cursor.close()
        return count

    def get_distinct_values(self, column):
        """
        Returns an iterator over the distinct values for the specified column.
        """
        cursor = self.__dbs[column].cursor()
        col = self.__schema.get_column(column)
        ret = cursor.first()
        while ret is not None:
            k, v = ret 
            yield col.decode(k) 
            ret = cursor.next_nodup()
        cursor.close()
       
         
@contextmanager
def opendb(dbname, dbdir=DEFAULT_DB_DIR):
    """
    Returns a new VCFDatabase for reading.
    """
    vi = VCFDatabase(os.path.join(dbdir, dbname))
    vi.open()
    try:    
        yield vi
    finally:
        vi.close()


