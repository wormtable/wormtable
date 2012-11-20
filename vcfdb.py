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

import _vcfdb

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
        return v 
    
    def get_db_column(self):
        """
        Returns a DBColumn instance suitable to represent this VCFFileColumn.
        """ 
        dbc = DBColumn()
        dbc.set_num_elements(self.number)
        if self.type == self.TYPE_INTEGER:
            # We default to 16 bit integers; this should probably be a 
            # configuration option.
            dbc.set_element_type(DBColumn.ELEMENT_TYPE_INT_2) 
        elif self.type == self.TYPE_FLOAT:
            dbc.set_element_type(DBColumn.ELEMENT_TYPE_FLOAT) 
        elif self.type == self.TYPE_FLAG:
            dbc.set_element_type(DBColumn.ELEMENT_TYPE_INT_1) 
        elif self.type in [self.TYPE_STRING, self.TYPE_CHAR]:
            dbc = DBEnumerationColumn()
            dbc.set_num_elements(self.number)
        dbc.set_description(self.description)
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
    def __init__(self, vcf_file, db_builder):
        """
        Allocates a new VCFFileParser for the specified file. This can be 
        either plain text, gzipped or bzipped.
        """
        self._file_path = vcf_file
        self._version = -1.0
        self._genotypes = []
        self._info_columns = {}
        self._genotype_columns = {}
        self._file_size = float(os.stat(vcf_file).st_size)

    def get_schema(self):
        """
        After the file headers have been read we can allocate the 
        schema, which defines the method of packing and unpacking 
        data from db_builder records.
        """
        schema = Schema()
        # TODO Get the text of these descriptions from the file format
        # definition and put them in here as constants.
        schema.add_integer_column(POS, 8, 1, "Chromosome position")
        schema.add_string_column(CHROM, 1, "Chromosome") #enum
        # TODO: this is a list 
        schema.add_string_column(ID, 1, "Identifiers")
        schema.add_string_column(REF, 1, "Reference allele")
        # TODO: this is a list 
        schema.add_string_column(ALT, 1, "Alternative allele")
        schema.add_float_column(QUAL, 1, "Quality")
        # TODO: this is a list 
        schema.add_string_column(FILTER, 1, "Filter") #enum
        for name, col in self._info_columns.items():
            schema.add_vcf_column(INFO + "_" + name, col)
        for genotype in self._genotypes:
            for name, col in self._genotype_columns.items():
                schema.add_vcf_column(genotype + "_" + name, col)
        schema.finalise()
        return schema 

    def open_file(self):
        """
        Opens the source file so that it can be read by the parsing code. 
        Throws an error if it is not one of the supported formats:
        .vcf or .vcf.gz 
        """
        path = self._file_path
        if path.endswith(".vcf.gz"):
            f = gzip.open(path, 'rb') 
            self._backing_file = f.fileobj
        elif path.endswith(".vcf"):
            f = open(path, 'rb') 
            self._backing_file = f
        else:
            raise ValueError("Unsupported file format")
        return f
   
    def get_progress(self):
        """
        Returns the progress through the file as a fraction.
        """
        return self._backing_file.tell() / self._file_size

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

    def parse_record(self, s, dbb):
        """
        Parses the specified vcf record calling set_record_value on the specified 
        DatabaseBuilder for each column.
        """
        l = s.split()
        # TODO: a lot of these should actually be lists anyway...
        dbb.set_record_value(CHROM, [l[0]])
        dbb.set_record_value(POS, [int(l[1])])
        dbb.set_record_value(ID, [l[2]])
        dbb.set_record_value(REF, [l[3]])
        dbb.set_record_value(ALT, [l[4]])
        dbb.set_record_value(QUAL, [float(l[5])])
        dbb.set_record_value(FILTER, [l[6]])
        for mapping in l[7].split(";"):
            tokens = mapping.split("=")
            col = self._info_columns[tokens[0]]
            db_name = INFO + "_" + col.name 
            if len(tokens) == 2:
                dbb.set_record_value(db_name, col.parse(tokens[1]))
            else:
                assert(col.type == VCFFileColumn.TYPE_FLAG)
                dbb.set_record_value(db_name, 1)
        j = 0
        fmt  = l[8].split(":")
        for genotype_values in l[9:]:
            tokens = genotype_values.split(":")
            if len(tokens) == len(fmt):
                for k in range(len(fmt)):
                    col = self._genotype_columns[fmt[k]]
                    db_name = self._genotypes[j] + "_" + fmt[k]
                    dbb.set_record_value(db_name, col.parse(tokens[k]))
            elif len(tokens) > 1:
                # We can treat a genotype value on its own as missing values.
                # We can have skipped columns at the end though, which we 
                # should deal with properly. So, put in a loud complaint 
                # here and fix later.
                print("PARSING CORNER CASE NOT HANDLED!!! FIXME!!!!")
            j += 1

    def read_header(self, f):
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
    
        
class DBColumn(object):
    """
    Class representing a single column in a Schema. 
    """
    
    _byte_order = ">" # big-endian for now so we can sort integers.
    VARIABLE_RECORD_OFFSET_FORMAT = ">HH" 
    
    ELEMENT_TYPE_CHAR = 0
    ELEMENT_TYPE_INT_1 = 1
    ELEMENT_TYPE_INT_2 = 2
    ELEMENT_TYPE_INT_4 = 3
    ELEMENT_TYPE_INT_8 = 4
    ELEMENT_TYPE_FLOAT = 5
    
    STORAGE_FORMAT_MAP = {
        ELEMENT_TYPE_CHAR : "c",
        ELEMENT_TYPE_INT_1 : "b",
        ELEMENT_TYPE_INT_2 : "h",
        ELEMENT_TYPE_INT_4 : "i",
        ELEMENT_TYPE_INT_8 : "q",
        ELEMENT_TYPE_FLOAT : "f"
    }

    def __init__(self):
        self._description = "" 
        self._name = "" 
        self._num_elements = -1 
        self._element_type = self.ELEMENT_TYPE_CHAR
        self._offset = 0

    def set_element_type(self, element_type):
        """
        Sets the column element type to the specified value.
        """
        self._element_type = element_type

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

    def get_fixed_region_size(self):
        """
        Returns the number of bytes occupied by this column within the 
        fixed region.
        """
        s = self.VARIABLE_RECORD_OFFSET_FORMAT 
        if self._num_elements > 0:
            fmt = self.STORAGE_FORMAT_MAP[self._element_type]
            s = self._byte_order + fmt * self._num_elements
        return struct.calcsize(s)

    
    def __str__(self):
        return "{0}\t{1}\t{2}\t{3}".format(self._name, self._num_elements, 
                self._offset, self._element_type)
    
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
   
    def set_num_elements(self, num_elements):
        """
        Sets the number of values in this column to the specified value.
        """
        self._num_elements = num_elements

    def get_name(self):
        """
        Returns the name of this column.
        """
        return self._name

    def get_num_elements(self):
        """
        Returns the number of values in this column.
        """
        return self._num_elements

    def set_value(self, values, record_buffer):
        """
        Encodes the specified Python value into its raw binary equivalent 
        and stores it in the specified record buffer. 
        """
        #print("setting ", self, " = ", values)
        ret = None
        s = self.STORAGE_FORMAT_MAP[self._element_type]
        fmt = self._byte_order + s 
        element_size = struct.calcsize(s)
        offset = self._offset
        n  = self._num_elements
        if n < 0:
            n = len(values)
            start = record_buffer.current_record_size
            record_buffer.current_record_size += n * element_size 
            struct.pack_into(self.VARIABLE_RECORD_OFFSET_FORMAT, record_buffer, 
                    self._offset, start, n)
            offset = start
        for j in range(n):
            struct.pack_into(fmt, record_buffer, offset + j * element_size, values[j])
       
    def get_value(self, record_buffer):
        """
        Returns the value of this column in the specifed record buffer.
        """
        ret = None
        s = self.STORAGE_FORMAT_MAP[self._element_type]
        fmt = self._byte_order + s 
        element_size = struct.calcsize(s)
        offset = self._offset
        n  = self._num_elements
        if n < 0: 
            offset, n = struct.unpack_from(self.VARIABLE_RECORD_OFFSET_FORMAT,
                    record_buffer, self._offset)
        ret = []
        for j in range(n):
            t = struct.unpack_from(fmt, record_buffer, offset + j * element_size) 
            ret.append(t[0])
        return ret


class DBEnumerationColumn(DBColumn):
    """
    Class of column in which we have a set of strings mapped 
    to integer values.
    """
    def __init__(self):
        self._element_type = self.ELEMENT_TYPE_INT_2
        self._value_map = {}
        self._key_map = {}
        self._next_key = 0
    
    def __str__(self):
        return "{0}\t{1}\t{2}\t{3}\t{4}".format(self._name, self._num_elements, 
                self._offset, self._element_type, self._key_map)

    def set_value(self, values, record_buffer):
        """
        Sets the value of this column to the specified string. This 
        is first looked up in the value map, and if it does not exist,
        it is then added to the map.
        """
        for v in values:
            if v not in self._value_map:
                self._value_map[v] = self._next_key
                self._key_map[self._next_key] = v
                self._next_key += 1
        keys = [self._value_map[v] for v in values]
        #print("setting ", self._name, " = ", keys, "(", values, ")")
        super().set_value(keys, record_buffer)

    def get_value(self, record_buffer):
        """
        Returns the value of this column.
        """
        keys = super().get_value(record_buffer)
        return [self._key_map[k] for k in keys]


class Schema(object):
    """
    Class representing a schema for database records. In this schema 
    we have a set of columns supporting storage for arrays of 
    integer, float and string data.
    """
    def __init__(self):
        self.__columns = {} 
        self.__primary_key_format = ">Q"
        self.__fixed_region_size = 0 
        
    def get_primary_key_format(self):
        """
        Returns the packing format for the primary key.
        """
        return self.__primary_key_format

    def get_fixed_region_size(self):
        """
        Returns the length of the fixed region of a record from this 
        schema.
        """
        return self.__fixed_region_size
       
    def get_columns(self):
        """
        Returns the list of columns.
        """
        return self.__columns.values()    

    def get_column(self, name):
        """
        Returns the column with the specified name.
        """
        return self.__columns[name]
    
    def add_integer_column(self, name, size, num_values, description):
        """
        Adds a column with the specified name to hold the specified 
        number of integer values of the specified size in bytes. 
        """
        size_map = {
            1: DBColumn.ELEMENT_TYPE_INT_1,
            2: DBColumn.ELEMENT_TYPE_INT_2,
            4: DBColumn.ELEMENT_TYPE_INT_4,
            8: DBColumn.ELEMENT_TYPE_INT_8
        }
        col = DBColumn()
        col.set_name(name)
        col.set_element_type(size_map[size])
        col.set_num_elements(num_values)
        col.set_description(description)
        self.__columns[name] = col 
    
    def add_float_column(self, name, num_values, description):
        """
        Adds a column with the specified name to hold the specified 
        number of float values.
        """
        col = DBColumn()
        col.set_name(name)
        col.set_element_type(DBColumn.ELEMENT_TYPE_FLOAT)
        col.set_num_elements(num_values)
        col.set_description(description)
        self.__columns[name] = col 

    def add_string_column(self, name, num_values, description):
        """
        Adds a column with the specified name to hold the specified 
        number of string values.
        """
        col = DBEnumerationColumn()
        col.set_name(name)
        col.set_num_elements(num_values)
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
        offset = 0
        for col in self.__columns.values():
            col.set_offset(offset)
            offset += col.get_fixed_region_size() 
        self.__fixed_region_size = offset
        
class DatabaseBuilder(object):
    """
    Class that builds a database from an input file. 
    """
    def __init__(self, directory):
        self.__directory = directory
        self.__schema = None
        self.__bdb = _vcfdb.BerkeleyDatabase()
        self.__record_buffer = _vcfdb.RecordBuffer()
        self.__current_record_id = 0
    
    def set_record_value(self, column, value):
        """
        Sets the value for the specified column to the specified value 
        in the current database record.
        """
        #print("storing ", column, "->", value)
        col = self.__schema.get_column(column)
        col.set_value(value, self.__record_buffer)
        #v = col.get_value(self.__record_buffer)
        #print(col.get_name(), ":", value, "->", v)
          

    def __prepare_record(self):
        """
        Prepares to generate a new record.
        """
        self.__current_record_id += 1
        self.__record_buffer.current_record_size = self.__schema.get_fixed_region_size()

    def __commit_record(self):
        """
        Commits the current record into the database.
        """
        # build the key
        fmt = self.__schema.get_primary_key_format()
        key = struct.pack(fmt, self.__current_record_id) 
        self.__record_buffer.commit_record(key)


    def parse_vcf(self, vcf_file, indexed_columns=[], progress_callback=None):
        """
        Parses the specified vcf file, generating a new database in 
        with the specified indexed columns. The specified callback 
        function is called periodically if not None.
        """
        parser = VCFFileParser(vcf_file, self)
        self.__bdb.open()
        last_progress = 0 
        with parser.open_file() as f:
            parser.read_header(f)
            self.__schema = parser.get_schema()
            for line in f:
                self.__prepare_record()
                parser.parse_record(line.decode(), self)
                self.__commit_record()
                
                progress = int(parser.get_progress() * 1000)
                if progress != last_progress:
                    last_progress = progress 
                    if progress_callback is not None:
                        progress_callback(progress, self.__record_id)
        # TODO: flush the record buffer.
        self.__bdb.close()
        for col in self.__schema.get_columns():
            print(col)

######################## OLD CODE ############################################

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

if __name__ == "__main__":
    # temp development code.
    vcf_file = sys.argv[1]
    dbb = DatabaseBuilder("tmp")
    dbb.parse_vcf(vcf_file)

