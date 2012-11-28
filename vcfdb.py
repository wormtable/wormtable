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

from xml.etree import ElementTree
from xml.dom import minidom

import _vcfdb

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
            dbc.set_element_type(_vcfdb.ELEMENT_TYPE_INT_2) 
        elif self.type == self.TYPE_FLOAT:
            dbc.set_element_type(_vcfdb.ELEMENT_TYPE_FLOAT) 
        elif self.type == self.TYPE_FLAG:
            dbc.set_element_type(_vcfdb.ELEMENT_TYPE_INT_1) 
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
    # big-endian so we can sort integers lexicographically.
    
    # TODO we need different byte orders here for simplicity;
    # ints should be big endian and floats should be native.
    # This give us the best of both worlds, as we can define 
    # a native comparison function for floats but still make use 
    # the key prefix compression for ints.
    
    BYTE_ORDER = "="  
    # This is what determines the max record size. We can also have 
    # no more than 256 elements in a list. This is encoded in 
    # native format so we don't have to worry about byte swapping 
    # in C. This should probably be standardised to big-endian.
    VARIABLE_RECORD_OFFSET_FORMAT = "=HB" 
    
    STORAGE_FORMAT_MAP = {
        _vcfdb.ELEMENT_TYPE_CHAR : "c",
        _vcfdb.ELEMENT_TYPE_INT_1 : "b",
        _vcfdb.ELEMENT_TYPE_INT_2 : "h",
        _vcfdb.ELEMENT_TYPE_INT_4 : "i",
        _vcfdb.ELEMENT_TYPE_INT_8 : "q",
        _vcfdb.ELEMENT_TYPE_FLOAT : "f"
    }

    def __init__(self):
        self._description = "" 
        self._name = "" 
        self._num_elements = -1 
        self._element_type = _vcfdb.ELEMENT_TYPE_CHAR
        self._offset = 0

    def get_low_level_format(self):
        """
        Returns the format of this column in the low-level C 
        """
        col_type = _vcfdb.COLUMN_TYPE_FIXED 
        if self._num_elements == -1:
            col_type = vcfdb.COLUMN_TYPE_VARIABLE
        return [col_type, self._element_type, self._offset]


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
            s = self.BYTE_ORDER + fmt * self._num_elements
        return struct.calcsize(s)

    
    def __str__(self):
        return "{0}\t\t\t{1}\t{2}\t{3}".format(self._name, self._num_elements, 
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

    def get_new_style_column(self):
        s = self.STORAGE_FORMAT_MAP[self._element_type]
        fmt = self.BYTE_ORDER + s 
        element_size = struct.calcsize(s)
        new_col = _vcfdb.Column(self._name, self._description, 
                offset=self._offset, 
                element_type=self._element_type, 
                element_size=element_size, 
                num_elements=self._num_elements)
        return new_col

    def set_value(self, values, record_buffer):
        """
        Encodes the specified Python value into its raw binary equivalent 
        and stores it in the specified record buffer. 
        """
        #print("setting ", self, " = ", values)
        ret = None
        s = self.STORAGE_FORMAT_MAP[self._element_type]
        fmt = self.BYTE_ORDER + s 
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
        fmt = self.BYTE_ORDER + s 
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


# TODO add in an option to declare 16 bit enums also
class DBEnumerationColumn(DBColumn):
    """
    Class of column in which we have a set of strings mapped 
    to integer values.
    """
    def __init__(self):
        self._element_type = _vcfdb.ELEMENT_TYPE_INT_1
        self._value_map = {}
        self._key_map = {}
        self._next_key = 0
    
    def __str__(self):
        return "{0}\t\t\t{1}\t{2}\t{3}\t{4}".format(self._name, self._num_elements, 
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
            1: _vcfdb.ELEMENT_TYPE_INT_1,
            2: _vcfdb.ELEMENT_TYPE_INT_2,
            4: _vcfdb.ELEMENT_TYPE_INT_4,
            8: _vcfdb.ELEMENT_TYPE_INT_8
        }
        col = DBColumn()
        col.set_name(name)
        col.set_element_type(size_map[size])
        col.set_num_elements(num_values)
        col.set_description(description)
        self.__columns[name] = col 
   
        #print("adding column:")
        #c = _vcfdb.Column(name, description, offset=0, element_type=0, 
        #    element_size=5, num_elements=1)
        #print("adding column:", c.name, c.element_size)

        #c.set_record_value(123512)

    def add_float_column(self, name, num_values, description):
        """
        Adds a column with the specified name to hold the specified 
        number of float values.
        """
        col = DBColumn()
        col.set_name(name)
        col.set_element_type(_vcfdb.ELEMENT_TYPE_FLOAT)
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
       

    def write_xml(self):
        d = {"version":"1.0"}
        root = ElementTree.Element("schema", d)
        # doesn't work.
        comment = ElementTree.Comment("Generated VCF schema")
        root.append(comment)
        columns = ElementTree.Element("columns")
        root.append(columns)
        sorted_cols = sorted(self.__columns.values(), key=lambda col: col.get_offset())
        for old_col in sorted_cols:
            c = old_col.get_new_style_column()
            element_type = "tmp"
            d = {
                "name":c.name, 
                "description":c.description,
                "offset":str(c.offset), 
                "element_size":str(c.element_size),
                "num_elements":str(c.num_elements),
                "element_type":element_type
            }
            element = ElementTree.Element("column", d)
            columns.append(element)
        tree = ElementTree.ElementTree(root)
        raw_xml = ElementTree.tostring(root, 'utf-8')
        reparsed = minidom.parseString(raw_xml)
        pretty = reparsed.toprettyxml(indent="  ")
        with open("db_NOBACKUP_/schema.xml", "w") as f:
            f.write(pretty)

    def read_xml(self):
        tree = ElementTree.parse("db_NOBACKUP_/schema.xml")
        root = tree.getroot()
        if root.tag != "schema":
            # Should have a custom error for this.
            raise ValueError("invalid xml")
        # check version
        columns = root.find("columns")
        for xmlcol in columns.getchildren():
            if xmlcol.tag != "column":
                raise ValueError("invalid xml")
            name = xmlcol.get("name")
            offset = int(xmlcol.get("offset"))
            #print(name, offset)



class DatabaseBuilder(object):
    """
    Class that builds a database from an input file. 
    """
    def __init__(self, directory):
        self.__directory = directory
        self.__schema = None
        self.__database = _vcfdb.BerkeleyDatabase()
        self.__record_buffer = _vcfdb.RecordBuffer(self.__database)
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
          
    
    def __setup_indexes(self):
        """
        Sets up the default indexes for the database that is 
        to be built.
        """
        chromosome = self.__schema.get_column("CHROM")
        position = self.__schema.get_column("POS")
        quality = self.__schema.get_column("QUAL")
        i1 = [b"db_NOBACKUP_/chrom+pos.db", chromosome.get_low_level_format(), 
            position.get_low_level_format()]
        i2 = [b"db_NOBACKUP_/qual.db", quality.get_low_level_format()]
        # Disable indexes for now.
        #self.__database.add_index([i1, i2])

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
        # TODO: this should really be 48 bit - 64 is ridiculous.
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
        self.__database.create()
        last_progress = 0 
        with parser.open_file() as f:
            parser.read_header(f)
            self.__schema = parser.get_schema()
            self.__setup_indexes()
            for line in f:
                self.__prepare_record()
                parser.parse_record(line.decode(), self)
                self.__commit_record()
                

                progress = int(parser.get_progress() * 1000)
                if progress != last_progress:
                    last_progress = progress 



                    if progress_callback is not None:
                        progress_callback(progress, self.__current_record_id)
        self.__record_buffer.flush()
        self.__database.close()
        with open("db_NOBACKUP_/schema.pkl", "wb") as f:
            pickle.dump(self.__schema, f)

        for col in self.__schema.get_columns():
            print(col)

class DatabaseReader(object):
    """
    Class representing a database reader for a particular directory.
    """
    def __init__(self):
        with open("db_NOBACKUP_/schema.pkl", "rb") as f:
            self.__schema = pickle.load(f)
        
        self.__schema.write_xml()
        self.__schema.read_xml()

        self.__database = _vcfdb.BerkeleyDatabase()
        self.__record_buffer = _vcfdb.RecordBuffer(self.__database)
        self.__database.open()

    
    def get_records(self, column_names): 
        """
        Returns an iterator over records.
        """
        cols = []
        for name in column_names:
            old_col = self.__schema.get_column(name) 
            cols.append(old_col.get_new_style_column())
        print(cols) 
        self.__record_buffer.set_selected_columns(cols) 
        self.__record_buffer.fill()
        record = self.__record_buffer.retrieve_record()
        while record is not None:
            yield record 
            record = self.__record_buffer.retrieve_record()
        #print("got record:", record_id) 
    
    def close(self):
        """
        Closes the backing database.
        """
        self.__database.close()



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
        dbb = DatabaseBuilder("db_NOBACKUP_")
        dbb.parse_vcf(vcf_file, progress_callback=progress_monitor)
    else:
        dbr = DatabaseReader()
        """
        records = 0
        for r in dbr.get_records(["POS", "QUAL"]):
            print(r)
            #print(r.record_id, r.POS[0], r.QUAL[0], sep="\t")
            records += 1
        print("read ", records, "records")
        """
        dbr.close()

if __name__ == "__main__":
    # temp development code.
    main()

