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
    


class Schema(object):
    """
    Class representing a database schema, consisting of a list of 
    Column objects.
    """
    ELEMENT_TYPE_STRING_MAP = {
        _vcfdb.ELEMENT_TYPE_INT: "int",
        _vcfdb.ELEMENT_TYPE_CHAR: "char",
        _vcfdb.ELEMENT_TYPE_FLOAT: "float",
        _vcfdb.ELEMENT_TYPE_ENUM: "enum"
    }

    def __init__(self, columns):
        self.__columns = columns 
    
    def get_column(self):
        """
        Returns the list of columns in this schema.
        """
        return self.__columns

    def show(self):
        """
        Writes out the schema to the console in human readable format.
        """
        # print("name\telement_type\telement_size\tnum_elements")
        max_width = max(len(c.name) for c in self.__columns)
        s = "{0:<25}{1:<12}{2:<12}{3:<12}"
        print(65 * "=")
        print(s.format("name", "type", "size", "num_elements"))
        print(65 * "=")
        for c in self.__columns:
            t = self.ELEMENT_TYPE_STRING_MAP[c.element_type]
            print(s.format(c.name, t, c.element_size, c.num_elements))

    def write_xml(self, directory, filename="schema.xml"):
        """
        Writes out this schema to the specified file.
        """ 
        d = {"version":"1.0"}
        root = ElementTree.Element("schema", d)
        columns = ElementTree.Element("columns")
        root.append(columns)
        for c in self.__columns:
            element_type = self.ELEMENT_TYPE_STRING_MAP[c.element_type]
            d = {
                "name":c.name, 
                "description":c.description,
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
        name = os.path.join(directory, filename)
        with open(name, "w") as f:
            f.write(pretty)

    def read_xml(directory, filename="schema.xml"):
        """
        Returns a new schema object read from the specified file.
        """
        name = os.path.join(directory, filename)
        tree = ElementTree.parse(name)
        root = tree.getroot()
        if root.tag != "schema":
            # Should have a custom error for this.
            raise ValueError("invalid xml")
        # TODO check version
        columns = root.find("columns")
        for xmlcol in columns.getchildren():
            if xmlcol.tag != "column":
                raise ValueError("invalid xml")
            name = xmlcol.get("name")
            offset = int(xmlcol.get("offset"))
            #print(name, offset)



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
            _vcfdb.Column("POS", "position", int_type, 6, 1),
            _vcfdb.Column("CHROM", "Chromosome", enum_type, 2, 1),
            _vcfdb.Column("ID", "ID", char_type, 1, 0),
            _vcfdb.Column("REF", "Reference allele", enum_type, 1, 1),
            _vcfdb.Column("ALT", "Alternatve allele", enum_type, 1, 0),
            _vcfdb.Column("QUAL", "Quality", float_type, 4, 1),
            _vcfdb.Column("FILTER", "Filter", enum_type, 1, 0),
        ]
        for name, col in self._info_columns.items():
            columns.append(col.get_db_column("INFO"))
        for genotype in self._genotypes:
            for name, col in self._genotype_columns.items():
                columns.append(col.get_db_column(genotype))
        schema = Schema(columns)
        schema.show()
        return schema





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

class DatabaseWriter(object):
    """
    Class responsible for generating databases.
    """
    def __init__(self):
        self._database_dir = None 
        self._schema = []
        self._database = None # Database()
        self._record_buffer = None # WriteBuffer()
        self._current_record_id = 0
        # filenames, indexes, ?? 

class VCFDatabaseWriter(DatabaseWriter):
    """
    Class responsible for parsing a VCF file and creating a database. 
    """
    def __init__(self, vcf_file, database_dir):
        self._directory = database_dir
        self._vcf_file = vcf_file
        self._backing_file = None
        self._source_file = self._open_file()  
        self._vcf_version = -1.0
        self._source_file_size = float(os.stat(vcf_file).st_size)
        # Not sure about these
        self._genotypes = []
        self._info_columns = {}
        self._genotype_columns = {}

    def _open_file(self):
        """
        Opens the source file so that it can be read by the parsing code. 
        Throws an error if it is not one of the supported formats:
        .vcf or .vcf.gz 
        """
        path = self._vcf_file
        if path.endswith(".vcf.gz"):
            f = gzip.open(path, 'rb') 
            self._backing_file = f.fileobj
        elif path.endswith(".vcf"):
            f = open(path, 'rb') 
            self._backing_file = f
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
    
     
    def get_progress(self):
        """
        Returns the progress through the file as a fraction.
        """
        return self._backing_file.tell() / self._source_file_size

   


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
        #dbb = DatabaseBuilder("db_NOBACKUP_")
        #dbb.parse_vcf(vcf_file, progress_callback=progress_monitor)
        dbdir = "db_NOBACKUP_"
        
        # TODO: put back in open_vcf_file
        with open(vcf_file, "r") as f:
            sg = VCFSchemaFactory(f)
            schema = sg.generate_schema()
            schema.write_xml(dbdir)
       
        #dbw = VCFDatabaseWriter(dbdir, vcf_file)
        #dbw.process_header()
        #dbw.process_records(progress_monitor)
        #ib = IndexBuilder(dbdir, ["chrom", "pos"])
        #ib.build(progress_monitor)

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

