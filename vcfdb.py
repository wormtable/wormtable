"""
Prototype implementation of the Berkeley DB VCF record store.
"""
from __future__ import print_function
from __future__ import division 

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
    
    def get_columns(self):
        """
        Returns the list of columns in this schema.
        """
        return self.__columns

    def show(self):
        """
        Writes out the schema to the console in human readable format.
        """
        s = "{0:<25}{1:<12}{2:<12}{3:<12}"
        print(65 * "=")
        print(s.format("name", "type", "size", "num_elements"))
        print(65 * "=")
        for c in self.__columns:
            t = self.ELEMENT_TYPE_STRING_MAP[c.element_type]
            print(s.format(c.name, t, c.element_size, c.num_elements))
            print("enum_values = ", c.enum_values)
            #if c.element_type == _vcfdb.ELEMENT_TYPE_ENUM:
                #for k, v in c.enum_values.items():
                #    print(k, "\t", v)

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
    
    @classmethod 
    def read_xml(theclass, directory, filename="schema.xml"):
        """
        Returns a new schema object read from the specified file.
        """
        reverse = {}
        for k, v in theclass.ELEMENT_TYPE_STRING_MAP.items():
            reverse[v] = k
        columns = []
        tree = ElementTree.parse(os.path.join(directory, filename))
        root = tree.getroot()
        if root.tag != "schema":
            # Should have a custom error for this.
            raise ValueError("invalid xml")
        # TODO check version
        xml_columns = root.find("columns")
        for xmlcol in xml_columns.getchildren():
            if xmlcol.tag != "column":
                raise ValueError("invalid xml")
            name = xmlcol.get("name")
            description = xmlcol.get("description")
            # TODO some error checking here.
            element_size = int(xmlcol.get("element_size"))
            num_elements = int(xmlcol.get("num_elements"))
            element_type = reverse[xmlcol.get("element_type")]
            col = _vcfdb.Column(name, description, element_type, element_size,
                    num_elements)
            # TODO read in enum values
            columns.append(col)
        schema = theclass(columns)
        return schema


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
            _vcfdb.Column("FILTER", "Filter", char_type, 1, 0),
        ]
        for name, col in self._info_columns.items():
            columns.append(col.get_db_column("INFO"))
        for genotype in self._genotypes:
            for name, col in self._genotype_columns.items():
                columns.append(col.get_db_column(genotype))
        schema = Schema(columns)
        return schema

class WriteBufferTmp(object):
    """
    Temporary shim class to present the WriteBuffer interface.
    """ 
        
    def set_record_value(self, column, value):
        print("setting ", column.name, "(", column, ") t = ", column.element_type, " n=",
                column.num_elements, " to ", value)


    def commit_record(self):
        print("Commiting")
class DatabaseWriter(object):
    """
    Class responsible for generating databases.
    """
    def __init__(self, schema, database_dir):
        self._database_dir = database_dir 
        self._build_db_name = os.path.join(database_dir, 
                "__building_primary.db")
        self._final_db_name = os.path.join(database_dir, "primary.db")
        self._schema = schema 
        self._database = None 
        self._record_buffer = WriteBufferTmp() 
        self._current_record_id = 0

    def open_database(self):
        """
        Opens the database and gets ready for writing records.
        """
        cols = self._schema.get_columns()
        # More parameters here for cachesize and so on.
        #self._database = _vcfdb.Database(self._build_db_name, cols)
        #self._database.open()
        #self._record_buffer = _vcfdb.WriteBuffer(self._database)


    def close_database(self):
        """
        Closes the underlying database and moves the db file to its
        permenent name. 
        """
        #self._record_buffer.flush()
        #self._database.close()

    def finalise(self):
        """
        Moves the database file to its permenent filename and updates
        the schema to contain enum values.
        """
        # TODO implement

class VCFDatabaseWriter(DatabaseWriter):
    """
    Class responsible for parsing a VCF file and creating a database. 
    """
    def _prepare(self, f):
        """
        Prepares for parsing records by getting the database columns 
        ready and skipping the file header.
        """
        # Skip the header
        s = f.readline()
        while s.startswith("##"):
            s = f.readline()
        # Get the genotypes from the header
        genotypes = s.split()[9:] 
        # TODO make this more elegant...
        all_columns = {c.name: c for c in self._schema.get_columns()}
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
                    self._genotype_columns[index][split[1]] = c
 

    def build(self, f):
        """
        Builds the database in opened file.
        """
        self._prepare(f)
        fixed_columns = self._fixed_columns
        info_columns = self._info_columns
        genotype_columns = self._genotype_columns
        rb = self._record_buffer
        # TODO: this doesn't handle missing values properly. We should 
        # test each value to see if it is "." before adding.
        for s in f:
            l = s.split()
            # Read in the fixed columns
            for col, index in fixed_columns:
                rb.set_record_value(col, l[index])
            # Now process the info columns.
            for mapping in l[7].split(";"):
                tokens = mapping.split("=")
                name = tokens[0]
                if name in info_columns:
                    col = info_columns[name]
                    if len(tokens) == 2:
                        rb.set_record_value(col, tokens[1])
                    else:
                        # This is a Flag column.
                        rb.set_record_value(col, "1")
            # Process the genotype columns. 
            j = 0
            fmt = l[8].split(":")
            for genotype_values in l[9:]:
                tokens = genotype_values.split(":")
                if len(tokens) == len(fmt):
                    for k in range(len(fmt)):
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
            

class DatabaseReader(object):
    """
    Class representing a database reader for a particular directory.
    """
    def __init__(self):
        with open("db_NOBACKUP_/schema.pkl", "rb") as f:
            self.__schema = pickle.load(f)
        

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
        dbdir = "db_NOBACKUP_"
        # TODO: put back in open_vcf_file
        with open(vcf_file, "r") as f:
            sg = VCFSchemaFactory(f)
            schema = sg.generate_schema()
            schema.write_xml(dbdir)
        
        # Start again - read the schema back
        schema = Schema.read_xml(dbdir)
        #schema.show()
        
        dbw = VCFDatabaseWriter(schema, dbdir)
        dbw.open_database()
        with open(vcf_file, "r") as f:
            dbw.build(f)
        dbw.close_database()
        dbw.finalise() 
        

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

