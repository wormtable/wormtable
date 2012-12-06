
from __future__ import print_function
from __future__ import division 

import os
import sys
import gzip

from xml.etree import ElementTree
from xml.dom import minidom

import _vcfdb

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
            if c.element_type == _vcfdb.ELEMENT_TYPE_ENUM:
                for k, v in c.enum_values.items():
                    print(k, "\t", v)

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

        
class DatabaseWriter(object):
    """
    Class responsible for generating databases.
    """
    def __init__(self, schema, database_dir):
        self._database_dir = database_dir 
        self._build_db_name = os.path.join(database_dir, "__build_primary.db")
        self._final_db_name = os.path.join(database_dir, "primary.db")
        self._schema = schema 
        self._database = None 
        self._record_buffer = None 
        self._cache_size = 64 * 1024 * 1024
        self._buffer_size = 1024 * 1024

    def set_buffer_size(self, buffer_size):
        """
        Sets the write buffer size to the specified value.
        """
        self._buffer_size = buffer_size

    def set_cache_size(self, cache_size):
        """
        Sets the database write-cache size fo the specified value. 
        """
        self._cache_size = cache_size

    def open_database(self):
        """
        Opens the database and gets ready for writing records.
        """
        self._database = _vcfdb.BerkeleyDatabase(self._build_db_name.encode(), 
            self._schema.get_columns(), self._cache_size)
        self._database.create()
        max_records = self._buffer_size // 256 
        self._record_buffer = _vcfdb.WriteBuffer(self._database, self._buffer_size, 
                max_records)
        #print(self._record_buffer.key_buffer_size)
        #print(self._record_buffer.data_buffer_size)
        #print(self._record_buffer.max_num_records)

        #print(self._database.variable_region_offset)
        #print(self._database.filename)
        #print(self._database.cache_size)

    def close_database(self):
        """
        Closes the underlying database and moves the db file to its
        permenent name. 
        """
        self._record_buffer.flush()
        self._database.close()

    def finalise(self):
        """
        Moves the database file to its permenent filename and updates
        the schema to contain enum values.
        """
        # TODO implement

           

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


