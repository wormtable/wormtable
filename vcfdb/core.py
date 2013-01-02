from __future__ import print_function
from __future__ import division 

"""
Core facilities in the vcfdb package.
   
TODO: document.
"""

import os
import sys
import gzip

from xml.etree import ElementTree
from xml.dom import minidom

import _vcfdb

DEFAULT_READ_CACHE_SIZE = 32 * 2**20

SCHEMA_VERSION = "0.2-dev"

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
                    print("\t\t", k, "\t", v)

    def write_xml(self, filename):
        """
        Writes out this schema to the specified file.
        """ 
        d = {"version":SCHEMA_VERSION}
        root = ElementTree.Element("schema", d)
        columns = ElementTree.Element("columns")
        root.append(columns)
        for c in self.__columns:
            element_type = self.ELEMENT_TYPE_STRING_MAP[c.element_type]
            d = {
                "name":c.name.decode(), 
                "description":c.description.decode(),
                "element_size":str(c.element_size),
                "num_elements":str(c.num_elements),
                "element_type":element_type
            }
            element = ElementTree.Element("column", d)
            columns.append(element)
            if c.element_type == _vcfdb.ELEMENT_TYPE_ENUM:
                enumeration_values = ElementTree.Element("enum_values")
                element.append(enumeration_values)
                for k, v in c.enum_values.items():
                    d = {"key": str(k), "value": str(v)}
                    value = ElementTree.Element("enum_value", d)
                    enumeration_values.append(value)

        tree = ElementTree.ElementTree(root)
        raw_xml = ElementTree.tostring(root, 'utf-8')
        reparsed = minidom.parseString(raw_xml)
        pretty = reparsed.toprettyxml(indent="  ")
        with open(filename, "w") as f:
            f.write(pretty)
    
    @classmethod 
    def read_xml(theclass, filename):
        """
        Returns a new schema object read from the specified file.
        """
        reverse = {}
        for k, v in theclass.ELEMENT_TYPE_STRING_MAP.items():
            reverse[v] = k
        columns = []
        tree = ElementTree.parse(filename)
        root = tree.getroot()
        if root.tag != "schema":
            # Should have a custom error for this.
            raise ValueError("invalid xml")
        version = root.get("version")
        if version is None:
            # Should have a custom error for this.
            raise ValueError("invalid xml")
        if version != SCHEMA_VERSION:
            raise ValueError("Unsupported schema version - rebuild required.")

        xml_columns = root.find("columns")
        for xmlcol in xml_columns.getchildren():
            if xmlcol.tag != "column":
                raise ValueError("invalid xml")
            name = xmlcol.get("name").encode()
            description = xmlcol.get("description").encode()
            # TODO some error checking here.
            element_size = int(xmlcol.get("element_size"))
            num_elements = int(xmlcol.get("num_elements"))
            element_type = reverse[xmlcol.get("element_type")]
            col = _vcfdb.Column(name, description, element_type, element_size,
                    num_elements)
            columns.append(col)
            if col.element_type == _vcfdb.ELEMENT_TYPE_ENUM:
                d = {}    
                xml_enum_values = xmlcol.find("enum_values")
                for xmlvalue in xml_enum_values.getchildren():
                    if xmlvalue.tag != "enum_value":
                        raise ValueError("invalid xml")
                    k = xmlvalue.get("key")
                    v = xmlvalue.get("value")
                    d[k] = int(v)
                col.enum_values = d 
        schema = theclass(columns)
        return schema

        
class TableBuilder(object):
    """
    Class responsible for generating databases.
    """
    def __init__(self, homedir, input_schema):
        self._homedir = homedir 
        self._build_db_name = os.path.join(homedir, "__build_primary.db")
        self._final_db_name = os.path.join(homedir, "primary.db")
        self._schema = Schema.read_xml(input_schema)
        self._schema_name = os.path.join(homedir, "schema.xml")
        self._database = None 
        self._row_buffer = None 
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
        Opens the database and gets ready for writing row.
        """
        self._database = _vcfdb.BerkeleyDatabase(self._build_db_name.encode(), 
            self._schema.get_columns(), self._cache_size)
        self._database.create()
        max_rows = self._buffer_size // 256 
        self._row_buffer = _vcfdb.WriteBuffer(self._database, self._buffer_size, 
                max_rows)

    def close_database(self):
        """
        Closes the underlying database and moves the db file to its
        permenent name. 
        """
        self._row_buffer.flush()
        self._database.close()

    def finalise(self):
        """
        Moves the database file to its permenent filename and updates
        the schema to contain enum values.
        """
        os.rename(self._build_db_name, self._final_db_name)  
        self._schema.write_xml(self._schema_name)
           
class Table(object):
    """
    Class representing a table object.
    """
    def __init__(self, homedir, cache_size=DEFAULT_READ_CACHE_SIZE):
        self._homedir = homedir
        ## TODO Add constants for these filenames.
        self._primary_db_file = os.path.join(homedir, "primary.db")
        self._schema_file = os.path.join(homedir, "schema.xml") 
        self._schema = Schema.read_xml(self._schema_file)
        self._cache_size = cache_size 
        self._database = _vcfdb.BerkeleyDatabase(
                self._primary_db_file.encode(), 
                self._schema.get_columns(), self._cache_size)
        self._database.open()

    def get_num_rows(self):
        """
        Returns the number of rows in this table.
        """
        return self._database.get_num_rows()

    def get_row(self, index):
        """
        Returns the row at the specified zero-based index.
        """
        return self._database.get_row(index) 
        
    def close(self):
        """
        Closes the underlying database, releasing resources.
        """
        self._database.close()


