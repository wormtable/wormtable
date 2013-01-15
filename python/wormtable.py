
"""
Core facilities in the wormtable package.
   
TODO: document.
"""
from __future__ import print_function
from __future__ import division 

import os
import sys
import gzip

from xml.etree import ElementTree
from xml.dom import minidom

import _wormtable as _wt

# Must be single quotes for parsing in setup.py
__version__ = '0.0.1dev'
SCHEMA_VERSION = "0.4-dev"

class Column(object):
    """
    Class representing a Column object in wormtable. Each table consists 
    of a list of columns, each of which contains a vector of values. The 
    size of each elements in the vector is fixed, and the number elements
    can either be fixed or variable.
    """
    def __init__(self, name, description, num_elements=0, element_size=1):
        element_type = self._get_element_type()
        self.__column = _wt.Column(name, description, element_type, 
            element_size, num_elements)
   
    def get_name(self): 
        """
        Returns the name of this column.
        """
        return self.__column.name

    def get_description(self):
        """
        Returns the description of this column.
        """
        return self.__column.description

    def is_variable(self):
        """
        Returns True if this column can hold a variable number of elements.
        """
        return self.__column.num_elements == _wt.NUM_ELEMENTS_VARIABLE

    def get_num_elements(self):
        """
        Returns the number of elements in this column, or 0 if the column
        is variable lenght.
        """
        return self.__column.num_elements

    def get_element_size(self):
        """
        Returns the size of each element in this column in bytes.
        """
        return self.__column.element_size

    def get_ll_column(self):
        """
        Returns the low-level column associated with this column. 

        TODO this is a temporary method as this sort of detail should 
        not be exposed in the public API.
        """
        return self.__column


    def _get_element_type_string(self):
        return self.ELEMENT_TYPE_STRING

    def show(self):
        c = self.__column
        s = "{0:8} {1:8} {2:8} {3:8} {4:8}".format(
            c.name.decode(), 
            c.description.decode(),
            self._get_element_type_string(),
            c.element_size,
            c.num_elements)
        print(s)

    def to_xml_element(self):
        """
        Returns an XML description of this element.
        """
        c = self.__column
        d = {
            "name":c.name.decode(), 
            "description":c.description.decode(),
            "element_size":str(c.element_size),
            "num_elements":str(c.num_elements),
            "element_type":self._get_element_type_string(),
        }
        return ElementTree.Element("column", d)
        


class StringColumn(Column):
    """
    Class representing a column of textual data. 
    """
    ELEMENT_TYPE_STRING = "char"
    def _get_element_type(self):
        return _wt.ELEMENT_TYPE_CHAR
    

class NumericColumn(Column):
    """
    Class representing a column of numeric data.
    """

class IntegerColumn(NumericColumn):
    """
    Class representing a column of integer data. Integers are represented as 
    signed values of a fixed size of between 1 and 8 bytes. 
    """
    ELEMENT_TYPE_STRING = "int"
    def _get_element_type(self):
        return _wt.ELEMENT_TYPE_INT

class FloatColumn(NumericColumn):
    """
    Class representing a column of floating point data. Values are represented
    as IEEE floating point values of either 4 (single precision) or 8
    (double precision) bytes.
    """
    ELEMENT_TYPE_STRING = "float"
    def _get_element_type(self):
        return _wt.ELEMENT_TYPE_FLOAT


class Table(object):
    """
    Class represting a data Table. Tables consist of a list of rows, within
    which data is arranged in columns. Tables reside in a specific home 
    directory.
    """
    DEFAULT_CACHE_SIZE = 32 * 2**20 # 32 MiB
    PRIMARY_DB_NAME = "primary.db"
    def __init__(self, homedir):
        self.__homedir = homedir
        self.__cache_size = self.DEFAULT_CACHE_SIZE
        self.__columns = [] 
        self.__table = None  
    
    def set_cache_size(self, cache_size):
        """
        Sets the Berkeley DB cache size for this table to the specified number
        of bytes.
        """
        self.__cache_size = cache_size

    def get_cache_size(self):
        """
        Returns the size of the Berkeley DB cache size for this table in bytes.
        """
        return self.__cache_size

        
    def get_columns(self):
        """
        Returns the list of columns in this table.
        """
        return self.__columns

class Index(object):
    """
    Class representing an index for a Table. Indexes are basically the values
    of a list of columns in sorted order, allowing us to find rows with 
    specific values quickly.
    """
    DEFAULT_CACHE_SIZE = 32 * 2**10 # KiB
    def __init__(self, table):
        self.__table = table
        self.__cache_size = self.DEFAULT_CACHE_SIZE
        self.__index = None  

    def set_cache_size(self, cache_size):
        """
        Sets the Berkeley DB cache size for this index to the specified number
        of bytes.
        """
        self.__cache_size = cache_size

    def get_cache_size(self):
        """
        Returns the size of the Berkeley DB cache size for this index in bytes.
        """
        return self.__cache_size


class TableWriter(object):
    """
    Class representing the writer interface for a table. 
    """
    def __init__(self, homedir, input_schema):
        # TODO: fix this up. A lot of this stuff should be done in the table 
        # class.
        self.__table = Table(homedir) 
        self.__build_db_name = os.path.join(homedir, 
                "__build_tmp.db") # TODO use tmpnam
        self.__final_db_name = os.path.join(homedir, Table.PRIMARY_DB_NAME) 
        schema = Schema.read_xml(input_schema)
        schema_name = os.path.join(homedir, "schema.xml")
        schema.write_xml(schema_name)
        self._schema = schema
        self.__columns = [c.get_ll_column() for c in schema.get_columns()]
        self.__database = _wt.BerkeleyDatabase(self.__build_db_name.encode(), 
                self.__columns, self.__table.get_cache_size())
        self.__database.create()
        # Row buffer is going to be deprecated, so set the max_records to 1.
        self.__row_buffer = _wt.WriteBuffer(self.__database, 64 * 2**10, 1)

    def update_row(self, column_num, value):
        """
        Sets the value for the current row in the specified column to the 
        specified value.
        """
        col = self.__columns[column_num]
        self.__row_buffer.insert_elements(col, value)
        

    def commit_row(self):
        """
        Commits the current row to the table.
        """
        self.__row_buffer.commit_row()

    def finalise(self):
        """
        Closes the underlying database and moves the db file to its
        permenent name. 
        """
        self.__row_buffer.flush()
        self.__database.close()
        os.rename(self.__build_db_name, self.__final_db_name)  


class Schema(object):
    """
    Class representing a database schema, consisting of a list of 
    Column objects.
    """
    def __init__(self, columns):
        self.__columns = columns 
    
    def get_columns(self):
        """
        Returns the list of columns in this schema.
        """
        return self.__columns
    
    def get_column(self, name):
        """
        Returns the column with the specified name.
        """
        cols = dict((col.get_name(), col) for col in self.__columns)
        return cols[name]

    def show(self):
        """
        Writes out the schema to the console in human readable format.
        """
        s = "{0:<25}{1:<12}{2:<12}{3:<12}"
        print(65 * "=")
        print(s.format("name", "type", "size", "num_elements"))
        print(65 * "=")
        for c in self.__columns:
            c.show() 

    def write_xml(self, filename):
        """
        Writes out this schema to the specified file.
        """ 
        d = {"version":SCHEMA_VERSION}
        root = ElementTree.Element("schema", d)
        columns = ElementTree.Element("columns")
        root.append(columns)
        for c in self.__columns:
            element = c.to_xml_element()
            columns.append(element)
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
        reverse = {
            IntegerColumn.ELEMENT_TYPE_STRING: IntegerColumn,
            FloatColumn.ELEMENT_TYPE_STRING: FloatColumn,
            StringColumn.ELEMENT_TYPE_STRING: StringColumn
        }
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
            c = reverse[xmlcol.get("element_type")]
            col = c(name, description, num_elements, element_size)
            columns.append(col)
        schema = theclass(columns)
        return schema

 



###############################################################################
### Old version
###############################################################################

DEFAULT_READ_CACHE_SIZE = 32 * 2**20

class OldSchema(object):
    """
    Class representing a database schema, consisting of a list of 
    Column objects.
    """
    ELEMENT_TYPE_STRING_MAP = {
        _wt.ELEMENT_TYPE_INT: "int",
        _wt.ELEMENT_TYPE_CHAR: "char",
        _wt.ELEMENT_TYPE_FLOAT: "float",
        _wt.ELEMENT_TYPE_ENUM: "enum"
    }

    def __init__(self, columns):
        self.__columns = columns 
    
    def get_columns(self):
        """
        Returns the list of columns in this schema.
        """
        return self.__columns
    
    def get_column(self, name):
        """
        Returns the column with the specified name.
        """
        cols = dict((col.name, col) for col in self.__columns)
        return cols[name]

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
            if c.element_type == _wt.ELEMENT_TYPE_ENUM:
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
            if c.element_type == _wt.ELEMENT_TYPE_ENUM:
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
            col = _wt.Column(name, description, element_type, element_size,
                    num_elements)
            columns.append(col)
            if col.element_type == _wt.ELEMENT_TYPE_ENUM:
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

        
class OldTableBuilder(object):
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
        self._database = _wt.BerkeleyDatabase(self._build_db_name.encode(), 
            self._schema.get_columns(), self._cache_size)
        self._database.create()
        max_rows = self._buffer_size // 256 
        self._row_buffer = _wt.WriteBuffer(self._database, self._buffer_size, 
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
           
class OldTable(object):
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
        self._database = _wt.BerkeleyDatabase(
                self._primary_db_file.encode(), 
                self._schema.get_columns(), self._cache_size)
        self._database.open()

    
    def get_database(self):
        return self._database

    def get_homedir(self):
        return self._homedir

    def get_schema(self):
        """
        Returns the schema for this table.
        """
        return self._schema

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

class OldIndex(object):
    """
    Class representing an index over a set of columns in a table.
    """
    def __init__(self, table, columns, cache_size):
        self._table = table
        self._columns = columns
        name = b"+".join(c.name for c in columns)
        filename = os.path.join(table.get_homedir().encode(), name + b".db")        
        self._index = _wt.Index(table.get_database(), filename, columns, 
                cache_size)
        
    def build(self, progress_callback=None, callback_interval=100):
        if progress_callback is not None:
            self._index.create(progress_callback, callback_interval)
        else:
            self._index.create()
        self._index.close()

    def open(self):
        self._index.open()
    
    def close(self):
        self._index.close()

    def get_rows(self, columns, min_val=None, max_val=None):
        row_iter = _wt.RowIterator(self._table.get_database(), columns, 
                self._index)
        if min_val is not None:
            row_iter.set_min(min_val)
        if max_val is not None:
            row_iter.set_max(max_val)
        for row in row_iter:
            yield row
