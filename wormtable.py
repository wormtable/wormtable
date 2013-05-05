"""
Wormtable.
"""
from __future__ import print_function
from __future__ import division 

import os
import sys
import time
from xml.etree import ElementTree
from xml.dom import minidom

import _wormtable

__version__ = '0.0.1-dev'
SCHEMA_VERSION = "0.4-dev"
INDEX_METADATA_VERSION = "0.1-dev"

DEFAULT_READ_CACHE_SIZE = 32 * 2**20

WT_VARIABLE = _wormtable.NUM_ELEMENTS_VARIABLE 
WT_INT   = _wormtable.ELEMENT_TYPE_INT   
WT_FLOAT = _wormtable.ELEMENT_TYPE_FLOAT 
WT_CHAR  = _wormtable.ELEMENT_TYPE_CHAR 


class Schema(object):
    """
    Class representing a database schema, consisting of a list of 
    Column objects.
    """
    ELEMENT_TYPE_STRING_MAP = {
        _wormtable.ELEMENT_TYPE_INT: "int",
        _wormtable.ELEMENT_TYPE_CHAR: "char",
        _wormtable.ELEMENT_TYPE_FLOAT: "float",
        _wormtable.ELEMENT_TYPE_ENUM: "enum"
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
            if c.element_type == _wormtable.ELEMENT_TYPE_ENUM:
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
            if c.element_type == _wormtable.ELEMENT_TYPE_ENUM:
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
            col = _wormtable.Column(name, description, element_type, element_size,
                    num_elements)
            columns.append(col)
            if col.element_type == _wormtable.ELEMENT_TYPE_ENUM:
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
        self._database = _wormtable.BerkeleyDatabase(self._build_db_name.encode(), 
            self._schema.get_columns(), self._cache_size)
        self._database.create()
        max_rows = self._buffer_size // 256 
        self._row_buffer = _wormtable.WriteBuffer(self._database, self._buffer_size, 
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
        self._database = _wormtable.BerkeleyDatabase(
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

class Index(object):
    """
    Class representing an index over a set of columns in a table.
    """
    def __init__(self, table, columns, cache_size=DEFAULT_READ_CACHE_SIZE):
        self._table = table
        self._column_names = columns
        self._cache_size = cache_size
        cols = []
        s = table.get_schema()
        for c in columns:
            cols.append(s.get_column(c.encode()))
        self._columns = cols 
        name = b"+".join(c.name for c in self._columns)
        prefix = os.path.join(table.get_homedir().encode(), name)
        self._db_filename = prefix + b".db"
        self._metadata_filename = prefix + b".xml"
        self._index = None

    def set_bin_widths(self, bin_widths):
        """
        Sets the bin widths for the columns to the specified values. Only has 
        an effect if called before build.
        """
        self._bin_widths = bin_widths


    def build(self, progress_callback=None, callback_interval=100):
        build_filename = self._db_filename + b".build" 
        self._index = _wormtable.Index(self._table.get_database(), build_filename, 
                self._columns, self._cache_size)
        self._index.set_bin_widths(self._bin_widths)
        if progress_callback is not None:
            self._index.create(progress_callback, callback_interval)
        else:
            self._index.create()
        self._index.close()
        # Create the metadata file and move the build file to its final name
        self._write_metadata_file()
        os.rename(build_filename, self._db_filename)

    def open(self):
        if not os.path.exists(self._metadata_filename):
            homedir = self._table.get_homedir()
            cols = " ".join(self._column_names)
            s = "Index not found for {0};".format(cols) 
            s += " run 'wtadmin add {0} {1}'".format(homedir, cols)
            raise IOError(s)
        self._read_metadata_file()
        self._index = _wormtable.Index(self._table.get_database(), 
                self._db_filename, self._columns, self._cache_size)
        self._index.set_bin_widths(self._bin_widths)
        self._index.open()
    
    def close(self):
        self._index.close()

    def get_rows(self, columns, min_val=None, max_val=None):
        s = self._table.get_schema()
        cols = [s.get_column(c.encode()) for c in columns]
        row_iter = _wormtable.RowIterator(self._table.get_database(), cols, 
                self._index)
        n = len(self._index.columns)
        if min_val is not None:
            v = min_val
            if n == 1:
                v = (min_val,)
            row_iter.set_min(v)
        if max_val is not None:
            v = max_val
            if n == 1:
                v = (max_val,)
            row_iter.set_max(v)
        for row in row_iter:
            yield row
    

    def get_num_rows(self, v):
        """
        Returns the number of rows in this index with value equal to v.
        """
        # It would make more logical sense to put this method in the 
        # Index class; the infrastructure is available in the RowIterator
        # class though, so it's easier to reuse this.
        row_iter = _wormtable.RowIterator(self._table.get_database(), 
                self._index.columns, self._index)
        t = v
        if len(self._index.columns) == 1:
            t = (v,) 
        row_iter.set_min(t)
        return row_iter.get_num_rows() 

    
    def get_distinct_values(self):
        """
        Returns the distinct values in this index.
        """
        dvi = _wormtable.DistinctValueIterator(self._table.get_database(), 
                self._index)
        if len(self._columns) == 1:
            for v in dvi:
                yield v[0]
        else:
            for v in dvi:
                yield v

    def get_min(self, partial_key=None):
        """
        Returns the minimum key in this index. If partial_key is specified, 
        this must be a tuple of values giving the leftmost values to 
        match against.
        """
        key = []
        for c in self._columns:
            missing = None
            if c.element_type == WT_CHAR:
                missing = b''
            elif c.num_elements == WT_VARIABLE:
                missing = tuple() 
            key.append(missing)
        if partial_key is not None:
            if len(partial_key) > len(self._columns):
                raise ValueError("len(partial_key) must be <= num_columns")
            for j in range(len(partial_key)):
                key[j] = partial_key[j]
        print("key = ", key)
        return self._index.get_min(tuple(key))

    def _write_metadata_file(self):
        """
        Writes the metadata XML file.
        """
        d = {"version":INDEX_METADATA_VERSION}
        root = ElementTree.Element("index", d)
        columns = ElementTree.Element("columns")
        root.append(columns)
        for j in range(len(self._columns)):
            c = self._columns[j]
            w = self._bin_widths[j]
            if c.element_type == WT_INT:
                w = int(w)
            d = {
                "name":c.name.decode(), 
                "bin_width":str(w),
            }
            element = ElementTree.Element("column", d)
            columns.append(element)
        tree = ElementTree.ElementTree(root)
        raw_xml = ElementTree.tostring(root, 'utf-8')
        reparsed = minidom.parseString(raw_xml)
        pretty = reparsed.toprettyxml(indent="  ")
        with open(self._metadata_filename, "w") as f:
            f.write(pretty)
    
    def _read_metadata_file(self):
        """
        Reads the metadata for this index.
        """
        self._bin_widths = [0 for c in self._column_names]
        tree = ElementTree.parse(self._metadata_filename)
        root = tree.getroot()
        if root.tag != "index":
            # Should have a custom error for this.
            raise ValueError("invalid xml")
        version = root.get("version")
        if version is None:
            # Should have a custom error for this.
            raise ValueError("invalid xml")
        if version != INDEX_METADATA_VERSION:
            raise ValueError("Unsupported index metadata version - rebuild required.")
        xml_columns = root.find("columns")
        for xmlcol in xml_columns.getchildren():
            if xmlcol.tag != "column":
                raise ValueError("invalid xml")
            name = xmlcol.get("name")
            bin_width = float(xmlcol.get("bin_width"))
            j = self._column_names.index(name)
            self._bin_widths[j] = bin_width             



#
# Utilities for the command line programs.
#

class ProgressMonitor(object):
    """
    Class representing a progress monitor for a terminal based interface.
    """
    def __init__(self, total, units):
        self.__total = total
        self.__units = units
        self.__progress_width = 40
        self.__bar_index = 0
        self.__bars = "/-\\|"
        self.__start_time = time.clock()

    def update(self, processed):
        """
        Updates this progress monitor to display the specified number 
        of processed items.
        """
        complete = processed / self.__total
        filled = int(complete * self.__progress_width)
        spaces = self.__progress_width - filled 
        bar = self.__bars[self.__bar_index]
        self.__bar_index = (self.__bar_index + 1) % len(self.__bars)
        elapsed = max(1, time.clock() - self.__start_time)
        rate = processed / elapsed
        s = '\r[{0}{1}] {2:2.2f}% @{3:4.2G} {4}/s {5}'.format('#' * filled, 
            ' ' * spaces, complete * 100, rate, self.__units, bar)
        sys.stdout.write(s)
        sys.stdout.flush()
         
    def finish(self):
        """
        Completes the progress monitor.
        """
        print()

def parse_cache_size(s):
    """
    Parses the specified string into a cache size in bytes. Accepts either 
    no suffix (bytes), K for Kibibytes, M for Mibibytes or G for Gibibytes.
    """
    multiplier = 1
    value = s
    if s.endswith("K"):
        multiplier = 2**10
        value = s[:-1]
    elif s.endswith("M"):
        multiplier = 2**20
        value = s[:-1]
    elif s.endswith("G"):
        multiplier = 2**30
        value = s[:-1]
    
    n = int(value)
    return n * multiplier



