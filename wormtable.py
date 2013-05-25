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

import contextlib

import collections

import _wormtable

__version__ = '0.0.1-dev'
SCHEMA_VERSION = "0.1-dev"
INDEX_METADATA_VERSION = "0.1-dev"

DEFAULT_READ_CACHE_SIZE = 32 * 2**20

WT_VARIABLE = _wormtable.NUM_ELEMENTS_VARIABLE 
WT_UINT   = _wormtable.ELEMENT_TYPE_UINT   
WT_INT   = _wormtable.ELEMENT_TYPE_INT   
WT_FLOAT = _wormtable.ELEMENT_TYPE_FLOAT 
WT_CHAR  = _wormtable.ELEMENT_TYPE_CHAR 

WT_READ = _wormtable.WT_READ
WT_WRITE = _wormtable.WT_WRITE


class Schema(object):
    """
    Class representing a database schema, consisting of a list of 
    Column objects.
    """
    ELEMENT_TYPE_STRING_MAP = {
        WT_INT:"int",
        WT_UINT:"uint",
        WT_CHAR: "char",
        WT_FLOAT: "float",
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
        self._database = _wormtable.Table(self._build_db_name.encode(), 
            self._schema.get_columns(), self._cache_size)
        self._database.open(WT_WRITE)
        self._row_buffer = self._database

    def close_database(self):
        """
        Closes the underlying database and moves the db file to its
        permenent name. 
        """
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
        self._database = _wormtable.Table(
                self._primary_db_file.encode(), 
                self._schema.get_columns(), self._cache_size)
        self._database.open(WT_READ)

    
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
        cols = self._table.get_schema().get_columns()
        col_positions = [cols.index(c) for c in self._columns]
        self._index = _wormtable.Index(self._table.get_database(), build_filename, 
                col_positions, self._cache_size)
        self._index.set_bin_widths(self._bin_widths)
        self._index.open(WT_WRITE)
        if progress_callback is not None:
            self._index.build(progress_callback, callback_interval)
        else:
            self._index.build()
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
        cols = self._table.get_schema().get_columns()
        col_positions = [cols.index(c) for c in self._columns]
        self.__num_columns = len(col_positions)
        self._index = _wormtable.Index(self._table.get_database(), 
                self._db_filename, col_positions, self._cache_size)
        self._index.set_bin_widths(self._bin_widths)
        self._index.open(WT_READ)
    
    def close(self):
        self._index.close()

    def get_rows(self, columns, min_val=None, max_val=None):
        s = self._table.get_schema()
        cols = [s.get_column(c.encode()) for c in columns]
        col_positions = [s.get_columns().index(c) for c in cols]
        row_iter = _wormtable.RowIterator(self._index, col_positions)
        n = len(cols)
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
        t = v
        if self.__num_columns == 1:
            t = (v,) 
        return self._index.get_num_rows(t) 

    
    def get_distinct_values(self):
        """
        Returns the distinct values in this index.
        """
        dvi = _wormtable.DistinctValueIterator(self._index)
        if self.__num_columns == 1:
            for v in dvi:
                yield v[0]
        else:
            for v in dvi:
                yield v

    def get_min(self, partial_key=[]):
        """
        Returns the minimum key in this index. If partial_key is specified, 
        this must be a sequence of values giving the leftmost values to 
        match against.
        """
        return self._index.get_min(tuple(partial_key))

    def get_max(self, partial_key=[]):
        """
        Returns the maximum key in this index. If a partial key is specified,
        we return the largest key with initial values less than or equal to 
        the value provided.
        """
        return self._index.get_max(tuple(partial_key))

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

#######################################################################

## New implementation.

#######################################################################

@contextlib.contextmanager
def open_table(homedir):
    """
    Opens a table in read mode ready for use.
    """
    t = NewTable(homedir)
    t.open("r")
    try:
        yield t
    finally:
        t.close()


class Column(object):
    """
    Class representing a column in a table.
    """
    ELEMENT_TYPE_STRING_MAP = {
        WT_INT: "int",
        WT_UINT: "uint",
        WT_CHAR: "char",
        WT_FLOAT: "float",
    }

    def __init__(self, column):
        self.__column = column 
    
    def TEMP_get_column(self):
        return self.__column
   
    def get_position(self):
        """
        Returns the position of this column in the table.
        """
        return self.__column.position

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
    
    def get_type(self):
        """
        Returns the type code for this column.
        """
        return self.__column.element_type

    def get_type_name(self):
        """
        Returns the string representation of the type of this Column.
        """
        return self.ELEMENT_TYPE_STRING_MAP[self.__column.element_type]
    
    def get_element_size(self):
        """
        Returns the size of each element in the column in bytes.
        """
        return self.__column.element_size

    def get_num_elements(self):
        """
        Returns the number of elements in this column; 0 means the number 
        of elements is variable.
        """
        return self.__column.num_elements
   
    def get_xml(self):
        """
        Returns an ElementTree.Element representing this Column.
        """
        c = self.__column
        element_type = self.ELEMENT_TYPE_STRING_MAP[c.element_type]
        d = {
            "name":c.name.decode(), 
            "description":c.description.decode(),
            "element_size":str(c.element_size),
            "num_elements":str(c.num_elements),
            "element_type":element_type
        }
        return ElementTree.Element("column", d)

    # Factory methods for creating Columns

    @classmethod 
    def parse_xml(theclass, xmlcol):
        """
        Parses the specified XML column description and returns a new 
        Column instance.
        """
        reverse = {}
        for k, v in theclass.ELEMENT_TYPE_STRING_MAP.items():
            reverse[v] = k
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
        return theclass(col) 
    
class NewTable(object):
    """
    The table object.
    """ 
    def __init__(self, homedir):
        self.__homedir = homedir
        self.__cache_size = DEFAULT_READ_CACHE_SIZE 
        self.__primary_db_file = os.path.join(homedir, "primary.db")
        self.__schema_file = os.path.join(homedir, "schema.xml") 
        self.__columns = []
        self.__column_name_map = {}
        self.__num_rows = 0
        self.__table = None
   
    def __read_schema(self):
        """
        Reads the schema from the schema_file and sets up the columns 
        appropriately.
        """
        tree = ElementTree.parse(self.__schema_file)
        root = tree.getroot()
        if root.tag != "schema":
            raise ValueError("invalid xml")
        version = root.get("version")
        if version is None:
            raise ValueError("invalid xml")
        if version != SCHEMA_VERSION:
            raise ValueError("Unsupported schema version - rebuild required.")
        xml_columns = root.find("columns")
        for xmlcol in xml_columns.getchildren():
            col = Column.parse_xml(xmlcol)
            self.__column_name_map[col.get_name()]= len(self.__columns)
            self.__columns.append(col)

    def __write_schema(self):
        """
        Writes the schema in XML format to the schema_file.
        """
        d = {"version":SCHEMA_VERSION}
        root = ElementTree.Element("schema", d)
        columns = ElementTree.Element("columns")
        root.append(columns)
        for c in self.__columns:
            columns.append(c.get_xml())
        tree = ElementTree.ElementTree(root)
        raw_xml = ElementTree.tostring(root, 'utf-8')
        reparsed = minidom.parseString(raw_xml)
        pretty = reparsed.toprettyxml(indent="  ")
        with open(self.__schema_file, "w") as f:
            f.write(pretty)
   
    # Helper methods for adding Columns of the various types.
    def __add_column(self, column):
        """
        Adds a new Column to this wormtable. Can only be called before the table 
        has been opened in write mode.
        """
        if self.__table is not None:
            raise ValueError("Cannot add columns to open table")
        self.__columns.append(column)
    
    def add_uint_column(self, name, description="", size=2, num_elements=1):
        """
        Creates a new unsigned integer column with the specified name, 
        element size (in bytes) and number of elements. If num_elements=0
        then the column can hold a variable number of elements.
        """
        nb = name.encode()
        db = description.encode()
        col = _wormtable.Column(nb, db, WT_UINT, size, num_elements)
        self.__add_column(Column(col)) 

    def add_int_column(self, name, description="", size=2, num_elements=1):
        """
        Creates a new integer column with the specified name, 
        element size (in bytes) and number of elements. If num_elements=0
        then the column can hold a variable number of elements.
        """
        nb = name.encode()
        db = description.encode()
        col = _wormtable.Column(nb, db, WT_INT, size, num_elements)
        self.__add_column(Column(col)) 
    
    def add_float_column(self, name, description="", size=4, num_elements=1):
        """
        Creates a new float column with the specified name, 
        element size (in bytes) and number of elements. If num_elements=0
        then the column can hold a variable number of elements. Only 4 and 
        8 byte floats are supported by wormtable; these correspond to the 
        usual float and double types.
        """
        nb = name.encode()
        db = description.encode()
        col = _wormtable.Column(nb, db, WT_FLOAT, size, num_elements)
        self.__add_column(Column(col)) 

    def add_char_column(self, name, description="", num_elements=0):
        """
        Creates a new character column with the specified name, description 
        and number of elements. If num_elements=0 then the column can hold 
        variable length strings; otherwise, it can contain strings of a fixed 
        length only.
        """
        nb = name.encode()
        db = description.encode()
        col = _wormtable.Column(nb, db, WT_CHAR, 1, num_elements)
        self.__add_column(Column(col)) 

    # Methods for accessing the columns
    def columns(self):
        """
        Returns an iterator over the list of columns.
        """
        for c in self.__columns:
            yield c

    def get_column(self, col_id):
        """
        Returns a column corresponding to the specified id. If this is an
        integer, we return the column at this position; if it is a string
        we return the column with the specified name.
        """
        ret = None
        if isinstance(col_id, int):
            ret = self.__columns[col_id]
        elif isinstance(col_id, bytes):
            k = self.__column_name_map[col_id]
            ret = self.__columns[k]
        else:
            raise TypeError("column ids must be bytes or integers")
        return ret

    def get_homedir(self):
        """
        Returns the home directory of this table.
        """ 
        return self.__homedir

    def get_cache_size(self):
        """
        Returns the cache size of this table in bytes.
        """
        return self.__cache_size

    def open(self, mode):
        """
        Open the table in the specified mode.
        """
        modes = {'r': _wormtable.WT_READ, 'w': _wormtable.WT_WRITE}
        if mode not in modes:
            raise ValueError("mode string must be one of 'r' or 'w'")
        m = modes[mode]
        if m == WT_READ:
            self.__read_schema()
        else:
            self.__write_schema()
        columns = [c.TEMP_get_column() for c in self.__columns]
        self.__table = _wormtable.Table(self.__primary_db_file.encode(), 
                    columns, self.__cache_size)
        self.__table.open(m)
        self.__num_rows = 0
        if m == WT_READ:
            self.__num_rows = self.__table.get_num_rows()

    def close(self):
        """
        Closes the table, releasing cache and other resources. 
        """
        self.__table.close()
        self.__table = None
        self.__columns = []

    def __len__(self):
        """
        Implement the len(t) function.
        """
        return self.__num_rows
    
    def __getitem__(self, key):
        """
        Implements the t[key] function.
        """
        ret = None
        n = len(self)
        if isinstance(key, slice):
            ret = [self[j] for j in range(*key.indices(n))]
        elif isinstance(key, int):
            k = key
            if k < 0:
                k = n + k
            if k >= n:
                raise IndexError("table index out of range")
            ret = self.__table.get_row(k)    
        else:
            raise TypeError("table indices must be integers")
        return ret
  
    def append(self, row):
        """
        Appends the specified row to this table.
        """
        j = 1 
        for v in row:
            self.__table.insert_elements(j, v)
            j += 1
        self.__table.commit_row()
        self.__num_rows += 1

    def TEMP_get_table(self):
        return self.__table


    def cursor(self, read_columns):
        columns = [self.get_column(c.encode()) for c in read_columns]
        return TableCursor(self.__table, [c.get_position() for c in columns])

class IndexCounter(collections.Mapping):
    """
    A counter for Indexes, based on the collections.Counter class. This class 
    is a dictionary-like object that represents a mapping of the distinct 
    keys in the index to the number of times those keys appear.
    """
    def __init__(self, ll_index):
        self.__ll_index = ll_index
    
    def __getitem__(self, key):
        return self.__ll_index.get_num_rows(key) 
   
    def __iter__(self):
        dvi = _wormtable.DistinctValueIterator(self.__ll_index)
        return dvi
    
    def __len__(self):
        n = 0
        dvi = _wormtable.DistinctValueIterator(self.__ll_index)
        for v in dvi:
            n += 1
        return n

class Cursor(object):
    """
    Superclass of Cursor objects. Subclasses are responsible for allocating 
    an iterator.
    """

    def __iter__(self):
        for r in self._row_iterator:
            yield r 

class TableCursor(Cursor):
    """
    A cursor over the rows of the table in the order defined by an index. 
    """
    def __init__(self, ll_table, columns):
        self._row_iterator = _wormtable.TableRowIterator(ll_table,  columns)

    def set_min(self, v):
        if v < 0:
            raise ValueError("negative row_ids not supported")
        self._row_iterator.set_min(v)
    def set_max(self, v):
        if v < 0:
            raise ValueError("negative row_ids not supported")
        self._row_iterator.set_max(v)

class IndexCursor(Cursor):
    """
    A cursor over the rows of the table in the order defined by an index. 
    """
    def __init__(self, index, ll_index, columns):
        self.__index = index
        self._row_iterator = _wormtable.IndexRowIterator(ll_index,  columns)
    
    def set_min(self, *v):
        key = self.__index.translate_key(v)
        self._row_iterator.set_min(key)
    
    def set_max(self, *v):
        key = self.__index.translate_key(v)
        self._row_iterator.set_max(key)


class NewIndex(object):
    """
    Class representing an Index for a set of columns.
    """
    def __init__(self, table, colspec):
        columns = colspec.split("+")
        self.__table = table 
        self.__columns = [self.__table.get_column(c.encode()) for c in columns]
        prefix = os.path.join(table.get_homedir().encode(), colspec.encode())
        self.__db_filename = prefix + b".db"
        self.__metadata_filename = prefix + b".xml"
        self.__cache_size = 0

    def open(self, mode="r"):
        self.__index = _wormtable.Index(self.__table.TEMP_get_table(),
                self.__db_filename, [c.get_position() for c in self.__columns], 
                self.__cache_size)
        self.__index.open(_wormtable.WT_READ)
        
    
    def close(self):
        self.__index.close()
    
    def TEMP_get_index(self):
        return self.__index

    def translate_key(self, v):
        """
        Translates the specified arguments tuple as a key to a tuple ready to 
        for use in the low-level API.
        """
        n = len(v)
        l = [None for j in range(n)]
        for j in range(n):
            l[j] = v[j]
            if isinstance(l[j], str):
                l[j] = l[j].encode()
        return tuple(l)

    def get_min(self, *v):
        key = self.translate_key(v)
        return self.__index.get_min(key)

    def get_max(self, *v):
        """
        Returns the maximum key in this index. If a partial key is specified,
        we return the largest key with initial values less than or equal to 
        the value provided.
        """
        key = self.translate_key(v)
        return self.__index.get_max(key)


    def counter(self):
        """
        Returns an IndexCounter object for this index. This provides an efficient 
        method of iterating over the keys in the index.
        """
        return IndexCounter(self.__index)
    
    def cursor(self, read_columns):
        columns = [self.__table.get_column(c.encode()) for c in read_columns]
        return IndexCursor(self, self.__index, [c.get_position() for c in columns])

