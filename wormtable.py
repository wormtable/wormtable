"""
Wormtable.
"""
from __future__ import print_function
from __future__ import division 

import os
import sys
import time
import glob
import collections
from xml.dom import minidom
from xml.etree import ElementTree


import _wormtable

__version__ = '0.1.0a1'
SCHEMA_VERSION = "0.1"
INDEX_METADATA_VERSION = "0.1"

DEFAULT_CACHE_SIZE = 16 * 2**20 # 16M

WT_INT = _wormtable.WT_INT
WT_UINT = _wormtable.WT_UINT
WT_FLOAT = _wormtable.WT_FLOAT
WT_CHAR = _wormtable.WT_CHAR

WT_READ = _wormtable.WT_READ
WT_WRITE = _wormtable.WT_WRITE
WT_VAR_1  = _wormtable.WT_VAR_1

def open_table(homedir, cache_size=DEFAULT_CACHE_SIZE):
    """
    Opens a table in read mode ready for use.
    """
    t = Table(homedir)
    if not t.exists():
        raise IOError("table '" + homedir + "' not found")
    t.set_cache_size(cache_size)
    t.open("r")
    return t   

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

    def __init__(self, ll_object):
        self.__ll_object = ll_object 
   
    def __str__(self):
        s = "NULL Column"
        if self.__ll_object != None:
            s = "'{0}':{1}({2})".format(self.get_name(), self.get_type_name(),
                    self.get_num_elements())
        return s

    def get_ll_object(self):
        """
        Returns the low level Column object that this class is a facade for. 
        """
        return self.__ll_object
   
    def get_position(self):
        """
        Returns the position of this column in the table.
        """
        return self.__ll_object.position

    def get_name(self):
        """
        Returns the name of this column.
        """
        return self.__ll_object.name.decode()
   
    def get_description(self):
        """
        Returns the description of this column.
        """
        return self.__ll_object.description.decode()
    
    def get_type(self):
        """
        Returns the type code for this column.
        """
        return self.__ll_object.element_type

    def get_type_name(self):
        """
        Returns the string representation of the type of this Column.
        """
        return self.ELEMENT_TYPE_STRING_MAP[self.__ll_object.element_type]
    
    def get_element_size(self):
        """
        Returns the size of each element in the column in bytes.
        """
        return self.__ll_object.element_size

    def get_num_elements(self):
        """
        Returns the number of elements in this column; 0 means the number 
        of elements is variable.
        """
        return self.__ll_object.num_elements
  
    def get_missing_value(self):
        """
        Returns the missing value for this column.

        TODO: document
        """
        t = self.get_type()
        n = self.get_num_elements()
        ret = None
        if t == WT_CHAR:
            ret = b''
        elif n != 1: 
            if n == WT_VAR_1:
                ret = tuple()
            else:
                ret = tuple(None for j in range(n))
        return ret

    def format_value(self, v):
        """
        Formats the specified value from this column for printing.
        """ 
        # TODO do something a bit better with missing values
        n = self.get_num_elements()
        if self.get_type() == WT_CHAR:
            s = v.decode()
        elif n == 1:
            s = str(v)
        else:
            s = ",".join(str(u) for u in v) 
            s = "(" + s + ")"
        return s

    def get_xml(self):
        """
        Returns an ElementTree.Element representing this Column.
        """
        n = self.get_num_elements()
        if n == WT_VAR_1:
            num_elements = "var(1)"
        else:
            num_elements = str(self.get_num_elements())
        d = {
            "name":self.get_name(), 
            "description":self.get_description(),
            "element_size":str(self.get_element_size()),
            "num_elements":num_elements,
            "element_type":self.get_type_name()
        }
        return ElementTree.Element("column", d)

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
        s = xmlcol.get("num_elements")
        if s == "var(1)":
            num_elements = WT_VAR_1
        else:
            num_elements = int(s)
        element_type = reverse[xmlcol.get("element_type")]
        col = _wormtable.Column(name, description, element_type, element_size,
                num_elements)
        return theclass(col) 

   
class Database(object):
    """
    The superclass of database objects. Databases are located in a home 
    directory and are backed by a two files: a database file and an 
    xml metadata file. 
    """
    DB_SUFFIX = ".db"
    def __init__(self, homedir, db_name):
        """
        Allocates a new database object held in the specified homedir
        with the specified db_name.
        """
        self.__homedir = homedir
        self.__db_name = db_name
        self.__cache_size = DEFAULT_CACHE_SIZE
        self.__ll_object = None
        self.__open_mode = None

    def __enter__(self):
        """
        Context manager entry.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit; closes the database.
        """
        self.close()
        return False

    def get_ll_object(self):
        """
        Returns the low-level object that this Database is a facade for.
        """
        return self.__ll_object

    def _create_ll_object(self, path):
        """
        Returns a newly created instance of the low-level object that this 
        Database is a facade for.
        """
        raise NotImplementedError() 

    def exists(self):
        """
        Returns True if this Database exists.
        """
        p1 = os.path.exists(self.get_metadata_path()) 
        p2 = os.path.exists(self.get_db_path())
        return p1 and p2

    def get_homedir(self):
        """
        Returns the home directory for this database object.
        """
        return self.__homedir

    def get_db_name(self):
        """
        Returns the db name of this database object.
        """
        return self.__db_name

    def get_cache_size(self):
        """
        Returns the cache size for this database in bytes.
        """
        return self.__cache_size

    def get_db_path(self):
        """
        Returns the path of the permanent file used to store the database.
        """
        return os.path.join(self.get_homedir(), self.get_db_name() + 
                self.DB_SUFFIX)
    
    def get_db_build_path(self):
        """
        Returns the path of the file used to build the database.
        """
        s = "_build_{0}_{1}.db".format(os.getpid(), self.get_db_name()) 
        return os.path.join(self.get_homedir(), s) 

    def get_db_file_size(self):
        """
        Returns the size of the database file in bytes.    
        """
        statinfo = os.stat(self.get_db_path())
        return statinfo.st_size 

    def get_metadata_path(self):
        """
        Returns the path of the file used to store metadata for the 
        database.
        """
        return os.path.join(self.get_homedir(), self.get_db_name() + ".xml")

    def set_cache_size(self, cache_size):
        """
        Sets the cache size for this database object to the specified 
        value. If cache_size is a string, it can be suffixed with 
        K, M or G to specify units of Kibibytes, Mibibytes or Gibibytes.
        """
        if isinstance(cache_size, str):
            s = cache_size
            d = {"K":2**10, "M":2**20, "G":2**30}
            multiplier = 1
            value = s
            if s.endswith(tuple(d.keys())):
                value = s[:-1]
                multiplier = d[s[-1]] 
            n = int(value)
            self.__cache_size = n * multiplier 
        else:
            self.__cache_size = int(cache_size)

    def write_metadata(self, filename):
        """
        Writes the metadata for this database to the specified file. 
        """
        tree = self.get_metadata()
        root = tree.getroot()
        raw_xml = ElementTree.tostring(root, 'utf-8')
        reparsed = minidom.parseString(raw_xml)
        pretty = reparsed.toprettyxml(indent="  ")
        with open(filename, "w") as f:
            f.write(pretty)
   
    def read_metadata(self, filename):
        """
        Reads metadata for this database from the specified filename,
        and calls set_metadata with the result.
        """
        tree = ElementTree.parse(filename)
        self.set_metadata(tree)

    def finalise_build(self):
        """
        Move the build file to its final location and write the metadata file.
        """
        new = self.get_db_path()
        old = self.get_db_build_path()
        os.rename(old, new)
        self.write_metadata(self.get_metadata_path())

    def is_open(self):
        """
        Returns True if this database is open for reading or writing. 
        """
        return self.__ll_object is not None
   
    def get_open_mode(self):
        """
        Returns the mode that this database is opened in, WT_READ or 
        WT_WRITE. If the database is not open, return None.
        """
        return self.__open_mode

    def open(self, mode):
        """
        Opens this database in the specified mode. Mode must be one of 
        'r' or 'w'.
        """
        modes = {'r': _wormtable.WT_READ, 'w': _wormtable.WT_WRITE}
        if mode not in modes:
            raise ValueError("mode string must be one of 'r' or 'w'")
        m = modes[mode]
        self.__open_mode = None
        self.__ll_object = None
        path = self.get_db_path()
        if m == WT_WRITE:
            path = self.get_db_build_path()
        else:
            self.read_metadata(self.get_metadata_path())
        llo = self._create_ll_object(path)
        llo.open(m)
        self.__ll_object = llo
        self.__open_mode = m 

    def close(self):
        """
        Closes this database object, freeing underlying resources.
        """
        try:
            self.__ll_object.close()
            if self.__open_mode == WT_WRITE:
                self.finalise_build()
        finally:
            self.__open_mode = None
            self.__ll_object = None

    def delete(self):
        """
        Deletes the DB and metadata files for this database.
        """
        self.verify_closed()
        os.unlink(self.get_db_path())
        os.unlink(self.get_metadata_path())

    def verify_closed(self):
        """
        Ensures this database is closed.
        """
        if self.is_open():
            raise ValueError("Database must be closed")

    def verify_open(self, mode=None):
        """
        Ensures this database is open in the specified mode..
        """
        if mode is None:
            if not self.is_open():
                raise ValueError("Database must be opened") 
        else:
            if self.__open_mode != mode or not self.is_open():
                m = {WT_WRITE: "write", WT_READ: "read"}
                s = "Database must be opened in {0} mode".format(m[mode])
                raise ValueError(s)
    


class Table(Database):
    """
    The main storage table class. 
    """
    DB_NAME = "table"
    PRIMARY_KEY_NAME = "row_id"
    def __init__(self, homedir):
        Database.__init__(self, homedir, self.DB_NAME)
        self.__columns = []
        self.__column_name_map = {}
        self.__num_rows = 0

    def _create_ll_object(self, path):
        """
        Returns a new instance of _wormtable.Table using the specified 
        path as the backing file.
        """ 
        filename = path.encode() 
        ll_cols = [c.get_ll_object() for c in self.__columns]
        t = _wormtable.Table(filename, ll_cols, self.get_cache_size())
        return t

    def get_fixed_region_size(self):
        """
        Returns the size of the fixed region in rows. This is the minimum 
        size that a row can be; if there are no variable sized columns in 
        the table, then this is the exact size of each row.
        """
        return self.get_ll_object().fixed_region_size 

    # Helper methods for adding Columns of the various types.
   
    def add_id_column(self, size=4):
        """
        Adds the ID column with the specified size in bytes.
        """
        name = self.PRIMARY_KEY_NAME
        desc = 'Primary key column'
        self.add_uint_column(name, desc, size, 1)

    def add_uint_column(self, name, description="", size=2, num_elements=1):
        """
        Creates a new unsigned integer column with the specified name, 
        element size (in bytes) and number of elements. If num_elements=0
        then the column can hold a variable number of elements.
        """
        self.add_column(name, description, WT_UINT, size, num_elements)

    def add_int_column(self, name, description="", size=2, num_elements=1):
        """
        Creates a new integer column with the specified name, 
        element size (in bytes) and number of elements. If num_elements=0
        then the column can hold a variable number of elements.
        """
        self.add_column(name, description, WT_INT, size, num_elements)
    
    def add_float_column(self, name, description="", size=4, num_elements=1):
        """
        Creates a new float column with the specified name, 
        element size (in bytes) and number of elements. If num_elements=0
        then the column can hold a variable number of elements. Only 4 and 
        8 byte floats are supported by wormtable; these correspond to the 
        usual float and double types.
        """
        self.add_column(name, description, WT_FLOAT, size, num_elements)

    def add_char_column(self, name, description="", num_elements=0):
        """
        Creates a new character column with the specified name, description 
        and number of elements. If num_elements=0 then the column can hold 
        variable length strings; otherwise, it can contain strings of a fixed 
        length only.
        """
        self.add_column(name, description, WT_CHAR, 1, num_elements)

    def add_column(self, name, description, element_type, size, num_elements):
        """
        Creates a new column with the specified name, description, element type,
        element size and number of elements.
        """
        if self.is_open():
            raise ValueError("Cannot add columns to open table")
        nb = name
        if isinstance(name, str):
            nb = name.encode()
        db = description
        if isinstance(description, str):
            db = description.encode()
        col = _wormtable.Column(nb, db, element_type, size, num_elements)
        self.__columns.append(Column(col))

    # Methods for accessing the columns
    def columns(self):
        """
        Returns the list of columns. 
        """
        return list(self.__columns)

    def get_column(self, col_id):
        """
        Returns a column corresponding to the specified id. If this is an
        integer, we return the column at this position; if it is a string
        we return the column with the specified name.
        """
        ret = None
        if isinstance(col_id, int):
            ret = self.__columns[col_id]
        elif isinstance(col_id, str):
            k = self.__column_name_map[col_id]
            ret = self.__columns[k]
        else:
            raise TypeError("column ids must be strings or integers")
        return ret
    
    def get_metadata(self):
        """
        Returns an ElementTree instance describing the metadata for this 
        Table.
        """
        d = {"version":SCHEMA_VERSION}
        root = ElementTree.Element("schema", d)
        columns = ElementTree.Element("columns")
        root.append(columns)
        for c in self.__columns:
            columns.append(c.get_xml())
        return ElementTree.ElementTree(root)
       
    def set_metadata(self, tree):
        """
        Sets up this Table to reflect the metadata in the specified xml 
        ElementTree.
        """
        root = tree.getroot()
        if root.tag != "schema":
            raise ValueError("invalid xml")
        version = root.get("version")
        if version is None:
            raise ValueError("invalid xml")
        supported_versions = ["0.1-alpha", SCHEMA_VERSION]
        if version not in supported_versions: 
            raise ValueError("Unsupported schema version - rebuild required.")
        xml_columns = root.find("columns")
        for xmlcol in xml_columns.getchildren():
            col = Column.parse_xml(xmlcol)
            self.__column_name_map[col.get_name()]= len(self.__columns)
            self.__columns.append(col)

    def append(self, row):
        """
        Appends the specified row to this table.
        """
        t = self.get_ll_object()
        j = 0 
        for v in row:
            if v is not None:
                t.insert_elements(j, v)
            j += 1
        t.commit_row()
        self.__num_rows += 1
    
    def append_encoded(self, row):
        """
        Appends the specified row to this table.
        """
        t = self.get_ll_object()
        j = 0 
        for v in row:
            if v is not None:
                t.insert_encoded_elements(j, v)
            j += 1
        t.commit_row()
        self.__num_rows += 1
     

    def __len__(self):
        """
        Implement the len(t) function.
        """
        self.verify_open()
        mode = self.get_open_mode()
        if mode == WT_READ:
            if self.__num_rows == 0:
                self.__num_rows = self.get_ll_object().get_num_rows()
        return self.__num_rows
    
    def __getitem__(self, key):
        """
        Implements the t[key] function.
        """
        self.verify_open(WT_READ)
        t = self.get_ll_object()
        ret = None
        n = len(self)
        if isinstance(key, slice):
            ret = [self[j] for j in range(*key.indices(n))]
        elif isinstance(key, int):
            k = key
            if k < 0:
                k = n + k
            if k >= n:
                raise IndexError("table position out of range")
            ret = t.get_row(k)    
        else:
            raise TypeError("table positions must be integers")
        return ret
    
    def close(self):
        """
        Closes this Table.
        """
        self.verify_open()
        try:
            Database.close(self)
        finally:
            self.__num_rows = 0
            self.__columns = []
            self.__column_name_map = {}
    
    
    def cursor(self, columns, index=None):
        """
        Returns a cursor over the rows in this database, retreiving the specified 
        columns. If index is provided, the cursor will iterate over the rows in 
        the order defined by the index.

        Columns must be a list of column identifiers, or Column instances.
        """
        self.verify_open(WT_READ)
        c = None
        cols = []
        for col_id in columns:
            if isinstance(col_id, Column):
                cols.append(col_id)
            elif isinstance(col_id, int):
                cols.append(self.__columns[col_id])
            else:
                cols.append(self.get_column(col_id))
        if index is None:
            c = TableCursor(self, cols) 
        else:
            index.verify_open(WT_READ)
            c = IndexCursor(index, cols)
        return c

    def indexes(self):
        """
        Returns an interator over the names of the indexes in this table. 
        """
        self.verify_open(WT_READ)
        prefix = os.path.join(self.get_homedir(), Index.DB_PREFIX) 
        suffix = Index.DB_SUFFIX
        for g in glob.glob(prefix + "*" + suffix):
            name = g.replace(prefix, "")
            name = name.replace(suffix, "")
            yield name 


    def open_index(self, index_name, cache_size=DEFAULT_CACHE_SIZE):
        """
        Returns an open index on the this table. Supports the contextmanager
        protocol.
        """
        self.verify_open(WT_READ)
        index = Index(self, index_name) 
        if not index.exists():
            raise IOError("index '" + index_name + "' not found")
        index.set_cache_size(cache_size)
        index.open("r")
        return index

class Index(Database):
    """
    An index is an auxiliary table that sorts the rows according to 
    column values.
    """
    DB_PREFIX = "index_"
    def __init__(self, table, name):
        Database.__init__(self, table.get_homedir(), self.DB_PREFIX + name)
        self.__name = name
        self.__table = table
        self.__key_columns = []
        self.__bin_widths = []
    
    def get_name(self):
        """
        Return the name of this index.
        """
        return self.__name

    def get_colspec(self):
        """
        Returns the column specification for this index.
        """
        s = ""
        for c, w in zip(self.__key_columns, self.__bin_widths):
            s += c.get_name()
            if w != 0.0:
                s += "[{0}]".format(w)
            s += "+"
        return s[:-1]

    # Methods for accessing the key_columns
    def key_columns(self):
        """
        Returns the list of key columns.
        """
        return list(self.__key_columns)
    
    def bin_widths(self):
        """
        Returns the list of bin widths in this index.
        """
        return list(self.__bin_widths)

    def add_key_column(self, key_column, bin_width=0):
        """
        Adds the specified key_column to the list of key_columns we are indexing.
        """
        self.__key_columns.append(key_column)
        self.__bin_widths.append(bin_width)

    def _create_ll_object(self, path):
        """
        Returns a new instance of _wormtable.Table using the specified 
        path as the backing file.
        """ 
        filename = path.encode() 
        cols = [c.get_position() for c in self.__key_columns]
        i = _wormtable.Index(self.__table.get_ll_object(), filename, 
                cols, self.get_cache_size())
        i.set_bin_widths(self.__bin_widths)
        return i
    
    def get_metadata(self):
        """
        Returns an ElementTree instance describing the metadata for this 
        Index.
        """
        d = {"version":INDEX_METADATA_VERSION}
        root = ElementTree.Element("index", d)
        key_columns = ElementTree.Element("key_columns")
        root.append(key_columns)
        for j in range(len(self.__key_columns)):
            c = self.__key_columns[j]
            w = self.__bin_widths[j]
            if c.get_type() == WT_INT | c.get_type() == WT_UINT:
                w = int(w)
            d = {
                "name":c.get_name(), 
                "bin_width":str(w),
            }
            element = ElementTree.Element("key_column", d)
            key_columns.append(element)
        return ElementTree.ElementTree(root)
      
    def set_metadata(self, tree):
        """
        Sets up this Index to reflect the metadata in the specified xml 
        ElementTree.
        """
        root = tree.getroot()
        if root.tag != "index":
            # Should have a custom error for this.
            raise ValueError("invalid xml")
        version = root.get("version")
        if version is None:
            # Should have a custom error for this.
            raise ValueError("invalid xml")
        supported_versions = ["0.1-alpha", INDEX_METADATA_VERSION]
        if version not in supported_versions:
            raise ValueError("Unsupported index metadata version - rebuild required.")
        xml_key_columns = root.find("key_columns")
        for xmlcol in xml_key_columns.getchildren():
            if xmlcol.tag != "key_column":
                raise ValueError("invalid xml")
            name = xmlcol.get("name")
            col = self.__table.get_column(name)
            bin_width = float(xmlcol.get("bin_width"))
            self.__key_columns.append(col)
            self.__bin_widths.append(bin_width)

    def build(self, progress_callback=None, callback_rows=100):
        """
        Builds this index. If progress_callback is not None, invoke this 
        calback after every callback_rows have been processed.
        """
        llo = self.get_ll_object() 
        if progress_callback is not None:
            llo.build(progress_callback, callback_rows)
        else:
            llo.build() 

    def open(self, mode):
        """
        Opens this index in the specified mode.
        """
        self.__table.verify_open(WT_READ)
        Database.open(self, mode)

    def close(self):
        """
        Closes this Index.
        """
        try:
            Database.close(self)
        finally:
            self.__key_columns = []
            self.__bin_widths = []
    
    def keys(self): 
        """
        Returns an iterator over all the keys in this Index in sorted 
        order.
        """
        dvi = _wormtable.DistinctValueIterator(self.get_ll_object())
        for k in dvi:
            yield self.translate_value(k)

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
    
    def translate_value(self, v):
        """ 
        Translates the specified value from the low-level value to its
        high-level equivalent.
        """
        ret = v
        if len(v) == 1:
            ret = v[0]
        return ret

    def get_min(self, *k):
        """
        Returns the smallest index key greater than or equal to the specified 
        prefix.
        """
        key = self.translate_key(k)
        v = self.get_ll_object().get_min(key)
        return self.translate_value(v) 

    def get_max(self, *k):
        """
        Returns the largest index key less than the specified prefix. 
        """
        key = self.translate_key(k)
        v = self.get_ll_object().get_max(key)
        return self.translate_value(v) 

    def counter(self):
        """
        Returns an IndexCounter object for this index. This provides an efficient 
        method of iterating over the keys in the index.
        """
        self.verify_open(WT_READ)
        return IndexCounter(self)
    
 
class IndexCounter(collections.Mapping):
    """
    A counter for Indexes, based on the collections.Counter class. This class 
    is a dictionary-like object that represents a mapping of the distinct 
    keys in the index to the number of times those keys appear.
    """
    def __init__(self, index):
        self.__index = index
    
    def __getitem__(self, key):
        if isinstance(key, tuple):
            k = self.__index.translate_key(key)
        else:
            k = self.__index.translate_key((key,))
        return self.__index.get_ll_object().get_num_rows(k) 
   
    def __iter__(self):
        dvi = _wormtable.DistinctValueIterator(self.__index.get_ll_object())
        for v in dvi:
            yield self.__index.translate_value(v)

    def __len__(self):
        n = 0
        dvi = _wormtable.DistinctValueIterator(self.__index.get_ll_object())
        for v in dvi:
            n += 1
        return n

class Cursor(object):
    """
    Superclass of Cursor objects. Subclasses are responsible for allocating 
    an iterator.
    """
    def __iter__(self):
        #if len(self._columns) == 1:
        #    for r in self._row_iterator:
        #        yield r[0]
        #else:
        for r in self._row_iterator:
            yield r

class TableCursor(Cursor):
    """
    A cursor over the rows of the table in the order defined by an index. 
    """
    def __init__(self, table, columns):
        self._columns = columns
        col_positions = [c.get_position() for c in columns]
        self._row_iterator = _wormtable.TableRowIterator(table.get_ll_object(), 
                col_positions)

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
    def __init__(self, index, columns):
        self.__index = index
        self._columns = columns
        col_positions = [c.get_position() for c in columns]
        self._row_iterator = _wormtable.IndexRowIterator(
                self.__index.get_ll_object(), col_positions)
    
    def set_min(self, *v):
        key = self.__index.translate_key(v)
        self._row_iterator.set_min(key)
    
    def set_max(self, *v):
        key = self.__index.translate_key(v)
        self._row_iterator.set_max(key)


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


