"""
Prototype implementation of the Berkeley DB VCF record store.
"""
import os
import sys
import gzip
import pickle
import struct 
import tempfile
from contextlib import contextmanager

import bsddb3.db as bdb 

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

class VCFColumn(object):
    """
    Class representing a column in a VCF database. This encapsulates information 
    about the type, desciption, and so on, and provides encoders and decoders
    appropriate to the column.
    """
    def __init__(self):
        self._description = None 
        self._id = None

    def set_description(self, description):
        """
        Sets the column description to the specified value.
        """
        self._description = description

    def set_id(self, cid):
        """
        Sets the ID for this column to the specified value.
        """
        self._id = cid
    
    def get_id(self):
        """
        Returns the ID of this column.
        """
        return self._id

    def encode(self, value):
        """
        Encodes the specified value as bytes object.
        """
        return value.encode()
    
    def decode(self, b):
        """
        Decodes the specified bytes into a python value of the 
        appropriate type.
        """
        return b.decode()
    
    def default_value(self):
        return "."

    def __str__(self):
        return "ID={0}; DESC={1}".format(self._id, self._description)


class CompoundColumn(VCFColumn):
    """
    Class representing a compound column in the VCF database. Compound
    columns are obtained by concatenating two or more columns 
    together.
    """
    def __init__(self, colid, columns):
        self._id = colid
        self._columns = columns

    def encode(self, value):
        """
        Encodes the specified tuple of values as a concatenated list 
        of bytes.
        """
        v = b""
        for j in range(len(self._columns)):
            col = self._columns[j]
            v += col.encode(value[j]) + b"_"
        return v[:-1] 
    
    def decode(self, b):
        """
        Decodes the specified _ delimited value into a tuple of values.
        """ 
        values = b.split(b"_")
        t = list(values) 
        for j in range(len(self._columns)):
            t[j] = self._columns[j].decode(values[j])
        return tuple(t)


class NumberColumn(VCFColumn):
    """
    Class representing columns with numeric values.
    """
    def __init__(self, width):
        super().__init__()
        self._width = width
    
    def __str__(self):
        return "width={0};".format(self._width) + super().__str__() 
    
    def default_value(self):
        return -1 

class LongIntegerColumn(NumberColumn):
    """
    Class representing columns with large integer values.
    """
    def __init__(self):
        super().__init__(12)

    def __str__(self):
        return "Integer:" + super().__str__()
        
    def encode(self, value):
        """
        Encodes the specified integer value as bytes object.
        """
        fmt = "{0:0" +  str(self._width) + "d}" 
        s = fmt.format(value)
        return s.encode()
    
    def decode(self, string):
        """
        Decodes the specified string into an integer value.
        """
        return int(string)


class ShortIntegerColumn(NumberColumn):
    """
    Class representing columns with small integer values.
    """
    def __init__(self):
        super().__init__(6)

    def __str__(self):
        return "Integer:" + super().__str__()
        
    def encode(self, value):
        """
        Encodes the specified integer value as bytes object.
        """
        fmt = "{0:0" +  str(self._width) + "d}" 
        s = fmt.format(value)
        return s.encode()
    
    def decode(self, string):
        """
        Decodes the specified string into an integer value.
        """
        return int(string)

         
         
class FloatColumn(NumberColumn):
    """
    Class representing columns with floating point values.
    """
    def __str__(self):
        return "Float:" + super().__str__()

    def encode(self, value):
        """
        Encodes the specified float value as bytes object.
        """
        fmt = "{0:0" +  str(self._width) + ".2f}" 
        s = fmt.format(value)
        return s.encode()

    def decode(self, string):
        """
        Decodes the specified string into a float value.
        """
        return float(string)
     


class StringColumn(VCFColumn):
    """
    Class representing a column with arbitrary string values.
    """
    def __str__(self):
        return "String:" + super().__str__()
   

def column_type_factory(ctype):
    """
    Returns a column of the appropriate type given the specified type string.
    """ 
    if ctype == "Integer":
        col = ShortIntegerColumn()
    elif ctype == "Float":
        col = FloatColumn(10)
    elif ctype == "String":
        col = StringColumn()
    else:
        raise ValueError("Unexpected column:", d)
    return col

def column_factory(cid, d):
    """
    Generates a column object representing a column descibed by the 
    specified dictionary. 
    """ 
    #print("column factory:", d)
    description = d["Description"]
    ctype = d["Type"]
    number = d["Number"]
    #print("\t", cid, ":", description, ":", ctype, ":",  number)
    columns = {}
    if number == "1":
        col = column_type_factory(ctype)
        columns[cid] = col
    elif number == "." or number == "0":
        # For now treat as strings
        col = StringColumn()
        columns[cid] = col
    else:
        # number should be an integer
        n = int(number)
        for j in range(n):
            cid_col = cid + "_{0}".format(j)
            col = column_type_factory(ctype) 
            columns[cid_col] = col
    for col_id, col in columns.items():
        col.set_description(description)
        col.set_id(col_id)
    return columns 

class VCFSchema(object):
    """
    Class representing the columns of a VCF database, including information 
    on the id, type and width of each column.
    """
    def __init__(self):
        self.__genotypes = []
        self.__genotype_columns = []
        self.__columns = {}
        self.__add_fixed_columns()

    def __add_fixed_columns(self):
        """
        Sets up the fixed column schema.
        """
        string_cols = [CHROM, ID, REF, ALT, FILTER]
        for c in string_cols:
            col = StringColumn()
            col.set_id(c)
            self.__columns[c] = col
        c = RECORD_ID 
        col = LongIntegerColumn()
        col.set_id(c)
        self.__columns[c] = col
        c = POS
        col = LongIntegerColumn()
        col.set_id(c)
        self.__columns[c] = col
        c = QUAL
        col = FloatColumn(10)
        col.set_id(c)
        self.__columns[c] = col
    
    def add_genotype(self, genotype):
        """
        Adds the specified  genotype to this schema
        """
        self.__genotypes.append(genotype)
        for d in self.__genotype_columns:
            cid = genotype + "_" + d["ID"]
            for col_id, col in column_factory(cid, d).items():
                self.__columns[col_id] = col 

    def add_info_column(self, d):
        """
        Adds an INFO column with the specified desrciption and type.
        """
        cid = "INFO_" + d["ID"]
        for col_id, col in column_factory(cid, d).items():
            self.__columns[col_id] = col 

    def add_genotype_column(self, d):
        """
        Adds a genotype column with the specified description and type.
        """
        self.__genotype_columns.append(d)
    
    def add_compound_column(self, cid):
        """
        Adds a compound column with the specified id.
        """
        cols = [self.get_column(scid) for scid in cid] 
        col = CompoundColumn(cid, cols)
        self.__columns[cid] = col 

    def get_columns(self):
        """
        Returns an iterator over all columns.
        """
        return self.__columns.values()
   
    def get_column_ids(self):
        """
        Returns the list of column ids.
        """
        return self.__columns.keys()
    
    def get_column(self, cid):
        """
        Returns the column with the specified id.
        """
        return self.__columns[cid]

    def get_genotype(self, j):
        """
        Returns the jth genotype id.
        """
        return self.__genotypes[j]
   

    def generate_packing_format(self):
        """
        All data for the headers has been read in and we can now decide the 
        canonical packing format for the records.
        """
        self.__short_integer_columns = [] 
        self.__long_integer_columns = [] 
        self.__float_columns = []
        self.__string_columns = []
        for k, v in self.__columns.items():
            if isinstance(v, LongIntegerColumn):
                self.__long_integer_columns.append(k)
            elif isinstance(v, ShortIntegerColumn):
                self.__short_integer_columns.append(k)
            elif isinstance(v, FloatColumn):
                self.__float_columns.append(k)
            elif isinstance(v, StringColumn):
                self.__string_columns.append(k)
        self.__long_integer_columns.sort()
        self.__short_integer_columns.sort()
        self.__float_columns.sort() 
        self.__string_columns.sort()
         
        print("long integers = ", self.__long_integer_columns)
        print("short integers = ", self.__short_integer_columns)
        print("floats = ", self.__float_columns)
        print("strings = ", self.__string_columns)


    def pack_record(self, d, record_id):
        """
        Packs the specified record into a binary form suitable for 
        storage and retreival.
        """
        d[RECORD_ID] = record_id
        b = bytearray(8192)
        offset = 0
        for col in self.__long_integer_columns:
            value = d[col]
            fmt = ">q"
            struct.pack_into(fmt, b, offset, value)
            offset += struct.calcsize(fmt) 
            #print("\t", col, "->", value, ":", offset)
        for col in self.__short_integer_columns:
            value = -1
            if col in d:
                value = d[col]
            fmt = ">h"
            struct.pack_into(fmt, b, offset, value)
            offset += struct.calcsize(fmt) 
            #print("\t", col, "->", value, ":", offset)
        for col in self.__float_columns:
            value = -1.0
            if col in d:
                value = d[col]
            fmt = ">f"
            struct.pack_into(fmt, b, offset, value)
            offset += struct.calcsize(fmt) 
            #print("\t", col, "->", value, ":", offset)
        for col in self.__string_columns:
            value = "." 
            if col in d:
                value = d[col]
            #print("\t", col, "->", value.encode())
            v = value.encode()
            b[offset:offset + len(v)] = v
            offset += len(v)
            b[offset] = 0
            offset += 1
        #print("b = ", b[:offset] )
        print(offset, len(d["record"]))

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


