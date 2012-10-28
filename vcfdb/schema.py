
# Temporary home for schema information while we are still using pickle
# for state.

RECORD_ID = "RECORD_ID"
CHROM = "CHROM"
POS = "POS"
ID = "ID"
REF = "REF"
ALT = "ALT"
QUAL = "QUAL"
FILTER = "FILTER"
INFO = "INFO"


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

class IntegerColumn(NumberColumn):
    """
    Class representing columns with integer values.
    """
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
    if number == "1":
        if ctype == "Integer":
            col = IntegerColumn(6)
        elif ctype == "Float":
            col = FloatColumn(10)
        elif ctype == "String":
            col = StringColumn()
        else:
            raise ValueError("Unexpected column:", d)
    else:
        # for now, treat these columns as strings. Ultimately we should 
        # try to parse them and present them as individual columns 
        # available for indexing.
        col = StringColumn()
    col.set_description(description)
    col.set_id(cid)
    return col

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
        col = IntegerColumn(12)
        col.set_id(c)
        self.__columns[c] = col
        c = POS
        col = IntegerColumn(12)
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
            self.__columns[cid] = column_factory(cid, d)

    def add_info_column(self, d):
        """
        Adds an INFO column with the specified desrciption and type.
        """
        cid = "INFO_" + d["ID"]
        col = column_factory(cid, d)
        self.__columns[cid] = col

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
