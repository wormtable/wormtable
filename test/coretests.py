from __future__ import print_function
from __future__ import division 

import os
import tempfile
import unittest
import random
import string

import _wormtable

__column_id = 0

# module variables used to control the number of tests that we do.
num_random_test_rows = 10


def get_uint_column(element_size, num_elements=_wormtable.NUM_ELEMENTS_VARIABLE):
    """
    Returns an unsigned integer column with the specified element size and 
    number of elements.
    """
    global __column_id
    __column_id += 1
    name = "uint_{0}_{1}_{2}".format(element_size, num_elements, __column_id)
    c = _wormtable.Column(name.encode(), b"", _wormtable.ELEMENT_TYPE_UINT, 
            element_size, num_elements)
    return c

def get_int_column(element_size, num_elements=_wormtable.NUM_ELEMENTS_VARIABLE):
    """
    Returns an integer column with the specified element size and 
    number of elements.
    """
    global __column_id
    __column_id += 1
    name = "int_{0}_{1}_{2}".format(element_size, num_elements, __column_id)
    return _wormtable.Column(name.encode(), b"", _wormtable.ELEMENT_TYPE_INT, 
            element_size, num_elements)

def get_float_column(element_size, num_elements):
    """
    Returns a float column with the specified element size and 
    number of elements.
    """
    global __column_id
    __column_id += 1
    name = "float_{0}_{1}_{2}".format(element_size, num_elements, __column_id)
    return _wormtable.Column(name.encode(), b"", _wormtable.ELEMENT_TYPE_FLOAT, 
            element_size, num_elements)

def get_char_column(num_elements):
    """
    Returns a char column with the specified number of elements.
    """
    global __column_id
    __column_id += 1
    name = "char_{0}_{1}".format(num_elements, __column_id)
    return _wormtable.Column(name.encode(), b"", _wormtable.ELEMENT_TYPE_CHAR, 1, 
            num_elements)

def get_int_range(element_size):
    """
    Returns the tuple min, max defining the acceptable bounds for an
    integer of the specified size.
    """
    min_v = -2**(8 * element_size - 1) + 1
    max_v = 2**(8 * element_size - 1) - 1
    return min_v, max_v

def get_uint_range(element_size):
    """
    Returns the tuple min, max defining the acceptable bounds for an
    unsigned integer of the specified size.
    """
    min_v = 0 
    max_v = 2**(8 * element_size) - 2
    return min_v, max_v


def random_string(n):
    """
    Returns a random string of length n.
    """
    s = ''.join(random.choice(string.printable) for x in range(n)) 
    return s

class TestDatabase(unittest.TestCase):
    """
    Superclass of all database tests. Takes care of allocating a database and
    clearing it up afterwards. Subclasses must define the get_columns 
    method.
    """
    def setUp(self):
        fd, self._db_file = tempfile.mkstemp("-test.db") 
        self._columns = self.get_columns()
        self._database = _wormtable.BerkeleyDatabase(self._db_file.encode(), 
                self._columns, cache_size=1024)
        self._database.create()
        # We can close the open fd now that db has opened it.
        os.close(fd)
        buffer_size = 64 * 1024
        self._row_buffer = _wormtable.WriteBuffer(self._database, buffer_size, 1)
        self.num_random_test_rows = num_random_test_rows 

    def open_reading(self):
        """
        Flushes any records and opens the database for reading.
        """
        self._row_buffer.flush()
        self._database.close()
        self._database.open()

    def tearDown(self):
        self._row_buffer.flush()
        self._database.close()
        os.unlink(self._db_file)


class TestEmptyDatabase(TestDatabase):
    """
    Tests to see if an empty database is correctly handled.
    
    TODO: there is some nasty bug lurking here somewhere - it caused a crash
    when looking for another bug.
    """
   
    def get_columns(self):
        return []

    def test_empty_rows(self):
        self._row_buffer.commit_row()
        self.open_reading()
        self.assertEqual(self._database.get_num_rows(), 1)
        

  
class TestElementParsers(TestDatabase):
    """
    Test the element parsers to ensure they accept and reject 
    values correctly.
    """     
    def get_columns(self):
        self._int_columns = {}
        self._uint_columns = {}
        for j in range(1, 9):
            self._int_columns[j] = get_int_column(j, 1)
            self._uint_columns[j] = get_uint_column(j, 1)
        self._float_columns = {4: get_float_column(4, 1)}
        cols = list(self._int_columns.values()) + list(self._float_columns.values()) 
        return cols

    def test_bad_types(self):
        """
        Throw bad types at all the columns and expect a type error.
        """
        f = self._row_buffer.insert_elements
        values = [[], {}, self, tuple()]
        for c in self._columns:
            for v in values:
                self.assertRaises(TypeError, f, c, v)
    
    def FIX_test_good_float_values(self):
        rb = self._row_buffer
        values = ["-1", "-2", "0", "4", "14", "100",
            "0.01", "-5.224234345235", "1E12"]
        for c in self._float_columns.values():
            for v in values:
                self.assertEqual(rb.insert_encoded_elements(c, v.encode()), None)
                self.assertEqual(rb.insert_elements(c, float(v)), None)
            for k in range(10):
                v = random.uniform(-100, 100)
                b = str(v).encode()
                self.assertEqual(rb.insert_encoded_elements(c, b), None)
                self.assertEqual(rb.insert_elements(c, v), None)
     
    def FIX_test_bad_float_values(self):
        rb = self._row_buffer
        values = ["", "--1", "sdasd", "[]", "3qsd", "1Q0.023"]
        for c in self._float_columns.values():
            for v in values:
                e = v.encode()
                self.assertRaises(ValueError, rb.insert_encoded_elements, c, e)
        values = [None, [], 234, "1.23"]
        for c in self._float_columns.values():
            for v in values:
                self.assertRaises(TypeError, rb.insert_elements, c, v)
        

class TestListParsers(TestDatabase):
    """
    Test the list parsers to make sure that malformed lists are correctly
    detected.
    """     
    def get_columns(self): 
        self._uint_columns = {}
        self._int_columns = {}
        self._float_columns = {}
        for j in range(1, 5):
            self._uint_columns[j] = get_uint_column(1, j)
            self._int_columns[j] = get_int_column(1, j)
            self._float_columns[j] = get_float_column(4, j)
        cols = list(self._int_columns.values()) + list(self._float_columns.values()) 
        return cols
     
    def test_malformed_python_lists(self):
        rb = self._row_buffer
        i2 = self._int_columns[2]
        f2 = self._int_columns[2]
        for s in [[1], [1, 2, 3], (1, 2, 3), range(40)]:
            self.assertRaises(ValueError, rb.insert_elements, f2, s)
            self.assertRaises(ValueError, rb.insert_elements, i2, s)


class TestDatabaseLimits(TestDatabase):
    """
    Tests the limits of various aspects of the database to see if errors
    are flagged correctly.
    """ 
    def get_columns(self):
        n = _wormtable.MAX_ROW_SIZE // (_wormtable.MAX_NUM_ELEMENTS * 8 
                + _wormtable.NUM_ELEMENTS_VARIABLE_OVERHEAD)
        columns = [get_int_column(8) for j in range(n + 1)]
        return columns
    
    def test_column_overflow(self):
        """
        If we set the maximum number of elements to n - 1 columns, 
        it should overflow whatever we do.
        """
        rb = self._row_buffer
        v = [j for j in range(_wormtable.MAX_NUM_ELEMENTS)]
        n = len(self._columns)
        for k in range(n - 1):
            rb.insert_elements(self._columns[k], v) 
        self.assertRaises(ValueError, rb.insert_elements,
                self._columns[0], v)  
        self.assertRaises(ValueError, rb.insert_elements,
                self._columns[0], v)
        rb.commit_row()

    def test_column_allocation_limits(self):
        # Test zero size columns
        self.assertRaises(ValueError, get_int_column, 0, 1)
        self.assertRaises(ValueError, get_float_column, 0, 1)
        
        # Test negative size columns
        self.assertRaises(ValueError, get_int_column, -1, 1)
        self.assertRaises(ValueError, get_float_column, -100, 1)

        # Test negative num_elements 
        self.assertRaises(ValueError, get_int_column, 1, -1)
        self.assertRaises(ValueError, get_float_column, 1, -100)

class TestDatabaseInteger(TestDatabase):
    """
    Tests the integrity of the database by inserting values and testing 
    to ensure they are retreived correctly.
    """ 
    def get_columns(self):
        q = _wormtable.NUM_ELEMENTS_VARIABLE
        columns = [get_int_column(j, q) for j in range(1, 9)] \
            + [get_int_column(j, 1) for j in range(1, 9)] \
            + [get_int_column(j, 2) for j in range(1, 9)] \
            + [get_int_column(j, 3) for j in range(1, 9)] \
            + [get_int_column(j, 4) for j in range(1, 9)] \
            + [get_uint_column(j, q) for j in range(1, 9)] \
            + [get_uint_column(j, 1) for j in range(1, 9)] \
            + [get_uint_column(j, 2) for j in range(1, 9)] \
            + [get_uint_column(j, 3) for j in range(1, 9)] \
            + [get_uint_column(j, 4) for j in range(1, 9)] 
        # randomise the columns so we don't have all the variable 
        # columns at the start.
        random.shuffle(columns)
        return columns
   
    def populate_boundary_values(self):
        """
        Populate with boundary values. 
        """
        rb = self._row_buffer
        db = self._database
        cols = self._columns
        num_cols = len(cols)
        num_rows = 2
        self.rows = [[0 for c in self._columns] for j in range(num_rows)]
        for j in range(num_rows):
            for k in range(num_cols): 
                c = cols[k]
                min_v, max_v = c.min_element, c.max_element
                v = min_v
                if j == 1:
                    v = max_v
                if c.num_elements == 1:
                    self.rows[j][k] = v 
                else:
                    n = c.num_elements
                    if n == _wormtable.NUM_ELEMENTS_VARIABLE:
                        n = random.randint(1, _wormtable.MAX_NUM_ELEMENTS)
                    v = tuple([v for l in range(n)])
                    self.rows[j][k] = v
                rb.insert_elements(c, self.rows[j][k]) 
            rb.commit_row()

    def populate_randomly(self, num_rows):
        """
        Generates random values for the columns and inserts them
        into database. Store these as lists in the instance variable
        self.rows.
        """
        rb = self._row_buffer
        db = self._database
        cols = self._columns
        num_cols = len(cols)
        self.rows = [[0 for c in self._columns] for j in range(num_rows)]
        for j in range(num_rows):
            for k in range(num_cols): 
                c = cols[k]
                min_v, max_v = c.min_element, c.max_element
                if c.num_elements == 1:
                    self.rows[j][k] = random.randint(min_v, max_v) 
                else:
                    n = c.num_elements
                    if n == _wormtable.NUM_ELEMENTS_VARIABLE:
                        n = random.randint(1, _wormtable.MAX_NUM_ELEMENTS)
                    v = tuple([random.randint(min_v, max_v) for l in range(n)])
                    self.rows[j][k] = v
                rb.insert_elements(c, self.rows[j][k]) 
            rb.commit_row()

class TestDatabaseIntegerLimits(TestDatabaseInteger):
    """
    Tests the limits of integer columns to see if they are dealt with 
    correctly.
    """
    def insert_bad_value(self, column, value):
        rb = self._row_buffer
        v = value
        if column.num_elements == _wormtable.NUM_ELEMENTS_VARIABLE:
            v = [value]
        elif column.num_elements != 1:
            v = [value for j in range(column.num_elements)]
        def f():
            rb.insert_elements(column, v)
        self.assertRaises(OverflowError, f)

    def insert_good_value(self, column, value):
        rb = self._row_buffer
        v = value
        if column.num_elements == _wormtable.NUM_ELEMENTS_VARIABLE:
            v = [value]
        elif column.num_elements != 1:
            v = [value for j in range(column.num_elements)]
        self.assertEqual(rb.insert_elements(column, v), None)


    def test_outside_range(self):
        for c in self._columns:
            min_v, max_v = c.min_element, c.max_element
            for j in range(1, 5):
                v = min_v - j
                self.insert_bad_value(c, v) 
                v = max_v + j
                self.insert_bad_value(c, v) 

    def test_inside_range(self):
        for c in self._columns:
            min_v, max_v = c.min_element, c.max_element
            for j in range(0, 5):
                v = min_v + j
                self.insert_good_value(c, v) 
                v = max_v - j
                self.insert_good_value(c, v) 

    def test_max_min(self):
        """
        Test the max and min values from the columns.
        """
        for j in range(1, 9):
            c = get_int_column(j, 1) 
            min_v, max_v = get_int_range(c.element_size)
            self.assertEqual(min_v, c.min_element)
            self.assertEqual(max_v, c.max_element)
            c = get_uint_column(j, 1) 
            min_v, max_v = get_uint_range(c.element_size)
            self.assertEqual(min_v, c.min_element)
            self.assertEqual(max_v, c.max_element)


class TestDatabaseIntegerIntegrity(TestDatabaseInteger):


    def test_small_int_retrieval(self):
        rb = self._row_buffer
        db = self._database
        values = range(40)
        for v in values:
            for c in self._columns:
                if c.num_elements == 1:
                    rb.insert_elements(c, v)
            rb.commit_row()
        self.open_reading()
        self.assertEqual(db.get_num_rows(), len(values))
        j = 0
        for v in values:
            r = self._database.get_row(j)
            for c in self._columns:
                if c.num_elements == 1:
                    self.assertEqual(v, r[c.name])
            j += 1

    def test_boundary_int_retrieval(self):
        self.populate_boundary_values()
        self.open_reading()
        for j in range(len(self.rows)):
            r = self._database.get_row(j)
            for k in range(len(self._columns)): 
                c = self._columns[k]
                self.assertEqual(self.rows[j][k], r[c.name])


    def test_random_int_retrieval(self):
        num_rows = self.num_random_test_rows 
        self.populate_randomly(num_rows)
        self.open_reading()
        self.assertEqual(self._database.get_num_rows(), num_rows)
        for j in range(num_rows): 
            r = self._database.get_row(j)
            for k in range(len(self._columns)): 
                c = self._columns[k]
                self.assertEqual(self.rows[j][k], r[c.name])


class TestDatabaseFloat(TestDatabase):
    """
    Tests the integrity of the database by inserting values and testing 
    to ensure they are retreived correctly.
    """ 
    def get_columns(self):
        q = _wormtable.NUM_ELEMENTS_VARIABLE
        columns = [
                get_float_column(4, q), get_float_column(8, q), 
                get_float_column(4, 1), get_float_column(8, 1), 
                get_float_column(4, 2), get_float_column(8, 2), 
                get_float_column(4, 3), get_float_column(8, 3), 
                get_float_column(4, 4), get_float_column(8, 4), 
                get_float_column(4, 5), get_float_column(8, 5), 
                ]
        # randomise the columns so we don't have all the variable 
        # columns at the start.
        random.shuffle(columns)
        return columns

    def populate_randomly(self, num_rows):
        """
        Generates random values for the columns and inserts them
        into database. Store these as lists in the instance variable
        self.rows.
        """
        rb = self._row_buffer
        db = self._database
        cols = self._columns
        num_cols = len(cols)
        self.rows = [[0 for c in self._columns] for j in range(num_rows)]
        for j in range(num_rows):
            for k in range(num_cols): 
                c = cols[k]
                min_v, max_v = -10, 10 
                if c.num_elements == 1:
                    self.rows[j][k] = random.uniform(min_v, max_v) 
                else:
                    n = c.num_elements
                    if n == _wormtable.NUM_ELEMENTS_VARIABLE:
                        n = random.randint(1, _wormtable.MAX_NUM_ELEMENTS)
                    v = tuple([random.uniform(min_v, max_v) for l in range(n)])
                    self.rows[j][k] = v
                rb.insert_elements(c, self.rows[j][k]) 
            rb.commit_row()

class TestDatabaseFloatIntegrity(TestDatabaseFloat):

    def test_random_float_retrieval(self):
        num_rows = self.num_random_test_rows 
        cols = self._columns
        num_cols = len(cols)
        self.populate_randomly(num_rows)
        self.open_reading()
        self.assertEqual(self._database.get_num_rows(), num_rows)
        for j in range(num_rows):
            r = self._database.get_row(j)
            for k in range(num_cols): 
                c = cols[k]
                if c.element_size == 8:
                    self.assertEqual(self.rows[j][k], r[c.name])
                else:
                    #print(rows[j][k])
                    if c.num_elements == 1:
                        self.assertAlmostEqual(self.rows[j][k], r[c.name], places=6)
                    else:
                        for u, v in zip(self.rows[j][k], r[c.name]):
                            self.assertAlmostEqual(u, v, places=6)



class TestDatabaseChar(TestDatabase):
    """
    Tests the integrity of the database by inserting values and testing 
    to ensure they are retreived correctly.
    """ 
    def get_columns(self):
        columns = [get_char_column(j) for j in range(1, 20)]
        columns.append(get_char_column(_wormtable.NUM_ELEMENTS_VARIABLE))
        columns.append(get_char_column(_wormtable.NUM_ELEMENTS_VARIABLE))
        columns.append(get_char_column(_wormtable.NUM_ELEMENTS_VARIABLE))
        random.shuffle(columns)
        return columns
    
    def populate_randomly(self, num_rows):
        """
        Generates random values for the columns and inserts them
        into database. Store these as lists in the instance variable
        self.rows.
        """
        rb = self._row_buffer
        db = self._database
        cols = self._columns
        num_cols = len(cols)
        self.rows = [[0 for c in self._columns] for j in range(num_rows)]
        for j in range(num_rows):
            for k in range(num_cols): 
                c = cols[k]
                n = c.num_elements
                if n == _wormtable.NUM_ELEMENTS_VARIABLE:
                    n = random.randint(1, _wormtable.MAX_NUM_ELEMENTS)
                self.rows[j][k] = random_string(n).encode() 
                rb.insert_elements(c, self.rows[j][k]) 
            rb.commit_row()

class TestDatabaseCharIntegrity(TestDatabaseChar):
    
    def test_illegal_long_strings(self):
        """
        Test to ensure that long strings are trapped correctly.
        """
        rb = self._row_buffer
        for c in self._columns:
            n = c.num_elements
            if n == _wormtable.NUM_ELEMENTS_VARIABLE:
                n = _wormtable.MAX_NUM_ELEMENTS
            for j in [1, 2, 3, 10, 500, 1000]:
                s = random_string(n + j).encode() 
                self.assertRaises(ValueError, rb.insert_elements, c, s)
        

    def test_variable_char_retrieval(self):
        rb = self._row_buffer
        db = self._database
        cols = [c for c in self._columns if 
                c.num_elements == _wormtable.NUM_ELEMENTS_VARIABLE]
        num_cols = len(cols)
        num_rows = self.num_random_test_rows 
        rows = [[None for c in self._columns] for j in range(num_rows)]
        for j in range(num_rows):
            for k in range(num_cols): 
                c = cols[k]
                rows[j][k] = random_string(min(j, 50)).encode() 
                rb.insert_elements(c, rows[j][k]) 
            rb.commit_row()
        self.open_reading()
        self.assertEqual(db.get_num_rows(), num_rows)
        for j in range(num_rows):
            r = db.get_row(j)
            for k in range(num_cols): 
                c = cols[k]
                self.assertEqual(rows[j][k], r[c.name])
    
    def test_short_char_retrieval(self):
        rb = self._row_buffer
        db = self._database
        cols = [c for c in self._columns if 
                c.num_elements != _wormtable.NUM_ELEMENTS_VARIABLE]
        num_cols = len(cols)
        num_rows = self.num_random_test_rows 
        rows = [[None for c in self._columns] for j in range(num_rows)]
        for j in range(num_rows):
            for k in range(num_cols): 
                c = cols[k]
                n = random.randint(0, c.num_elements)
                rows[j][k] = random_string(n).encode() 
                rb.insert_elements(c, rows[j][k]) 
            rb.commit_row()
        self.open_reading()
        self.assertEqual(db.get_num_rows(), num_rows)
        for j in range(num_rows):
            r = db.get_row(j)
            for k in range(num_cols): 
                c = cols[k]
                self.assertEqual(rows[j][k], r[c.name])
    
    def test_random_char_retrieval(self):
        num_rows = self.num_random_test_rows 
        self.populate_randomly(num_rows)
        self.open_reading()
        cols = self._columns
        num_cols = len(cols)
        self.assertEqual(self._database.get_num_rows(), num_rows)
        for j in range(num_rows):
            r = self._database.get_row(j)
            for k in range(num_cols): 
                c = cols[k]
                self.assertEqual(self.rows[j][k], r[c.name])


    

class TestIndexIntegrity(object):
    """
    Tests the integrity of indexes. Concrete test should subclass this and one of the 
    Test classes above to get an implementation of populate_randomly.
    """
    def create_indexes(self):
        """
        Creates a bunch of indexes.
        """
        num_rows = self.num_random_test_rows 
        self.populate_randomly(num_rows)
        self.open_reading()
        cache_size = 64 * 1024
        self._indexes = [[], [], []]
        self._index_files = []
        # make the single column indexes
        for c in self._columns:
            fd, index_file = tempfile.mkstemp("-index-test.db") 
            index = _wormtable.Index(self._database, index_file.encode(), [c], 
                    cache_size)
            os.close(fd)
            index.create()
            index.close()
            self._index_files.append(index_file)
            self._indexes[1].append(index)

    def destroy_indexes(self):
        """
        Delete the index files.
        """
        for f in self._index_files:
            os.unlink(f)
    
    def test_column_sort_order(self):
         self.create_indexes()
         for j in range(len(self._columns)):
            col = self._columns[j]
            index = self._indexes[1][j]
            index.open()
            row_iter = _wormtable.RowIterator(self._database, [col], index)
            l = [row[0] for row in row_iter]
            l2 = sorted(l)
            self.assertEqual(l, l2)
            # get the list from the original rows
            l3 = [row[j] for row in self.rows]
            l3.sort()
            # TODO: push the comparison up into the superclass
            if not (col.element_type == _wormtable.ELEMENT_TYPE_FLOAT and col.element_size == 4):
                self.assertEqual(l, l3)
            index.close()
         self.destroy_indexes()

    def test_column_min_max(self):
        self.create_indexes()
        for j in range(len(self._columns)):
            col = self._columns[j]
            index = self._indexes[1][j]
            index.open()
            original = [row[j] for row in self.rows] 
            s = random.sample(original, 2)
            min_val = min(s),
            max_val = max(s),
            row_iter = _wormtable.RowIterator(self._database, [col], index)
            row_iter.set_min(max_val)
            row_iter.set_max(min_val)
            l = [row[0] for row in row_iter]
            if len(l) != 0:
                print("TODO: fix this bug!")
                print(min_val, max_val)
            self.assertEqual(len(l), 0)
            # Check if the correct lists are returned. 
            row_iter = _wormtable.RowIterator(self._database, [col], index)
            row_iter.set_min(min_val)
            row_iter.set_max(max_val)
            l = [row[0] for row in row_iter]
            l2 = sorted([v for v in original if min_val[0] <= v and v < max_val[0]])
            # TODO: push the comparison up into the superclass
            if not (col.element_type == _wormtable.ELEMENT_TYPE_FLOAT and col.element_size == 4):
                if l != l2:
                    self.assertEqual(l, l2)
                min_value = index.get_min(tuple()) 
                self.assertEqual(min(original), min_value[0]) 
                max_value = index.get_max(tuple()) 
                self.assertEqual(max(original), max_value[0]) 
            index.close()
        self.destroy_indexes()

    def test_two_column_sort_order(self):
        num_rows = self.num_random_test_rows 
        self.populate_randomly(num_rows)
        self.open_reading()
        cache_size = 64 * 1024
        indexes = []
        index_files = []
        original_values = []
        n = len(self._columns)
        pairs = [(j, k) for j in range(n) for k in range(j + 1, n)]
        max_pairs = 100
        if len(pairs) > max_pairs:
            pairs = random.sample(pairs, max_pairs)
        for j, k in pairs:
            c1 = self._columns[j]
            c2 = self._columns[k]
            # TODO: variable length columns don't sort the same way
            # this might be a bug or it might not - this should be 
            # verified and the correct sorting procedure used here.
            if c1.num_elements != _wormtable.NUM_ELEMENTS_VARIABLE and \
                    c2.num_elements != _wormtable.NUM_ELEMENTS_VARIABLE:
            
                fd, index_file = tempfile.mkstemp("-index-test.db") 
                index = _wormtable.Index(self._database, index_file.encode(), [c1, c2], 
                        cache_size)
                os.close(fd)
                index.create()
                index.close()
                index_files.append(index_file)
                indexes.append(index)
                original_values.append([(row[j], row[k]) for row in self.rows])

        for index, original in zip(indexes, original_values):
            index.open()
            row_iter = _wormtable.RowIterator(self._database, index.columns, index)
            l = [row for row in row_iter]
            l2 = sorted(original) 
            o = [col.element_type == _wormtable.ELEMENT_TYPE_FLOAT and col.element_size == 4
                for col in index.columns]
            if not any(o):
                self.assertEqual(l, l2)
            index.close()

        for f in index_files:
            os.unlink(f)

    def test_distinct_values(self):
        """
        Test if the distinct values function works correctly on an index.
        """
        self.create_indexes()
        k = 0
        for c in self._columns:
            distinct_values = {}
            for j in range(self._database.get_num_rows()):
                r = self._database.get_row(j)
                v = r[c.name]
                if v not in distinct_values:
                    distinct_values[v] = 0
                distinct_values[v] += 1
            index = self._indexes[1][k]
            index.open()
            u = sorted(distinct_values.keys())
            dvi = _wormtable.DistinctValueIterator(self._database, index)
            v = list(t[0] for t in dvi)
            self.assertEqual(u, v)
            for key, count in distinct_values.items():
                nr = index.get_num_rows((key,)) 
                self.assertEqual(count, nr)
            index.close()
            k += 1

        self.destroy_indexes()
        

    def test_interface_integrity(self):
        """
        Tests the methods on the index to make sure they react correctly 
        when passed different types of arguments.
        """
        self.create_indexes()
        index = self._indexes[1][0] 
        self.assertRaises(TypeError, index.open, "string") 
        self.assertRaises(TypeError, index.get_num_rows) 
        self.assertRaises(TypeError, index.get_num_rows, "string") 
        self.assertRaises(TypeError, index.get_num_rows, 0) 
        # try to do stuff before the index is opened. 
        self.assertRaises(_wormtable.BerkeleyDatabaseError, 
                index.get_num_rows, tuple()) 
        self.assertRaises(_wormtable.BerkeleyDatabaseError, 
                index.get_max, tuple()) 
        self.assertRaises(_wormtable.BerkeleyDatabaseError, 
                index.get_min, tuple()) 
        # check bin widths        
        self.assertRaises(TypeError, index.set_bin_widths) 
        self.assertRaises(TypeError, index.set_bin_widths, "str") 
        self.assertRaises(TypeError, index.set_bin_widths, b"str") 
        self.assertRaises(TypeError, index.set_bin_widths, [b"str"]) 

        self.destroy_indexes()
        


class TestDatabaseFloatIndexIntegrity(TestDatabaseFloat, TestIndexIntegrity):
    """
    Test the integrity of float indexes.
    """

class TestDatabaseIntegerIndexIntegrity(TestDatabaseInteger, TestIndexIntegrity):
    """
    Test the integrity of integer indexes.
    """
 
class TestDatabaseCharIndexIntegrity(TestDatabaseChar, TestIndexIntegrity):
    """
    Test the integrity of char indexes.
    """

  


class TestMissingValues(object):
    def test_missing_values(self):
        self._row_buffer.commit_row()
        self.open_reading()
        r = self._database.get_row(0)
        for c in self._columns:
            if c.num_elements == _wormtable.NUM_ELEMENTS_VARIABLE:
                self.assertEqual(r[c.name], tuple())
            elif c.num_elements < 2:
                self.assertEqual(r[c.name], None)
            else:
                v = [None for j in range(c.num_elements)]
                self.assertEqual(tuple(v), r[c.name])


class TestIntegerMissingValues(TestMissingValues, TestDatabaseInteger):
    pass

class TestFloatMissingValues(TestMissingValues, TestDatabaseFloat):
    pass
    
class TestCharMissingValues(TestMissingValues, TestDatabaseChar):
   
   
    def test_missing_values(self):
        self._row_buffer.commit_row()
        self.open_reading()
        r = self._database.get_row(0)
        for c in self._columns:
            self.assertEqual(b"", r[c.name])






