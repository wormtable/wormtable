# 
# Copyright (C) 2013, wormtable developers (see AUTHORS.txt).
#
# This file is part of wormtable.
# 
# Wormtable is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# Wormtable is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public License
# along with wormtable.  If not, see <http://www.gnu.org/licenses/>.
#

from __future__ import print_function
from __future__ import division 

import os
import tempfile
import unittest
import random
import string

import _wormtable

from _wormtable import WT_READ 
from _wormtable import WT_WRITE
from _wormtable import MAX_ROW_SIZE 
from _wormtable import WT_VAR_1 as WT_VARIABLE 
from _wormtable import WormtableError 

__column_id = 0

TEMPFILE_PREFIX = "wtll_"

# module variables used to control the number of tests that we do.
num_random_test_rows = 10


def get_uint_column(element_size, num_elements=_wormtable.WT_VAR_1):
    """
    Returns an unsigned integer column with the specified element size and 
    number of elements.
    """
    global __column_id
    __column_id += 1
    name = "uint_{0}_{1}_{2}".format(element_size, num_elements, __column_id)
    c = _wormtable.Column(name.encode(), b"", _wormtable.WT_UINT, 
            element_size, num_elements)
    return c

def get_int_column(element_size, num_elements=_wormtable.WT_VAR_1):
    """
    Returns an integer column with the specified element size and 
    number of elements.
    """
    global __column_id
    __column_id += 1
    name = "int_{0}_{1}_{2}".format(element_size, num_elements, __column_id)
    return _wormtable.Column(name.encode(), b"", _wormtable.WT_INT, 
            element_size, num_elements)

def get_float_column(element_size, num_elements):
    """
    Returns a float column with the specified element size and 
    number of elements.
    """
    global __column_id
    __column_id += 1
    name = "float_{0}_{1}_{2}".format(element_size, num_elements, __column_id)
    return _wormtable.Column(name.encode(), b"", _wormtable.WT_FLOAT, 
            element_size, num_elements)

def get_char_column(num_elements):
    """
    Returns a char column with the specified number of elements.
    """
    global __column_id
    __column_id += 1
    name = "char_{0}_{1}".format(num_elements, __column_id)
    return _wormtable.Column(name.encode(), b"", _wormtable.WT_CHAR, 1, 
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
    s = ''.join(random.choice(string.ascii_letters) for x in range(n)) 
    return s

class TestDatabase(unittest.TestCase):
    """
    Superclass of all database tests. Takes care of allocating a database and
    clearing it up afterwards. Subclasses must define the get_columns 
    method.
    """
    def setUp(self):
        fd, self._db_file = tempfile.mkstemp("-test.db", prefix=TEMPFILE_PREFIX) 
        os.close(fd)
        self._data_file = self._db_file + ".dat"
        self._key_size = 4
        self._columns = [get_uint_column(self._key_size, 1)] + self.get_columns()
        self._database = _wormtable.Table(self._db_file.encode(), 
                self._data_file.encode(), self._columns, cache_size=1024)
        self._database.open(WT_WRITE)
        self._row_buffer = self._database 
        self.num_random_test_rows = num_random_test_rows 
        self.num_columns = len(self._columns) 
        self.rows = []

    def open_reading(self):
        """
        Flushes any records and opens the database for reading.
        """
        self._database.close()
        self._database.open(WT_READ)

    def tearDown(self):
        self._database.close()
        os.unlink(self._db_file)
        os.unlink(self._data_file)
        self.rows = None

    def assertRowListsEqual(self, l1, l2):
        """
        Verifies that the two specified lists of rows are equal.
        """
        self.assertEqual(l1, l2)

    def verify_table(self):
        """
        Verifies that the table is equal to the rows stored locally.
        """
        self.assertEqual(self._database.get_num_rows(), len(self.rows))
        for j in range(self._database.get_num_rows()):
            rd = self._database.get_row(j)
            rl = self.rows[j]
            self.assertEqual(rd, rl)


class TestEmptyDatabase(TestDatabase):
    """
    Tests to see if an empty database is correctly handled.
    """
    
    def get_columns(self):
        return [get_uint_column(1, 1)]

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
        self._float_columns = {2: get_float_column(2, 1), 4: get_float_column(4, 1),
                8: get_float_column(8, 1)}
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
    
    def test_good_float_values(self):
        rb = self._row_buffer
        values = ["-1", "-2", "0", "4", "14", "100",
            "0.01", "-5.224234345235", "1E12", "Inf", "NaN"]
        for c in self._float_columns.values():
            for v in values:
                self.assertEqual(rb.insert_encoded_elements(c.position, v.encode()), None)
                self.assertEqual(rb.insert_elements(c.position, float(v)), None)
            for k in range(10):
                v = random.uniform(-100, 100)
                b = str(v).encode()
                self.assertEqual(rb.insert_encoded_elements(c.position, b), None)
                self.assertEqual(rb.insert_elements(c.position, v), None)
     
    def test_bad_float_values(self):
        rb = self._row_buffer
        values = ["", "--1", "sdasd", "[]", "3qsd", "1Q0.023"]
        for c in self._float_columns.values():
            for v in values:
                e = v.encode()
                self.assertRaises(ValueError, rb.insert_encoded_elements, c.position, e)
        values = [[], {}, "", b"", ValueError]
        for c in self._float_columns.values():
            for v in values:
                self.assertRaises(TypeError, rb.insert_elements, c.position, v)
        

class TestListParsers(TestDatabase):
    """
    Test the list parsers to make sure that malformed lists are correctly
    detected.
    """     
    def get_columns(self): 
        self._uint_columns = {}
        self._int_columns = {}
        self._float_columns = {}
        k = 1
        cols = []
        for j in range(1, 5):
            self._uint_columns[j] = k
            cols.append(get_uint_column(1, j))
            k += 1
            self._int_columns[j] = k
            cols.append(get_int_column(1, j))
            k += 1
            self._float_columns[j] = k
            cols.append(get_float_column(4, j))
            k += 1
        self._variable_cols = [k]
        cols.append(get_int_column(1, _wormtable.WT_VAR_1))
        k += 1
        self._variable_cols.append(k)
        cols.append(get_float_column(4, _wormtable.WT_VAR_1))
        return cols
     
    def test_malformed_python_lists(self):
        rb = self._row_buffer
        i2 = self._int_columns[2]
        f2 = self._int_columns[2]
        for s in [[1], [1, 2, 3], (1, 2, 3), range(40)]:
            self.assertRaises(ValueError, rb.insert_elements, f2, s)
            self.assertRaises(ValueError, rb.insert_elements, i2, s)
    
    def test_long_lists(self):
        rb = self._row_buffer
        i2 = self._int_columns[2]
        f2 = self._float_columns[2]
        for j in [0, 1, 3, 4, 50]:
            s = [0 for k in range(j)]
            self.assertRaises(ValueError, rb.insert_elements, f2, s)
            self.assertRaises(ValueError, rb.insert_elements, i2, s)
            ss = ",".join([str(u) for u in s])
            sse = ss.encode()
            self.assertRaises(ValueError, rb.insert_encoded_elements, f2, sse)
            self.assertRaises(ValueError, rb.insert_encoded_elements, i2, sse)
        for k in self._variable_cols:
            for l in range(1, 50):
                s = [0 for j in range(_wormtable.MAX_NUM_ELEMENTS + l)]
                self.assertRaises(ValueError, rb.insert_elements, k, s)
                ss = ",".join([str(u) for u in s])
                sse = ss.encode()
                self.assertRaises(ValueError, rb.insert_encoded_elements, k, sse)
    

class TestDatabaseLimits(TestDatabase):
    """
    Tests the limits of various aspects of the database to see if errors
    are flagged correctly.
    """ 
    def get_columns(self):
        var_1_overhead = 3
        n = (_wormtable.MAX_ROW_SIZE - self._key_size) // (
                _wormtable.MAX_NUM_ELEMENTS * 8 + var_1_overhead)
                
        columns = [get_int_column(8) for j in range(n)]
        return columns
    
    def test_column_overflow(self):
        """
        If we set the maximum number of elements to n - 1 columns, 
        it should overflow whatever we do.
        """
        rb = self._row_buffer
        v = [j for j in range(_wormtable.MAX_NUM_ELEMENTS)]
        n = len(self._columns)
        for k in range(1, n):
            rb.insert_elements(k, v) 
        self.assertRaises(ValueError, rb.insert_elements, 1, v)  
        self.assertRaises(ValueError, rb.insert_elements, 2, v)
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
        q = _wormtable.WT_VAR_1
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
        self.num_rows = 2
        self.rows = []
        for j in range(self.num_rows):
            row = [None for c in self._columns]
            row[0] = j
            for k in range(1, num_cols): 
                c = cols[k]
                min_v, max_v = c.min_element, c.max_element
                v = min_v
                if j == 1:
                    v = max_v
                if c.num_elements == 1:
                    row[k] = v 
                else:
                    n = c.num_elements
                    if n == _wormtable.WT_VAR_1:
                        n = random.randint(1, _wormtable.MAX_NUM_ELEMENTS)
                    v = tuple([v for l in range(n)])
                    row[k] = v
                if j % 2 == 0:
                    rb.insert_elements(k, row[k]) 
                else:
                    if c.num_elements == 1:
                        s = str(row[k])
                    else:
                        s = ",".join(str(v) for v in row[k])
                    rb.insert_encoded_elements(k, s.encode())
            rb.commit_row()
            self.rows.append(tuple(row))

    def populate_randomly(self):
        """
        Generates random values for the columns and inserts them
        into database. Store these as lists in the instance variable
        self.rows.
        """
        num_rows = self.num_random_test_rows
        rb = self._row_buffer
        db = self._database
        cols = self._columns
        num_cols = len(cols)
        self.num_rows = num_rows
        self.rows = []
        for j in range(num_rows):
            row = [None for c in self._columns]
            row[0] = j
            for k in range(1, num_cols): 
                c = cols[k]
                min_v, max_v = c.min_element, c.max_element
                if c.num_elements == 1:
                    row[k] = random.randint(min_v, max_v) 
                else:
                    n = c.num_elements
                    if n == _wormtable.WT_VAR_1:
                        n = random.randint(1, _wormtable.MAX_NUM_ELEMENTS)
                    v = tuple([random.randint(min_v, max_v) for l in range(n)])
                    row[k] = v
                if j % 2 == 1:
                    rb.insert_elements(k, row[k])
                else:
                    if c.num_elements == 1:
                        s = str(row[k])
                    else:
                        s = ",".join(str(v) for v in row[k])
                    rb.insert_encoded_elements(k, s.encode())
            rb.commit_row()
            self.rows.append(tuple(row))



class TestDatabaseIntegerLimits(TestDatabaseInteger):
    """
    Tests the limits of integer columns to see if they are dealt with 
    correctly.
    """
    def insert_bad_value(self, column, value):
        rb = self._row_buffer
        v = value
        if column.num_elements == _wormtable.WT_VAR_1:
            v = [value]
        elif column.num_elements != 1:
            v = [value for j in range(column.num_elements)]
        index = self._columns.index(column)
        def f():
            rb.insert_elements(index, v)
        self.assertRaises((OverflowError, TypeError), f)

    def insert_good_value(self, column, value):
        rb = self._row_buffer
        v = value
        if column.num_elements == _wormtable.WT_VAR_1:
            v = [value]
        elif column.num_elements != 1:
            v = [value for j in range(column.num_elements)]
        index = self._columns.index(column)
        self.assertEqual(rb.insert_elements(index, v), None)


    def test_outside_range(self):
        for c in self._columns[1:]:
            min_v, max_v = c.min_element, c.max_element
            for j in range(1, 5):
                v = min_v - j
                self.insert_bad_value(c, v) 
                v = max_v + j
                self.insert_bad_value(c, v) 

    def test_inside_range(self):
        for c in self._columns[1:]:
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
        values = range(1, 40)
        for v in values:
            for k in range(1, self.num_columns): 
                if self._columns[k].num_elements == 1:
                    rb.insert_elements(k, v)
            rb.commit_row()
        self.open_reading()
        self.assertEqual(db.get_num_rows(), len(values))
        j = 0
        for v in values:
            r = self._database.get_row(j)
            for k in range(1, self.num_columns): 
                if self._columns[k].num_elements == 1:
                    self.assertEqual(v, r[k])
            j += 1
        j = 0
        cols = list(range(self.num_columns))
        ri = _wormtable.TableRowIterator(self._database, cols)
        for r in ri:
            self.assertEqual(r, self._database.get_row(j))
            j += 1 


    def test_boundary_int_retrieval(self):
        self.populate_boundary_values()
        self.open_reading()
        for j in range(self.num_rows):
            r = self._database.get_row(j)
            self.assertEqual(r, self.rows[j])
        j = 0
        cols = list(range(self.num_columns))
        ri = _wormtable.TableRowIterator(self._database, cols)
        for r in ri:
            self.assertEqual(r, self._database.get_row(j))
            j += 1 

    def test_random_int_retrieval(self):
        self.populate_randomly()
        self.open_reading()
        self.assertEqual(self._database.get_num_rows(), self.num_rows)
        for j in range(self.num_rows): 
            r = self._database.get_row(j)
            self.assertEqual(self.rows[j], r)
        j = 0
        cols = list(range(self.num_columns))
        ri = _wormtable.TableRowIterator(self._database, cols)
        for r in ri:
            self.assertEqual(r, self._database.get_row(j))
            j += 1 
    
    def test_row_iterator_ranges(self):
        self.populate_randomly()
        self.open_reading()
        n = self.num_rows
        cols = list(range(self.num_columns))
        for j in range(10):
            l = [random.randint(0, n - 1), random.randint(0, n - 1)]
            bottom = min(l)
            top = max(l)
            ri = _wormtable.TableRowIterator(self._database, cols)
            ri.set_min(top)
            ri.set_max(bottom)
            self.assertEqual([], [r for r in ri])
            ri.set_min(bottom)
            self.assertEqual([], [r for r in ri])
            ri.set_min(bottom)
            ri.set_max(top)
            j = bottom
            for r in ri:
                self.assertEqual(r, self._database.get_row(j))
                j += 1
            self.assertEqual(j, top)
            ri.set_min(0)
            j = 0
            for r in ri:
                self.assertEqual(r, self._database.get_row(j))
                j += 1
            self.assertEqual(j, top)
            ri.set_min(top)
            ri.set_max(n + 1)
            j = top 
            for r in ri:
                self.assertEqual(r, self._database.get_row(j))
                j += 1
            self.assertEqual(j, n)
            
    def test_row_iterator_columns(self):
        self.populate_randomly()
        self.open_reading()
        n = self.num_rows
        cols = list(range(self.num_columns))
        ri = _wormtable.TableRowIterator(self._database, cols)
        j = 0
        for r1 in ri:
            r2 = self._database.get_row(j)
            self.assertEqual(r1, r2) 
            j += 1
        ri = _wormtable.TableRowIterator(self._database, list(reversed(cols)))
        j = 0
        for r1 in ri:
            r2 = tuple(reversed(self._database.get_row(j)))
            self.assertEqual(r1, r2) 
            j += 1
        # Get a subset of the columns.
        for k in range(1, 10):
            c = [random.randint(0, self.num_columns - 1) for j in range(k)]
            ri = _wormtable.TableRowIterator(self._database, c)
            j = 0
            for r1 in ri:
                r = self._database.get_row(j)
                r2 = tuple(r[q] for q in c)
                self.assertEqual(r1, r2) 
                j += 1



class TestDatabaseFloat(TestDatabase):
    """
    Tests the integrity of the database by inserting values and testing 
    to ensure they are retreived correctly.
    """ 
    def get_columns(self):
        q = _wormtable.WT_VAR_1
        columns = [
                get_float_column(2, q), get_float_column(4, q), get_float_column(8, q), 
                get_float_column(2, 1), get_float_column(4, 1), get_float_column(8, 1), 
                get_float_column(2, 2), get_float_column(4, 2), get_float_column(8, 2), 
                get_float_column(2, 3), get_float_column(4, 3), get_float_column(8, 3), 
                get_float_column(2, 4), get_float_column(4, 4), get_float_column(8, 4), 
                get_float_column(2, 5), get_float_column(4, 5), get_float_column(8, 5), 
                ]
        # randomise the columns so we don't have all the variable 
        # columns at the start.
        random.shuffle(columns)
        return columns

    def populate_randomly(self):
        """
        Generates random values for the columns and inserts them
        into database. Store these as lists in the instance variable
        self.rows.
        """
        num_rows = self.num_random_test_rows
        rb = self._row_buffer
        db = self._database
        cols = self._columns
        self.num_rows = num_rows
        self.rows = []
        # either generate random ints or exactly representable fractions
        # to avoid rounding issues with 2 and 4 byte floats
        def generate_int():
            min_v, max_v = -20, 20 
            return random.randint(min_v, max_v) 
        def generate_fraction():
            denoms = [2**j for j in range(2, 10)]
            denoms += [-v for v in denoms]
            return 1.0 / denoms[random.randint(0, len(denoms) - 1)]
        for j in range(self.num_rows):
            row = [0 for c in cols]
            for k in range(1, self.num_columns): 
                c = cols[k]
                f = generate_int 
                if random.random() < 0.5:
                    f = generate_fraction 
                if c.num_elements == 1:
                    row[k] = f() 
                else:
                    n = c.num_elements
                    if n == _wormtable.WT_VAR_1:
                        n = random.randint(1, _wormtable.MAX_NUM_ELEMENTS)
                    row[k] = tuple([f() for l in range(n)])
                if j % 2 == 0:
                    rb.insert_elements(k, row[k]) 
                else:
                    if c.num_elements == 1:
                        s = str(row[k])
                    else:
                        s = ",".join(str(x) for x in row[k])
                    rb.insert_encoded_elements(k, s.encode())
            rb.commit_row()
            self.rows.append(tuple(row))

class TestDatabaseFloatIntegrity(TestDatabaseFloat):

    def test_random_float_retrieval(self):
        cols = self._columns
        self.populate_randomly()
        self.open_reading()
        self.assertEqual(self._database.get_num_rows(), self.num_rows)
        for j in range(self.num_rows):
            r = self._database.get_row(j)
            self.assertEqual(j, r[0])
            for k in range(1, self.num_columns): 
                c = cols[k]
                if c.element_size == 8:
                    self.assertEqual(self.rows[j][k], r[k])
                else:
                    #print(rows[j][k])
                    p = 2 if c.element_size == 2 else 6
                    if c.num_elements == 1:
                        self.assertAlmostEqual(self.rows[j][k], r[k], places=p)
                    else:
                        for u, v in zip(self.rows[j][k], r[k]):
                            self.assertAlmostEqual(u, v, places=p)



class TestDatabaseChar(TestDatabase):
    """
    Tests the integrity of the database by inserting values and testing 
    to ensure they are retreived correctly.
    """ 
    def get_columns(self):
        columns = [get_char_column(j) for j in range(1, 20)]
        columns.append(get_char_column(_wormtable.WT_VAR_1))
        columns.append(get_char_column(_wormtable.WT_VAR_1))
        columns.append(get_char_column(_wormtable.WT_VAR_1))
        random.shuffle(columns)
        return columns
    
    def populate_randomly(self):
        """
        Generates random values for the columns and inserts them
        into database. Store these as lists in the instance variable
        self.rows.
        """
        rb = self._row_buffer
        db = self._database
        self.rows = []
        self.num_rows = self.num_random_test_rows
        for j in range(self.num_random_test_rows):
            row = [0 for i in range(self.num_columns)]
            row[0] = j
            for k in range(1, self.num_columns): 
                c = self._columns[k]
                n = c.num_elements
                if n == _wormtable.WT_VAR_1:
                    n = random.randint(0, _wormtable.MAX_NUM_ELEMENTS)
                row[k] = random_string(n).encode() 
                if j % 2 == 0:
                    rb.insert_elements(k, row[k]) 
                else:
                    rb.insert_encoded_elements(k, row[k]) 
            self.rows.append(tuple(row))
            rb.commit_row()

class TestDatabaseCharIntegrity(TestDatabaseChar):
    
    def test_illegal_length_strings(self):
        """
        Test to ensure that long and short strings are trapped correctly.
        """
        rb = self._row_buffer
        for j in range(1, len(self._columns)):
            c = self._columns[j]
            n = c.num_elements
            if n == _wormtable.WT_VAR_1:
                n = _wormtable.MAX_NUM_ELEMENTS
                for k in [1, 2, 3, 10, 500, 1000]:
                    s = random_string(n + k).encode() 
                    self.assertRaises(ValueError, rb.insert_elements, j, s)
                    self.assertRaises(ValueError, rb.insert_encoded_elements, j, s)
            else:
                for k in [0, n - 1, n + 1, n + 2, n + 100]:
                    s = b"x" * k
                    self.assertRaises(ValueError, rb.insert_elements, j, s)
                    self.assertRaises(ValueError, rb.insert_encoded_elements, j, s)
                

    def test_variable_char_retrieval(self):
        rb = self._row_buffer
        db = self._database
        cols = []
        for j in range(1, len(self._columns)):
            if self._columns[j].num_elements == WT_VARIABLE:
                cols.append(j)
        num_rows = self.num_random_test_rows 
        rows = []
        for j in range(num_rows):
            row = [None for c in self._columns]
            row[0] = j
            for k in cols: 
                row[k] = random_string(min(j, 50)).encode() 
                if j % 2 == 0:
                    rb.insert_elements(k, row[k]) 
                else:
                    rb.insert_encoded_elements(k, row[k])
            rb.commit_row()
            rows.append(tuple(row))
        self.open_reading()
        self.assertEqual(db.get_num_rows(), num_rows)
        for j in range(num_rows):
            r = db.get_row(j)
            for k in cols: 
                self.assertEqual(rows[j][k], r[k])
    
    def test_fixed_char_retrieval(self):
        rb = self._row_buffer
        db = self._database
        cols = []
        for j in range(1, len(self._columns)):
            if self._columns[j].num_elements != WT_VARIABLE:
                cols.append(j)
        num_cols = len(cols)
        num_rows = self.num_random_test_rows 
        rows = []
        for j in range(num_rows):
            row = [None for c in self._columns]
            row[0] = j
            for k in cols: 
                c = self._columns[k] 
                n = random.randint(0, c.num_elements)
                row[k] = random_string(c.num_elements).encode() 
                if j % 2 == 0:
                    rb.insert_elements(k, row[k]) 
                else:
                    rb.insert_encoded_elements(k, row[k]) 
            rb.commit_row()
            rows.append(tuple(row))
        self.open_reading()
        self.assertEqual(db.get_num_rows(), num_rows)
        for j in range(num_rows):
            r = db.get_row(j)
            for k in cols: 
                c = self._columns[k]
                self.assertEqual(rows[j][k], r[k])
    
    def test_random_char_retrieval(self):
        self.populate_randomly()
        self.open_reading()
        cols = self._columns
        num_cols = len(cols)
        self.assertEqual(self._database.get_num_rows(), self.num_rows)
        for j in range(self.num_rows):
            r = self._database.get_row(j)
            self.assertEqual(self.rows[j], r)
    

class TestIndexIntegrity(object):
    """
    Tests the integrity of indexes. Concrete test should subclass this and one of the 
    Test classes above to get an implementation of populate_randomly.
    """
    def create_indexes(self):
        """
        Create some indexes.
        """
        self.populate_randomly()
        self.open_reading()
        cache_size = 64 * 1024
        self._indexes = [None]
        self._index_files = []
        # make the single column indexes
        for j in range(1, len(self._columns)):
            fd, index_file = tempfile.mkstemp("-index-test.db", prefix=TEMPFILE_PREFIX)  
            index = _wormtable.Index(self._database, index_file.encode(), [j], 
                    cache_size)
            os.close(fd)
            index.open(WT_WRITE)
            index.build()
            index.close()
            self._index_files.append(index_file)
            self._indexes.append(index)

    def destroy_indexes(self):
        """
        Delete the index files.
        """
        for f in self._index_files:
            os.unlink(f)
        self._indexes = None
    
    def test_column_sort_order(self):
         self.create_indexes()
         for j in range(1, len(self._columns)):
            col = self._columns[j]
            index = self._indexes[j]
            index.open(WT_READ)
            row_iter = _wormtable.IndexRowIterator(index, [j])
            l = [row[0] for row in row_iter]
            l2 = sorted(l)
            self.assertEqual(l, l2)
            # get the list from the original rows
            l3 = [row[j] for row in self.rows]
            l3.sort()
            self.assertEqual(l, l3)
            index.close()
            del row_iter
         self.destroy_indexes()

    def test_column_min_max(self):
        self.create_indexes()
        #self.verify_table()
        for j in range(1, len(self._columns)):
            col = self._columns[j]
            index = self._indexes[j]
            index.open(WT_READ)
            original = [row[j] for row in self.rows] 
            s = random.sample(original, 2)
            min_val = min(s),
            max_val = max(s),
            row_iter = _wormtable.IndexRowIterator(index, [j])
            row_iter.set_min(max_val)
            row_iter.set_max(min_val)
            l = [row[0] for row in row_iter]
            self.assertEqual(len(l), 0)
            # Check if the correct lists are returned. 
            row_iter = _wormtable.IndexRowIterator(index, [j])
            row_iter.set_min(min_val)
            row_iter.set_max(max_val)
            l = [row[0] for row in row_iter]
            l2 = sorted([v for v in original if min_val[0] <= v and v < max_val[0]])
            ri2 = _wormtable.IndexRowIterator(index, [j])
            l3 = [row[0] for row in ri2 if min_val[0] <= row[0] and row[0] < max_val[0]]
            self.assertRowListsEqual(l3, l2)
            self.assertRowListsEqual(l, l2)
            self.assertEqual(l, l3)
            self.assertEqual(l2, l3)
            min_value = index.get_min(tuple()) 
            self.assertEqual(min(original), min_value[0]) 
            max_value = index.get_max(tuple()) 
            self.assertEqual(max(original), max_value[0]) 
            index.close()
        self.destroy_indexes()

    def test_two_column_sort_order(self):
        num_rows = self.num_random_test_rows 
        self.populate_randomly()
        self.open_reading()
        cache_size = 64 * 1024
        indexes = []
        index_files = []
        original_values = []
        col_positions = []
        n = len(self._columns)
        pairs = [[j, k] for j in range(1, n) for k in range(j + 1, n)]
        max_pairs = 100
        if len(pairs) > max_pairs:
            pairs = random.sample(pairs, max_pairs)
        for j, k in pairs:
            c1 = self._columns[j]
            c2 = self._columns[k]
            fd, index_file = tempfile.mkstemp("-index-test.db", prefix=TEMPFILE_PREFIX)  
            index = _wormtable.Index(self._database, index_file.encode(), [j, k], 
                    cache_size)
            os.close(fd)
            index.open(WT_WRITE)
            index.build()
            index.close()
            index_files.append(index_file)
            indexes.append(index)
            col_positions.append([j, k])
            original_values.append([(row[j], row[k]) for row in self.rows])

        for index, original, cols in zip(indexes, original_values, col_positions):
            index.open(WT_READ)
            columns = [self._columns[j] for j in cols]
            c1 = columns[0]
            c2 = columns[1]
            row_iter = _wormtable.IndexRowIterator(index, cols)
            l = [row for row in row_iter]
            l2 = sorted(original) 
            self.assertEqual(l, l2)
            index.close()

        for f in index_files:
            os.unlink(f)

    def test_distinct_values(self):
        """
        Test if the distinct values function works correctly on an index.
        """
        self.create_indexes()
        for k in range(1, len(self._columns)):
            distinct_values = {}
            for j in range(self._database.get_num_rows()):
                r = self._database.get_row(j)
                v = r[k]
                if v not in distinct_values:
                    distinct_values[v] = 0
                distinct_values[v] += 1
            index = self._indexes[k]
            index.open(WT_READ)
            u = sorted(distinct_values.keys())
            dvi = _wormtable.IndexKeyIterator(index)
            v = list(t[0] for t in dvi)
            self.assertEqual(u, v)
            for key, count in distinct_values.items():
                nr = index.get_num_rows((key,)) 
                self.assertEqual(count, nr)
            index.close()

        self.destroy_indexes()
        

    def test_interface_integrity(self):
        """
        Tests the methods on the index to make sure they react correctly 
        when passed different types of arguments.
        """
        self.create_indexes()
        index = self._indexes[1] 
        self.assertRaises(TypeError, index.open, "string") 
        self.assertRaises(TypeError, index.get_num_rows) 
        self.assertRaises(TypeError, index.get_num_rows, "string") 
        self.assertRaises(TypeError, index.get_num_rows, 0) 
        # try to do stuff before the index is opened. 
        self.assertRaises(WormtableError, index.get_num_rows, tuple()) 
        self.assertRaises(WormtableError, index.get_max, tuple()) 
        self.assertRaises(WormtableError, index.get_min, tuple()) 
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

 
class TestMultiColumnIndex(object):
    """
    Tests the integrity of multi column indexes. Concrete test should subclass 
    this and one of the Test classes above to get an implementation of populate_randomly.
    """
    def create_indexes(self):
        """
        Creates a bunch of indexes.
        """
        self.populate_randomly()
        self.open_reading()
        self._indexes = {}  
        self._index_files = []
        # generate n indexes each with a random choice of 1,...,k columns.
        v = []
        for j in range(1, len(self._columns)):
            c = self._columns[j]
            v.append(j)
        max_columns = 5
        max_indexes = 10 
        for j in range(2, max_columns):
            for k in range(max_indexes):
                cols = random.sample(v, j)
                self._indexes[tuple(cols)] = None

        for cols in self._indexes.keys():
            fd, index_file = tempfile.mkstemp("-index-test.db", prefix=TEMPFILE_PREFIX) 
            index = _wormtable.Index(self._database, index_file.encode(), 
                    list(cols), 0)
            os.close(fd)
            index.open(WT_WRITE)
            index.build()
            index.close()
            index.open(WT_READ)
            self._index_files.append(index_file)
            self._indexes[cols] = index
    
    def destroy_indexes(self):
        """
        Delete the index files.
        """
        for f in self._index_files:
            os.unlink(f)
    
    def test_min_max(self):
        self.create_indexes()
        for cols, index in self._indexes.items():
            rows = [tuple(row[j] for j in cols) for row in self.rows]
            self.assertEqual(min(rows), index.get_min(tuple()))
            self.assertEqual(max(rows), index.get_max(tuple()))
            # pick a row at random with a given prefix and find the min 
            # and max with this prefix
            prefix_row = random.choice(rows)
            #print("row = ", prefix_row)
            ri = _wormtable.IndexRowIterator(index, list(cols))
            l1 = [r for r in ri]
            l2 = sorted(rows)
            self.assertEqual(l1, l2)
            for k in range(1, len(cols) + 1):
                prefix = prefix_row[:k]
                m = index.get_min(prefix)
                # find the first partial match in the sorted list
                j = 0
                while l2[j][:k] < prefix:
                    j += 1
                self.assertEqual(l2[j], m)
                j = len(l2) - 1 
                while l2[j][:k] > prefix:
                    j -= 1
                self.assertEqual(l2[j], index.get_max(prefix))
        self.destroy_indexes()

class TestDatabaseIntegerMultiColumnIndex(TestDatabaseInteger, TestMultiColumnIndex):
    """
    Test multicolumn integer indexes 
    """
class TestDatabaseFloatMultiColumnIndex(TestDatabaseInteger, TestMultiColumnIndex):
    """
    Test multicolumn integer indexes 
    """
class TestDatabaseCharMultiColumnIndex(TestDatabaseChar, TestMultiColumnIndex):
    """
    Test multicolumn integer indexes 
    """

class TestMissingValues(object):
    """
    Test that missing and empty values are correctly handled.
    """
    def test_missing_values(self):
        n = 10
        for j in range(n):
            # Insert empty row 
            self._row_buffer.commit_row()
            # Insert Empty values
            for k in range(1, len(self._columns)):
                self._row_buffer.insert_elements(k, None)
            self._row_buffer.commit_row()
            # Insert alternating randomly
            for k in range(1, len(self._columns)):
                if random.random() < 0.5:
                    self._row_buffer.insert_elements(k, None)
            self._row_buffer.commit_row()
        self.open_reading()
        for j in range(3 * n):
            r = self._database.get_row(j)
            for k in range(1, len(self._columns)):
                c = self._columns[k]
                self.assertEqual(None, r[k])
                    
    def get_empty_value(self):
        return tuple() 

    def test_empty_values(self):
        """
        Variable length columns support empty values. These are not the same 
        as missing values!
        """
        ev = self.get_empty_value()
        n = 10
        for j in range(n):
            for k, c in enumerate(self._columns):
                if c.num_elements == _wormtable.WT_VAR_1:
                    self._row_buffer.insert_elements(k, ev)
            self._row_buffer.commit_row()
        self.open_reading()
        for j in range(n):
            r = self._database.get_row(j)
            for k, c in enumerate(self._columns):
                v = None if k > 0 else j 
                if c.num_elements == _wormtable.WT_VAR_1:
                    v = ev
                self.assertEqual(r[k], v)



class TestIntegerMissingValues(TestMissingValues, TestDatabaseInteger):
    pass

class TestFloatMissingValues(TestMissingValues, TestDatabaseFloat):
    pass
    
class TestCharMissingValues(TestMissingValues, TestDatabaseChar):
    
    def get_empty_value(self):
        return b""
   
class TestTable(unittest.TestCase):
    """
    Base class for testing tables.
    """
    def setUp(self):
        fd, self._db_file = tempfile.mkstemp("-test.db", prefix=TEMPFILE_PREFIX) 
        os.close(fd)
        fd, self._data_file = tempfile.mkstemp("-dat.db", prefix=TEMPFILE_PREFIX) 
        os.close(fd)
    
    def tearDown(self):
        os.unlink(self._db_file)
        os.unlink(self._data_file)


class TestTableInitialisation(TestTable):
    """ 
    Tests the initialisation code for the Table class to make sure everything 
    is checked correctly.
    """
    def test_columns(self):
        t = _wormtable.Table
        c0 = get_uint_column(1, 1)
        c1 = get_uint_column(1, 1)
        f1 = b"file.db"
        f2 = b"file.dat"
        # check number of columns.
        self.assertRaises(ValueError, t, f1, f2, [], 0) 
        self.assertRaises(ValueError, t, f1, f2, [c0], 0) 
        # Make sure the row_id column is of the right type.
        for j in range(2, 5):
            self.assertRaises(ValueError, t, f1, f2, [get_uint_column(1, j), c0], 0) 
        for j in range(5):
            self.assertRaises(ValueError, t, f1, f2, [get_int_column(1, j), c0], 0) 
            self.assertRaises(ValueError, t, f1, f2, [get_float_column(4, j), c0], 0) 
            self.assertRaises(ValueError, t, f1, f2, [get_char_column(j), c0], 0) 
        # check for duplicate columns
        n = 10
        self.assertRaises(ValueError, t, f1, f2, [c0, c0], 0) 
        cols = [c1, c1] + [get_int_column(1, 1) for j in range(n)]
        self.assertRaises(ValueError, t, f1, f2, [c0] + cols, 0) 
        for j in range(n):
            random.shuffle(cols) 
            self.assertRaises(ValueError, t, f1, f2, [c0] + cols, 0) 
        def g(j):
            s = "c_{0}".format(j)
            return s.encode()
        # Try duplicate names
        cols = [_wormtable.Column(g(j), b"", _wormtable.WT_UINT, 1, 1)
                for j in range(n)]
        for j in range(n):
            c = _wormtable.Column(g(j), b"", _wormtable.WT_UINT, 1, 1) 
            cp = cols + [c]
            random.shuffle(cp)
            self.assertRaises(ValueError, t, f1, f2, cp, 0) 


    def test_parameters(self):
        # make sure the right types are accepted for the 
        t = _wormtable.Table
        c0 = get_uint_column(1, 1)
        c1 = get_uint_column(1, 1)
        self.assertRaises(TypeError, t, None, None, None) 
        self.assertRaises(TypeError, t, [], [], 0) 
        self.assertRaises(TypeError, t, b"", {}, 0) 
        f1 = b"file.db"
        self.assertRaises(TypeError, t, f1, f1, [None, None], 0) 
        self.assertRaises(TypeError, t, f1, f1, [None, c0], 0) 
        self.assertRaises(TypeError, t, f1, f1, [c1, ""], 0) 
        self.assertRaises(TypeError, t, [], f1, [c0, c1], 0) 
        self.assertRaises(TypeError, t, TypeError, [], [c0, c1], 0) 
    

    def test_column_limits(self):
        """
        See if we correctly catch an impossibly large column spec.
        """

        f1 = self._db_file.encode()
        f2 = self._data_file.encode()
        cols = [get_uint_column(8, 1) for k in range(MAX_ROW_SIZE // 8)]
        # This should be fine
        t = _wormtable.Table(f1, f2, cols, 0)
        cols += [get_uint_column(8, 1)]
        self.assertRaises(WormtableError, _wormtable.Table, f1, f2, cols, 0)

    def test_open(self):
        c0 = get_uint_column(1, 1)
        c1 = get_uint_column(1, 1)
        f1 = self._db_file.encode()
        f2 = self._data_file.encode()
        cache_size = 64 * 1024
        t = _wormtable.Table(f1, f2, [c0, c1], cache_size) 
        self.assertEqual(f1, t.db_filename)
        self.assertEqual(f2, t.data_filename)
        self.assertEqual(cache_size, t.cache_size)
        self.assertEqual(2, t.fixed_region_size)
        # Try bad mode values.
        for j in range(-10, 10):
            if j not in [WT_READ, WT_WRITE]:
                self.assertRaises(ValueError, t.open, j) 
        # Try to open table WT_READ that does not exist.
        self.assertRaises(_wormtable.WormtableError, t.open, WT_READ) 
        t.open(WT_WRITE)
        # Try to open an open table
        self.assertRaises(WormtableError, t.open, WT_READ) 
        self.assertRaises(WormtableError, t.open, WT_WRITE) 

    def test_close(self):
        c0 = get_uint_column(1, 1)
        c1 = get_uint_column(1, 1)
        f1 = self._db_file.encode()
        f2 = self._data_file.encode()
        cache_size = 64 * 1024
        t = _wormtable.Table(f1, f2, [c0, c1], cache_size) 
        for j in range(10):
            self.assertRaises(WormtableError, t.close) 
            t.open(WT_WRITE)
            t.close()
            self.assertRaises(WormtableError, t.close) 

    def test_write(self):
        """
        Tests if the correct exceptions are raised when we do silly things.
        """
        c0 = get_uint_column(1, 1)
        c1 = get_uint_column(1, 1)
        c2 = get_uint_column(1, 1)
        f1 = self._db_file.encode()
        f2 = self._data_file.encode()
        t = _wormtable.Table(f1, f2, [c0, c1, c2], 0)
        t.open(WT_WRITE)
        n = 10
        for j in range(n):
            b = b""
            self.assertRaises(WormtableError, t.get_num_rows)
            self.assertRaises(WormtableError, t.get_row, j)
            self.assertRaises(WormtableError, t.insert_elements, 0, j) 
            self.assertRaises(WormtableError, t.insert_encoded_elements, 0, b) 
            self.assertRaises(WormtableError, t.insert_encoded_elements, -j, b) 
            self.assertRaises(WormtableError, t.insert_elements, -j, j) 
            self.assertRaises(WormtableError, t.insert_elements, 3 + j, j) 
            self.assertRaises(WormtableError, 
                    t.insert_encoded_elements, 3 + j, b) 
            t.insert_elements(1, 2 * j)
            s = "{0}".format(3 * j)
            t.insert_encoded_elements(2, s.encode())
            t.commit_row()
        t.close()
        self.assertRaises(WormtableError, t.insert_elements, 1, 0) 
        self.assertRaises(WormtableError, t.commit_row) 
        t.open(WT_READ)
        self.assertRaises(WormtableError, t.insert_elements, 1, 0) 
        self.assertRaises(WormtableError, t.commit_row) 
        self.assertEqual(t.get_num_rows(), n)
        for j in range(n):
            r = t.get_row(j)  
            self.assertEqual(r[0], j)
            self.assertEqual(r[1], 2 * j)
            self.assertEqual(r[2], 3 * j)
            self.assertRaises(WormtableError, t.insert_elements, 1, 0) 
            self.assertRaises(WormtableError, t.commit_row) 
            self.assertEqual(t.get_num_rows(), n)
        t.close()

class TestIndex(unittest.TestCase):
    """
    Base class for testing tables.
    """
    def setUp(self):
        fd, self._table_db_file = tempfile.mkstemp("-test.db", prefix=TEMPFILE_PREFIX) 
        self._table_data_file = self._table_db_file + ".dat"
        self._columns = [get_uint_column(2, 1), get_uint_column(1, 1)]
        self._table = _wormtable.Table(self._table_db_file.encode(), 
                self._table_data_file.encode(), self._columns, 0)
        os.close(fd)
        fd, self._index_db_file = tempfile.mkstemp("-index.db", prefix=TEMPFILE_PREFIX) 
        os.close(fd)
    
    def tearDown(self):
        os.unlink(self._table_db_file)
        os.unlink(self._table_data_file)
        os.unlink(self._index_db_file)
        self._table = None
        self._columns = None

class TestIndexInitialisation(TestIndex):
    """ 
    Tests the initialisation code for the Index class to make sure everything 
    is checked correctly.
    """
    
    def test_constructor(self):
        t = self._table
        f = self._index_db_file.encode()
        g = _wormtable.Index
        # See if a closed table throws an error
        self.assertRaises(WormtableError, g, t, f, [1], 0)
        self._table.open(WT_WRITE);
        self.assertRaises(WormtableError, g, t, f, [1], 0)
        self._table.close()
        self._table.open(WT_READ);
        
        self.assertRaises(TypeError, g) 
        self.assertRaises(TypeError, g, t) 
        self.assertRaises(TypeError, g, t, f, []) 
        self.assertRaises(TypeError, g, t, f, [], None) 
        self.assertRaises(TypeError, g, t, f, None, 0) 
        self.assertRaises(TypeError, g, f, t, None, 0) 
        self.assertRaises(TypeError, g, None, t, [], 0) 
        # Check out of bounds columns
        self.assertRaises(ValueError, g, t, f, [], 0) 
        self.assertRaises(ValueError, g, t, f, [-1], 0) 
        self.assertRaises(ValueError, g, t, f, [2], 0) 
        self.assertRaises(ValueError, g, t, f, [2**32], 0) 
        cache_size = 1024
        index = g(t, f, [1], cache_size)
        self.assertEqual(t, index.table)
        self.assertEqual(f, index.db_filename)
        self.assertEqual(cache_size, index.cache_size)

    def test_open(self):
        f = self._index_db_file.encode()
        self._table.open(WT_WRITE)
        self._table.close()
        self._table.open(WT_READ);
        index = _wormtable.Index(self._table, f, [1],  8192)
        # Try bad mode values.
        for j in range(-10, 10):
            if j not in [WT_READ, WT_WRITE]:
                self.assertRaises(ValueError, index.open, j) 
        self.assertRaises(WormtableError, index.open, WT_READ) 
        self._table.close()
        self.assertRaises(WormtableError, index.open, WT_WRITE) 
        self.assertRaises(WormtableError, index.open, WT_READ) 

    def test_build(self):
        f = self._index_db_file.encode()
        self._table.open(WT_WRITE)
        n = 10
        for j in range(n):
            self._table.insert_elements(1, j)
            self._table.commit_row()
        self._table.close()
        self._table.open(WT_READ)
        index = _wormtable.Index(self._table, f, [1],  8192)
        index.open(WT_WRITE)
        g = index.build
        self.assertRaises(TypeError, g, None) 
        self.assertRaises(TypeError, g, None, None) 
        self.assertRaises(TypeError, g, lambda x: x, None) 
        self.assertRaises(TypeError, g, lambda x: x, "") 
        self.assertRaises(TypeError, g, "", "") 
        def cb():
            print("this has the wrong number of args")
        self.assertRaises(TypeError, g, cb, 1) 
        def cb(x, y):
            print("this has the wrong number of args")
        self.assertRaises(TypeError, g, cb, 4) 
        def evil(x):
            index.close()
        self.assertRaises(WormtableError, g, evil, 4) 
        index.open(WT_WRITE)
        def evil(x):
            self._table.close()
        self.assertRaises(WormtableError, g, evil, 4) 
            
    def test_set_bin_widths(self):
        f = self._index_db_file.encode()
        self._table.open(WT_WRITE)
        self._table.close()
        self._table.open(WT_READ)
        index = _wormtable.Index(self._table, f, [1],  8192)
        g = index.set_bin_widths
        self.assertRaises(TypeError, g, None) 
        self.assertRaises(TypeError, g, "") 
        self.assertRaises(TypeError, g, 1) 
        self.assertRaises(TypeError, g, ["sdf"]) 
        self.assertRaises(TypeError, g, [None]) 
        self.assertRaises(ValueError, g, []) 
        self.assertRaises(ValueError, g, [1, 2]) 
        index.open(WT_WRITE)
        self.assertRaises(WormtableError, g, [1]) 
        index.close()
        index.open(WT_READ)
        self.assertRaises(WormtableError, g, [1]) 
    

    def test_min_max(self):
        f = self._index_db_file.encode()
        self._table.open(WT_WRITE)
        n = 10
        for j in range(n):
            self._table.insert_elements(1, j)
            self._table.commit_row()
        self._table.close()
        self._table.open(WT_READ)
        for j in range(n):
            r = self._table.get_row(j)
        index = _wormtable.Index(self._table, f, [1],  8192)
        t = tuple()
        self.assertRaises(WormtableError, index.get_min, t) 
        self.assertRaises(WormtableError, index.get_max, t) 
        index.open(WT_WRITE)
        self.assertRaises(WormtableError, index.get_min, t) 
        self.assertRaises(WormtableError, index.get_max, t) 
        index.build()
        self.assertRaises(WormtableError, index.get_min, t) 
        self.assertRaises(WormtableError, index.get_max, t) 
        index.close()
        self.assertRaises(WormtableError, index.get_min, t) 
        self.assertRaises(WormtableError, index.get_max, t) 
        index.open(WT_READ)
        self.assertEqual(0, index.get_min(t)[0])
        self.assertEqual(n - 1, index.get_max(t)[0])


    def test_distinct_values(self):
        f = self._index_db_file.encode()
        self._table.open(WT_WRITE)
        n = 10
        for j in range(n):
            self._table.insert_elements(1, j)
            self._table.commit_row()
        self._table.close()
        self._table.open(WT_READ)
        for j in range(n):
            r = self._table.get_row(j)
        g = _wormtable.IndexKeyIterator
        self.assertRaises(TypeError, g) 
        self.assertRaises(TypeError, g, None) 
        self.assertRaises(TypeError, g, self._table) 
        index = _wormtable.Index(self._table, f, [1],  8192)
        self.assertRaises(WormtableError, g, index) 
        index.open(WT_WRITE)
        self.assertRaises(WormtableError, g, index) 
        index.build()
        self.assertRaises(WormtableError, g, index) 
        index.close()
        self.assertRaises(WormtableError, g, index) 
        index.open(WT_READ)
        dvi = _wormtable.IndexKeyIterator(index)
        index.close()
        def f():
            for x in dvi:
                pass
        self.assertRaises(WormtableError, f) 
        index.open(WT_READ)
        c = 0
        for v in dvi:
            self.assertEqual(c, v[0])
            self.assertEqual(index.get_num_rows(v), 1)
            c += 1
        self.assertEqual(c, n)
        f()

    def test_row_iterator(self):
        f = self._index_db_file.encode()
        self._table.open(WT_WRITE)
        n = 10
        for j in range(n):
            self._table.insert_elements(1, j)
            self._table.commit_row()
        self._table.close()
        self._table.open(WT_READ)
        g = _wormtable.IndexRowIterator
        index = _wormtable.Index(self._table, f, [1],  8192)
        self.assertRaises(TypeError, g) 
        self.assertRaises(TypeError, g, None) 
        self.assertRaises(TypeError, g, self._table) 
        self.assertRaises(TypeError, g, index, None) 
        self.assertRaises(TypeError, g, None, index) 
        self.assertRaises(TypeError, g, index, "") 
        self.assertRaises(WormtableError, g, index, [0, 1]) 
        index.open(WT_WRITE)
        self.assertRaises(WormtableError, g, index, [0, 1]) 
        index.build()
        self.assertRaises(WormtableError, g, index, [0, 1]) 
        index.close()
        self.assertRaises(WormtableError, g, index, [0, 1]) 
        index.open(WT_READ)
        self.assertRaises(ValueError, g, index, []) 
        self.assertRaises(ValueError, g, index, [-1]) 
        self.assertRaises(ValueError, g, index, [0, 1, -1]) 
        self.assertRaises(ValueError, g, index, [2**32, 0, 1]) 
        ri = _wormtable.IndexRowIterator(index, [0, 1])
        index.close()
        def f():
            for x in ri:
                pass
        self.assertRaises(WormtableError, f) 
        index.open(WT_READ)
        c = 0
        for v in ri:
            self.assertEqual((c, c), v)
            c += 1
        self.assertEqual(c, n)
        f() 

