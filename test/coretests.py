
import os
import tempfile
import unittest
import random

import vcfdb
import _vcfdb


def get_int_column(element_size, num_elements=_vcfdb.NUM_ELEMENTS_VARIABLE):
    """
    Returns an integer column with the specified element size and 
    number of elements.
    """
    name = "int_{0}_{1}".format(element_size, num_elements)
    return _vcfdb.Column(name.encode(), b"", _vcfdb.ELEMENT_TYPE_INT, 
            element_size, num_elements)

def get_float_column(element_size, num_elements):
    """
    Returns a float column with the specified element size and 
    number of elements.
    """
    name = "float_{0}_{1}".format(element_size, num_elements)
    return _vcfdb.Column(name.encode(), b"", _vcfdb.ELEMENT_TYPE_FLOAT, 
            element_size, num_elements)

# This must be wrong, as we're not triggering an overflow with this 
# test code.
def get_int_range(element_size):
    """
    Returns the tuple min, max defining the acceptable bounds for an
    integer of the specified size.
    """
    min_v = -2**(8 * element_size - 1)
    max_v = 2**(8 * element_size - 1) - 1
    return min_v, max_v


class TestElementParsers(unittest.TestCase):
    """
    Test the element parsers to ensure they accept and reject 
    values correctly.
    """     
    def setUp(self):
        fd, self._db_file = tempfile.mkstemp("-parse-test.db") 
        self._int_columns = {}
        for j in range(1, 9):
            self._int_columns[j] = get_int_column(j, 1)
        self._float_columns = {4: get_float_column(4, 1)}
        
        self._columns = list(self._int_columns.values()) + list(self._float_columns.values()) 
        self._database = _vcfdb.BerkeleyDatabase(self._db_file.encode(), 
            self._columns, cache_size=1024)
        self._database.create()
        # We can close the open fd now that db has opened it.
        os.close(fd)
        buffer_size = 64 * 1024
        self._row_buffer = _vcfdb.WriteBuffer(self._database, buffer_size, 1)

    def tearDown(self):
        self._row_buffer.flush()
        self._database.close()
        os.unlink(self._db_file)


    def test_bad_types(self):
        """
        Throw bad types at all the columns and expect a type error.
        """
        f = self._row_buffer.insert_encoded_elements
        values = [None, [], {}, self]
        for c in self._columns:
            for v in values:
                self.assertRaises(TypeError, f, c, v)

    def test_good_integer_values(self):
        rb = self._row_buffer
        c = self._int_columns[1]
        values = ["-1", "-2", "0", "4", "14", "100"]
        for c in self._int_columns.values():
            for v in values:
                self.assertIsNone(rb.insert_encoded_elements(c, v.encode()))
        # try some values inside of the acceptable range.
        for j in range(1, 9):
            min_v, max_v = get_int_range(j)
            c = self._int_columns[j]
            for k in range(10):
                v = random.randint(min_v, max_v)
                b = str(v).encode()
                self.assertIsNone(rb.insert_encoded_elements(c, b))
             
    def test_bad_integer_values(self):
        rb = self._row_buffer
        values = ["", "--1", "0.25", "sdasd", "[]", "3qsd"]
        for c in self._int_columns.values():
            for v in values:
                e = v.encode()
                self.assertRaises(ValueError, rb.insert_encoded_elements, c, e)
         # try some values outside of the acceptable range.
        for j in range(1, 9):
            min_v, max_v = get_int_range(j)
            c = self._int_columns[j]
            b = str(min_v - 1).encode()
            self.assertRaises(ValueError, rb.insert_encoded_elements, c, b)
            b = str(max_v + 1).encode()
            self.assertRaises(ValueError, rb.insert_encoded_elements, c, b)
            for k in range(10):
                v = random.randint(2 * min_v, min_v - 1)
                b = str(v).encode()
                self.assertRaises(ValueError, rb.insert_encoded_elements, c, b)
                v = random.randint(max_v, 2 * max_v - 1)
                b = str(v).encode()
                self.assertRaises(ValueError, rb.insert_encoded_elements, c, b)

    def test_good_float_values(self):
        rb = self._row_buffer
        c = self._int_columns[1]
        values = ["-1", "-2", "0", "4", "14", "100",
            "0.01", "-5.224234345235", "1E12"]
        for c in self._float_columns.values():
            for v in values:
                self.assertIsNone(rb.insert_encoded_elements(c, v.encode()))
            for k in range(10):
                v = random.uniform(-100, 100)
                b = str(v).encode()
                self.assertIsNone(rb.insert_encoded_elements(c, b))
     
    def test_bad_float_values(self):
        rb = self._row_buffer
        values = ["", "--1", "sdasd", "[]", "3qsd", "1Q0.023"]
        for c in self._float_columns.values():
            for v in values:
                e = v.encode()
                self.assertRaises(ValueError, rb.insert_encoded_elements, c, e)
             


class TestDatabaseLimits(unittest.TestCase):
    """
    Tests the limits of various aspects of the database to see if errors
    are flagged correctly.
    """     
    def setUp(self):
        fd, self._db_file = tempfile.mkstemp("-parse-test.db") 
        n = _vcfdb.MAX_ROW_SIZE // (_vcfdb.MAX_NUM_ELEMENTS * 8 
                + _vcfdb.NUM_ELEMENTS_VARIABLE_OVERHEAD)
        self._columns = [get_int_column(8) for j in range(n + 1)]
        self._database = _vcfdb.BerkeleyDatabase(self._db_file.encode(), 
            self._columns, cache_size=1024)
        self._database.create()
        # We can close the open fd now that db has opened it.
        os.close(fd)
        buffer_size = 64 * 1024
        self._row_buffer = _vcfdb.WriteBuffer(self._database, buffer_size, 1)

    def tearDown(self):
        self._row_buffer.flush()
        self._database.close()
        os.unlink(self._db_file)
    
    def test_column_overflow(self):
        """
        If we set the maximum number of elements to n - 1 columns, 
        it should overflow whatever we do.
        """
        rb = self._row_buffer
        v = [j for j in range(_vcfdb.MAX_NUM_ELEMENTS)]
        b = str(v).strip("[]").encode()
        n = len(self._columns)
        for j in range(50):
            for k in range(n - 1):
                rb.insert_encoded_elements(self._columns[k], b) 
            v2 = [k for k in range(j + 1)]
            b2 = str(v).strip("[]").encode()
            print(b2)
            self.assertRaises(ValueError, rb.insert_encoded_elements,
                    self._columns[0], b2)  
            self.assertRaises(ValueError, rb.insert_encoded_elements,
                    self._columns[0], b)
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

        
        

