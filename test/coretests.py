
import os
import tempfile
import unittest

import vcfdb
import _vcfdb

class TestDatabase(unittest.TestCase):
    """
    Superclass of tests for the class. 
    """

    def setUp(self):
        """
        Set up the default values for the simulator.
        """
        homedir = "tmp"



   

class TestElementParsers(unittest.TestCase):
    """
    Test the element parsers to ensure they accept and reject 
    values correctly.
    """     
    def setUp(self):
        fd, self._db_file = tempfile.mkstemp("-parse-test.db") 
        self._int_columns = {
            1:_vcfdb.Column(b"int1", b"", _vcfdb.ELEMENT_TYPE_INT, 1, 1),
            2:_vcfdb.Column(b"int2", b"", _vcfdb.ELEMENT_TYPE_INT, 2, 1),
        }
        self._columns = list(self._int_columns.values()) 
        self._database = _vcfdb.BerkeleyDatabase(self._db_file.encode(), 
            self._columns, cache_size=1024)
        self._database.create()
        # We can close the open fd now that db has opened it.
        os.close(fd)
        buffer_size = 64 * 1024
        self._row_buffer = _vcfdb.WriteBuffer(self._database, buffer_size, 1)


    def test_bad_types(self):
        """
        Throw bad types at all the columns and exect a value error.
        """
        f = self._row_buffer.insert_encoded_elements
        # This is problematic - in Python3 it's segfaulting and in 2 it's
        # not raising the error we expect.
        values = ["12", [], {}, self]
        for c in self._columns:
            for v in values:
                # PROBLEM!!!
                self.assertRaises(TypeError, f, c, v)

    def test_good_values(self):
        rb = self._row_buffer
        c = self._int_columns[1]
        # Errors integer bounds not handled correctly.
        values = ["-1", "-2", "0", "4", "14", "100", "-126"]
        for v in values:
            self.assertIsNone(rb.insert_encoded_elements(c, v.encode()))

    def test_bad_values(self):
        rb = self._row_buffer
        c = self._int_columns[1]
        # Errors found: empty string not detected as bad.
        values = ["--1", "0.25", "sdasd", "[]", "3qsd"]
        for v in values:
            self.assertRaises(ValueError, rb.insert_encoded_elements, c, v.encode())




    def tearDown(self):
        self._row_buffer.flush()
        self._database.close()
        os.unlink(self._db_file)


