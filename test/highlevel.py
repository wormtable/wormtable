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

import wormtable as wt

import os
import math
import unittest
import tempfile
import shutil 
import os.path
import random
import itertools

from xml.etree import ElementTree

# module variables used to control the number of tests that we do.
num_random_test_rows = 10

def histogram(l, width):
    """
    Returns a histogram over the data in v with bin widths width.
    """
    d = {}
    for u in l:
        if isinstance(u, tuple):
            v = [uu - math.fmod(uu, width) for uu in u]
            v = tuple(v)
        else:
            v = None
            if u is not None:
                v = u - math.fmod(u, width)
        if v in d:
            d[v] += 1
        else:
            d[v] = 1
    return d

class WormtableTest(unittest.TestCase):
    """
    Superclass of all wormtable tests. Create a homedir for working in
    on startup and clear it on teardown.
    """
    def setUp(self):
        self._homedir = tempfile.mkdtemp(prefix="wthl_") 
    
    def tearDown(self):
        shutil.rmtree(self._homedir)

    def make_random_table(self):
        """
        Make a small random table with small random values.
        """
        num_rows = num_random_test_rows 
        max_value = 10
        self._table = wt.Table(self._homedir)
        t = self._table
        t.add_id_column(4)
        t.add_uint_column("uint")
        t.add_int_column("int")
        t.add_float_column("float", size=4)
        t.add_char_column("char", num_elements=3)
        t.add_uint_column("uintv", num_elements=wt.WT_VAR_1)
        t.open("w")
        def g():
            return random.random() < 0.25
        for j in range(num_rows):
            u = None if g() else random.randint(0, max_value)
            i = None if g() else random.randint(0, max_value)
            f = None if g() else random.uniform(0, max_value)
            s = str(random.randint(0, max_value)).zfill(3)
            c = None if g() else s.encode()
            n = random.randint(0, 5)
            v = [0 for j in range(n)]
            t.append([None, u, i, f, c, v]) 
            if random.random() < 0.33:
                t.append([])
        t.close()
        t.open("r")

class VacuousDatabase(wt.Database):
    """
    Minimal subclass of wt.Database that implements the required methods.
    """
    XML_ROOT_TAG = "TEST"
    def get_metadata(self):
        root = ElementTree.Element(self.XML_ROOT_TAG)
        return ElementTree.ElementTree(root)


class DatabaseClassTests(WormtableTest):
    """
    Tests the functionality of the database superclass of Index and Table.
    """ 
    def test_db_cache_size(self):
        db = wt.Database(self._homedir, "test") 
        self.assertEqual(db.get_db_cache_size(), wt.DEFAULT_CACHE_SIZE)
        # Test integer interface
        for j in range(10):
            db.set_db_cache_size(j)
            self.assertEqual(db.get_db_cache_size(), j)
        # Test suffixes
        for j in range(10):
            db.set_db_cache_size("{0}K".format(j))
            self.assertEqual(db.get_db_cache_size(), j * 1024)
            db.set_db_cache_size("{0}M".format(j))
            self.assertEqual(db.get_db_cache_size(), j * 1024 * 1024)
            db.set_db_cache_size("{0}G".format(j))
            self.assertEqual(db.get_db_cache_size(), j * 1024 * 1024 * 1024)

    def test_names(self):
        names = ["some", "example", "names"]
        for n in names:
            db = wt.Database(self._homedir, n) 
            self.assertEqual(db.get_db_name(), n)
            self.assertEqual(db.get_homedir(), self._homedir)
            path = os.path.join(self._homedir, n + ".db")
            self.assertEqual(db.get_db_path(), path)
            s = "_build_{0}_{1}.db".format(os.getpid(), n) 
            path = os.path.join(self._homedir, s)
            self.assertEqual(db.get_db_build_path(), path)
            path = os.path.join(self._homedir, n + ".xml")
            self.assertEqual(db.get_metadata_path(), path)

    def test_write_metadata(self):
        vdb = VacuousDatabase(self._homedir, "test")
        f = vdb.get_metadata_path()
        vdb.write_metadata(f)
        self.assertTrue(os.path.exists(f))
        tree = ElementTree.parse(f)
        root = tree.getroot()
        self.assertEqual(root.tag, VacuousDatabase.XML_ROOT_TAG)

    def test_finalise_build(self):
        name = "finalise_test"
        vdb = VacuousDatabase(self._homedir, name)
        build = vdb.get_db_build_path() 
        with open(build, "w") as f:
            f.write(name)
        vdb.finalise_build()
        permanent = vdb.get_db_path()
        self.assertTrue(os.path.exists(permanent))
        self.assertFalse(os.path.exists(build))
        with open(permanent, "r") as f:
            s = f.read()
        self.assertEqual(s, name)

    def test_open_api(self):
        """
        Tests the open_table/index api to ensure everything works correctly.
        """
        t = wt.Table(self._homedir)
        t.add_id_column()
        t.add_uint_column("u1")
        t.open("w")
        self.assertTrue(t.is_open())
        t.close()
        # open_table returns a table opened
        self.assertFalse(t.is_open())
        t = wt.open_table(self._homedir)
        self.assertTrue(t.is_open())
        t.close()
        self.assertFalse(t.is_open())
        # try now with the context manager
        with wt.open_table(self._homedir) as t:
            self.assertTrue(t.is_open())
        self.assertFalse(t.is_open()) 
        # Now do the same for an index.
        t = wt.open_table(self._homedir)
        name = "test"
        i = wt.Index(t, name) 
        i.add_key_column(t.get_column(1))
        i.open("w")
        i.build()
        i.close()
        # The index is built, so we can open it.
        i = t.open_index(name)
        self.assertTrue(i.is_open())
        i.close()
        self.assertFalse(i.is_open())
        with t.open_index(name) as i:
            self.assertTrue(i.is_open())
        self.assertFalse(i.is_open())
        t.close()

class TableBuildTest(WormtableTest):
    """
    Tests for the build process in tables. 
    """

    def test_open(self):
        t = wt.Table(self._homedir)
        self.assertEqual(t.get_db_name(), t.DB_NAME)
        t.add_id_column()
        t.add_uint_column("u1")
        self.assertFalse(t.is_open())
        t.open("w")
        self.assertTrue(t.is_open())
        self.assertTrue(os.path.exists(t.get_db_build_path()))
        t.append([None, 1])
        t.close()
        self.assertFalse(os.path.exists(t.get_db_build_path()))
        self.assertTrue(os.path.exists(t.get_db_path()))
        self.assertTrue(os.path.exists(t.get_metadata_path()))
        t.open("r")
        self.assertTrue(len(t) == 1)
        self.assertTrue(t[0] == (0, 1))
        t.close()

    def test_missing_values(self):
        """
        Tests if missing values are correctly inserted.
        """
        t = wt.Table(self._homedir)
        t.add_id_column()
        for j in range(1, 5):
            t.add_uint_column("u_" + str(j), num_elements=j) 
            t.add_int_column("i_" + str(j),  num_elements=j) 
            t.add_float_column("f_" + str(j), num_elements=j) 
            t.add_char_column("c_" + str(j), num_elements=j) 
        t.add_uint_column("u_v", num_elements=wt.WT_VAR_1)  
        t.add_int_column("i_v", num_elements=wt.WT_VAR_1)  
        t.add_float_column("f_v", num_elements=wt.WT_VAR_1)  
        t.add_char_column("c_v", num_elements=wt.WT_VAR_1) 
        t.open("w")
        t.append([])
        t.close()
        t.open("r")
        self.assertTrue(len(t) == 1)
        r = t[0]
        n = len(t.columns())
        for j in range(1, n):
            c = t.get_column(j)
            self.assertEqual(None, r[j])
        t.close()

 

class IndexBuildTest(WormtableTest):
    """
    Tests for the build process in indexes. 
    """
    
    def setUp(self):
        super(IndexBuildTest, self).setUp()
        t = wt.Table(self._homedir)
        t.add_id_column()
        t.add_uint_column("u1")
        t.open("w")
        n = 10
        for j in range(n):
            t.append([None, j])
        t.close()
        t.open("r")
        self.assertTrue(n == len(t))
        self._table = t

    def test_open(self):
        name = "col1"
        i = wt.Index(self._table, name) 
        self.assertEqual(i.get_db_name(), i.DB_PREFIX + name)
        i.add_key_column(self._table.get_column(1))
        self.assertFalse(i.is_open())
        i.open("w")
        self.assertTrue(i.is_open())
        self.assertTrue(os.path.exists(i.get_db_build_path()))
        i.build()
        i.close()
        self.assertFalse(os.path.exists(i.get_db_build_path()))
        self.assertTrue(os.path.exists(i.get_metadata_path()))
        self.assertTrue(os.path.exists(i.get_db_path()))
        i.open("r")
        keys = [k for k in i.keys()]
        col = [r[1] for r in self._table]
        self.assertEqual(keys, col)
        i.close()

class ColumnValue(object):
    """
    A class that represents a value from a given column. This class 
    is primarily to ensure that values from Columns sort correctly.
    """
    def __init__(self, column, row):
        self.__column = column
        self.__value = row[column.get_position()]

    def get_value(self):
        return self.__value

    def __repr__(self):
        return "<'" + repr(self.__value) + "' " + str(self.__column) + ">"

    def __lt__(self, other):
        v1 = self.__value
        v2 = other.__value
        missing = None 
        ret = False
        if v1 == missing and v2 != missing:
            ret = True
        elif v1 != missing and v2 != missing:
            ret = v1 < v2 
        return ret 

    def __eq__(self, other):
        if isinstance(other, ColumnValue):
            ret = self.__value == other.__value
        else:
            ret = self.__value == other
        return ret

    def __hash__(self):
        return self.__value.__hash__()

class IndexIntegrityTest(WormtableTest):
    """
    Tests the integrity of indexes by building a small table with a
    a variety of columns and making indexes over these.
    """
    def setUp(self):
        super(IndexIntegrityTest, self).setUp()
        self.make_random_table()
        # make some indexes
        cols = [c for c in self._table.columns()][1:]
        self._indexes = []
        for c in cols:
            name = c.get_name()
            i = wt.Index(self._table, name) 
            i.add_key_column(c)
            i.open("w")
            i.build()
            i.close()
            self._indexes.append(i)
        for c1, c2 in itertools.permutations(cols, 2):
            name = c1.get_name() + "+" + c2.get_name()
            i = wt.Index(self._table, name) 
            i.add_key_column(c1)
            i.add_key_column(c2)
            i.open("w")
            i.build()
            i.close()
            self._indexes.append(i)
        for c1, c2, c3 in itertools.permutations(cols, 3):
            name = c1.get_name() + "+" + c2.get_name() + "+" + c3.get_name()
            i = wt.Index(self._table, name) 
            i.add_key_column(c1)
            i.add_key_column(c2)
            i.add_key_column(c3)
            i.open("w")
            i.build()
            i.close()
            self._indexes.append(i)


    def test_keys(self):
        """
        Test if the key operations are working properly.
        """
        for i in self._indexes:
            i.open("r")
            cols = i.key_columns()
            if len(cols) == 1:
                t = [ColumnValue(c, r) for c in cols for r in self._table] 
            else:
                t = [tuple(ColumnValue(c, r) for c in cols) for r in self._table] 
            keys = [k for k in i.keys()]
            self.assertEqual(i.min_key(), min(t)) 
            self.assertEqual(i.max_key(), max(t)) 
            c1 = {}
            for k in t:
                if k not in c1:
                    c1[k] = 0 
                c1[k] += 1
            l1 = list(i.keys())
            l2 = sorted(list(c1.keys()))
            self.assertEqual(l1, l2) 
            c2 = i.counter()
            for k, v in c2.items():
                self.assertEqual(v, c1[k])
            for k, v in c1.items():
                if len(cols) == 1:
                    key = k.get_value()
                else:
                    key = tuple(kk.get_value() for kk in k)
                self.assertEqual(v, c2[key])
            i.close()

    def test_cursors(self):
        read_cols = self._table.columns()
        for i in self._indexes:
            i.open("r")
            cols = i.key_columns()
            t = [[tuple(ColumnValue(c, r) for c in cols), r] for r in self._table] 
            t.sort(key=lambda x: x[0])
            for (k, r1), r2  in zip(t, i.cursor(read_cols)):
                self.assertEqual(r1, r2)
            if len(cols) == 1:
                keys = [(k,) for k in i.keys()]
            else:
                keys = [k for k in i.keys()]

            key_rows = [tuple(v.get_value() for v in k) for k, r in t]
            # Now generate some slices
            for j in range(10):
                k = random.randint(0, len(keys) - 1)
                start_key = keys[k]
                start_index = key_rows.index(start_key)
                k = random.randint(k, len(keys) - 1)
                stop_key = keys[k]
                stop_index = key_rows.index(stop_key)
                l = [r for k, r in t[start_index:stop_index]]
                c = 0
                if len(cols) == 1:
                    start_key = start_key[0]
                    stop_key = stop_key[0]
                for r1, r2 in zip(i.cursor(read_cols, start_key, stop_key), l):
                    self.assertEqual(r1, r2)
                    c += 1
                self.assertEqual(c, stop_index - start_index)

            i.close()
            

class BinnedIndexIntegrityTest(WormtableTest):
    """
    Tests the integrity of indexes by building a small table with a
    a variety of columns and making indexes with bins over these. 
    """
    def setUp(self):
        super(BinnedIndexIntegrityTest, self).setUp()
        self.make_random_table()
        # make some indexes
        cols = [c for c in self._table.columns()][1:]
        self._indexes = []
        for c in cols:
            if c.get_type() in [wt.WT_INT, wt.WT_UINT]:
                name = c.get_name()
                for j in range(1, 10):
                    i = wt.Index(self._table, name + "_" + str(j)) 
                    i.add_key_column(c, j)
                    i.open("w")
                    i.build()
                    i.close()
                    self._indexes.append(i)
            elif c.get_type() == wt.WT_FLOAT: 
                name = c.get_name()
                w = 0.125
                while w < 10: 
                    i = wt.Index(self._table, name + "_" + str(w)) 
                    i.add_key_column(c, w)
                    i.open("w")
                    i.build()
                    i.close()
                    self._indexes.append(i)
                    w += 0.125
        
    def test_count(self):
        """
        Tests if the counter function is operating correctly.
        """
        for i in self._indexes:
            i.open("r")
            c = i.key_columns()[0]
            w = i.bin_widths()[0]
            t = [r[c.get_position()] for r in self._table] 
            d = histogram(t, w)
            d2 = dict(i.counter().items())
            for k, v in i.counter().items():
                self.assertTrue(k in d)
                self.assertEqual(d[k], v)
            i.close()


class MultivalueColumnTest(IndexIntegrityTest):
    """
    Tests specific to the properties of multivalue columns.
    """

    def setUp(self):
        super(MultivalueColumnTest, self).setUp()
        # Add some indexes with specific properties.
        index_cols = [["var_uint"], ["var_uint", "var_int"], 
                ["var_uint", "var_int", "var_char"]]
        index_cols = [[self._table.get_column(c) for c in cols]
                for cols in index_cols]
        for cols in index_cols: 
            name = "multi" + "+".join(c.get_name() for c in cols) 
            i = wt.Index(self._table, name) 
            for c in cols:
                i.add_key_column(c)
            i.open("w")
            i.build()
            i.close()
            self._indexes.append(i)

    def make_random_table(self):
        """
        Make a small random table with small random values.
        """
        num_rows = num_random_test_rows 
        max_value = 2 
        max_len = 5
        self._table = wt.Table(self._homedir)
        t = self._table
        t.add_id_column(4)
        t.add_uint_column("fixed_uint", num_elements=max_len)
        t.add_uint_column("var_uint", num_elements=wt.WT_VAR_1)
        t.add_int_column("fixed_int", num_elements=max_len)
        t.add_int_column("var_int", num_elements=wt.WT_VAR_1)
        t.add_char_column("fixed_char", num_elements=max_len)
        t.add_char_column("var_char", num_elements=wt.WT_VAR_1)
        t.open("w")
        def fixed():
            return [random.randint(0, max_value) for j in range(max_len)]
        def var():
            n = random.randint(0, max_len)
            return [random.randint(0, max_value) for j in range(n)]
        for j in range(num_rows):
            fu = fixed()
            vu = var()
            fi = fixed() 
            vi = var()
            fc = "".join(str(u) for u in fixed())
            vc = "".join(str(u) for u in var())
            t.append([None, fu, vu, fi, vi, fc.encode(), vc.encode()])
            if random.random() < 0.33:
                t.append([])
        t.close()
        t.open("r")



class StringIndexIntegrityTest(WormtableTest):
    """
    Tests the integrity of indexes over variable length length 
    columns by checking some simple cases over strings.
    """
    
    def test_unique_keys(self):
        """
        Test the simplest possible case where we have keys that 
        cannot be distinguished if we concatentate the values
        together.
        """
        t = wt.Table(self._homedir) 
        t.add_id_column(1)
        t.add_char_column("s1")
        t.add_char_column("s2")
        t.open("w")
        t.append([None, b"A", b"AA"])
        t.append([None, b"AA", b"A"])
        t.close()
        t.open("r")
        i = wt.Index(t, "test")
        i.add_key_column(t.get_column("s1"))
        i.add_key_column(t.get_column("s2"))
        i.open("w")
        i.build()
        i.close()
        i.open("r")
        c = i.counter()
        self.assertEqual(c[(b"A", b"AA")], 1)
        self.assertEqual(c[(b"AA", b"A")], 1)
        t.close()
 
    def test_max_prefix(self):
        """
        Test the simplest possible case where we must get the maximum 
        key for a given prefix.
        """
        t = wt.Table(self._homedir) 
        t.add_id_column(1)
        t.add_char_column("s1")
        t.add_char_column("s2")
        t.open("w")
        t.append([None, b"A", b"A"])
        t.append([None, b"A", b"AA"])
        t.append([None, b"A", b"B"])
        t.append([None, b"AA", b""])
        t.append([None, b"B", b"A"])
        t.close()
        t.open("r")
        i = wt.Index(t, "test")
        i.add_key_column(t.get_column("s1"))
        i.add_key_column(t.get_column("s2"))
        i.open("w")
        i.build()
        i.close()
        i.open("r")
        self.assertEqual(i.max_key(), (b"B", b"A"))
        self.assertEqual(i.max_key("A"), (b"A", b"B"))
        self.assertEqual(i.max_key("AA"), (b"AA", b""))
        t.close()
 


class TableCursorTest(WormtableTest):
    """
    Tests the cursor over a table.
    """
    def setUp(self):
        super(TableCursorTest, self).setUp()
        self.make_random_table()

    def test_all_rows(self):
        t = self._table
        j = 0
        for r in t.cursor(t.columns()):
            self.assertEqual(t[j], r)
            j += 1
        self.assertEqual(len(t), j)
        # TODO more tests - permute the columns, etc.


    def test_range(self):
        t = self._table
        cols = [c.get_name() for c in t.columns()]
        for j in range(10):
            start = random.randint(0, len(t))
            stop = random.randint(start, len(t))
            v = [r[0] for r in t.cursor(["row_id"], start=start, stop=stop)]
            self.assertEqual(v, [j for j in range(start, stop)])
            v = [r[0] for r in t.cursor(["row_id"], start=start)]
            self.assertEqual(v, [j for j in range(start, len(t))])
            v = [r[0] for r in t.cursor(["row_id"], stop=stop)]
            self.assertEqual(v, [j for j in range(stop)])
            c = t.cursor(cols, start=start, stop=stop)
            k = start
            for r in c:
                self.assertEqual(t[k], r)
                k += 1
            self.assertEqual(k, stop)
        
    def test_empty(self):
        t = self._table
        cols = ["row_id"]
        v = [r for r in t.cursor(cols, start=0, stop=0)]
        self.assertEqual(v, [])
        v = [r for r in t.cursor(cols, start=0, stop=-1)]
        self.assertEqual(v, [])
        v = [r for r in t.cursor(cols, start=1, stop=1)]
        self.assertEqual(v, [])
        v = [r for r in t.cursor(cols, start=len(t), stop=len(t))]
        self.assertEqual(v, [])
        v = [r for r in t.cursor(cols, start=2 * len(t), stop=3 * len(t))]
        self.assertEqual(v, [])


class FloatTest(WormtableTest):
    """
    Tests the limits of the floating point types to see if they are correct
    under IEEE rules.
    """
    # Maximum representable numbers
    max_half = (2 - 2**-10) * 2**15
    max_float = (2 - 2**-23) * 2**127
    max_double = (1 + (1 - 2**-52)) * 2**1023
    # Minimum values 
    min_half_normal = 2**-14 
    min_half_denormal = 2**-24
    min_float_normal = 2**-126
    min_float_denormal = 2**-149
    min_double_normal = 2**-1022
    min_double_denormal = 2**-1074
    # The number of decimal digits of precision
    max_half_digits = 3
    max_float_digits = 6
    max_double_digits = 15

    def create_table(self):
        t = wt.Table(self._homedir)
        t.add_id_column()
        t.add_float_column("half", size=2)
        t.add_float_column("single", size=4)
        t.add_float_column("double", size=8)
        t.open("w")
        return t 

    def assert_tables_equal(self, in_values, out_values):
        t = self.create_table() 
        for r in in_values:
            t.append([None] + list(r)) 
        t.close()
        t.open("r")
        for r1, r2 in zip(out_values, t):
            self.assertEqual(tuple(r1), r2[1:])
            #print(r1, "->", r2)
        t.close() 
        
    def test_missing_values(self):
        """
        Checks to ensure missing values are correctly handled.
        """
        values = [[None, None, None], []]
        results = [[None, None, None] for v in values]
        self.assert_tables_equal(values, results)

    def test_exact_values(self):
        """
        Checks that exact values are stored correctly.
        """
        # Get a bunch of small integers
        in_values = []
        for j in range(64):
            x = (j, j, j)
            in_values.append(x)
            x = (-j, -j, -j)
            in_values.append(x)
        # And some easy fractions 
        in_values = []
        for j in range(10):
            x = (2**-j, 2**-j, 2**-j)
            in_values.append(x)
            in_values.append([-v for v in x])
        # We can also represent values up to 2*v exactly
        v = (11, 24, 53)
        for s in [1, -1]:
            x = []
            y = []
            for j in v:
                x.append(s * 2**j)
            in_values.append(x)
        v = [self.max_half, self.max_float, self.max_double]
        in_values.append(v) 
        in_values.append([-1 * x for x in v]) 
        v = [[self.min_half_normal, self.min_float_normal, 
                    self.min_double_normal],
            [self.min_half_denormal, self.min_float_denormal, 
                    self.min_double_denormal]]
        in_values.extend(v) 
        self.assert_tables_equal(in_values, in_values)
    
    def test_infinity(self):
        """
        Checks to see if overflow values are detected correctly.
        """
        inf = 1e1000 
        values = [
            [inf, inf, inf],
            [1e6, 1e300, 1e500],
            [2**1000, 2**1000, inf],
            [self.max_half * 10 , self.max_float * 10 , self.max_double * 10],
            [(1 + 2 - 2**-10) * 2**15, (1 + 2 - 2**-23) * 2**127, (1 + 1 + (1 - 2**-52)) * 2**1023],
        ]
        result = [[inf for j in range(3)] for x in values]
        self.assert_tables_equal(values, result)
        neg = [[-1 * v for v in x] for x in values]
        neg_inf = [[-inf for v in x] for x in values]
        self.assert_tables_equal(neg, neg_inf)
    
    def test_half_infinity(self):
        inf = 1e1000
        x = self.max_half + 16 
        values = [x + 0.1, x + 1, x + 10, x + 100, x + 1000]
        self.assert_tables_equal([[v] for v in values],
                [[inf, None, None] for v in values])
        self.assert_tables_equal([[-v] for v in values],
                [[-inf, None, None] for v in values])
        values = [self.max_half + j for j in range(16)]
        self.assert_tables_equal([(v, None, None) for v in values], 
                [(self.max_half, None, None) for v in values])
        self.assert_tables_equal([(-v, None, None) for v in values], 
                [[-self.max_half, None, None] for v in values])
        # Below this we can exactly represent values % 32.
        values = [self.max_half - j * 32 for j in range(16)]
        y = [(v, None, None) for v in values]
        self.assert_tables_equal(y, y)
        y = [(-v, None, None) for v in values]
        self.assert_tables_equal(y, y)

    def test_underflow(self):
        """
        Tests to see if the values less than the minimum representable values 
        stored underflow to 0 correctly.
        """
        h = self.min_half_denormal
        f = self.min_float_denormal
        d = self.min_double_denormal
        # Anything less than minimum denormal should underflow.
        values = [[h / j , f / j, d / j] for j in range(2, 20)]
        result = [[0 for v in r] for r in values]
        self.assert_tables_equal(values, result)
        values = [[-v for v in r] for r in values]
        self.assert_tables_equal(values, result)
        # Values less than the minimum normal should not result in underflow
        h = self.min_half_normal
        f = self.min_float_normal
        d = self.min_double_normal
        values = [[h / 2**j , f / 2**j, d / 2**j] for j in range(11)]
        self.assert_tables_equal(values, values)

    def test_denormal(self):
        """
        Tests to see if denormal values are stored and retrieved correctly. 
        """
        h = self.min_half_denormal
        f = self.min_float_denormal
        d = self.min_double_denormal
        values = [[h * 2**j , f * 2**j, d * 2**j] for j in range(5)] 
        self.assert_tables_equal(values, values)
        values = [[-v for v in r] for r in values]
        self.assert_tables_equal(values, values)
       
    def test_decimal_digits(self):
        """
        Generates a string of decimal digits of maximum lenght for 
        each type and verifies that these are stored exactly.
        """
        md = [self.max_half_digits, self.max_float_digits, self.max_double_digits]
        values = []
        t = self.create_table() 
        for j in range(100):
            v = []
            for d in md:
                s = "0." + "".join([str(random.randint(0, 9)) for j in range(d)])
                u = float(s)
                v.append(s)
            values.append(v)
            t.append([None] + [float(x) for x in v])
        t.close()
        t.open("r")
        for r1, r2 in zip(values, t):
            u = []
            for x, d in zip(r2[1:], md):
                s = "{0:.{1}f}".format(x, d)
                u.append(s) 
            self.assertEqual(r1, u) 
        t.close() 

    def test_nan(self):
        """
        Test to see if NaNs inserted are recovered correctly.
        NaN does not equal itself, so we must do things 
        differently.
        """
        nan = float("NaN")
        t = wt.Table(self._homedir)
        t.add_id_column()
        t.add_float_column("half", size=2)
        t.add_float_column("single", size=4)
        t.add_float_column("double", size=8)
        t.open("w")
        t = self.create_table()
        t.append([None, nan, nan, nan])
        t.append_encoded([None, b"nan", b"NaN", b"NAN"])
        t.close()
        t.open("r")
        for r in t:
            for v in r[1:]:
                self.assertTrue(math.isnan(v))
        t.close() 
 

    def test_compare_numpy(self):
        """
        Compare with numpy arrays for some critical values.
        """
        import numpy as np
        inf = float("Inf") 
        values = [
            [inf, inf, inf], [-inf, -inf, -inf],
            [0.0, 0.0, 0.0], [-0.0, -0.0, -0.0], 
            [self.max_half, self.max_float, self.max_double],
            [2 * self.max_half, 2 * self.max_float, 2 * self.max_double],
            [self.min_half_normal, self.min_float_normal, 
                    self.min_double_normal],
            [self.min_half_denormal, self.min_float_denormal, 
                    self.min_double_denormal],
            [2* self.min_half_normal, 2 * self.min_float_normal, 
                    2* self.min_double_normal],
            [2 * self.min_half_denormal, 2 * self.min_float_denormal, 
                    2* self.min_double_denormal],
        ]
        for j in range(1, 128):
            # try some fractions
            x = 1 / j
            v = (x, x, x)
            values.append(v)
            values.append([-x for x in v])
            # random numbers in (0, 1) 
            x = random.random()
            v = (x, x, x)
            values.append(v)
            values.append([-x for x in v])
            # random numbers in (0, max_half) 
            x = self.max_half * random.random()
            v = (x, x, x)
            values.append(v)
            values.append([-x for x in v])
            v = (self.max_half + j, self.max_float + j, self.max_double + j)
            values.append(v)
            values.append([-x for x in v])
            v = (self.min_half_normal + 2**-j, self.min_float_normal + 2**-j,
                    self.min_float_normal + 2**-j)
            values.append(v)
            values.append([-x for x in v])
            v = (self.min_half_denormal + 2**-j, self.min_float_denormal + 2**-j,
                    self.min_float_denormal + 2**-j)
            values.append(v)
            values.append([-x for x in v])

        results = []
        for h, s, d in values:
            sa = np.array([s], dtype=np.float32)
            ha = np.array([h], dtype=np.float16)
            da = np.array([d], dtype=np.float64)
            results.append((float(ha[0]), float(sa[0]), float(da[0])))
        self.assert_tables_equal(values, results)



