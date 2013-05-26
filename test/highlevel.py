from __future__ import print_function
from __future__ import division 

import wormtable as wt

import unittest
import tempfile
import shutil 
import os.path
import random
import collections
import itertools

from xml.etree import ElementTree

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
        n = 100
        max_value = 10
        self._table = wt.Table(self._homedir)
        t = self._table
        t.add_id_column(1)
        t.add_uint_column("uint")
        t.add_int_column("int")
        t.add_float_column("float", size=8)
        t.add_char_column("char", num_elements=3)
        t.open("w")
        for j in range(n):
            u = random.randint(0, max_value)
            i = random.randint(0, max_value)
            f = random.randint(0, max_value)
            # We need to do this to make sorting behave live Python's.
            c = str(random.randint(0, max_value))
            c = c.zfill(3)
            t.append([u, i, f, c.encode()])
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
    def test_cache_size(self):
        db = wt.Database(self._homedir, "test") 
        self.assertEqual(db.get_cache_size(), wt.DEFAULT_CACHE_SIZE)
        # Test integer interface
        for j in range(10):
            db.set_cache_size(j)
            self.assertEqual(db.get_cache_size(), j)
        # Test suffixes
        for j in range(10):
            db.set_cache_size("{0}K".format(j))
            self.assertEqual(db.get_cache_size(), j * 1024)
            db.set_cache_size("{0}M".format(j))
            self.assertEqual(db.get_cache_size(), j * 1024 * 1024)
            db.set_cache_size("{0}G".format(j))
            self.assertEqual(db.get_cache_size(), j * 1024 * 1024 * 1024)

    def test_names(self):
        names = ["some", "example", "names"]
        for n in names:
            db = wt.Database(self._homedir, n) 
            self.assertEqual(db.get_db_name(), n)
            self.assertEqual(db.get_homedir(), self._homedir)
            path = os.path.join(self._homedir, n + ".db")
            self.assertEqual(db.get_db_path(), path)
            path = os.path.join(self._homedir, n + "__build__.db")
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
        t.append([1])
        t.close()
        self.assertFalse(os.path.exists(t.get_db_build_path()))
        self.assertTrue(os.path.exists(t.get_db_path()))
        self.assertTrue(os.path.exists(t.get_metadata_path()))
        t.open("r")
        self.assertTrue(len(t) == 1)
        self.assertTrue(t[0] == (0, 1))
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
            t.append([j])
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


    def test_accessors(self):
        """
        Test if the accessor operations are working properly.
        """
        for i in self._indexes:
            i.open("r")
            cols = i.key_columns()
            if len(cols) == 1:
                c = cols[0]
                t = [r[c.get_position()] for r in self._table] 
            else:
                t = [tuple(r[c.get_position()] for c in cols) for r in self._table] 
            self.assertEqual(i.get_min(), min(t)) 
            self.assertEqual(i.get_max(), max(t)) 
            keys = [k for k in i.keys()]
            c1 = collections.Counter(t)
            l1 = list(i.keys())
            l2 = sorted(list(c1.keys()))
            self.assertEqual(l1, l2) 
            c2 = i.counter()
            for k, v in c2.items():
                self.assertEqual(v, c1[k])
            for k, v in c1.items():
                self.assertEqual(v, c2[k])
            # now test the cursor
            read_cols = self._table.columns()
            c = wt.IndexCursor(i, read_cols)
            j = 0
            for r1 in c:
                j = r1[0]
                row = self._table[j]
                r2 = tuple(row[col.get_position()] for col in read_cols)
                self.assertEqual(r1, r2)
            i.close()
            
class TableCursorTest(WormtableTest):
    """
    Tests the cursor over a table.
    """
    def setUp(self):
        super(TableCursorTest, self).setUp()
        self.make_random_table()

    def test_all_rows(self):
        t = self._table
        c = wt.TableCursor(t, t.columns())
        j = 0
        for r in c:
            self.assertEqual(t[j], r)
            j += 1
        self.assertEqual(len(t), j)
        # TODO more tests - permute the columns, etc.

