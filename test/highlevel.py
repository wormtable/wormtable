from __future__ import print_function
from __future__ import division 

import wormtable as wt

import unittest
import tempfile
import shutil 
import os.path
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
            self.assertEqual(db.get_name(), n)
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

