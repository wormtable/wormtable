from __future__ import print_function
from __future__ import division 

import wormtable as wt

import unittest
import tempfile
import shutil 

class WormtableTest(unittest.TestCase):
    """
    Superclass of all wormtable tests. Create a homedir for working in
    on startup and clear it on teardown.
    """
    def setUp(self):
        self._homedir = tempfile.mkdtemp(prefix="wthl_") 
    
    def tearDown(self):
        shutil.rmtree(self._homedir)

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
