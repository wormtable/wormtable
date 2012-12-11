import unittest

import vcfdb
import _vcfdb

class TestTableBuilder(unittest.TestCase, vcfdb.TableBuilder):
    """
    Superclass of tests for the TableBuilder class. 
    """

    def setUp(self):
        """
        Set up the default values for the simulator.
        """
        self._database = _vcfdb.BerkeleyDatabase()        
    
    def test_stuff(self):
        pass

    
