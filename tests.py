from __future__ import print_function
from __future__ import division 
import unittest
import random
import optparse
import glob
import shutil 
import os

import test.lowlevel
import test.highlevel

def cleanup():
    """
    Remove temporary files after interrupt.
    """
    for f in glob.glob("/tmp/wthl_*"):
        shutil.rmtree(f) 
    for f in glob.glob("/tmp/wtll_*.db"):
        os.unlink(f) 

def main():
    usage = "usage: %prog [options] "
    parser = optparse.OptionParser(usage=usage) 
    parser.add_option("-s", "--random-seed", dest="random_seed",
            help="Random seed", default=1)
    parser.add_option("-r", "--rows", dest="num_rows",
            help="Number of rows for random data tests.", default=100)
    parser.add_option("-n", "--name-case", dest="name",
            help="Run this specified test", default=None)
    parser.add_option("-i", "--iterations", dest="iterations",
            help="Repeat for i iterations", default="1")
    (options, args) = parser.parse_args()
    num_rows = int(options.num_rows)
    iterations = int(options.iterations)
    if num_rows < 2:
        parser.error("At least 2 rows must be used for random tests")
    random.seed(int(options.random_seed))
    testloader = unittest.TestLoader()
    test.lowlevel.num_random_test_rows = num_rows 
    if options.name is not None:
        suite = testloader.loadTestsFromName(options.name)
    else:
        suite = testloader.loadTestsFromModule(test.highlevel) 
        l = testloader.loadTestsFromModule(test.lowlevel) 
        suite.addTests(l)
    try:
        for i in range(iterations):
            unittest.TextTestRunner(verbosity=2).run(suite)
    finally:
        cleanup()
if __name__ == '__main__':
    main()
