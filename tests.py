from __future__ import print_function
from __future__ import division 
import unittest
import random
import optparse

import test.coretests as coretests

def main():
    usage = "usage: %prog [options] "
    parser = optparse.OptionParser(usage=usage) 
    parser.add_option("-s", "--random-seed", dest="random_seed",
            help="Random seed", default=1)
    parser.add_option("-r", "--rows", dest="num_rows",
            help="Number of rows for random data tests.", default=1000)
    parser.add_option("-n", "--name-case", dest="name",
            help="Run this specified test", default=None)
    (options, args) = parser.parse_args()
    num_rows = int(options.num_rows)
    if num_rows < 2:
        parser.error("At least 2 rows must be used for random tests")
    random.seed(int(options.random_seed))
    testloader = unittest.TestLoader()
    coretests.num_random_test_rows = num_rows 
    if options.name is not None:
        suite = testloader.loadTestsFromName(options.name)
    else:
        suite = testloader.loadTestsFromModule(coretests) 
    unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == '__main__':
    main()
