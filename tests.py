import unittest
import random

import test.coretests as coretests

def main():
    # make tests reproducable
    random.seed(2)
    suite = unittest.TestLoader().loadTestsFromModule(coretests)
    unittest.TextTestRunner(verbosity=1).run(suite)

if __name__ == '__main__':
    main()
