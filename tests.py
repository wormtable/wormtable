import unittest

import test.coretests as coretests

def main():
    suite = unittest.TestLoader().loadTestsFromModule(coretests)
    unittest.TextTestRunner(verbosity=0).run(suite)

if __name__ == '__main__':
    main()
