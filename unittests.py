import unittest
from tests import test_datatype

if __name__ == '__main__':
    su = unittest.TestSuite()
    su.addTests(unittest.makeSuite(test_datatype.TestsCursor))
    runner = unittest.TextTestRunner()
    runner.run(su)
