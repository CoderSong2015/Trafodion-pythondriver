import unittest
from tests import test_datatype
from tests import test_module_interface

if __name__ == '__main__':
    su = unittest.TestSuite()
    # su.addTests(unittest.makeSuite(test_datatype.TestsCursor))
    su.addTests(unittest.makeSuite(test_module_interface.TestModuleInterface))
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(su)
