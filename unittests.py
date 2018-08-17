import unittest
from tests import test_datatype
from tests import test_module_interface
from tests import test_connection_object
from tests import test_cursor_objects
from tests import test_type_objects
from tests import Test

if __name__ == '__main__':
    su = unittest.TestSuite()
    su.addTests(unittest.makeSuite(test_datatype.TestsCursor))
    su.addTests(unittest.makeSuite(test_module_interface.TestModuleInterface))
    su.addTest(unittest.makeSuite(test_connection_object.TestConnectionObject))
    su.addTest(unittest.makeSuite(test_cursor_objects.TestCursorObject))
    su.addTest(unittest.makeSuite(test_type_objects.TestTypeObject))
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(su)
