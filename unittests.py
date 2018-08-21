import unittest
from tests import test_datatype
from tests import test_module_interface
from tests import test_connection_object
from tests import test_cursor_objects
from tests import test_type_objects
from tests import Test

if __name__ == '__main__':
    su = unittest.TestSuite()

    test_case_modules = [
        test_datatype.TestsCursor,
        test_module_interface.TestModuleInterface,
        test_connection_object.TestConnectionObject,
        test_cursor_objects.TestCursorObject,
        test_type_objects.TestTypeObject
    ]

    for test_case_module in test_case_modules:
        su.addTests(unittest.makeSuite(test_case_module))

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(su)
