import unittest
from pdbc.trafodion import connector
from .config import config

class TestModuleInterface(unittest.TestCase):

    @unittest.skip("unsupported feature")
    def test_connect_support_default_port(self):
        conx = connector.connect(user=config['user'], password=config['password'])

    @unittest.skip("unsupported feature")
    def test_connect_with_valid_parameters(self):
        conx = connector.connect(user=config['user'], password=config['password'],
                                 host=config['host'], port=config['port'])
        self.assertTrue(isinstance(conx, connector.TrafConnection))

    @unittest.skip("unsupported feature")
    def test_connect_with_invalid_user(self):
        with self.assertRaises(Exception):
            conx = connector.connect(user='notexistsuser', passwd='traf123',
                                     host=config['host'], port=config['port'])

    @unittest.skip("unsupported feature")
    def test_connect_with_invalid_user(self):
        try:
            conx = connector.connect(user='invalidusername', passwd='traf123',
                                     host=config['host'], port=config['port'])
        except Exception as err:
            self.assertEqual(err, Exception("invalid username"))

    @unittest.skip("mysql don't support this feature")
    def test_with_keyvalue_parameter(self):
        conx = connector.connect(config)
        self.assertTrue(isinstance(conx, connector.TrafConnection))

    @unittest.skip("mysql don't support this feature")
    def test_connect_keyvalue_args_1(self):
        config = {
            'host':'10.10.23.54',
            'user':'trafodion',
            'password':'traf123'
        }
        conx = connector.connect(config)
        self.assertTrue(isinstance(conx, connector.TrafConnection))

    @unittest.skip("unsupported feature, Error not in Module scope")
    def test_connect_with_invalid_kevalue_args(self):
        wrongconfig = {
            'host':config['host'],
            'port':config['port'],
            'user':'usernotexist',
            'password':'wrong password'
        }
        with self.assertRaises(Exception):
            conx = connector.connect(wrongconfig)

    @unittest.skip("unsupported feature")
    def test_globals_apilevel(self):
        self.assertEqual(connector.apilevel, 2)

    @unittest.skip("unsupported feature")
    def test_globals_threadsafety(self):
        self.assertTrue(hasattr(connector, 'threadsafety'))

    @unittest.skip("unsupported feature")
    def test_globals_paramstyle(self):
        self.assertTrue(hasattr(connector, 'paramstyle'))

    @unittest.skip("unsupported feature")
    def test_exception_warning(self):
        self.assertTrue(hasattr(connector, 'Warning'))

    def test_exception_error(self):
        self.assertTrue(hasattr(connector, 'Error'))

    def test_exception_interfaceError(self):
        self.assertTrue(hasattr(connector, 'InterfaceError'))

    def test_exception_DatabaseError(self):
        self.assertTrue(hasattr(connector, 'DatabaseError'))

    def test_exception_OperationalError(self):
        self.assertTrue(hasattr(connector, 'DataError'))

    def test_exception_IntegrityError(self):
        self.assertTrue(hasattr(connector, 'IntegrityError'))

    def test_exception_InternalError(self):
        self.assertTrue(hasattr(connector, 'InternalError'))

    def test_exception_ProgramingError(self):
        self.assertTrue(hasattr(connector, 'ProgrammingError'))

    def test_exception_NotSupportedError(self):
        self.assertTrue(hasattr(connector, 'NotSupportedError'))

    def test_exception_inheritance_Warning(self):
        self.assertTrue(issubclass(connector.Warning, Exception))

    def test_exception_inheritance_Error(self):
        self.assertTrue(issubclass(connector.Error, Exception))

    def test_exception_inheritance_InterfaceError(self):
        self.assertTrue(issubclass(connector.InterfaceError, connector.Error))

    def test_exception_inheritance_DatabaseError(self):
        self.assertTrue(issubclass(connector.DatabaseError, connector.Error))

    def test_exception_inheritance_DataError(self):
        self.assertTrue(issubclass(connector.DataError, connector.DatabaseError))

    def test_exception_inheritance_OperationalError(self):
        self.assertTrue(issubclass(connector.OperationalError, connector.DatabaseError))

    def test_exception_inheritance_IntegrityError(self):
        self.assertTrue(issubclass(connector.IntegrityError, connector.DatabaseError))

    def test_exception_inheritance_InternalError(self):
        self.assertTrue(issubclass(connector.InternalError, connector.DatabaseError))

    def test_exception_inheritance_ProgramingError(self):
        self.assertTrue(issubclass(connector.ProgrammingError, connector.DatabaseError))

    def test_exception_inheritance_NotSupportedError(self):
        self.assertTrue(issubclass(connector.NotSupportedError, connector.DatabaseError))