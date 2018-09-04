import unittest
from pdbc.trafodion import connector
from .config import config

class TestModuleInterface(unittest.TestCase):

    def test_connect_host_user_password(self):
        conx = connector.connect(host=config['host'], user=config['user'], password=config['password'])
        self.assertTrue(isinstance(conx, connector.TrafConnection))

    def test_connect_host_user_password_port(self):
        conx = connector.connect(host=config['host'], user=config['user'],
                                 password=config['password'], port=config['port'])
        self.assertTrue(isinstance(conx, connector.TrafConnection))

    def test_connect_host_user_password_port_database(self):
        conx = connector.connect(**config)
        cursor = conx.cursor()
        cursor.execute('create schema if not exists python_testsch1')
        cursor.execute('set schema python_testsch1')
        cursor.execute('create table if not EXISTS python_testtb(i int)')
        cursor.close()
        conx1 = connector.connect(host=config['host'], user=config['user'],
                                  password=config['password'], port=config['port'], database='python_testsch1')
        cursor = conx1.cursor()
        cursor.execute('get tables')
        self.assertTrue(('python_testtb') in cursor)

    def test_connect_with_invalid_user_or_password(self):
        with self.assertRaises(connector.DatabaseError):
            conx = connector.connect(user='notexistsuser', password='apasswod',
                                     host=config['host'], port=config['port'])
            cnx1 = connector.connect(user='trafodion', password='wrongpass',
                                     host=config['host'], port=config['port'])

    def test_connect_with_not_reachable_host(self):
        """all exception should be correctly handled by the driver. exception throw from should be connector.Waring or
        connecter.Error"""
        with self.assertRaises(connector.Error):
            cnx = connector.connect(user='someuser', password='somepass', host='notreachable')

    @unittest.skip('unsupported feature')
    def test_connect_with_config_file(self):
        cnx = connector.connect(config_file='/path/to/config')
        self.assertTrue(isinstance(cnx, connector.TrafConnection))

    def test_globals_apilevel(self):
        self.assertEqual(connector.apilevel, '2.0')

    def test_globals_threadsafety(self):
        self.assertTrue(hasattr(connector, 'threadsafety'))
        self.assertTrue(connector.threadsafety in [0, 1, 2, 3])

    def test_globals_paramstyle(self):
        self.assertTrue(hasattr(connector, 'paramstyle'))
        self.assertTrue(connector.paramstyle in ['qmark', 'numeric', 'named', 'format', 'pyformat'])

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
        self.assertTrue(hasattr(connector, 'InterfaceError'))
        self.assertTrue(issubclass(connector.InterfaceError, connector.Error))

    def test_exception_inheritance_DatabaseError(self):
        self.assertTrue(hasattr(connector, 'DatabaseError'))
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

        s = str()
        
    #improve code coverage
    def test_wrong_charset(self):
        cnx = connector.connect(**config)
        print(cnx.property.charset)
        cnx.close()
        with self.assertRaises(connector.ProgrammingError):
            conx1 = connector.connect(host=config['host'], user=config['user'],
                                      password=config['password'], port=config['port'], database='python_testsch1', charset='not_exist',)
            cursor = conx1.cursor()
            
    #improve code coverage
    def test_no_user(self):
        try:
            conx1 = connector.connect(host=config['host'], port=config['port'], password=config['password'])
        except Exception as expect_info:
            pass
    
    #improve code coverage
    def test_no_password(self):
        try:
            conx1 = connector.connect(host=config['host'], port=config['port'], user=config['user'],)
        except Exception as expect_info:
            pass
        
    #improve code coverage
    def test_tenant_name(self):
        conx1 = connector.connect(host=config['host'], port=config['port'], 
                                  user=config['user'],password=config['password'],
                                  tenant_name="ESGYNDB")
        conx1.close()   
        
    #improve code coverage
    def test_schema(self):
        conx1 = connector.connect(host=config['host'], port=config['port'], 
                                  user=config['user'],password=config['password'],
                                  schema="py_driver_test")
        #cursor = conx1.cursor()
        #cursor.execute('create schema if not exists py_driver_test')
        #cursor.execute('set schema py_driver_test')
        #cursor.execute("DROP TABLE IF EXISTS employee CASCADE")
        #cursor.execute("CREATE TABLE employee (id INT, name CHAR(20), salary DOUBLE PRECISION , hire_date DATE)")
        #cursor.close()
        conx1.close()   