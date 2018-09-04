import unittest
import datetime
from pdbc.trafodion import connector
from .config import config

class TestCursorObject(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.testdata = [
            (1, 'Manager A', 2.0, datetime.date(2008, 7, 16)),
            (2, 'Manager B', 2.1, datetime.date(2008, 7, 16)),
            (3, 'Dev A', 1.9, datetime.date(2018, 7, 16)),
            (4, 'Dev B', 1.8, datetime.date(2018, 7, 16)),
            (5, 'Dev C', 1.7, datetime.date(2018, 7, 16))
        ]
        cls.cnx = connector.connect(**config)
        cursor = cls.cnx.cursor()
        cursor.execute("DROP TABLE IF EXISTS employee CASCADE")
        cursor.execute("CREATE TABLE employee (id INT, name CHAR(20), salary DOUBLE PRECISION , hire_date DATE)")
        query = "INSERT INTO employee VALUES (?, ?, ?, ?)"
        cursor.executemany(query, cls.testdata)
        cursor.close()
    
    @classmethod
    def tearDownClass(cls):
        #cursor = cls.cnx.cursor()
        #cursor.execute("DROP TABLE employee CASCADE")
        cls.cnx.close()

    def test_description_before_operation(self):
        cursor = self.cnx.cursor()
        self.assertEqual(cursor.description, None)

    # TODO: expected result has no defination?
    def test_description_of_all_support_type(self):
        # col_name | col_def | expect_col_name | expect_type_code |
        #             expected_internal_size | expected_precision | scale | null_ok
        table_name = 'test_all_datatype'
        matrix = [
            ('c_char',      'char(10)'),
            ('c_nchar',     'nchar(10)'),
            ('c_varchar',   'varchar(10)'),
            ('c_nvarchar',  'nchar varying(10)'),
            ('c_date',       'date'),
            ('c_time',       'time'),
            ('c_time_with_precision', 'time(2)'),
            ('c_timestamp', 'timestamp'),
            ('c_timestamp_with_precision', 'timestamp(3)'),
            ('c_interval_year', 'interval year'),
            ('c_interval_year_month', 'interval year to month'),
            ('c_interval_day', 'interval day'),
            ('c_interval_hour', 'interval hour'),
            ('c_interval_minute', 'interval minute'),
            ('c_interval_second', 'interval second'),
            ('c_blob', 'blob'),
            ('c_clob', 'clob'),
            ('c_numeric', 'numeric(19, 0)'),
            ('c_numeric_precision', 'numeric(128)'),
            ('c_float', 'float'),
            ('c_real', 'real'),
            ('c_double', 'double precision'),
            ('c_decimal', 'decimal(18, 2)')
        ]

        ddlstr = f'create table {table_name}('
        for i, co in enumerate(matrix):
            ddlstr += co[0] + ' ' + co[1]
            if (i != len(matrix) - 1):
                ddlstr += ', '
        ddlstr += ')'

        cnx = connector.connect(**config)
        cursor = cnx.cursor()
        cursor.execute(f'drop table if exists {table_name} cascade')
        cursor.execute(ddlstr)
        cursor.execute(f'select * from {table_name}')
        desc = cursor.description
        # print('\n')
        # print('Name                    | TypeCode | DisplaySize | InternalSize | Precision | Scale | Null_ok\n')
        # for d in desc:
        #     print('%-30s, %10d, %10d, %12d, %10d, %8d, %s'% (d[0], d[1], d[2], d[3], d[4], d[5], d[6]))
        
        cursor.close()
        cnx.close()

    def test_description_no_rows(self):
        cursor = self.cnx.cursor()
        cursor.execute("DROP TABLE IF EXISTS test_description CASCADE")
        cursor.execute("CREATE TABLE test_description (id INT, name CHAR(10))")
        cursor.execute("INSERT INTO test_description VALUES (1, 'jim')")
        cursor.execute('select * from test_description')
        self.assertIsNotNone(cursor.description)
        cursor.execute("update test_description set name='jack' where id = 1")
        self.assertIsNone(cursor.description)
        cursor.execute("DELETE FROM test_description WHERE id = 1")
        cursor.execute("DROP TABLE test_description")
        cursor.close()

    def test_rowcount_before_execute(self):
        cursor = self.cnx.cursor()
        self.assertEqual(cursor.rowcount, -1)
        cursor.close()

    def test_rowcount_after_execute(self):
        # table 'employee' has 5 records
        cnx = connector.connect(**config)
        cursor = cnx.cursor()
        cursor.execute('select * from employee')
        self.assertEqual(cursor.rowcount, 5)
        cursor.close()
        cnx.close()

    def test_rowcount_after_DML(self):
        cursor = self.cnx.cursor()
        cursor.execute('update employee set salary = 2.2 where id < 3')
        self.assertEqual(cursor.rowcount, 2)
        cursor.close()

    # TODO: need to add some SPJ function on server side.
    @unittest.skip('not support yet')
    def test_callproc(self):
        pass

    def test_close(self):
        cursor = self.cnx.cursor()
        cursor.execute("SELECT * FROM employee")
        self.assertTrue((1, 'Manager A', 2.0, datetime.date(2008, 7, 16)) in
                      [(id, name.strip(), salary, hire_date) for id, name, salary, hire_date in cursor])
        cursor.reset()
        cursor.close()
        with self.assertRaises(connector.Error):
            cursor.execute("SELECT * FROM employee")

    def test_execute_with_parameters(self):
        cursor = self.cnx.cursor()
        cursor.execute("DROP TABLE IF EXISTS test_execute_with_parameters CASCADE")
        cursor.execute("CREATE TABLE test_execute_with_parameters (id INT, name CHAR(20), salary DOUBLE PRECISION , hire_date DATE)")
        query = "INSERT INTO test_execute_with_parameters VALUES (?, ?, ?, ?)"
        testdata = [
            (1, 'Manager A', 2.0, datetime.date(2008, 7, 16)),
            (2, 'Manager B', 2.1, datetime.date(2008, 7, 16)),
            (3, 'Dev A', 1.9, datetime.date(2018, 7, 16)),
            (4, 'Dev B', 1.8, datetime.date(2018, 7, 16)),
            (5, 'Dev C', 1.7, datetime.date(2018, 7, 16))
        ]
        cursor.executemany(query, testdata)
        
        query = "SELECT * FROM test_execute_with_parameters WHERE hire_date BETWEEN ? AND ?"
        hire_start = datetime.date(1991, 1, 1)
        hire_end = datetime.date(2018, 7, 16)
        cursor.execute(query, (hire_start, hire_end))
        self.assertTrue((5, 'Dev C', 1.7, datetime.date(2018, 7, 16)) in
                        [(id, name.strip(), salary, hire_date) for id, name, salary, hire_date in cursor])

        cursor.reset()
        query = "INSERT INTO test_execute_with_parameters VALUES (?, ?, ?, ?)"
        cursor.execute(query, (6, 'Tester', 1.7, datetime.date(2018, 7, 16)))
        self.assertEqual(cursor.rowcount, 1)

        query = "UPDATE test_execute_with_parameters SET salary = 1.8 WHERE name = ?"
        cursor.execute(query, ('Tester'))
        self.assertEqual(cursor.rowcount, 1)

        query = "DELETE FROM test_execute_with_parameters WHERE id = ?"
        cursor.execute(query, (6))
        self.assertEqual(cursor.rowcount, 1)

        cursor.close()

    def test_executemany(self):
        cursor = self.cnx.cursor()

        data = [
            (11, 'Jane', 1.9, datetime.date(2005, 2, 12)),
            (12, 'Joe', 1.9, datetime.date(2006, 5, 23)),
            (13, 'John', 1.9, datetime.date(2010, 10, 3)),
        ]

        stmt = "INSERT INTO employee VALUES (?, ?, ?, ?)"
        cursor.executemany(stmt, data)

        query = "SELECT * FROM employee"
        cursor.execute(query)

        res = [(id, name.strip(), salary, hire_date) for id, name, salary, hire_date in cursor]

        for (id, name, salary, hire_date) in data:
            self.assertTrue((id, name, salary, hire_date) in res)

        cursor.execute("DELETE FROM employee WHERE id>=11 and id<=13")

        cursor.close()

    def test_fetchone(self):
        cursor = self.cnx.cursor()

        query = "SELECT * FROM employee ORDER BY id"
        cursor.execute(query)
        one = cursor.fetchone()
        self.assertEqual((one[0], one[1].strip(), one[2], one[3]), (1, 'Manager A', 2.0, datetime.date(2008, 7, 16)))
        two = cursor.fetchone()
        self.assertEqual((two[0], two[1].strip(), two[2], two[3]), (2, 'Manager B', 2.1, datetime.date(2008, 7, 16)))
        cursor.reset()
        cursor.close()

    def test_fetchmany(self):
        cursor = self.cnx.cursor()
        query = "SELECT * FROM employee ORDER BY id"
        cursor.execute(query)
        many = cursor.fetchmany(3)
        many = [(id, name.strip(), salary, hire_date) for id, name, salary, hire_date in many]
        expected = self.testdata[:3]
        self.assertEqual(many, expected)
        cursor.reset()
        cursor.close()

    def test_fetchall(self):
        cursor = self.cnx.cursor()
        query = "SELECT * FROM employee ORDER BY id"
        cursor.execute(query)
        all = cursor.fetchall()
        all = [(id, name.strip(), salary, hire_date) for id, name, salary, hire_date in all]
        self.assertEqual(all, self.testdata)
        cursor.close()

    @unittest.skip('not support yet')
    def test_nextset(self):
        pass

    def test_arraysize_property(self):
        cursor = self.cnx.cursor()
        cursor.arraysize = 3
        cursor.execute('SELECT * FROM employee ORDER BY id')
        many = cursor.fetchmany()
        many = [(id, name.strip(), salary, hire_date) for id, name, salary, hire_date in many]
        expected = self.testdata[:3]
        self.assertEqual(many, expected)
        cursor.close()

    @unittest.skip("unsupported")
    def test_setinputsizes(self):
        cursor = self.cnx.cursor()
        cursor.setinputsizes(connector.NUMBER, connector.STRING)
        self.assertTrue(hasattr(cursor, 'setinputsizes'))
        cursor.close()

    @unittest.skip("unsupported")
    def test_setoutputsize(self):
        cursor = self.cnx.cursor()
        cursor.setoutputsize(connector.NUMBER, 1)
        self.assertTrue(hasattr(cursor, 'setoutputsize'))
        cursor.close()