import unittest
import datetime
from pdbc.trafodion import connector
from .config import config

class TestCursorObject(unittest.TestCase):
    def setUp(self):
        self.testdata = [
            (1, 'Manager A', 2.0, datetime.date(2008, 7, 16)),
            (2, 'Manager B', 2.1, datetime.date(2008, 7, 16)),
            (3, 'Dev A', 1.9, datetime.date(2018, 7, 16)),
            (4, 'Dev B', 1.8, datetime.date(2018, 7, 16)),
            (5, 'Dev C', 1.7, datetime.date(2018, 7, 16))
        ]
        self.cnx = connector.connect(**config)
        cursor = self.cnx.cursor()
        cursor.execute("DROP TABLE IF EXISTS employee CASCADE")
        cursor.execute("CREATE TABLE employee (id INT, name CHAR(20), salary DOUBLE PRECISION , hire_date DATE)")
        query = "INSERT INTO employee VALUES (%s, %s, %s, %s)"
        cursor.executemany(query, self.testdata)
        cursor.close()

    def tearDown(self):
        cursor = self.cnx.cursor()
        cursor.execute("DROP TABLE employee CASCADE")
        self.cnx.close()

    @unittest.skip("not support yet")
    def test_description_before_operation(self):
        cursor = self.cnx.cursor()
        self.assertEqual(cursor.name, None)
        self.assertEqual(cursor.type_code, None)
        self.assertEqual(cursor.display_size, None)
        self.assertEqual(cursor.internal_size, None)
        self.assertEqual(cursor.precision, None)
        self.assertEqual(cursor.scale, None)
        self.assertEqual(cursor.null_ok, None)

    # def test_rowcount_before_execute(self):
    #     cursor = self.cnx.cursor()
    #     self.assertEqual(cursor.rowcount, -1)

    # TODO: need to add some SPJ function on server side.
    # def test_callproc(self):
    #     pass

    def test_close(self):
        cursor = self.cnx.cursor()
        cursor.execute("SELECT * FROM employee")
        self.assertIn((1, 'Manager A', 2.0, datetime.date(2008, 7, 16)), cursor)
        cursor.reset()
        cursor.close()
        with self.assertRaises(connector.Error):
            cursor.execute("SELECT * FROM employee")

    def test_execute_normal(self):
        cursor = self.cnx.cursor()
        cursor.execute("DROP TABLE IF EXISTS test_execute CASCADE")
        cursor.execute("CREATE TABLE test_execute(id int, name char(20))")
        self.assertEqual(cursor.rowcount, 0)
        cursor.execute("INSERT INTO test_execute VALUES (1, 'hello')")
        self.assertEqual(cursor.rowcount, 1)
        cursor.execute("INSERT INTO test_execute VALUES (2, 'Trafodion')")
        self.assertEqual(cursor.rowcount, 1)
        cursor.execute("SELECT * FROM test_execute")
        # self.assertEqual(cursor.rowcount, 2)
        self.assertIn((1, 'hello'), cursor)
        self.assertIn((2, 'Trafodion'), cursor)
        cursor.execute("UPDATE test_execute SET id = 3 where id = 2")
        self.assertEqual(cursor.rowcount, 1)
        cursor.execute("DROP TABLE test_execute CASCADE")
        self.assertEqual(cursor.rowcount, 0)
        self.assertEqual(1, 1)
        cursor.close()

    def test_execute_with_parameters(self):
        cursor = self.cnx.cursor()

        query = "SELECT * FROM employee WHERE hire_date BETWEEN %s AND %s"
        hire_start = datetime.date(1991, 1, 1)
        hire_end = datetime.date(2018, 7, 16)

        cursor.execute(query, (hire_start, hire_end))
        self.assertIn((5, 'Dev C', 1.7, datetime.date(2018, 7, 16)), cursor)

        cursor.reset()
        query = "INSERT INTO employee VALUES (%s, %s, %s, %s)"
        cursor.execute(query, (6, 'Tester', 1.7, datetime.date(2018, 7, 16)))
        self.assertEqual(cursor.rowcount, 1)

        # query = "UPDATE employee SET salary = 1.8 WHERE name = %s"
        # cursor.execute(query, ('Tester'))
        # self.assertEqual(cursor.rowcount, 1)
        #
        # query = "DELETE FROM employee WHERE id = %s"
        # cursor.execute(query, (6))
        # self.assertEqual(cursor.rowcount, 1)

        cursor.close()

    def test_executemany(self):
        cursor = self.cnx.cursor()

        data = [
            (11, 'Jane', 1.9, datetime.date(2005, 2, 12)),
            (12, 'Joe', 1.9, datetime.date(2006, 5, 23)),
            (13, 'John', 1.9, datetime.date(2010, 10, 3)),
        ]

        stmt = "INSERT INTO employee VALUES (%s, %s, %s, %s)"
        cursor.executemany(stmt, data)

        query = "SELECT * FROM employee"
        cursor.execute(query)

        for (id, name, salary, hire_date) in data:
            self.assertIn((id, name, salary, hire_date), cursor)

        cursor.close()

    def test_fetchone(self):
        cursor = self.cnx.cursor()

        query = "SELECT * FROM employee ORDER BY id"
        cursor.execute(query)
        one = cursor.fetchone()
        self.assertEqual(one, (1, 'Manager A', 2.0, datetime.date(2008, 7, 16)))
        two = cursor.fetchone()
        self.assertEqual(two, (2, 'Manager B', 2.1, datetime.date(2008, 7, 16)))
        cursor.reset()
        cursor.close()

    def test_fetchmany(self):
        cursor = self.cnx.cursor()
        query = "SELECT * FROM employee ORDER BY id"
        cursor.execute(query)
        many = cursor.fetchmany(3)
        expected = self.testdata[:3]
        self.assertEqual(many, expected)
        cursor.reset()
        cursor.close()

    def test_fetchall(self):
        cursor = self.cnx.cursor()
        query = "SELECT * FROM employee ORDER BY id"
        cursor.execute(query)
        all = cursor.fetchall()
        self.assertEqual(all, self.testdata)
        cursor.close()

    # TODO: does Trafodion support multi-set?
    # def test_nextset(self):
    #     pass

    def test_arraysize_property(self):
        cursor = self.cnx.cursor()
        self.assertTrue(hasattr(cursor, 'arraysize'))
        # cursor.arraysize = 3
        # query = "SELECT * FROM employee ORDER BY id"
        # cursor.execute(query)
        # many = cursor.fetchmany(2)
        # expected = self.testdata[:2]
        # print(many)
        # print(expected)
        # self.assertEqual(many, expected)
        # self.assertEqual(cursor.arraysize, 2)
        # cursor.close()

    # TODO: unsupported feature
    @unittest.skip("unsupported")
    def test_setinputsize(self):
        pass

    # TODO: unsupported feature
    @unittest.skip("unsupported")
    def test_setoutputsize(self):
        pass