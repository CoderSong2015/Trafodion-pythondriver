import unittest
from pdbc.trafodion import connector
import datetime
import time
import random
import string
from pdbc.trafodion.connector.constants import FIELD_TYPE
from .config import config

class TestTypeObject(unittest.TestCase):

    def test_Date(self):
        d1 = connector.Date(2018, 1, 1)
        self.assertTrue(type(d1) is datetime.date)

    # TODO: should Time support time precision?
    def test_Time(self):
        t1 = connector.Time(10, 50, 20)
        self.assertTrue(type(t1) is datetime.time)

    def test_Timestamp(self):
        ts1 = connector.Timestamp(2018, 8, 16)
        self.assertTrue(type(ts1) is datetime.datetime)

    def test_DateFromTicks(self):
        try:
            ts1 = connector.DateFromTicks(time.mktime(time.localtime()))
        except Exception:
            self.fail('test_DateFromTicks raised unexpected exception')

    def test_TimeFromTicks(self):
        try:
            ts1 = connector.TimeFromTicks(time.mktime(time.localtime()))
        except Exception:
            self.fail('test_DateFromTicks raised unexpected exception')

    def test_TimestampFromTicks(self):
        try:
            ts1 = connector.TimestampFromTicks(time.mktime(time.localtime()))
        except Exception:
            self.fail('test_DateFromTicks raised unexpected exception')

    def test_Binary(self):
        try:
            bin = connector.Binary(''.join(random.choices(string.ascii_letters + string.digits, k=20000)).encode())
        except Exception:
            self.fail('test_Binary raised unexpected exception')

    # TODO: how to use this type?
    def test_STRING(self):
        self.assertTrue(hasattr(connector, 'STRING'))

    def test_BINARY(self):
        self.assertTrue(hasattr(connector, 'BINARY'))

    def test_NUMBER(self):
        self.assertTrue(hasattr(connector, 'NUMBER'))

    def test_DATETIME(self):
        self.assertTrue(hasattr(connector, 'DATETIME'))

    def test_ROWID(self):
        self.assertTrue(hasattr(connector, 'ROWID'))
        
    #improve code coverage
    def test_DBAPISet(self):
        self.assertTrue(connector.STRING==connector.STRING)
        self.assertTrue(connector.STRING==FIELD_TYPE.SQLTYPECODE_CHAR)
        self.assertTrue(connector.STRING!=connector.NUMBER)
        self.assertTrue(hash(connector.STRING)==hash(connector.STRING))
    
    #improve code coverage
    def test_insert_not_list_and_tuple(self):
        cnx=connector.connect(**config)
        cursor = cnx.cursor()
        cursor.execute("DROP TABLE IF EXISTS employee1 CASCADE")
        cursor.execute("CREATE TABLE employee1 (id INT, name CHAR(20), salary DOUBLE PRECISION , hire_date DATE)")
        query = "INSERT INTO employee1 VALUES (?, ?, ?, ?)"
        with self.assertRaises(connector.DataError):
            cursor.execute(query, 100)

        with self.assertRaises(connector.DataError):
            cursor.executemany(query, 100)
        cursor.close()
        cnx.close()
        