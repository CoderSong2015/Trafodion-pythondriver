import unittest
from pdbc.trafodion import connector
from .config import config

class TestConnectionObject(unittest.TestCase):
    def setUp(self):
        self.cnx = connector.connect(user=config['user'], password=config['password'], database=config['database'])
        self.cursor = self.cnx.cursor()
        self.cursor.execute('create table bank_account(id int, money float)')

    def tearDown(self):
        self.cursor.execute('drop table bank_account cascade')
        self.cursor.close()
        self.cnx.close()

    def test_close(self):
        self.cnx.close()
        with self.assertRaises(connector.OperationalError):
            cursor = self.cnx.cursor()

    # TODO: do this test, we need to set auto commit off first.
    @unittest.skip("don't know how to set auto commit off yet")
    def test_commit(self):
        """
        read committed level check.
        """
        cnx2 = connector.connect(user=config['user'], password=config['password'])
        cnx2.close()

    # TODO: to do this test, we need to set auto commit off first.
    @unittest.skip("don't know how to set auto commit off yet")
    def test_rollback(self):
        pass

    def test_cursor(self):
        cursor = self.cnx.cursor()
        self.assertTrue(hasattr(cursor, 'execute'))