import unittest
from pdbc.trafodion import connector
from .config import config

class TestConnectionObject(unittest.TestCase):
    def getId(self):
        if not hasattr(self, 'testID'):
            self.testID = 0
        self.testID += 1
        return  self.testID

    def setUp(self):
        self.cnx = connector.connect(**config)
        self.cursor = self.cnx.cursor()
        self.cursor.execute('create table if not exists bank_account(id int, money float)')

    def tearDown(self):
        self.cursor.execute('drop table bank_account cascade')
        self.cursor.close()
        self.cnx.close()

    def test_normal_close(self):
        try:
            cnx = connector.connect(**config)
            cursor = cnx.cursor()
            cnx.close()
        except Exception:
            self.fail('test_normal_close() raise Exception unexpectedly')

    def test_close_closed_connection(self):
        cnx = connector.connect(**config)
        cnx.close()
        with self.assertRaises(connector.OperationalError):
            cursor = self.cnx.cursor()

    def test_close_after_execute(self):
        try:
            cnx = connector.connect(**config)
            cursor = cnx.cursor()
            cursor.execute('insert into bank_account values(?, ?)', (self.getId(), 123.456))
            cnx.close()
        except Exception:
            self.fail('test_close_after_execute() raised Exception unexpectedly!')

    def test_close_uncommit_connection(self):
        try:
            cnx = connector.connect(**config)
            cnx.set_auto_commit(False)
            cursor = cnx.cursor()
            cursor.execute('insert into bank_account values(?, ?)', (self.getId(), 123.456))
            cnx.close()
        except Exception:
            self.fail('test_close_after_execute() raised Exception unexpectedly!')

    def test_close_connection_with_opened_cursor(self):
        try:
            self.cursor.execute('get tables')
            self.cnx.close()
        except Exception:
            self.fail('test_close_connection_with_opened_cursor() raise exception unexpectedly')

    def test_default_auto_commit(self):
        self.assertTrue(self.cnx._auto_commit is False)

    def test_normal_commit(self):
        """
        read committed level check.
        """
        row = (self.getId(), 2345.678)

        cnx = connector.connect(**config)
        cnx.set_auto_commit(False)
        cursor = cnx.cursor()
        cursor.execute('insert into bank_account values(?,?)', (id, money))
        cnx.commit()
        cnx.close()
        self.cnx.execute('select * from bank_account where id=?', (id,))
        res = self.cnx.fetchone()
        self.assertEqual(res, row)

    def test_auto_commit_true(self):
        row1 = (self.getId(), 2345.678)

        cnx = connector.connect(**config)
        cnx.set_auto_commit(True)
        cursor = cnx.cursor()
        cursor.execute('insert into bank_account values(?, ?)', row1)
        cnx.close()
        self.cursor.execute('select * from bank_account where id = ?', (row1[0],))
        res = self.cursor.fetchall()
        self.assertTrue(row1 in res)

    def test_commit_after_commit(self):
        row1 = (self.getId(), 789.12345)
        row2 = (self.getId(), 89123.456)

        cnx = connector.connect(**config)
        cnx.set_auto_commit(False)
        cursor = cnx.cursor()
        cursor.execute('insert into bank_account values(?, ?)', row1)
        cnx.commit()
        cursor.execute('insert into bank_account values(?, ?)', row2)
        cnx.commit()
        cnx.close()
        self.cursor.execute('select * from bank_account')
        results = self.cursor.fetchall()
        self.assertTrue(row1 in results)
        self.assertTrue(row2 in results)

    def test_commit_on_closed_connection(self):
        row1 = (self.getId(), 789.12345)

        cnx = connector.connect(**config)
        cnx.set_auto_commit(False)
        cursor = cnx.cursor()
        cursor.execute('insert into bank_account values(?, ?)', row1)
        cnx.close()
        with self.assertRaises(connector.Error):
            cnx.commit()

    def test_normal_rollback(self):
        row1 = (self.getId(), 1.23456)

        cnx = connector.connect(**config)
        cnx.set_auto_commit(False)
        cursor = cnx.cursor()
        cursor.execute('insert into bank_account values(?, ?)', row1)
        cnx.rollback()
        cnx.close()
        self.cursor.execute('select * from bank_account where id = ?', (row1[0],))
        resultset = self.cursor.fetchall()
        # result set should empty
        self.assertTrue(len(resultset) is 0)

    def test_rollback_rollback(self):
        row1 = (self.getId(), 1.23456)

        cnx = connector.connect(**config)
        cnx.set_auto_commit(False)
        cursor = cnx.cursor()
        cursor.execute('insert into bank_account values(?, ?)', row1)
        cnx.rollback()
        cnx.rollback()
        cnx.commit()
        cnx.close()
        self.cursor.execute('select * from bank_account where id=?', (row1[0],))
        results = self.cursor.fetchall()
        self.assertTrue(len(results) is 0)

    def test_rollback_closed_connection(self):
        row1 = (self.getId(), 1.23456)

        cnx = connector.connect(**config)
        cnx.set_auto_commit(False)
        cursor = cnx.cursor()
        cursor.execute('insert into bank_account values(?, ?)', row1)
        cnx.close()

        with self.assertRaises(connector.Error):
            cnx.rollback()

    def test_normal_cursor(self):
        cursor = self.cnx.cursor()
        self.assertTrue(hasattr(cursor, 'execute'))

    def test_get_cursor_from_closed_connection(self):
        cnx = connector.connect(**config)
        cnx.close()
        with self.assertRaises(connector.Error):
            cursor = cnx.cursor()

    def test_multi_cursor_one_connection(self):
        row1 = (self.getId(), 1.2345)
        row2 = (self.getId(), 2.3456)

        cnx = connector.connect(**config)
        cnx.set_auto_commit(True)
        cursor1 = cnx.cursor()
        cursor2 = cnx.cursor()

        cursor1.execute('insert into bank_account values(?, ?)', row1)
        cursor2.execute('insert into bank_account values(?, ?)', row2)

        cursor1.execute('select * from bank_account where id=?', (row2[0],))
        cursor2.execute('select * from bank_account where id=?', (row1[0],))

        result1 = cursor1.fetchall()
        result2 = cursor2.fetchall()

        self.assertTrue(row1 in result2)
        self.assertTrue(row2 in result1)

