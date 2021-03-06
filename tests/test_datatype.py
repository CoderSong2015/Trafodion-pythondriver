
import unittest
from decimal import Decimal

from pdbc.trafodion.connector import Connect
from .config import config


def _insert_query(tb_name, cols):
    insert = "INSERT INTO {table} ({columns}) values ({values})".format(
        table=tb_name,
        columns=','.join(cols),
        values=','.join(['?'] * len(cols))
    )
    return insert


class DataTypes(unittest.TestCase):

    tables = {
        'bit': 'type_test_bit',
        'int': 'type_test_int',
        'float': 'type_test_float',
        'decimal': 'type_test_decimal',
    }

    def drop_tables(self, conn):
        cur = conn.cursor()
        table_names = self.tables.values()
        for table_name in table_names:
            cur.execute("DROP TABLE IF EXISTS {table}".format(
                table=table_name)
            )


class TestsCursor(DataTypes):
    """
       def tearDown(self):
            self.conn = Connect(**self.config)
            self.drop_tables(self.conn)
            self.conn.close()
       """
    def setUp(self):
        self.config = config
        self.conn = Connect(**config)
        self.drop_tables(self.conn)

    @unittest.skip("no")
    def test_numeric_int(self):
        tb_name = self.tables['int']

        cur = self.conn.cursor()
        columns = [
            'smallint_signed',
            'smallint_unsigned',
            'int_signed',
            'int_unsigned',
            'bigint_signed',
            'bigint_unsigned',
        ]
        cur.execute((
            "CREATE TABLE {table} ("
            "smallint_signed SMALLINT SIGNED,"
            "smallint_unsigned SMALLINT UNSIGNED,"
            "int_signed INT SIGNED,"
            "int_unsigned INT UNSIGNED,"
            "bigint_signed largeint SIGNED,"
            "bigint_unsigned LARGEINT UNSIGNED)"
          ).format(table=tb_name)
        )

        data = [
            (
                -32768,  # smallint signed
                0,  # smallint unsigned
                -2147483648,  # int signed
                0,  # int unsigned
                -9223372036854775808,  # big signed
                0,  # big unsigned
            ),
            (
                32767,  # smallint signed
                65535,  # smallint unsigned
                2147483647,  # int signed
                4294967295,  # int unsigned
                9223372036854775807,  # big signed
                18446744073709551615,  # big unsigned
            )
        ]
        insert = _insert_query(tb_name, columns)
        for x in data:
            cur.execute(insert, x)

    @unittest.skip("no")
    def test_numeric_decimal(self):
        tb_name = self.tables['decimal']

        cur = self.conn.cursor()

        columns = [
            'decimal_signed',
            'decimal_unsigned',
        ]

        sql = ("CREATE TABLE {table} ("
               "decimal_signed DECIMAL(17,5) SIGNED,"
               "decimal_unsigned DECIMAL(8,7) UNSIGNED )"
               ).format(table=tb_name)

        print(sql)
        cur.execute(
            sql
        )

        insert = _insert_query(tb_name, columns)

        data = [
            (Decimal(
                '-9999999999.99999'),
             Decimal(
                 '+9.9999')),
            (Decimal('-1234567.1234'),
             Decimal('+0.126452')),
            (Decimal(
                '-1587.190'),
             Decimal(
                 '+1.190')),
        ]

        for x in data:
            cur.execute(insert, x)

    def test_numeric_float(self):
        tb_name = self.tables['float']

        cur = self.conn.cursor()
        columns = [
            'real_signed',
            'real_unsigned',
            'double_signed',
            'double_unsigned',
        ]
        cur.execute((
                        "CREATE TABLE {table} ("
                        "real_signed REAL ,"
                        "real_unsigned real ,"
                        "double_signed DOUBLE PRECISION ,"
                        "double_unsigned DOUBLE PRECISION )"
                    ).format(table=tb_name)
                    )

        insert = _insert_query(tb_name, columns)

        data = [
            (-3.402823466, 0.0, -1.7976931348623157, 0.0,),
            (-1.175494351, 3.402823466,
             1.7976931348623157, 2.2250738585072014),
            (-1.23455678, 2.999999, -1.3999999999999999, 1.9999999999999999),
        ]

        for x in data:
            cur.execute(insert, x)



