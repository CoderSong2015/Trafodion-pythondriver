

class DataTypes():

    tables = {
        'bit': 'myconnpy_mysql_bit',
        'int': 'myconnpy_mysql_int',
        'bool': 'myconnpy_mysql_bool',
        'float': 'myconnpy_mysql_float',
        'decimal': 'myconnpy_mysql_decimal',
        'temporal': 'myconnpy_mysql_temporal',
        'temporal_year': 'myconnpy_mysql_temporal_year',
        'set': 'myconnpy_mysql_set',
    }

    def drop_tables(self):

        table_names = self.tables.values()
        print("DROP TABLE IF EXISTS {tables}".format(
            tables=','.join(table_names))
            )
if __name__ == '__main__':
    a = DataTypes()
    a.drop_tables()