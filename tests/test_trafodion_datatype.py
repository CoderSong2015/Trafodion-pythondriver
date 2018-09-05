import unittest
import logging
from decimal import Decimal

from pdbc.trafodion.connector import *
from .config import config

from pdbc.trafodion import connector

import datetime
import time
#from datetime import date, timedelta, time

logging.basicConfig(filename='DataTypeTest.log',format='[%(asctime)s][%(filename)s line:%(lineno)d][%(levelname)s]:%(message)s', level = logging.DEBUG,filemode='w',datefmt='%Y-%m-%d %H:%M:%S')

def _create_table(tb_name, cols):
    create_tb_str = "create table {table} ({columns})".format(
        table=tb_name,
        columns=','.join(cols)
    )
    return create_tb_str

def _insert_query(tb_name, cols):
    insert = "INSERT INTO {table} ({columns}) values ({values})".format(
        table=tb_name,
        columns=','.join(cols),
        values=','.join(['?'] * len(cols))
    )
    return insert

def _get_select_stmt(tbl, cols):
    #select = "SELECT {columns} FROM {table} ORDER BY id".format(
    select = "SELECT {columns} FROM {table}".format(
        columns=','.join(cols),
        table=tbl
        )
    return select

def _insert_query_with_id(tb_name, cols):
    insert = "INSERT INTO {table} (id,{columns}) values (?,{values})".format(
        table=tb_name,
        columns=','.join(cols),
        values=','.join(['?'] * len(cols))
    )
    return insert

def _get_select_stmt_with_id(tbl, cols, id_begin=0):
    select = "SELECT {columns} FROM {table} where id >= {id_num} ORDER BY id".format(
        columns=','.join(cols),
        table=tbl,
        id_num=id_begin
        )
    print(select)
    return select

def WriteLog(msg, case_begin_flag=False):
    if(case_begin_flag == True):
        logging.debug("==================================Begin Case=====================================")
    logging.debug(msg)

class DataTypes(unittest.TestCase):

    tables = {
        'bit': 'type_test_bit',
        'int': 'type_test_int',
        'float': 'type_test_float',
        'real': 'type_test_real',
        'double': 'type_test_double',
        'decimal': 'type_test_decimal',
        'numeric': 'type_test_numeric',
        'string': 'type_test_string',
        'varchar': 'type_test_varchar',
        'char': 'type_test_char',
        'temporal': 'type_test_temporal',
        'time': 'type_test_time',
        'time_precision': 'type_test_time_precision',
        'date': 'type_test_date',
        'timestamp': 'type_test_timestamp',
        'timestamp_precision': 'type_test_timestamp_precision',
    }

    '''
    def drop_tables(self, conn):
        cur = conn.cursor()
        table_names = self.tables.values()
        for table_name in table_names:
            cur.execute("DROP TABLE IF EXISTS {table}".format(
                table=table_name)
            )
    '''
    
    @classmethod
    def drop_tables(cls):
        cur = cls.conn.cursor()
        table_names = cls.tables.values()
        for table_name in table_names:
            cur.execute("DROP TABLE IF EXISTS {table}".format(
                table=table_name)
            )
        cur.close()

class TestTrafDataType(DataTypes):
    def tearDown(self):
        #self.conn = Connect(**self.config)
        #self.drop_tables(self.conn)
        #self.conn.close()
        pass
        
            
    def setUp(self):
        #self.config = config
        #self.conn = Connect(**config)
        
        #self.drop_tables(self.conn)
        
        pass
            
    @classmethod
    def tearDownClass(cls):
        #super(TestTrafDataType, cls).tearDownClass()
        cls.conn.close()
        
    @classmethod
    def setUpClass(cls):
        #super(TestTrafDataType, cls).setUpClass()
        cls.config = config
        cls.conn = Connect(**config)
        cursor = cls.conn.cursor()
        cursor.execute('create schema if not exists py_driver_test')
        cursor.execute('set schema py_driver_test')
        cursor.close()
        cls.drop_tables()

    def compare(self, name, val1, val2):
        WriteLog( "name:%s, expect:%s, result:%s" %(name, val1, val2))
        self.assertEqual(val1, val2, "%s  %s != %s" % (name, val1, val2))

    #@unittest.skip("no")
    def test_numeric_int(self):
        tb_name = self.tables['int']

        cur = self.conn.cursor()
        
        cur.execute("DROP TABLE IF EXISTS %s" % tb_name)
        
        success_flag = True
        
        try:
            cur.execute((
                "CREATE TABLE {table} ("
                "tinyint_signed TINYINT SIGNED,"
                "tinyint_unsigned TINYINT UNSIGNED,"
                "smallint_signed SMALLINT SIGNED,"
                "smallint_unsigned SMALLINT UNSIGNED,"
                "int_signed INT SIGNED,"
                "int_unsigned INT UNSIGNED,"
                "bigint_signed largeint SIGNED,"
                "bigint_unsigned LARGEINT UNSIGNED)"
              ).format(table=tb_name)
            )
        except Exception as err:
            WriteLog( 'Exception:'+str(err))
            raise
        
        
        columns = [
            'tinyint_signed',
            'tinyint_unsigned',
            'smallint_signed',
            'smallint_unsigned',
            'int_signed',
            'int_unsigned',
            'bigint_signed',
            'bigint_unsigned',
        ]
        
        data = [
            (
                -128,  # tinyint signed
                0,  # tinyint unsigned
                -32768,  # smallint signed
                0,  # smallint unsigned
                -2147483648,  # int signed
                0,  # int unsigned
                -9223372036854775808,  # big signed
                0,  # big unsigned
            ),
            (
                100,    # tinyint signed
                100,    # tinyint unsigned
                100,  # smallint signed
                100,  # smallint unsigned
                100,  # int signed
                100,  # int unsigned
                100,  # big signed
                100,  # big unsigned
            ),
            (
                127,    # tinyint signed
                255,    # tinyint unsigned
                32767,  # smallint signed
                65535,  # smallint unsigned
                2147483647,  # int signed
                4294967295,  # int unsigned
                9223372036854775807,  # big signed
                18446744073709551615,  # big unsigned
            ),
        ]
        
        insert = _insert_query(tb_name, columns)
        WriteLog( insert)
        
        
        i=0
        err_flag=False
        for x in data:
            WriteLog( "<Begin Case>:" + insert + str(x), True)
            try:
                cur.execute(insert, x)
            except Exception as err:
                err_flag=True
                WriteLog( str(err))
            finally:
                if(err_flag==False):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                    
                WriteLog( "<End Case>:" + insert + str(x))
                err_flag=False
                
            i = i + 1
            #WriteLog( "%d, %s"%(i, str(x)))
                
        select = _get_select_stmt(tb_name, columns)
        cur.execute(select)
        #WriteLog( select)
            
        i=0
        err_flag=False
        for i in range(len(data)):
            row = cur.fetchone()
            WriteLog("Fetch one row:"+str(row))
            if(row == None):
                break
            for j, col in enumerate(columns):
                WriteLog( "<Begin Case>:test column {0}".format(j), True)
                try:
                    WriteLog( "###"+str(row[j])+"###")
                    self.compare(col, data[i][j], row[j])
                    WriteLog( str(type(row[j])))
                except Exception as err:
                    WriteLog( 'Unexpected Exception:' + str(err))
                    err_flag=True
                finally:
                    if(err_flag==False):
                        WriteLog( "[+++]Test Success")
                    else:
                        WriteLog( "[---]Test Fail")
                        success_flag = False
                    WriteLog( "<End Case>")
                    err_flag=False
        
        
        single_column = [
            ('tinyint_signed',),
            ('tinyint_unsigned',),
            ('smallint_signed',),
            ('smallint_unsigned',),
            ('int_signed',),
            ('int_unsigned',),
            ('bigint_signed',),
            ('bigint_unsigned',),
        ]

        invalid_data = [
            [(-129,),(128,) ],
            [(-1,),(256,) ],
            [(-32769,),(32768,) ],
            [(-1,),(65536,) ],
            [(-2147483649,),(2147483648,) ],
            [(-1,),(4294967296,) ],
            [(-9223372036854775809,),(9223372036854775808,) ],
            [(-1,),(18446744073709551616,) ],
        ]
        
        for index,column_name in enumerate(single_column):
            insert_pre = _insert_query(tb_name, column_name)
            WriteLog( insert_pre)
                
            expect_err_flag=False
            for insert_data in invalid_data[index]:
                WriteLog( "<Begin Case>:" + insert_pre + str(insert_data), True)
                try:
                    cur.execute(insert_pre, insert_data)
                except Exception as err:
                    expect_err_flag=True
                    WriteLog( str(err))
                finally:
                    if(expect_err_flag==True):
                        WriteLog( "[+++]Test Success")
                    else:
                        WriteLog( "[---]Test Fail")
                        success_flag = False
                        
                    WriteLog( "<End Case>:" + insert_pre + str(insert_data))
                    expect_err_flag=False    
        
        cur.close()
        
        self.assertTrue(success_flag==True, 'Data type case fail, please check the log in file')
        
       
    #@unittest.skip("no")
    def test_numeric_decimal(self):
        tb_name = self.tables['decimal']

        cur = self.conn.cursor()
        
        cur.execute("DROP TABLE IF EXISTS %s" % tb_name)
        
        success_flag = True
        
        tab_str_begind = ("create table %s (" % tb_name)
        tab_str_end = ")";

        
        columns_definition = [
            "col_1 decimal(-1,0)",
            "col_1 decimal(0,-1)",
            "col_1 decimal(0,0)",
            "col_1 decimal(1,-1)",
            "col_1 decimal(-1,-2)",
            "col_1 decimal(10,11)",
            "col_1 decimal(19,0)",
        ]
        
        expect_err_flag = False
        
        col = 0
        for col in columns_definition:
            create_tb_str = tab_str_begind + col + tab_str_end
            WriteLog( "<Begin Case>:" + create_tb_str, True)
            
            try:
                cur.execute(create_tb_str)
            except Exception as out_info:
                WriteLog( 'Expected Exception:' + str(out_info))
                expect_err_flag = True
            finally:
                if(expect_err_flag == True):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                WriteLog( "<End Case>:" + create_tb_str)
                expect_err_flag = False
        
        

        sql = ("CREATE TABLE {table} ("
               "col_decimal_0 DECIMAL(18,10),"
               "col_decimal_1 DECIMAL(18,10))"
               ).format(table=tb_name)
        WriteLog( sql)
        
        scale_num=10
        
        try:
            cur.execute(
                sql
            )
        except Exception as err:
            WriteLog( 'Exception:'+str(err))
            raise
            
        columns = [
            'col_decimal_0',
            'col_decimal_1',
        ]
        
        data = [
            (Decimal('-123.456'),Decimal('123.456'),),
            (Decimal('-0.01234567819'),Decimal('0.01234567819'),),
            (Decimal('-12345678.012345'),Decimal('12345678.012345'),),
            (Decimal('-987.01234567899'),Decimal('987.01234567899'),),
            (Decimal('-98765432.0123456700'),Decimal('98765432.0123456700'),),
            (Decimal('-98765432.01234567001'),Decimal('98765432.01234567001'),),
            (Decimal('-98765432.01234567009'),Decimal('98765432.01234567009'),),
        ]
        
        insert = _insert_query(tb_name, columns)
        WriteLog( insert)
        
        
        i=0
        err_flag=False
        for x in data:
            WriteLog( "<Begin Case>:" + insert + str(x), True)
            try:
                cur.execute(insert, x)
            except Exception as err:
                err_flag=True
                WriteLog( str(err))
            finally:
                if(err_flag==False):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                    
                WriteLog( "<End Case>:" + insert + str(x))
                err_flag=False
                
            i = i + 1
            #WriteLog( "%d, %s"%(i, str(x)))
                
        select = _get_select_stmt(tb_name, columns)
        cur.execute(select)
        #WriteLog( select)
            
        i=0
        err_flag=False
        for i in range(len(data)):
            row = cur.fetchone()
            WriteLog("Fetch one row:"+str(row))
            if(row == None):
                break
            for j, col in enumerate(columns):
                WriteLog( "<Begin Case>:test column {0}".format(j), True)
                try:
                    WriteLog( "fetch["+ str(row[j])+"]")
                    WriteLog( "raw["+str(data[i][j])+"]")
                    
                    num_part1 ,num_part2 = str(data[i][j]).split('.')
                    #WriteLog( num_part1, num_part2)
                    num_part2_new=num_part2[0:scale_num]
                    #WriteLog( num_part2_new)
                    exp=Decimal(num_part1+'.'+num_part2_new)
                    #WriteLog( exp)
                    #WriteLog( row[j])
                    self.compare(col, exp, row[j])
                    WriteLog( str(type(row[j])))
                    self.assertTrue(type(row[j]) is Decimal)
                except Exception as err:
                    WriteLog( 'Unexpected Exception:' + str(err))
                    err_flag=True
                finally:
                    if(err_flag==False):
                        WriteLog( "[+++]Test Success")
                    else:
                        WriteLog( "[---]Test Fail")
                        success_flag = False
                    WriteLog( "<End Case>")
                    err_flag=False
                    
        
        invalid_data = [
            (Decimal('-123456789.012345'), Decimal('0'),),
            (Decimal('123456789.012345'), Decimal('0'),),
        ]
                    
        i=0
        expect_err_flag=False
        for x in invalid_data:
            WriteLog( "<Begin Case>:" + insert + str(x), True)
            try:
                cur.execute(insert, x)
            except Exception as err:
                expect_err_flag=True
                WriteLog( str(err))
            finally:
                if(expect_err_flag==True):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                    
                WriteLog( "<End Case>:" + insert + str(x))
                expect_err_flag=False
                
        
        cur.close()
        
        self.assertTrue(success_flag==True, 'Data type case fail, please check the log in file')    
          
       
    
    #@unittest.skip("no")
    def test_numeric_real(self):
        tb_name = self.tables['real']

        cur = self.conn.cursor()
        
        cur.execute("DROP TABLE IF EXISTS %s" % tb_name)
        
        success_flag = True
        

        sql = ("CREATE TABLE {table} ("
               "col_real real)"
               ).format(table=tb_name)
        WriteLog( sql)
        
        try:
            cur.execute(
                sql
            )
        except Exception as err:
            WriteLog( 'Exception:'+str(err))
            raise
            
        columns = [
            'col_real',
        ]
    
        
        data = [
            (-3.4E38,),
            (-3.40282347e+38,),
            (-1.2E37,),
            (-3.402823466,),
            (0.0,),
            (0,),
            (-1.17549435e-38,),
            (1.17549435e-38,),
            (-1.175494351,),
            (1.402823466,),
            (11.402823466,),
            (111.402823466,),
            (1111.402823466,),
            (11111.402823466,),
            (111111.402823466,),
            (1111111.402823466,),
            (-1.23456789,),
            (2.999999,),
            (-100.0,),
            (100.0,),
            (-100,),
            (100,),
            (3.40282347e+38,),
            (1.2E37,),
            (3.4E38,),
        ]
        
        expect_data = [
            (-3.399999e+38,),
            (-3.402823e+38,),
            (-1.2E37,),
            (-3.402823,),
            (0.0,),
            (0,),
            (-1.175494e-38,),
            (1.175494e-38,),
            (-1.175494,),
            (1.402823,),
            (11.40282,),
            (111.4028,),
            (1111.402,),
            (11111.40,),
            (111111.4,),
            (1111111.0,),
            (-1.234567,),
            (2.999999,),
            (-100.0,),
            (100.0,),
            (-100,),
            (100,),
            (3.402823e+38,),
            (1.2E37,),
            (3.399999e+38,),
        ]
        insert = _insert_query(tb_name, columns)
        WriteLog( insert)
        
        index_map=[]
        index_insert=0
        
        i=0
        err_flag=False
        for x in data:
            WriteLog( "<Begin Case>:" + insert + str(x), True)
            try:
                cur.execute(insert, x)
            except Exception as err:
                err_flag=True
                WriteLog( str(err))
            finally:
                if(err_flag==False):
                    WriteLog( "[+++]Test Success")
                    index_insert=index_insert+1
                    index_map.insert(index_insert,i)
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                    
                WriteLog( "<End Case>:" + insert + str(x))
                err_flag=False
                
            i = i + 1
            #WriteLog( "%d, %s"%(i, str(x)))
        
        WriteLog( str(index_map))
                
        select = _get_select_stmt(tb_name, columns)
        cur.execute(select)
        #WriteLog( select)
            
        i=0
        #for i in range(len(data)):
        for i in range(len(index_map)):
            row = cur.fetchone()
            WriteLog("Fetch one row:"+str(row))
            if(row == None):
                break
            for j, col in enumerate(columns):
                WriteLog( "<Begin Case>:test column {0}".format(j), True)
        
                WriteLog( "fetch[" + str(row[j]) + "]")
                WriteLog( "raw[" + str(data[index_map[i]][j]) + "]")
                WriteLog( "expect[" + str(expect_data[index_map[i]][j]) + "]")

                '''
                rawdata=data[index_map[i]][j]
                raw_data_dot_pos=str(rawdata).find('.')
                if(raw_data_dot_pos == -1):
                    exp=rawdata
                else:
                    num_part1 ,num_part2 = str(rawdata).split('.')
                    WriteLog( num_part1, num_part2)
                    num_part2_new=num_part2[0:scale_num]
                    WriteLog( num_part2_new)
                    
                    raw_data_e_pos=(str(rawdata)).upper().find('E')
                        
                    if(raw_data_e_pos != -1):
                        postfix=str(rawdata)[raw_data_e_pos:]
                        if(str(num_part2_new).find(postfix)!=-1):
                            exp=float(num_part1 + '.' + num_part2_new)
                        else:
                            e_pos=(str(num_part2_new)).upper().find('E')
                            if(e_pos==-1):
                                exp=float(num_part1 + '.' + num_part2_new + postfix)
                            else:
                                exp=float(num_part1 + '.' + num_part2_new[0:e_pos] + postfix)
                            
                    else:
                        exp=float(num_part1 + '.' + num_part2_new)
                    #WriteLog( exp)
                '''
                
                exp=expect_data[index_map[i]][j]
                
                fetch_data_dot_pos=str(row[j]).find('.')
                if(fetch_data_dot_pos == -1):
                    res=row[j]
                else:   
                    if(str(row[j])[0]=='-'):
                        scale_num = 7-(fetch_data_dot_pos-1)
                    else:
                        scale_num = 7-fetch_data_dot_pos
                    
                    num_part1 ,num_part2 = str(row[j]).split('.')
                    WriteLog( num_part1, num_part2)
                    num_part2_new=num_part2[0:scale_num]
                    WriteLog( num_part2_new)
                    
                    fetch_data_e_pos=str(row[j]).upper().find('E')
                    if(fetch_data_e_pos != -1):
                        res=float(num_part1 + '.' + num_part2_new + str(row[j])[fetch_data_e_pos:])
                    else:
                        res=float(num_part1 + '.' + num_part2_new)
                    #WriteLog( res)
                
                #self.compare(col, exp, res)
                WriteLog( "name:%s, expect:%s, result:%s" %(col, exp, res))
                WriteLog( "name:{0}, expect:{1}, result:{2}".format(col, exp, res))
                
                if(exp==res):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag=False
                    
                WriteLog( str(type(row[j])))
                #if(type(row[j]) is float):
                if(type(row[j])==float):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag=False
                WriteLog( "<End Case>")

        
        invalid_data = [
            (-3.41E38,),
            (3.41E38,),
        ]
        
        i=0
        expect_err_flag=False
        for x in invalid_data:
            WriteLog( "<Begin Case>:" + insert + str(x), True)
            try:
                cur.execute(insert, x)
            except Exception as err:
                expect_err_flag=True
                WriteLog( str(err))
            finally:
                if(expect_err_flag==True):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                    
                WriteLog( "<End Case>:" + insert + str(x))
                expect_err_flag=False
                
        
        cur.close()
        
        self.assertTrue(success_flag==True, 'Data type case fail, please check the log in file')
        
    #@unittest.skip("no")
    def test_numeric_float(self):
        tb_name = self.tables['float']

        cur = self.conn.cursor()
        
        cur.execute("DROP TABLE IF EXISTS %s" % tb_name)
        
        success_flag = True
        

        sql = ("CREATE TABLE {table} ("
               "col_float float)"
               ).format(table=tb_name)
        WriteLog( sql)
        
        scale_num=16
        
        try:
            cur.execute(
                sql
            )
        except Exception as err:
            WriteLog( 'Exception:'+str(err))
            raise
            
        columns = [
            'col_float',
        ]
        
        data = [
            (-1.7976931348623157e+308,),
            (-1.79e+308,),
            (-3.41E38,),
            (-1.79769313,),
            (-1.7976931348623157,),
            (1.7976931348623157,),
            (-21.7976931348623157,),
            (21.7976931348623157,),
            (2.2250738585072014,),
            (-1.3999999999999999,),
            (-1.9999999999999999,),
            (-1.30123456789012344,),
            (-1.30123456789012345,),
            (-1.30123456789012346,),
            (-1.3999999999999919,),
            (-3.402823466,),
            (0.0,),
            (0,),
            (-2.2250738585072014e-308,),
            (-2.2250738585072013e-308,),
            (2.2250738585072013e-308,),
            (2.2250738585072014e-308,),
            (-1.175494351,),
            (3.402823466,),
            (-1.23456789,),
            (2.999999,),
            (-100.0,),
            (100.0,),
            (-100,),
            (100,),
            (-340282366920938463463374607431768211456.0,),
            (340282366920938463463374607431768211456.0,),
            (3.41E38,),
            (1.79e+308,),
            (1.7976931348623157e+308,),
        ]
        
        
        insert = _insert_query(tb_name, columns)
        WriteLog( insert)
        
        index_map=[]
        index_insert=0
        
        i=0
        err_flag=False
        for x in data:
            WriteLog( "<Begin Case>:" + insert + str(x), True)
            try:
                cur.execute(insert, x)
            except Exception as err:
                err_flag=True
                WriteLog( str(err))
            finally:
                if(err_flag==False):
                    WriteLog( "[+++]Test Success")
                    index_insert=index_insert+1
                    index_map.insert(index_insert,i)
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                    
                WriteLog( "<End Case>:" + insert + str(x))
                err_flag=False
                
            i = i + 1
            #WriteLog( "%d, %s"%(i, str(x)))
        
        WriteLog( str(index_map))
                
        select = _get_select_stmt(tb_name, columns)
        cur.execute(select)
        #WriteLog( select)
            
        i=0
        #for i in range(len(data)):
        for i in range(len(index_map)):
            row = cur.fetchone()
            WriteLog("Fetch one row:"+str(row))
            if(row == None):
                break
            for j, col in enumerate(columns):
                WriteLog( "<Begin Case>:test column {0}".format(j), True)
                '''
                WriteLog( "###"+str(row[j])+"###")
                num_part1 ,num_part2 = str(data[index_map[i]][j]).split('.')
                #WriteLog( num_part1, num_part2)
                num_part2_new=num_part2[0:scale_num]
                #WriteLog( num_part2_new)
                exp=float(num_part1+'.'+num_part2_new)
                #WriteLog( exp)
                num_part1 ,num_part2 = str(row[j]).split('.')
                #WriteLog( num_part1, num_part2)
                num_part2_new=num_part2[0:scale_num]
                #WriteLog( num_part2_new)
                res=float(num_part1+'.'+num_part2_new)
                #WriteLog( res)
                '''
                
                exp=data[index_map[i]][j]
                res=row[j]
                #self.compare(col, exp, res)
                WriteLog( "name:%s, expect:%s, result:%s" %(col, exp, res))
                WriteLog( "name:{0}, expect:{1}, result:{2}".format(col, exp, res))
                if(exp==res):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag=False
                    
                WriteLog( str(type(row[j])))         #if(type(row[j]) is float):
                if(type(row[j])==float):
                    WriteLog( "[+++]Test type Success")
                else:
                    WriteLog( "[---]Test type Fail")
                    success_flag=False
                

        
        invalid_data = [
            (-1.8e+308,),
            (-1.798e+308,),
            (1.798e+308,),
            (1.8e+308,),
        ]
        
        i=0
        expect_err_flag=False
        for x in invalid_data:
            WriteLog( "<Begin Case>:" + insert + str(x), True)
            try:
                cur.execute(insert, x)
            except Exception as err:
                expect_err_flag=True
                WriteLog( str(err))
            finally:
                if(expect_err_flag==True):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                    
                WriteLog( "<End Case>:" + insert + str(x))
                expect_err_flag=False
                
        
        cur.close()
        
        self.assertTrue(success_flag==True, 'Data type case fail, please check the log in file')
    
    #@unittest.skip("no")
    def test_numeric_double(self):
        tb_name = self.tables['double']

        cur = self.conn.cursor()
        
        cur.execute("DROP TABLE IF EXISTS %s" % tb_name)
        
        success_flag = True
        

        sql = ("CREATE TABLE {table} ("
               "col_double double precision)"
               ).format(table=tb_name)
        WriteLog( sql)
        
        scale_num=16
        
        try:
            cur.execute(
                sql
            )
        except Exception as err:
            WriteLog( 'Exception:'+str(err))
            raise
            
        columns = [
            'col_double',
        ]
        
        data = [
            (-1.7976931348623157e+308,),
            (-1.79e+308,),
            (-3.41E38,),
            (-1.79769313,),
            (-1.7976931348623157,),
            (1.7976931348623157,),
            (-21.7976931348623157,),
            (21.7976931348623157,),
            (2.2250738585072014,),
            (-1.3999999999999999,),
            (-1.9999999999999999,),
            (-1.30123456789012344,),
            (-1.30123456789012345,),
            (-1.30123456789012346,),
            (-1.3999999999999919,),
            (-3.402823466,),
            (0.0,),
            (0,),
            (-2.2250738585072014e-308,),
            (-2.2250738585072013e-308,),
            (2.2250738585072013e-308,),
            (2.2250738585072014e-308,),
            (-1.175494351,),
            (3.402823466,),
            (-1.23456789,),
            (2.999999,),
            (-100.0,),
            (100.0,),
            (-100,),
            (100,),
            (-340282366920938463463374607431768211456.0,),
            (340282366920938463463374607431768211456.0,),
            (3.41E38,),
            (1.79e+308,),
            (1.7976931348623157e+308,),
        ]
        
        
        insert = _insert_query(tb_name, columns)
        WriteLog( insert)
        
        index_map=[]
        index_insert=0
        
        i=0
        err_flag=False
        for x in data:
            WriteLog( "<Begin Case>:" + insert + str(x), True)
            try:
                cur.execute(insert, x)
            except Exception as err:
                err_flag=True
                WriteLog( str(err))
            finally:
                if(err_flag==False):
                    WriteLog( "[+++]Test Success")
                    index_insert=index_insert+1
                    index_map.insert(index_insert,i)
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                    
                WriteLog( "<End Case>:" + insert + str(x))
                err_flag=False
                
            i = i + 1
            #WriteLog( "%d, %s"%(i, str(x)))
        
        WriteLog( str(index_map))
                
        select = _get_select_stmt(tb_name, columns)
        cur.execute(select)
        #WriteLog( select)
            
        i=0
        #for i in range(len(data)):
        for i in range(len(index_map)):
            row = cur.fetchone()
            WriteLog("Fetch one row:"+str(row))
            if(row == None):
                break
            for j, col in enumerate(columns):
                WriteLog( "<Begin Case>:test column {0}".format(j), True)
                '''
                WriteLog( "###"+str(row[j])+"###")
                num_part1 ,num_part2 = str(data[index_map[i]][j]).split('.')
                #WriteLog( num_part1, num_part2)
                num_part2_new=num_part2[0:scale_num]
                #WriteLog( num_part2_new)
                exp=float(num_part1+'.'+num_part2_new)
                #WriteLog( exp)
                num_part1 ,num_part2 = str(row[j]).split('.')
                #WriteLog( num_part1, num_part2)
                num_part2_new=num_part2[0:scale_num]
                #WriteLog( num_part2_new)
                res=float(num_part1+'.'+num_part2_new)
                #WriteLog( res)
                '''
                
                exp=data[index_map[i]][j]
                res=row[j]
                #self.compare(col, exp, res)
                WriteLog( "name:%s, expect:%s, result:%s" %(col, exp, res))
                WriteLog( "name:{0}, expect:{1}, result:{2}".format(col, exp, res))
                if(exp==res):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag=False
                    
                WriteLog( str(type(row[j])))
                #if(type(row[j]) is float):
                if(type(row[j])==float):
                    WriteLog( "[+++]Test type Success")
                else:
                    WriteLog( "[---]Test type Fail")
                    success_flag=False
                

        
        invalid_data = [
            (-1.8e+308,),
            (-1.798e+308,),
            (1.798e+308,),
            (1.8e+308,),
        ]
        
        i=0
        expect_err_flag=False
        for x in invalid_data:
            WriteLog( "<Begin Case>:" + insert + str(x), True)
            try:
                cur.execute(insert, x)
            except Exception as err:
                expect_err_flag=True
                WriteLog( str(err))
            finally:
                if(expect_err_flag==True):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                    
                WriteLog( "<End Case>:" + insert + str(x))
                expect_err_flag=False
                
        
        cur.close()
        
        self.assertTrue(success_flag==True, 'Data type case fail, please check the log in file') 
    
    
    #@unittest.skip("no")
    def test_numeric(self):
        tb_name = self.tables['numeric']

        cur = self.conn.cursor()
        
        cur.execute("DROP TABLE IF EXISTS %s" % tb_name)
        
        success_flag = True
        
        tab_str_begind = ("create table %s (" % tb_name)
        tab_str_end = ")";

        
        columns_definition = [
            "col_1 numeric(-1,0)",
            "col_1 numeric(0,-1)",
            "col_1 numeric(0,0)",
            "col_1 numeric(1,-1)",
            "col_1 numeric(-1,-2)",
            "col_1 numeric(10,11)",
            "col_1 numeric(129,0)",
        ]
        
        expect_err_flag = False
        
        col = 0
        for col in columns_definition:
            create_tb_str = tab_str_begind + col + tab_str_end
            WriteLog( "<Begin Case>:" + create_tb_str, True)
            
            try:
                cur.execute(create_tb_str)
            except Exception as out_info:
                WriteLog( 'Expected Exception:' + str(out_info))
                expect_err_flag = True
            finally:
                if(expect_err_flag == True):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                WriteLog( "<End Case>:" + create_tb_str)
                expect_err_flag = False
        
        
        '''
        sql = ("CREATE TABLE {table} ("
               "col_numeric_int numeric(18,10),"
               "col_numeric_float numeric(18,10),"
               "col_numeric_decimal decimal(18,10),"
               "col_numeric_int2 numeric(128,10),"
               "col_numeric_float2 numeric(128,10),"
               "col_numeric_decimal2 numeric(128,10))"
               ).format(table=tb_name)
        '''
        sql = ("CREATE TABLE {table} ("
            "col_numeric_0 numeric(128,10),"
            "col_numeric_1 numeric(128,0),"
            "col_numeric_2 numeric(19,10),"
            "col_numeric_3 numeric(19,0),"
            "col_numeric_4 numeric(18,10),"
            "col_numeric_5 numeric(18,0),"
            "col_numeric_6 numeric(10,5),"
            "col_numeric_7 numeric(10,0))"
            ).format(table=tb_name)        
        WriteLog( sql)
        
        
        try:
            cur.execute(
                sql
            )
        except Exception as err:
            WriteLog( 'Exception:'+str(err))
            raise
            
        columns = [
            'col_numeric_0',
            'col_numeric_1',
            'col_numeric_2',
            'col_numeric_3',
            'col_numeric_4',
            'col_numeric_5',
            'col_numeric_6',
            'col_numeric_7',
        ]
        
        '''
        data = [
            (-9223372036854775808, 12345678.012345, Decimal('12345678.012345'),),
            (-4e37, 1.2e100, Decimal('123456789012345678.0123456789'),),
            (12345678900123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789,
             12345678900123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789.01234567,
             Decimal('1234567890.01234567'),),
            (18446744073709551615, -1.8e+308, Decimal('123456789001234567'),),
        ]
        '''
        
        data = [
            (
            1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678, 
            12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678,
            123456789,
            1234567890123456789,
            12345678,
            123456789012345678,
            12345,
            1234567890,
            ),
            
            (
            -1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678, 
            -12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678,
            -123456789,
            -1234567890123456789,
            -12345678,
            -123456789012345678,
            -12345,
            -1234567890,
            ),
            
            (
            #1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678.1234567890, 
            Decimal('1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678.1234567890'), 
            0,
            #123456789.1234567890,
            Decimal('123456789.1234567890'),
            0,
            12345678.1234567890,#12345678.12345679
            0,
            12345.12345,
            0,
            ),
            
            (
            Decimal('1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678.12345678909'), 
            0,
            Decimal('123456789.12345678909'),
            0,
            12345678.12345678909,#12345678.12345679
            0,
            12345.123456,
            0,
            ),
            
            (
            Decimal('1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678.123'), 
            0,
            Decimal('123456789.123'),
            0,
            12345678.123,
            0,
            12345.123,
            0,
            ),
        ]
        
        
        expect_data = [
            (
            1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678, 
            12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678,
            123456789,
            1234567890123456789,
            12345678,
            123456789012345678,
            12345,
            1234567890,
            ),
            
            (
            -1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678, 
            -12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678,
            -123456789,
            -1234567890123456789,
            -12345678,
            -123456789012345678,
            -12345,
            -1234567890,
            ),
            
            (
            #1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678.1234567890, 
            Decimal('1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678.1234567890'), 
            0,
            #123456789.1234567890,
            Decimal('123456789.1234567890'),
            0,
            12345678.1234567890,#12345678.12345679
            0,
            12345.12345,
            0,
            ),
            
            (
            Decimal('1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678.1234567890'), 
            0,
            Decimal('123456789.1234567890'),
            0,
            12345678.12345678909,#12345678.12345679
            0,
            12345.12345,#12345.12346
            0,
            ),
            
            (
            Decimal('1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678.123'), 
            0,
            Decimal('123456789.123'),
            0,
            12345678.123,
            0,
            12345.123,
            0,
            ),
        ]
        
        invalid_data = [
            (
            12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789, 
            ),
                        
            (
            0, 
            123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789,
            ),
            (
            0, 0,
            1234567890,
            ),
            (
            0, 0, 0,
            12345678901234567890,
            ),
            (
            0, 0, 0, 0,
            123456789,
            ),
            (
            0, 0, 0, 0, 0,
            1234567890123456789,
            ),
            (
            0, 0, 0, 0, 0, 0,
            123456,
            ),
            (
            0, 0, 0, 0, 0, 0, 0,
            12345678901,
            ),
            ]
        
        
        '''
        data = [
            (-18446744073709551615,-18446744073709551615,),
            (18446744073709551616,18446744073709551616,),
            (1.2e128,1.2e128,),
            (1.2e129,1.2e129,),
            (-1.8e+308,-1.8e+308,),
            (1.8e+308,1.8e+308,),
            ('0.1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567',),
            ('0.1234567890',),
            ('0.1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890',),
            ('0.1234567890',),
            (12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678,),
            (1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890,),
            ('12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678',),
            ('123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789',),
            ('1.23',),
            (100,),
            (100.0,),
            (1.23,),
            ('abc',),
            (12345678.012345,),
            (Decimal('12345678.012345'),),
        ]
        '''
        insert = _insert_query(tb_name, columns)
        WriteLog( insert)
        
        
        index_map=[]
        index_insert=0
        
        i=0
        err_flag=False
        for x in data:
            WriteLog( "<Begin Case>:" + insert + str(x), True)
            try:
                cur.execute(insert, x)
            except Exception as err:
                err_flag=True
                WriteLog( str(err))
            finally:
                if(err_flag==False):
                    WriteLog( "[+++]Test Success")
                    index_insert=index_insert+1
                    index_map.insert(index_insert,i)
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                    
                WriteLog( "<End Case>:" + insert + str(x))
                err_flag=False
                
            i = i + 1
            #WriteLog( "%d, %s"%(i, str(x)))
        
        WriteLog( str(index_map))
                
        select = _get_select_stmt(tb_name, columns)
        cur.execute(select)
        #WriteLog( select)
            
        i=0
        err_flag=False
        for i in range(len(index_map)):
            row = cur.fetchone()
            WriteLog("Fetch one row:"+str(row))
            if(row == None):
                break
            for j, col in enumerate(columns):
                WriteLog( "<Begin Case>:test column {0}".format(j), True)
                
                WriteLog( "fetch["+ str(row[j])+"]")
                WriteLog( "raw["+str(data[index_map[i]][j])+"]")
                WriteLog( "expect_data["+str(expect_data[index_map[i]][j])+"]")
                
                exp = expect_data[index_map[i]][j]
                res = row[j]
                
                '''
                num_part1 ,num_part2 = str(expect_data[i][j]).split('.')
                #WriteLog( num_part1, num_part2)
                num_part2_new=num_part2[0:scale_num]
                #WriteLog( num_part2_new)
                exp=Decimal(num_part1+'.'+num_part2_new)
                #WriteLog( exp)
                #WriteLog( row[j])
                '''
                
                #self.compare(col, exp, row[j])
                
                WriteLog( "name:%s, expect:%s, result:%s" %(col, exp, res))
                WriteLog( "name:{0}, expect:{1}, result:{2}".format(col, exp, res))
                
                if(exp==res):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag=False
                
                WriteLog( str(type(row[j])))
                if((type(row[j])==float) or (type(row[j])==Decimal) or (type(row[j])==int)):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag=False
                WriteLog( "<End Case>")
                    

                    
        i=0
        expect_err_flag=False
        for x in invalid_data:
            WriteLog( "<Begin Case>:" + insert + str(x), True)
            try:
                cur.execute(insert, x)
            except Exception as err:
                expect_err_flag=True
                WriteLog( str(err))
            finally:
                if(expect_err_flag==True):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                    
                WriteLog( "<End Case>:" + insert + str(x))
                expect_err_flag=False
                
        
        cur.close()
        
        self.assertTrue(success_flag==True, 'Data type case fail, please check the log in file')   

    
    
    
    #@unittest.skip("no")
    def test_char(self):
        tb_name = self.tables['char']

        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS %s" % tb_name)
        
        
        tab_str_begind = ("create table %s (" % tb_name)
        tab_str_end = ")";
        
        
        columns_definition = [
            "col_1 char(-1)",
            "col_1 char(0)",
            "col_1 char(16777217)",
            "col_1 nchar(-1)",
            "col_1 nchar(0)",
            "col_1 nchar(8388609)",
            "col_1 char(-1) character set ucs2",
            "col_1 char(0) character set ucs2",
            "col_1 char(8388609) character set ucs2",
            "col_1 char(-1) character set utf8",
            "col_1 char(0) character set utf8",
            "col_1 char(4194305) character set utf8",
        ]
        
        success_flag = True
        flag = False
        
        col = 0
        for col in columns_definition:
            create_tb_str = tab_str_begind + col + tab_str_end
            WriteLog( "<Begin Case>:" + create_tb_str, True)
            
            try:
                cur.execute(create_tb_str)
            except Exception as out_info:
                WriteLog( 'Expected Exception:' + str(out_info))
                flag = True
            finally:
                if(flag == True):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                WriteLog( "<End Case>:" + create_tb_str)
                flag = False
                
                #cur.execute("DROP TABLE IF EXISTS %s" % tb_name)
        
        columns_definition = [
            "col_1 char(16777216)",
            "col_1 char(8388608) character set ucs2",
            "col_1 char(4194304) character set utf8",
        ]
        
        success_flag = True
        err_flag = False
        
        col = 0
        for col in columns_definition:
            create_tb_str = tab_str_begind + col + tab_str_end
            WriteLog( "<Begin Case>:" + create_tb_str, True)
            
            try:
                cur.execute(create_tb_str)
            except Exception as err:
                WriteLog( 'Unxpected Exception:' + str(out_info))
                err_flag = True
            finally:
                if(err_flag == False):
                    WriteLog( "[+++]Test Success")
                    cur.execute("DROP TABLE %s" % tb_name)
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                WriteLog( "<End Case>:" + create_tb_str)
                err_flag = False

        
        columns_definition = [
            "col_char_0 char(1)",
            "col_char_1 char(10)",
            "col_nchar_0 nchar(1)",
            "col_nchar_1 nchar(10)",
            "col_char_ucs2_0 char(1) character set ucs2",
            "col_char_ucs2_1 char(10)  character set ucs2",
            "col_char_utf8_0 char(1) character set utf8",
            "col_char_utf8_1 char(10) character set utf8",
        ]
        
        columns_name = [
            "col_char_0",
            "col_char_1",
            "col_nchar_0",
            "col_nchar_1",
            "col_char_ucs2_0",
            "col_char_ucs2_1",
            "col_char_utf8_0",
            "col_char_utf8_1",
        ]
           
                
        data = [
            ('0','0123456789', '0','0123456789', '0','0123456789', '0','0123456789',),
            ('0','01234567', '0','01234567', '0','01234567', '0','01234567',),
            ('0',' 0123456', '0',' 0123456', '0',' 0123456', '0',' 0123456',),
            
            ('01','0',          '0','0', '0','0',  '0','0'), 
            ('0','01234567890', '0','0', '0','0',  '0','0'),
            
            ('0','0',  '01','0',          '0','0', '0','0'),
            ('0','0',  '0','01234567890', '0','0', '0','0'),

            ('0','0',  '0','0', '01','0',          '0','0'),
            ('0','0',  '0','0', '0','01234567890', '0','0'),
            
            ('0','0',  '0','0', '0','0', '01','0'),
            ('0','0',  '0','0', '0','0', '0','01234567890'),
        ]
        
        expect_len = [
            (1,10, 1,10, 1,10, 1,10),
            (1,10, 1,10, 1,10, 1,10),
            (1,10, 1,10, 1,10, 1,10),
            None
        ]
        
        
        create_tb_str = _create_table(tb_name, columns_definition)
        #print (create_tab_str)
        WriteLog( "<Begin Case>:" + create_tb_str, True)
        try:
            cur.execute(create_tb_str)
        except Exception as err:
            WriteLog( "[---]Test Fail")
            WriteLog( 'Unexpected Exception:' + str(err))
            success_flag = False
        finally:
            WriteLog( "[+++]Test Success")
            WriteLog( "<End Case>:" + create_tb_str)

        insert = _insert_query(tb_name, columns_name)
        WriteLog( insert)
        
        
        index_map=[]
        index_insert=0
        
        i=0
        err_flag=False
        for x in data:
            WriteLog( "<Begin Case>:" + insert + str(x), True)
            try:
                cur.execute(insert, x)
            except Exception as err:
                err_flag=True
                WriteLog( str(err))
            finally:
                if(i<=2):
                    if(err_flag==False):
                        WriteLog( "[+++]Test Success")
                        index_insert=index_insert+1
                        index_map.insert(index_insert,i)
                    else:
                        WriteLog( "[---]Test Fail")
                        success_flag = False
                else:
                    if(err_flag==True):
                        WriteLog( "[+++]Test Success")
                    else:
                        WriteLog( "[---]Test Fail")
                        success_flag = False
                    
                WriteLog( "<End Case>:" + insert + str(x))
                err_flag=False
                
            i = i + 1
            #WriteLog( "%d, %s"%(i, str(x)))
            
        WriteLog( str(index_map))
                
        select = _get_select_stmt(tb_name, columns_name)
        cur.execute(select)
        #WriteLog( select)
            
        i=0
        err_flag=False
        for i in range(len(index_map)):
            row = cur.fetchone()
            WriteLog("Fetch one row:"+str(row))
            if(row == None):
                break
            for j, col in enumerate(columns_name):
                WriteLog( "<Begin Case>:test column {0}".format(j), True)
                
                WriteLog( "fetch["+ str(row[j])+"]")
                WriteLog( "raw["+str(data[index_map[i]][j])+"]")
                
                exp = data[index_map[i]][j]
                #res = row[j].rstrip()
                res = row[j]
                
                '''
                num_part1 ,num_part2 = str(data[i][j]).split('.')
                #WriteLog( num_part1, num_part2)
                num_part2_new=num_part2[0:scale_num]
                #WriteLog( num_part2_new)
                exp=Decimal(num_part1+'.'+num_part2_new)
                #WriteLog( exp)
                #WriteLog( row[j])
                '''
                
                #self.compare(col, exp, row[j])
                
                WriteLog( "name:%s, expect:%s, result:%s" %(col, exp, res))
                WriteLog( "name:{0}, expect:{1}, result:{2}".format(col, exp, res))
                
                if(exp==res):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag=False
                WriteLog( "<End Case>")
                
                WriteLog( str(type(row[j])))
                    
                WriteLog( "<Begin Case>:test column {0} fetch len".format(j), True)
                WriteLog( "col %s fetch len is %s, expect len is %s" % (j, len(row[j]), expect_len[i][j]))
                
                if(len(row[j])!=expect_len[i][j]):
                    WriteLog( "[---]Test Fail")
                else:
                    WriteLog( "[+++]Test Success")
                
                WriteLog( "<End Case>")
             
        cur.close() 
                    
                
        self.assertTrue(success_flag==True, 'Data type case fail, please check the log in file') 
        
         
        
    #@unittest.skip("no")
    def test_varchar(self):
        tb_name = self.tables['varchar']

        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS %s" % tb_name)
        
        
        tab_str_begind = ("create table %s (" % tb_name)
        tab_str_end = ")";
        
        
        columns_definition = [
            "col_1 varchar",
            "col_1 varchar(-1)",
            "col_1 varchar(0)",
            "col_1 varchar(16777217)",
            "col_1 varchar(-1) character set ucs2",
            "col_1 varchar(0) character set ucs2",
            "col_1 varchar(8388609) character set ucs2",
            "col_1 varchar(-1) character set utf8",
            "col_1 varchar(0) character set utf8",
            "col_1 varchar(4194305) character set utf8",
        ]
        
        success_flag = True
        flag = False
        
        col = 0
        for col in columns_definition:
            create_tb_str = tab_str_begind + col + tab_str_end
            WriteLog( "<Begin Case>:" + create_tb_str, True)
            
            try:
                cur.execute(create_tb_str)
            except Exception as out_info:
                WriteLog( 'Expected Exception:' + str(out_info))
                flag = True
            finally:
                if(flag == True):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                WriteLog( "<End Case>:" + create_tb_str)
                flag = False
                
                #cur.execute("DROP TABLE IF EXISTS %s" % tb_name)
        
        columns_definition = [
            "col_1 varchar(16777216)",
            "col_1 varchar(8388608) character set ucs2",
            "col_1 varchar(4194304) character set utf8",
        ]
        
        success_flag = True
        err_flag = False
        
        col = 0
        for col in columns_definition:
            create_tb_str = tab_str_begind + col + tab_str_end
            WriteLog( "<Begin Case>:" + create_tb_str, True)
            
            try:
                cur.execute(create_tb_str)
            except Exception as err:
                WriteLog( 'Unxpected Exception:' + str(err))
                err_flag = True
            finally:
                if(err_flag == False):
                    WriteLog( "[+++]Test Success")
                    cur.execute("DROP TABLE %s" % tb_name)
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                WriteLog( "<End Case>:" + create_tb_str)
                err_flag = False
                
        
        columns_definition = [
            "col_varchar_0 varchar(1)",
            "col_varchar_1 varchar(10)",
            "col_varchar_ucs2_0 varchar(1) character set ucs2",
            "col_varchar_ucs2_1 varchar(10)  character set ucs2",
            "col_varchar_utf8_0 varchar(1) character set utf8",
            "col_varchar_utf8_1 varchar(10) character set utf8",
        ]
        
        columns_name = [
            "col_varchar_0",
            "col_varchar_1",
            "col_varchar_ucs2_0",
            "col_varchar_ucs2_1",
            "col_varchar_utf8_0",
            "col_varchar_utf8_1",
        ]
        

        data = [
            ('0','0123456789', '0','0123456789', '0','0123456789',),
            
            ('0','01234567', '0','01234567', '0','01234567',),
            
            ('0',' 0123456', '0',' 0123456', '0',' 0123456',),
            
            ('0','0123456 ', '0','0123456 ', '0','0123456 ',),
            
            ('01','0',  '0','0',  '0','0'), 
            
            
            ('0','01234567890',  '0','0',  '0','0'),
            
            ('0','0',  '01','0',  '0','0'),
            
            
            ('0','0',  '0','01234567890',  '0','0'),

            ('0','0',  '0','0',  '01','0'),
            
            
            ('0','0',  '0','0',  '0','01234567890'),
        ]
        
        expect_len = [
            (1,10, 1,10, 1,10,),
            (1,8, 1,8, 1,8),
            (1,8, 1,8, 1,8),
            (1,8, 1,8, 1,8),
            None
        ]
                
                
        
        create_tb_str = _create_table(tb_name, columns_definition)
        #print (create_tab_str)
        WriteLog( "<Begin Case>:" + create_tb_str, True)
        try:
            cur.execute(create_tb_str)
        except Exception as err:
            WriteLog( "[---]Test Fail")
            WriteLog( 'Unexpected Exception:' + str(err))
            success_flag = False
        finally:
            WriteLog( "[+++]Test Success")
            WriteLog( "<End Case>:" + create_tb_str)

        insert = _insert_query(tb_name, columns_name)
        WriteLog( insert)
        
        
        index_map=[]
        index_insert=0
        
        i=0
        err_flag=False
        for x in data:
            WriteLog( "<Begin Case>:" + insert + str(x), True)
            try:
                cur.execute(insert, x)
            except Exception as err:
                err_flag=True
                WriteLog( str(err))
            finally:
                if(i<=3):
                    if(err_flag==False):
                        WriteLog( "[+++]Test Success")
                        index_insert=index_insert+1
                        index_map.insert(index_insert,i)
                    else:
                        WriteLog( "[---]Test Fail")
                        success_flag = False
                else:
                    if(err_flag==True):
                        WriteLog( "[+++]Test Success")
                    else:
                        WriteLog( "[---]Test Fail")
                        success_flag = False
                    
                WriteLog( "<End Case>:" + insert + str(x))
                err_flag=False
                
            i = i + 1
            #WriteLog( "%d, %s"%(i, str(x)))
            
        WriteLog( str(index_map))
                
        select = _get_select_stmt(tb_name, columns_name)
        cur.execute(select)
        #WriteLog( select)
            
        i=0
        err_flag=False
        for i in range(len(index_map)):
            row = cur.fetchone()
            WriteLog("Fetch one row:"+str(row))
            if(row == None):
                break
            for j, col in enumerate(columns_name):
                WriteLog( "<Begin Case>:test column {0}".format(j), True)
                
                
                WriteLog( "fetch["+ str(row[j])+"]")
                WriteLog( "raw["+str(data[index_map[i]][j])+"]")
                
                exp = data[index_map[i]][j]
                #res = row[j].rstrip()
                res = row[j]
                
                #self.compare(col, exp, res)
                
                WriteLog( "name:%s, expect:%s, result:%s" %(col, exp, res))
                WriteLog( "name:{0}, expect:{1}, result:{2}".format(col, exp, res))
                
                if(exp==res):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag=False
                WriteLog( "<End Case>")
                
                WriteLog( str(type(row[j])))
                    
                WriteLog( "<Begin Case>:test column {0} fetch len".format(j), True)
                WriteLog( "col %s fetch len is %s, expect len is %s" % (j, len(row[j]), expect_len[i][j]))
                
                if(len(row[j])!=expect_len[i][j]):
                    WriteLog( "[---]Test Fail")
                else:
                    WriteLog( "[+++]Test Success")
                
                WriteLog( "<End Case>")
             
        cur.close() 
                    
                
        self.assertTrue(success_flag==True, 'Data type case fail, please check the log in file')
       
    
    #@unittest.skip("no")
    def test_time(self):
        tb_name = self.tables['time']

        cur = self.conn.cursor()
        
        cur.execute("DROP TABLE IF EXISTS %s" % tb_name)
        
        success_flag = True
        

        sql = ("CREATE TABLE {table} ("
               "col_time Time)"
               ).format(table=tb_name)
        WriteLog( sql)
        
        try:
            cur.execute(
                sql
            )
        except Exception as err:
            WriteLog( 'Exception:'+str(err))
            raise
            
        columns = [
            'col_time',
        ]
        
        data = [
            (Time(hour=0, minute=0, second=0),),
            (Time(hour=8, minute=23, second=40),),
            (Time(hour=20, minute=12, second=23),),
            (Time(hour=23, minute=59, second=59),),
        ]
        
        
        insert = _insert_query(tb_name, columns)
        WriteLog( insert)
        
        index_map=[]
        index_insert=0
        
        i=0
        err_flag=False
        for x in data:
            WriteLog( "<Begin Case>:" + insert + str(x), True)
            try:
                cur.execute(insert, x)
            except Exception as err:
                err_flag=True
                WriteLog( str(err))
            finally:
                if(err_flag==False):
                    WriteLog( "[+++]Test Success")
                    index_insert=index_insert+1
                    index_map.insert(index_insert,i)
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                    
                WriteLog( "<End Case>:" + insert + str(x))
                err_flag=False
                
            i = i + 1
            #WriteLog( "%d, %s"%(i, str(x)))
        
        WriteLog( str(index_map))
                
        select = _get_select_stmt(tb_name, columns)
        cur.execute(select)
        #WriteLog( select)
            
        i=0
        #for i in range(len(data)):
        for i in range(len(index_map)):
            row = cur.fetchone()
            WriteLog("Fetch one row:"+str(row))
            if(row == None):
                break
            for j, col in enumerate(columns):
                WriteLog( "<Begin Case>:test column {0}".format(j), True)
                
                exp=data[index_map[i]][j]
                res=row[j]
                #self.compare(col, exp, res)
                WriteLog( "name:%s, expect:%s, result:%s" %(col, exp, res))
                WriteLog( "name:{0}, expect:{1}, result:{2}".format(col, exp, res))
                if(exp==res):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag=False
                    
                WriteLog( str(type(row[j])))
                #if(type(row[j]) is float):
                if(type(row[j])==Time):
                    WriteLog( "[+++]Test type Success")
                else:
                    WriteLog( "[---]Test type Fail")
                    success_flag=False

        
        invalid_data = [
            (24, 0, 0,),
            (-1, 0, 0,),
            (0, -1, 0,),
            (0, 0, -1,),
            (0, 60, 0,),
            (0, 23, 60,),
        ]
        
        i=0
        expect_err_flag=False
        for x in invalid_data:
            WriteLog( "<Begin Case>:" + "set time" + str(x), True)
            try:
                Time(x[0], x[1], x[2])
            except Exception as err:
                expect_err_flag=True
                WriteLog( str(err))
            finally:
                if(expect_err_flag==True):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                    
                WriteLog( "<End Case>:" + "set time" + str(x))
                expect_err_flag=False
                
        
        cur.close()
        
        self.assertTrue(success_flag==True, 'Data type case fail, please check the log in file')
    
    #@unittest.skip("no")
    def test_time_precision(self):
        tb_name = self.tables['time_precision']

        cur = self.conn.cursor()
        
        cur.execute("DROP TABLE IF EXISTS %s" % tb_name)
        
        success_flag = True
        

        sql = ("CREATE TABLE {table} ("
               "col_time Time,"
               "col_time_0 Time(0),"
               "col_time_1 Time(1),"
               "col_time_2 Time(2),"
               "col_time_3 Time(3),"
               "col_time_4 Time(4),"
               "col_time_5 Time(5),"
               "col_time_6 Time(6))"
               ).format(table=tb_name)
        WriteLog( sql)
        
        try:
            cur.execute(
                sql
            )
        except Exception as err:
            WriteLog( 'Exception:'+str(err))
            raise
            
        columns = [
            'col_time',
            'col_time_0',
            'col_time_1',
            'col_time_2',
            'col_time_3',
            'col_time_4',
            'col_time_5',
            'col_time_6',
        ]
        
        
        data = [
            (Time(1, 1, 1, 123456), Time(1, 1, 1, 123456), Time(1, 1, 1, 123456), Time(1, 1, 1, 123456),
             Time(1, 1, 1, 123456), Time(1, 1, 1, 123456), Time(1, 1, 1, 123456), Time(1, 1, 1, 123456),),
            
            (Time(1, 1, 1, 12345), Time(1, 1, 1, 12345), Time(1, 1, 1, 12345), Time(1, 1, 1, 12345),
             Time(1, 1, 1, 12345), Time(1, 1, 1, 12345), Time(1, 1, 1, 12345), Time(1, 1, 1, 12345),),
            
            (Time(1, 1, 1, 1234), Time(1, 1, 1, 1234), Time(1, 1, 1, 1234), Time(1, 1, 1, 1234),
             Time(1, 1, 1, 1234), Time(1, 1, 1, 1234), Time(1, 1, 1, 1234), Time(1, 1, 1, 1234),),
            
            (Time(1, 1, 1, 123), Time(1, 1, 1, 123), Time(1, 1, 1, 123), Time(1, 1, 1, 123),
             Time(1, 1, 1, 123), Time(1, 1, 1, 123), Time(1, 1, 1, 123), Time(1, 1, 1, 123),),
            
            (Time(1, 1, 1, 12), Time(1, 1, 1, 12), Time(1, 1, 1, 12), Time(1, 1, 1, 12),
             Time(1, 1, 1, 12), Time(1, 1, 1, 12), Time(1, 1, 1, 12), Time(1, 1, 1, 12),),
            
            (Time(1, 1, 1, 1), Time(1, 1, 1, 1), Time(1, 1, 1, 1), Time(1, 1, 1, 1),
             Time(1, 1, 1, 1), Time(1, 1, 1, 1), Time(1, 1, 1, 1), Time(1, 1, 1, 1),),
            
            (Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 0),
             Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 0),),
        ]
        
        expect_data = [
            (Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 100000), Time(1, 1, 1, 120000), 
             Time(1, 1, 1, 123000), Time(1, 1, 1, 123400), Time(1, 1, 1, 123450), Time(1, 1, 1, 123456)),
            
            (Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 10000), 
             Time(1, 1, 1, 12000), Time(1, 1, 1, 12300), Time(1, 1, 1, 12340), Time(1, 1, 1, 12345)),
            
            (Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 0), 
             Time(1, 1, 1, 1000), Time(1, 1, 1, 1200), Time(1, 1, 1, 1230), Time(1, 1, 1, 1234)),
            
            (Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 0), 
             Time(1, 1, 1, 0), Time(1, 1, 1, 100), Time(1, 1, 1, 120), Time(1, 1, 1, 123)),
            
            (Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 0), 
             Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 10), Time(1, 1, 1, 12)),
            
            (Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 0), 
             Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 1)),
            
            (Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 0),
             Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 0), Time(1, 1, 1, 0),),
        ]
        
        insert = _insert_query(tb_name, columns)
        WriteLog( insert)
        
        index_map=[]
        index_insert=0
        
        i=0
        err_flag=False
        for x in data:
            WriteLog( "<Begin Case>:" + insert + str(x), True)
            try:
                cur.execute(insert, x)
            except Exception as err:
                err_flag=True
                WriteLog( str(err))
            finally:
                if(err_flag==False):
                    WriteLog( "[+++]Test Success")
                    index_insert=index_insert+1
                    index_map.insert(index_insert,i)
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                    
                WriteLog( "<End Case>:" + insert + str(x))
                err_flag=False
                
            i = i + 1
            #WriteLog( "%d, %s"%(i, str(x)))
        
        WriteLog( str(index_map))
                
        select = _get_select_stmt(tb_name, columns)
        cur.execute(select)
        #WriteLog( select)
            
        i=0
        #for i in range(len(data)):
        for i in range(len(index_map)):
            row = cur.fetchone()
            WriteLog("Fetch one row:"+str(row))
            if(row == None):
                break
            for j, col in enumerate(columns):
                WriteLog( "<Begin Case>:test column {0}".format(j), True)
                
                exp=expect_data[index_map[i]][j]
                res=row[j]
                #self.compare(col, exp, res)
                WriteLog( "name:%s, expect:%s, result:%s" %(col, exp, res))
                WriteLog( "name:{0}, expect:{1}, result:{2}".format(col, exp, res))
                if(exp==res):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag=False
                    
                WriteLog( str(type(row[j])))
                #if(type(row[j]) is float):
                if(type(row[j])==Time):
                    WriteLog( "[+++]Test type Success")
                else:
                    WriteLog( "[---]Test type Fail")
                    success_flag=False
                
        
        cur.close()
        
        self.assertTrue(success_flag==True, 'Data type case fail, please check the log in file') 


    #@unittest.skip("no")
    def test_date(self):
        tb_name = self.tables['date']

        cur = self.conn.cursor()
        
        cur.execute("DROP TABLE IF EXISTS %s" % tb_name)
        
        success_flag = True
        

        sql = ("CREATE TABLE {table} ("
               "col_date Date)"
               ).format(table=tb_name)
        WriteLog( sql)
        
        try:
            cur.execute(
                sql
            )
        except Exception as err:
            WriteLog( 'Exception:'+str(err))
            raise
            
        columns = [
            'col_date',
        ]
        
        data = [
            (Date(1, 1, 1),),
            (Date(2016, 2, 29),),
            (Date(9999, 12, 31),),
            (Date(1835, 2, 1),),
        ]
        
        
        insert = _insert_query(tb_name, columns)
        WriteLog( insert)
        
        index_map=[]
        index_insert=0
        
        i=0
        err_flag=False
        for x in data:
            WriteLog( "<Begin Case>:" + insert + str(x), True)
            try:
                cur.execute(insert, x)
            except Exception as err:
                err_flag=True
                WriteLog( str(err))
            finally:
                if(err_flag==False):
                    WriteLog( "[+++]Test Success")
                    index_insert=index_insert+1
                    index_map.insert(index_insert,i)
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                    
                WriteLog( "<End Case>:" + insert + str(x))
                err_flag=False
                
            i = i + 1
            #WriteLog( "%d, %s"%(i, str(x)))
        
        WriteLog( str(index_map))
                
        select = _get_select_stmt(tb_name, columns)
        cur.execute(select)
        #WriteLog( select)
            
        i=0
        #for i in range(len(data)):
        for i in range(len(index_map)):
            row = cur.fetchone()
            WriteLog("Fetch one row:"+str(row))
            if(row == None):
                break
            for j, col in enumerate(columns):
                WriteLog( "<Begin Case>:test column {0}".format(j), True)
                
                exp=data[index_map[i]][j]
                res=row[j]
                #self.compare(col, exp, res)
                WriteLog( "name:%s, expect:%s, result:%s" %(col, exp, res))
                WriteLog( "name:{0}, expect:{1}, result:{2}".format(col, exp, res))
                if(exp==res):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag=False
                    
                WriteLog( str(type(row[j])))
                #if(type(row[j]) is float):
                if(type(row[j])==Date):
                    WriteLog( "[+++]Test type Success")
                else:
                    WriteLog( "[---]Test type Fail")
                    success_flag=False

        
        invalid_data = [
            (0, 1, 1,),
            (1999, 0, 1,),
            (1752, 13, 12,),
            (2001, 12, 0,),
            (2001, 12, 32,),
            (2018, 2, 29,),
            (10000, 1, 1,),
        ]
        
        i=0
        expect_err_flag=False
        for x in invalid_data:
            WriteLog( "<Begin Case>:" + "set date" + str(x), True)
            try:
                Date(x[0], x[1], x[2])
            except Exception as err:
                expect_err_flag=True
                WriteLog( str(err))
            finally:
                if(expect_err_flag==True):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                    
                WriteLog( "<End Case>:" + "set date" + str(x))
                expect_err_flag=False
                
        
        cur.close()
        
        self.assertTrue(success_flag==True, 'Data type case fail, please check the log in file')
    
    #@unittest.skip("no")
    def test_timestamp(self):
        tb_name = self.tables['timestamp']

        cur = self.conn.cursor()
        
        cur.execute("DROP TABLE IF EXISTS %s" % tb_name)
        
        success_flag = True
        

        sql = ("CREATE TABLE {table} ("
               "col_timestamp Timestamp)"
               ).format(table=tb_name)
        WriteLog( sql)
        
        try:
            cur.execute(
                sql
            )
        except Exception as err:
            WriteLog( 'Exception:'+str(err))
            raise
            
        columns = [
            'col_timestamp',
        ]
        
        data = [
            (Timestamp(1, 1, 1),),
            (Timestamp(2016, 2, 29),),
            (Timestamp(9999, 12, 31),),
            (Timestamp(2, 1, 1),),
            (Timestamp(1835, 2, 1),),
            (Timestamp(2016, 2, 29, 15, 1, 22, 123456),),
            (Timestamp(year=2016, month=2, day=29, hour=15, minute=1, second=22, microsecond=999999),),
            (Timestamp(1981, 1, 1, 0, 0, 0, 0),),
            (Timestamp(2002, 8, 9, 8, 23, 40),),
            (Timestamp(2005, 10, 2, 20, 12, 23),),
            (Timestamp(2006, 9, 18, 23, 59, 59),),
            (Timestamp(1992, 2, 1, 0, 0, 0),),
            (Timestamp(1992, 2, 1, 0, 0, 0, 123),),
            (Timestamp(1992, 2, 1, 0, 0, 0, 999999),),
        ]
        
        
        insert = _insert_query(tb_name, columns)
        WriteLog( insert)
        
        index_map=[]
        index_insert=0
        
        i=0
        err_flag=False
        for x in data:
            WriteLog( "<Begin Case>:" + insert + str(x), True)
            try:
                cur.execute(insert, x)
            except Exception as err:
                err_flag=True
                WriteLog( str(err))
            finally:
                if(err_flag==False):
                    WriteLog( "[+++]Test Success")
                    index_insert=index_insert+1
                    index_map.insert(index_insert,i)
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                    
                WriteLog( "<End Case>:" + insert + str(x))
                err_flag=False
                
            i = i + 1
            #WriteLog( "%d, %s"%(i, str(x)))
        
        WriteLog( str(index_map))
                
        select = _get_select_stmt(tb_name, columns)
        cur.execute(select)
        #WriteLog( select)
            
        i=0
        #for i in range(len(data)):
        for i in range(len(index_map)):
            row = cur.fetchone()
            WriteLog("Fetch one row:"+str(row))
            if(row == None):
                break
            for j, col in enumerate(columns):
                WriteLog( "<Begin Case>:test column {0}".format(j), True)
                
                exp=data[index_map[i]][j]
                res=row[j]
                #self.compare(col, exp, res)
                WriteLog( "name:%s, expect:%s, result:%s" %(col, exp, res))
                WriteLog( "name:{0}, expect:{1}, result:{2}".format(col, exp, res))
                if(exp==res):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag=False
                    
                WriteLog( str(type(row[j])))
                #if(type(row[j]) is float):
                if(type(row[j])==Timestamp):
                    WriteLog( "[+++]Test type Success")
                else:
                    WriteLog( "[---]Test type Fail")
                    success_flag=False

        
        invalid_data = [
            (0, 1, 1,),
            (1999, 0, 1,),
            (1752, 13, 12,),
            (2001, 12, 0,),
            (2001, 12, 32,),
            (2018, 2, 29,),
            (10000, 1, 1,),
            (1987, 12, 21, 24, 0, 0),
            (1992, 3, 2, -1, 0, 0),
            (1995, 1, 1, 0, -1, 0,),
            (1998, 2, 6, 0, 60, 0,),
            (2001, 12, 21, 0, 1, -1,),
            (2002, 7, 8, 0, 23, 60,),
            (2001, 1, 1, 16, 22, 30,-1),
            (2001, 1, 1, 16, 22, 30,1000000),
        ]
        
        i=0
        expect_err_flag=False
        for x in invalid_data:
            WriteLog( "<Begin Case>:" + "set timestamp" + str(x), True)
            try:
                Timestamp(x[0], x[1], x[2],x[3], x[4], x[5], x[6])
            except Exception as err:
                expect_err_flag=True
                WriteLog( str(err))
            finally:
                if(expect_err_flag==True):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                    
                WriteLog( "<End Case>:" + "set timestamp" + str(x))
                expect_err_flag=False
                
        
        cur.close()
        
        self.assertTrue(success_flag==True, 'Data type case fail, please check the log in file')
    
    
    #@unittest.skip("no")
    def test_timestamp_precision(self):
        tb_name = self.tables['timestamp_precision']

        cur = self.conn.cursor()
        
        cur.execute("DROP TABLE IF EXISTS %s" % tb_name)
        
        success_flag = True
        

        sql = ("CREATE TABLE {table} ("
               "col_timestamp_0 timestamp(0),"
               "col_timestamp_1 timestamp(1),"
               "col_timestamp_2 timestamp(2),"
               "col_timestamp_3 timestamp(3),"
               "col_timestamp_4 timestamp(4),"
               "col_timestamp_5 timestamp(5),"
               "col_timestamp_6 timestamp(6),"
               "col_timestamp Timestamp"
               ")"
               ).format(table=tb_name)
        WriteLog( sql)
        
        try:
            cur.execute(
                sql
            )
        except Exception as err:
            WriteLog( 'Exception:'+str(err))
            raise
            
        columns = [
            'col_timestamp_0',
            'col_timestamp_1',
            'col_timestamp_2',
            'col_timestamp_3',
            'col_timestamp_4',
            'col_timestamp_5',
            'col_timestamp_6',
            'col_timestamp',
        ]

        data = [
            (Timestamp(1, 1, 1, 1, 1, 1, 123456), Timestamp(1, 1, 1, 1, 1, 1, 123456), Timestamp(1, 1, 1, 1, 1, 1, 123456), Timestamp(1, 1, 1, 1, 1, 1, 123456),
             Timestamp(1, 1, 1, 1, 1, 1, 123456), Timestamp(1, 1, 1, 1, 1, 1, 123456), Timestamp(1, 1, 1, 1, 1, 1, 123456), Timestamp(1, 1, 1, 1, 1, 1, 123456),),
            
            (Timestamp(1, 1, 1, 1, 1, 1, 12345), Timestamp(1, 1, 1, 1, 1, 1, 12345), Timestamp(1, 1, 1, 1, 1, 1, 12345), Timestamp(1, 1, 1, 1, 1, 1, 12345),
             Timestamp(1, 1, 1, 1, 1, 1, 12345), Timestamp(1, 1, 1, 1, 1, 1, 12345), Timestamp(1, 1, 1, 1, 1, 1, 12345), Timestamp(1, 1, 1, 1, 1, 1, 12345),),
            
            (Timestamp(1, 1, 1, 1, 1, 1, 1234), Timestamp(1, 1, 1, 1, 1, 1, 1234), Timestamp(1, 1, 1, 1, 1, 1, 1234), Timestamp(1, 1, 1, 1, 1, 1, 1234),
             Timestamp(1, 1, 1, 1, 1, 1, 1234), Timestamp(1, 1, 1, 1, 1, 1, 1234), Timestamp(1, 1, 1, 1, 1, 1, 1234), Timestamp(1, 1, 1, 1, 1, 1, 1234),),
            
            (Timestamp(1, 1, 1, 1, 1, 1, 123), Timestamp(1, 1, 1, 1, 1, 1, 123), Timestamp(1, 1, 1, 1, 1, 1, 123), Timestamp(1, 1, 1, 1, 1, 1, 123),
             Timestamp(1, 1, 1, 1, 1, 1, 123), Timestamp(1, 1, 1, 1, 1, 1, 123), Timestamp(1, 1, 1, 1, 1, 1, 123), Timestamp(1, 1, 1, 1, 1, 1, 123),),
            
            (Timestamp(1, 1, 1, 1, 1, 1, 12), Timestamp(1, 1, 1, 1, 1, 1, 12), Timestamp(1, 1, 1, 1, 1, 1, 12), Timestamp(1, 1, 1, 1, 1, 1, 12),
             Timestamp(1, 1, 1, 1, 1, 1, 12), Timestamp(1, 1, 1, 1, 1, 1, 12), Timestamp(1, 1, 1, 1, 1, 1, 12), Timestamp(1, 1, 1, 1, 1, 1, 12),),
            
            (Timestamp(1, 1, 1, 1, 1, 1, 1), Timestamp(1, 1, 1, 1, 1, 1, 1), Timestamp(1, 1, 1, 1, 1, 1, 1), Timestamp(1, 1, 1, 1, 1, 1, 1),
             Timestamp(1, 1, 1, 1, 1, 1, 1), Timestamp(1, 1, 1, 1, 1, 1, 1), Timestamp(1, 1, 1, 1, 1, 1, 1), Timestamp(1, 1, 1, 1, 1, 1, 1),),
            
            (Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0),
             Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0),),
        ]

        
        
        expect_data = [
            (Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 100000), Timestamp(1, 1, 1, 1, 1, 1, 120000), Timestamp(1, 1, 1, 1, 1, 1, 123000),
             Timestamp(1, 1, 1, 1, 1, 1, 123400), Timestamp(1, 1, 1, 1, 1, 1, 123450), Timestamp(1, 1, 1, 1, 1, 1, 123456), Timestamp(1, 1, 1, 1, 1, 1, 123456),),
            
            (Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 10000), Timestamp(1, 1, 1, 1, 1, 1, 12000),
             Timestamp(1, 1, 1, 1, 1, 1, 12300), Timestamp(1, 1, 1, 1, 1, 1, 12340), Timestamp(1, 1, 1, 1, 1, 1, 12345), Timestamp(1, 1, 1, 1, 1, 1, 12345),),
            
            (Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 1000),
             Timestamp(1, 1, 1, 1, 1, 1, 1200), Timestamp(1, 1, 1, 1, 1, 1, 1230), Timestamp(1, 1, 1, 1, 1, 1, 1234), Timestamp(1, 1, 1, 1, 1, 1, 1234),),
            
            (Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0),
             Timestamp(1, 1, 1, 1, 1, 1, 100), Timestamp(1, 1, 1, 1, 1, 1, 120), Timestamp(1, 1, 1, 1, 1, 1, 123), Timestamp(1, 1, 1, 1, 1, 1, 123),),
            
            (Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0),
             Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 10), Timestamp(1, 1, 1, 1, 1, 1, 12), Timestamp(1, 1, 1, 1, 1, 1, 12),),
            
            (Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0),
             Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 1), Timestamp(1, 1, 1, 1, 1, 1, 1),),
            
            (Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0),
             Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0), Timestamp(1, 1, 1, 1, 1, 1, 0),),
        ]

        insert = _insert_query(tb_name, columns)
        WriteLog( insert)
        
        index_map=[]
        index_insert=0
        
        i=0
        err_flag=False
        for x in data:
            WriteLog( "<Begin Case>:" + insert + str(x), True)
            try:
                cur.execute(insert, x)
            except Exception as err:
                err_flag=True
                WriteLog( str(err))
            finally:
                if(err_flag==False):
                    WriteLog( "[+++]Test Success")
                    index_insert=index_insert+1
                    index_map.insert(index_insert,i)
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag = False
                    
                WriteLog( "<End Case>:" + insert + str(x))
                err_flag=False
                
            i = i + 1
            #WriteLog( "%d, %s"%(i, str(x)))
        
        WriteLog( str(index_map))
                
        select = _get_select_stmt(tb_name, columns)
        cur.execute(select)
        #WriteLog( select)
            
        i=0
        #for i in range(len(data)):
        for i in range(len(index_map)):
            row = cur.fetchone()
            WriteLog("Fetch one row:"+str(row))
            if(row == None):
                break
            for j, col in enumerate(columns):
                WriteLog( "<Begin Case>:test column {0}".format(j), True)
                
                exp=expect_data[index_map[i]][j]
                res=row[j]
                #self.compare(col, exp, res)
                WriteLog( "name:%s, expect:%s, result:%s" %(col, exp, res))
                WriteLog( "name:{0}, expect:{1}, result:{2}".format(col, exp, res))
                if(exp==res):
                    WriteLog( "[+++]Test Success")
                else:
                    WriteLog( "[---]Test Fail")
                    success_flag=False
                    
                WriteLog( str(type(row[j])))
                #if(type(row[j]) is float):
                if(type(row[j])==Timestamp):
                    WriteLog( "[+++]Test type Success")
                else:
                    WriteLog( "[---]Test type Fail")
                    success_flag=False

        
        self.assertTrue(success_flag==True, 'Data type case fail, please check the log in file')
    
    
    #@unittest.skip("no")
    def test_DateFromTicks(self):
        ticks = time.mktime(time.localtime())
        #ticks=1
        #time.localtime(ticks)[:3]
        exp = datetime.date(*time.localtime(ticks)[:3])
        self.assertEqual(
            connector.DateFromTicks(ticks), exp,
            "Interface DateFromTicks should return a datetime.date")
    
    #@unittest.skip("no")
    def test_TimeFromTicks(self):
        ticks = time.mktime(time.localtime())
        exp = datetime.time(*time.localtime(ticks)[3:6])
        self.assertEqual(
            connector.TimeFromTicks(ticks), exp,
            "Interface TimeFromTicks should return a datetime.time")

    #@unittest.skip("no")
    def test_TimestampFromTicks(self):
        ticks = time.mktime(time.localtime())
        exp = datetime.datetime(*time.localtime(ticks)[:6])
        self.assertEqual(
            connector.TimestampFromTicks(ticks), exp,
            "Interface TimestampFromTicks should return a datetime.datetime")
    
    #@unittest.skip("no")
    def test_none(self):
        cur = self.conn.cursor()
        

        cur.execute("create table if not exists type_test_none (id int )")
        #cur.execute("insert into type_test_none values(?)", [1,])
        
        cur.execute("select * from type_test_none")
        
        res = cur.fetchone()
        
        self.assertEqual(res, None)
        
    # TODO: unsupported feature
    @unittest.skip("unsupported")
    def test_interval(self):
        pass
    
    # TODO: unsupported feature
    @unittest.skip("unsupported")
    def test_blob(self):
        pass