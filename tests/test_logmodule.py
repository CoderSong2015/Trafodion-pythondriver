import unittest
from pdbc.trafodion import connector
from .config import config
from configparser import ConfigParser
import os
import logging
import sys

class TestLogModule(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        '''
        [logger_simpleExample]
        level=DEBUG
        handlers=fileHandler
        qualname=simpleExample
        propagate=0
        
        
        [handler_fileHandler]
        class=FileHandler
        args=('logging.log', 'a')
        level=DEBUG
        formatter=simpleFormatter
        '''
        
        cls.log_level=None
        cls.log_file_name=None
        logger_level=None
        handler_level=None
        
        if 'logging_path' not in config:
            print("please config logging_path in data base connect config")
            return

        conf=ConfigParser()
        conf.read(config['logging_path'])

        root_section='logger_root'
        try:
            root_handler=conf.get(root_section, 'handlers')
        except Exception as e:
            print("please config handlers, e.g, handlers=fileHandler in",root_section)
            return
        finally:
            if not root_handler:
                print("please assign a value, e.g, handlers=fileHandler in",root_section)
                return

        
        try:
            item_value=conf.get(root_section, 'level')
            print(item_value)
            root_level=logging._checkLevel(item_value)
            print("root_level is", root_level)
        except Exception as e:
            print("warning: level is not configured in", root_section)
    
        if 'loggger_name' in config:
            logger_section = 'logger_'+ config['loggger_name']
        else:
            logger_section = 'logger_'+ 'root'
            
        try:
            logger_handlers=conf.get(logger_section, 'handlers')
        except Exception as e:
            print("please config handlers, e.g, handlers=fileHandler in",logger_section)
            return
        finally:
            if not logger_handlers:
                print("please assign a value, e.g, handlers=fileHandler in",logger_section)
                return
                
        handlers_list = logger_handlers.split(',')
        print(handlers_list)
        
        if 'fileHandler' not in handlers_list:
            print("please config handlers=fileHandler in",logger_section)
            return
        
        try:
            item_value=conf.get(logger_section, 'level')
            print(item_value)
            logger_level=logging._checkLevel(item_value)
            print("logger_level is", logger_level)
        except Exception as e:
            print("warning: level is not configured in", logger_section)
        
        '''
        logger_items=conf.items(logger_section)
        print(logger_items)
        
        for item in logger_items:
            if item[0]=='level':
                cls.log_level=item[1]
        '''
        
        handler_section='handler_'+'fileHandler'
        
        try:
            cls.log_class=conf.get(handler_section, 'class')
            print(cls.log_class)
            if not cls.log_class:
                print("please config class, e.g, class=FileHandler, in",handler_section)
                return
        except Exception as e:
            print("warning: class is not configured in", handler_section)
        

        try:
            log_file_info=conf.get(handler_section, 'args')
            cls.log_file_name=log_file_info.split("'")[1]
            if not cls.log_file_name:
                print("please config args, e.g, args=('logging.log', 'a'), in",handler_section)
                return
        except Exception as e:
            print("please config args, e.g, args=('logging.log', 'a'), in",handler_section)
            return
        
        
        '''
        handler_section='handler_'+'fileHandler'
        handler_items=conf.items(handler_section)
       
        for item in handler_items:
            if item[0]=='class':
                cls.log_class=item[1]
            elif item[0]=='args':
                cls.log_file_name=item[1].split("'")[1]
                if not cls.log_file_name:
                    print("please config args , e.g, args=('logging.log', 'a'), in",handler_section)
                    #return
            elif item[0]=='level':
                cls.log_level=item[1]
        '''
       
        try:
            item_value=conf.get(handler_section, 'level')
            print(item_value)
            handler_level=logging._checkLevel(item_value)
            print("handler_level is", handler_level)
        except Exception as e:
            print("warning: level is not configured in", handler_section)
            
            
        if logger_level and handler_level:
            cls.log_level=max(logger_level, handler_level)
        elif logger_level:
            cls.log_level=logger_level
        elif handler_level:
            cls.log_level=handler_level
        elif root_level:
            cls.log_level=root_level
        else:
            cls.log_level=logging._checkLevel("WARNING")
        
        print("cls.log_level is", cls.log_level)
    
    @classmethod
    def tearDownClass(cls):
        pass
    
    def test_ddl_syntax_err_0(self):
        try:
            cnx=None
            if not self.log_file_name:
                print("log file's name is not configured in logger handler")
                return
            
            with open(self.log_file_name, 'w') as f:
                f.truncate()
                
            cnx = connector.connect(**config)
            cursor = cnx.cursor()
            
            cursor.execute('cr table if not exists tb_test_logmodule(c1 int)')
        except Exception as e:
            if not cnx:
                raise e
            
            with open(self.log_file_name, 'r') as f:
                expect_str='*** ERROR[15001] A syntax error occurred at or before: \n\
cr table if not exists tb_test_logmodule(c1 int);'
         
                log_content = f.read()
                print (log_content)
                print (expect_str)

                if log_content.find(expect_str)==-1:
                    self.fail('test_ddl_syntax_err_0 fail, expect log not found in log file, case Fail!!!')
        finally:
            if cnx:
                cnx.close()

    def test_ddl_syntax_err_1(self):
        try:
            cnx=None
            if not self.log_file_name:
                print("log file's name is not configured in logger handler")
                return
            
            with open(self.log_file_name, 'w') as f:
                f.truncate()
                
            cnx = connector.connect(**config)
            cursor = cnx.cursor()
            
            cursor.execute('create table if not exists tb_test_logmodule(c1 type_not_exist)')
        except Exception as e:
            if not cnx:
                raise e
            
            with open(self.log_file_name, 'r') as f:
                expect_str='*** ERROR[15001] A syntax error occurred at or before: \n\
create table if not exists tb_test_logmodule(c1 type_not_exist);'
         
                log_content = f.read()
                print (log_content)
                print (expect_str)

                if log_content.find(expect_str)==-1:
                    self.fail('test_ddl_syntax_err_1 fail, expect log not found in log file, case Fail!!!')
        finally:
            if cnx:
                cnx.close()

    def test_ddl_err_0(self):
        try:
            cnx=None
            if not self.log_file_name:
                print("log file's name is not configured in logger handler")
                return
            
            with open(self.log_file_name, 'w') as f:
                f.truncate()
            
            cnx = connector.connect(**config)
            cursor = cnx.cursor()
            cursor.execute('create table if not exists tb_test_logmodule(c1 int)')
            cursor.execute('create table tb_test_logmodule(c1 int)')

        except Exception as e:
            if not cnx:
                raise e
            
            with open(self.log_file_name, 'r') as f:
                expect_str='TB_TEST_LOGMODULE already exists in Trafodion'
         
                log_content = f.read()
                print (log_content)
                print (expect_str)

                if log_content.find(expect_str)==-1:
                    self.fail('test_ddl_err_0 fail, expect log not found in log file, case Fail!!!')
        finally:
            if cnx:
                cnx.close()

    def test_ddl_err_1(self):
        try:
            cnx=None
            if not self.log_file_name:
                print("log file's name is not configured in logger handler")
                return
            
            with open(self.log_file_name, 'w') as f:
                f.truncate()
            
            cnx = connector.connect(**config)
            cursor = cnx.cursor()
            cursor.execute('drop table tb_not_exist')
        except Exception as e:
            if not cnx:
                raise e
            
            with open(self.log_file_name, 'r') as f:
                expect_str='TB_NOT_EXIST does not exist in Trafodion'
         
                log_content = f.read()
                print (log_content)
                print (expect_str)

                if log_content.find(expect_str)==-1:
                    self.fail('test_ddl_err_0 fail, expect log not found in log file, case Fail!!!')
        finally:
            if cnx:
                cnx.close()

    def test_ddl_err_tmp(self):
        try:
            cnx=None
            cnx = connector.connect(**config)
            cursor = cnx.cursor()
            cursor.execute('drop table tb_not_exist')
        finally:
            if cnx:
                cnx.close()

    def test_ddl_success(self):
        try:
            cnx=None
            if not self.log_file_name:
                print("log file's name is not configured in logger handler")
                return
            
            with open(self.log_file_name, 'w') as f:
                f.truncate()
            
            cnx = connector.connect(**config)
            cursor = cnx.cursor()
            cursor.execute('create table if not exists tb_test_logmodule(c1 int)')
            cursor.execute('drop table if exists tb_test_logmodule')
            
            with open(self.log_file_name, 'r') as f:
                log_content = f.read()
                print (log_content)
                
                
                expect_str="EXECUTE SUCCESS\n\
Execute Type: SQLEXECDIRECT\n\
Query: b'create table if not exists tb_test_logmodule(c1 int)'"
                print (expect_str)
                
                if self.log_level==logging._checkLevel("DEBUG"):
                    if log_content.find(expect_str)==-1:
                        self.fail('log about create table, debug info should be logged, case Fail!!!')
                else:
                    if log_content.find(expect_str)!=-1:
                        self.fail('log about create table, debug info should not be logged, case Fail!!!')
                    
                expect_str="EXECUTE SUCCESS\n\
Execute Type: SQLEXECDIRECT\n\
Query: b'drop table if exists tb_test_logmodule'"
                print (expect_str)

                if self.log_level==logging._checkLevel("DEBUG"):
                    if log_content.find(expect_str)==-1:
                        self.fail('log about drop table, debug info should be logged, case Fail!!!')
                else:
                    if log_content.find(expect_str)!=-1:
                        self.fail('log about drop table, debug info should not be logged, case Fail!!!')

        except Exception as e:
            raise e
        finally:
            if cnx:
                cnx.close()
    
    def test_dml_syntax_err_0(self):
        try:
            cnx=None
            if not self.log_file_name:
                print("log file's name is not configured in logger handler")
                return
            
            with open(self.log_file_name, 'w') as f:
                f.truncate()
                
            cnx = connector.connect(**config)
            cursor = cnx.cursor()
            
            cursor.execute('create table if not exists tb_test_logmodule(c1 int)')
            cursor.execute('inserrrrrt into tb_test_logmodule values(1)')
        except Exception as e:
            if not cnx:
                raise e
            
            with open(self.log_file_name, 'r') as f:
                expect_str='*** ERROR[15001] A syntax error occurred at or before: \n\
inserrrrrt into tb_test_logmodule values(1);'
         
                log_content = f.read()
                print (log_content)
                print (expect_str)

                if log_content.find(expect_str)==-1:
                    self.fail('test_dml_syntax_err_0 fail, expect log not found in log file, case Fail!!!')
        finally:
            if cnx:
                cnx.close()
         
         
    def test_dml_syntax_err_1(self):
        try:
            cnx=None
            if not self.log_file_name:
                print("log file's name is not configured in logger handler")
                return
            
            with open(self.log_file_name, 'w') as f:
                f.truncate()
                
            cnx = connector.connect(**config)
            cursor = cnx.cursor()
            
            cursor.execute('create table if not exists tb_test_logmodule(c1 int)')
            cursor.execute("insert into tb_test_logmodule values('abc')")
        except Exception as e:
            if not cnx:
                raise e
            
            with open(self.log_file_name, 'r') as f:
                expect_str='*** ERROR[8413] The string argument contains characters that cannot be converted.'
         
                log_content = f.read()
                print (log_content)
                print (expect_str)

                if log_content.find(expect_str)==-1:
                    self.fail('test_dml_syntax_err_1 fail, expect log not found in log file, case Fail!!!')
        finally:
            if cnx:
                cnx.close()
                       
    def test_dml_execute(self):
        try:
            cnx=None
            if not self.log_file_name:
                print("log file's name is not configured in logger handler")
                return
            
            with open(self.log_file_name, 'w') as f:
                f.truncate()
            
            cnx = connector.connect(**config)
            cursor = cnx.cursor()
            cursor.execute('create table if not exists tb_test_logmodule(c1 int)')
            cursor.execute('insert into tb_test_logmodule values(1)')
            cursor.execute('select * from tb_test_logmodule')
            res=cursor.fetchone()
            cursor.execute('select c1 from tb_test_logmodule where c1=1')
            res=cursor.fetchall()
            cursor.execute('update tb_test_logmodule set c1=2 where c1=1')
            cursor.execute('delete from tb_test_logmodule where c1=2')


            with open(self.log_file_name, 'r') as f:
                log_content = f.read()
                print (log_content)
                
                expect_str="EXECUTE SUCCESS\n\
Execute Type: SQLEXECDIRECT\n\
Query: b'insert into tb_test_logmodule values(1)'"
                print (expect_str)

                if self.log_level==logging._checkLevel("DEBUG"):
                    if log_content.find(expect_str)==-1:
                        self.fail('log about insert, debug info should be logged, case Fail!!!')
                else:
                    if log_content.find(expect_str)!=-1:
                        self.fail('log about insert, debug info should not be logged, case Fail!!!')
                        
                expect_str="EXECUTE SUCCESS\n\
Execute Type: SQLEXECDIRECT\n\
Query: b'select * from tb_test_logmodule'"
                print (expect_str)
                
                if self.log_level==logging._checkLevel("DEBUG"):
                    if log_content.find(expect_str)==-1:
                        self.fail('log about select, debug info should be logged, case Fail!!!')
                else:
                    if log_content.find(expect_str)!=-1:
                        self.fail('log about select, debug info should not be logged, case Fail!!!') 
                           
                expect_str="EXECUTE SUCCESS\n\
Execute Type: SQLEXECDIRECT\n\
Query: b'select c1 from tb_test_logmodule where c1=1'"
                print (expect_str)

                if self.log_level==logging._checkLevel("DEBUG"):
                    if log_content.find(expect_str)==-1:
                        self.fail('log about select, debug info should be logged, case Fail!!!')
                else:
                    if log_content.find(expect_str)!=-1:
                        self.fail('log about select, debug info should not be logged, case Fail!!!') 
                    
                expect_str="EXECUTE SUCCESS\n\
Execute Type: SQLEXECDIRECT\n\
Query: b'update tb_test_logmodule set c1=2 where c1=1'"
                print (expect_str)

                if self.log_level==logging._checkLevel("DEBUG"):
                    if log_content.find(expect_str)==-1:
                        self.fail('log about update, debug info should be logged, case Fail!!!')
                else:
                    if log_content.find(expect_str)!=-1:
                        self.fail('log about update, debug info should not be logged, case Fail!!!') 
                    
                expect_str="EXECUTE SUCCESS\n\
Execute Type: SQLEXECDIRECT\n\
Query: b'delete from tb_test_logmodule where c1=2'"
                print (expect_str)

                if self.log_level==logging._checkLevel("DEBUG"):
                    if log_content.find(expect_str)==-1:
                        self.fail('log about delete, debug info should be logged, case Fail!!!')
                else:
                    if log_content.find(expect_str)!=-1:
                        self.fail('log about delete, debug info should not be logged, case Fail!!!') 
        except Exception as e:
            raise e
        finally:
            if cnx:
                cnx.close()
     
    def test_dml_executemany(self):
        try:
            cnx=None
            if not self.log_file_name:
                print("log file's name is not configured in logger handler")
                return
            
            with open(self.log_file_name, 'w') as f:
                f.truncate()
            
            cnx = connector.connect(**config)
            cursor = cnx.cursor()
            cursor.execute('create table if not exists tb_test_logger2(id int, age int)')
            cursor.executemany('insert into tb_test_logger2 values(?,?)', [(1,1),(2,2),(3,3),(4,4),(5,5)])
            cursor.executemany('update tb_test_logger2 set age=? where id=?', [(10,1),(20,2),(30,3),(40,4),(50,5)])
            cursor.executemany('delete from tb_test_logger2 where id=?', [(1,),(2,),(3,),(4,),(5,)])

            with open(self.log_file_name, 'r') as f:
                log_content = f.read()
                print (log_content)
                
                expect_str="EXECUTE SUCCESS\n\
Execute Type: SQLEXECUTE2\n\
Query: b'insert into tb_test_logger2 values(?,?)'"
                print (expect_str)

                if self.log_level==logging._checkLevel("DEBUG"):
                    if log_content.find(expect_str)==-1:
                        self.fail('log about executemany insert, debug info should be logged, case Fail!!!')
                else:
                    if log_content.find(expect_str)!=-1:
                        self.fail('log about executemany insert, debug info should not be logged, case Fail!!!')

                expect_str="EXECUTE SUCCESS\n\
Execute Type: SQLEXECUTE2\n\
Query: b'update tb_test_logger2 set age=? where id=?'"
                print (expect_str)

                if self.log_level==logging._checkLevel("DEBUG"):
                    if log_content.find(expect_str)==-1:
                        self.fail('log about executemany update, debug info should be logged, case Fail!!!')
                else:
                    if log_content.find(expect_str)!=-1:
                        self.fail('log about executemany update, debug info should not be logged, case Fail!!!')
                    
                expect_str="EXECUTE SUCCESS\n\
Execute Type: SQLEXECUTE2\n\
Query: b'delete from tb_test_logger2 where id=?'"
                print (expect_str)

                if self.log_level==logging._checkLevel("DEBUG"):
                    if log_content.find(expect_str)==-1:
                        self.fail('log about executemany delete, debug info should be logged, case Fail!!!')
                else:
                    if log_content.find(expect_str)!=-1:
                        self.fail('log about executemany delete, debug info should not be logged, case Fail!!!')
                    
        except Exception as e:
            raise e
        finally:
            if cnx:
                cnx.close()
