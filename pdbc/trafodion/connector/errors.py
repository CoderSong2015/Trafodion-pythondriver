
from .catch23 import PY2
def get_client_error(error, language='eng'):

    pass

    return "Unknown Trafodion error"


class Error(Exception):
    """Exception that is base class for all other error exceptions"""
    def __init__(self, full_msg=None, msg_list=None, errno=None, sqlstate=None, ):
        super(Error, self).__init__()

        # msg is a turple
        self.msg = []
        self.errno = []
        self.sqlstate = []
        # _full_msg is the real message which will be raised
        # we need to fommat the full_msg
        self._full_msg = full_msg



        if msg_list and isinstance(msg_list,list or tuple):
            errno_list = []
            sqlstate_list = []
            tmp_msg_list= []
            for item in msg_list:
                errno_list.append(item.sql_code)
                sqlstate_list.append(item.sql_state)
                tmp_msg_list.append(item.text)
            self.errno = errno_list
            self.sqlstate = sqlstate_list
            self.msg = tmp_msg_list
        else:
            self.errno.append(-1)
            self.sqlstate.append(-1)
            self.msg.append(self._full_msg)
        self.args = (self.errno, self._full_msg, self.sqlstate)

    def __str__(self):
        return self._full_msg

    def get_errno(self):
        return self.errno

    def get_sqlstate(self):
        return self.sqlstate

    def get_msg(self):
        return self.msg

class Warning(Exception):  # pylint: disable=W0622
    """Exception for important warnings"""
    pass


class InterfaceError(Error):
    """Exception for errors related to the interface"""
    pass


class DatabaseError(Error):
    """Exception for errors related to the database"""
    pass


class InternalError(DatabaseError):
    """Exception for errors internal database errors"""
    pass


class OperationalError(DatabaseError):
    """Exception for errors related to the database's operation"""
    pass


class ProgrammingError(DatabaseError):
    """Exception for errors programming errors"""
    pass


class IntegrityError(DatabaseError):
    """Exception for errors regarding relational integrity"""
    pass


class DataError(DatabaseError):
    """Exception for errors reporting problems with processed data"""
    pass


class NotSupportedError(DatabaseError):
    """Exception for errors when an unsupported database feature was used"""
    pass


class PoolError(Error):
    """Exception for errors relating to connection pooling"""
    pass


