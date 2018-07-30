
from .catch23 import PY2
def get_client_error(error, language='eng'):

    pass

    return "Unknown Trafodion error"


class Error(Exception):
    """Exception that is base class for all other error exceptions"""
    def __init__(self, msg=None, errno=None, values=None, sqlstate=None):
        super(Error, self).__init__()
        self.msg = msg
        self._full_msg = self.msg
        self.errno = errno or -1
        self.sqlstate = sqlstate

        self.msg = get_client_error(self.errno)
        if values is not None:
            try:
                self.msg = self.msg % values
            except TypeError as err:
                self.msg = "{0} (Warning: {1})".format(self.msg, str(err))
        elif not self.msg:
            self._full_msg = self.msg = 'Unknown error'

        if self.msg and self.errno != -1:
            fields = {
                'errno': self.errno,
                'msg': self.msg.encode('utf8')
            }
            if self.sqlstate:
                fmt = '{errno} ({state}): {msg}'
                fields['state'] = self.sqlstate
            else:
                fmt = '{errno}: {msg}'
            self._full_msg = fmt.format(**fields)

        self.args = (self.errno, self._full_msg, self.sqlstate)

    def __str__(self):
        return self._full_msg


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


