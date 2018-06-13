
from .catch23 import PY2
def get_client_error(error, language='eng'):

    pass
    """
    try:
        tmp = __import__('mysql.connector.locales.{0}'.format(language),
                         globals(), locals(), ['client_error'])
    except ImportError:
        raise ImportError("No localization support for language '{0}'".format(
            language))
    client_error = tmp.client_error

    if isinstance(error, int):
        errno = error
        for key, value in errorcode.__dict__.items():
            if value == errno:
                error = key
                break

    if isinstance(error, (str)):
        try:
            return getattr(client_error, error)
        except AttributeError:
            return None

    raise ValueError("error argument needs to be either an integer or string")
    """

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



_SQLSTATE_CLASS_EXCEPTION = {
    '02': DataError,  # no data
    '07': DatabaseError,  # dynamic SQL error
    '08': OperationalError,  # connection exception
    '0A': NotSupportedError,  # feature not supported
    '21': DataError,  # cardinality violation
    '22': DataError,  # data exception
    '23': IntegrityError,  # integrity constraint violation
    '24': ProgrammingError,  # invalid cursor state
    '25': ProgrammingError,  # invalid transaction state
    '26': ProgrammingError,  # invalid SQL statement name
    '27': ProgrammingError,  # triggered data change violation
    '28': ProgrammingError,  # invalid authorization specification
    '2A': ProgrammingError,  # direct SQL syntax error or access rule violation
    '2B': DatabaseError,  # dependent privilege descriptors still exist
    '2C': ProgrammingError,  # invalid character set name
    '2D': DatabaseError,  # invalid transaction termination
    '2E': DatabaseError,  # invalid connection name
    '33': DatabaseError,  # invalid SQL descriptor name
    '34': ProgrammingError,  # invalid cursor name
    '35': ProgrammingError,  # invalid condition number
    '37': ProgrammingError,  # dynamic SQL syntax error or access rule violation
    '3C': ProgrammingError,  # ambiguous cursor name
    '3D': ProgrammingError,  # invalid catalog name
    '3F': ProgrammingError,  # invalid schema name
    '40': InternalError,  # transaction rollback
    '42': ProgrammingError,  # syntax error or access rule violation
    '44': InternalError,   # with check option violation
    'HZ': OperationalError,  # remote database access
    'XA': IntegrityError,
    '0K': OperationalError,
    'HY': DatabaseError,  # default when no SQLState provided by MySQL server
}

_ERROR_EXCEPTIONS = {
    1243: ProgrammingError,
    1210: ProgrammingError,
    2002: InterfaceError,
    2013: OperationalError,
    2049: NotSupportedError,
    2055: OperationalError,
    2061: InterfaceError,
    2026: InterfaceError,
}