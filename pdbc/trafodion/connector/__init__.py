from .connection import TrafConnection
from .errors import (Warning, InterfaceError, DatabaseError, InternalError, OperationalError, ProgrammingError,
                     IntegrityError, DataError,
                     NotSupportedError)

threadsafety = 1
apilevel = "2.0"
paramstyle = "qmark"


def connect(*args, **kwargs):
    return TrafConnection(*args, **kwargs)

Connection = Connect = connect

__all__ = [
    'DataError', 'DatabaseError', 'Error', 'IntegrityError',
    'InterfaceError', 'InternalError',
    'NotSupportedError', 'OperationalError', 'ProgrammingError',
    'apilevel', 'Connect',
    'paramstyle', 'threadsafety', 'version_info',

]
