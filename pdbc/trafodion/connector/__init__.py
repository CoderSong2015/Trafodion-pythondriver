from .connection import TrafConnection
from .errors import (Warning, InterfaceError, DatabaseError, InternalError, OperationalError, ProgrammingError,
                     IntegrityError, DataError,
                     NotSupportedError)
from .constants import FIELD_TYPE
import datetime, time

threadsafety = 1
apilevel = "2.0"
paramstyle = "qmark"


def Connect(*args, **kwargs):
    return TrafConnection(*args, **kwargs)

Connection = connect = Connect


class _DBAPISet(frozenset):

    def __ne__(self, other):
        if isinstance(other, set):
            return frozenset.__ne__(self, other)
        else:
            return other not in self

    def __eq__(self, other):
        if isinstance(other, frozenset):
            return frozenset.__eq__(self, other)
        else:
            return other in self

    def __hash__(self):
        return frozenset.__hash__(self)

Binary = bytes

STRING = _DBAPISet([FIELD_TYPE.SQLTYPECODE_CHAR, FIELD_TYPE.SQLTYPECODE_VARCHAR,
                    FIELD_TYPE.SQLTYPECODE_VARCHAR_WITH_LENGTH])
BINARY = _DBAPISet([FIELD_TYPE.SQLTYPECODE_BLOB,
                    FIELD_TYPE.SQLTYPECODE_CLOB,
                    FIELD_TYPE.SQLTYPECODE_CHAR_DBLBYTE,
                    FIELD_TYPE.SQLTYPECODE_VARCHAR_DBLBYTE])
NUMBER = _DBAPISet([FIELD_TYPE.SQLTYPECODE_NUMERIC,
                    FIELD_TYPE.SQLTYPECODE_NUMERIC_UNSIGNED,
                    FIELD_TYPE.SQLTYPECODE_DECIMAL,
                    FIELD_TYPE.SQLTYPECODE_DECIMAL_UNSIGNED,
                    FIELD_TYPE.SQLTYPECODE_DECIMAL_LARGE,
                    FIELD_TYPE.SQLTYPECODE_DECIMAL_LARGE_UNSIGNED,
                    FIELD_TYPE.SQLTYPECODE_INTEGER,
                    FIELD_TYPE.SQLTYPECODE_INTEGER_UNSIGNED,
                    FIELD_TYPE.SQLTYPECODE_LARGEINT,
                    FIELD_TYPE.SQLTYPECODE_LARGEINT_UNSIGNED,
                    FIELD_TYPE.SQLTYPECODE_SMALLINT,
                    FIELD_TYPE.SQLTYPECODE_SMALLINT_UNSIGNED,
                    FIELD_TYPE.SQLTYPECODE_BPINT_UNSIGNED,
                    FIELD_TYPE.SQLTYPECODE_TINYINT,
                    FIELD_TYPE.SQLTYPECODE_TINYINT_UNSIGNED,
                    FIELD_TYPE.SQLTYPECODE_FLOAT,
                    FIELD_TYPE.SQLTYPECODE_REAL,
                    FIELD_TYPE.SQLTYPECODE_DOUBLE])

Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime

DATETIME = _DBAPISet([FIELD_TYPE.SQLTYPECODE_DATETIME])
ROWID = _DBAPISet()


def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])

__all__ = [

    'DataError', 'DatabaseError', 'Error', 'IntegrityError',
    'InterfaceError', 'InternalError',
    'NotSupportedError', 'OperationalError', 'ProgrammingError',

    'Connect', 'connect', 'apilevel', 'threadsafety', 'paramstyle',
    'Date', 'Time', 'Timestamp', 'Binary',
    'DateFromTicks', 'DateFromTicks', 'TimestampFromTicks', 'TimeFromTicks',
    'STRING', 'BINARY', 'NUMBER',
    'DATETIME', 'ROWID',
]
