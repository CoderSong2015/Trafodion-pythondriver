"""Module gathering all abstract base classes"""
from abc import ABCMeta, abstractmethod, abstractproperty
from .catch23 import make_abc
from .struct_def import TrafProperty
import os
from .network import TrafTCPSocket


@make_abc(ABCMeta)
class TrafConnectionAbstract(object):
    """Abstract class for classes connecting to a Trafodion server"""

    def __init__(self, **kwargs):

        self.user = 0
        self.property = {}
        self._init_property()

    def config(self, **kwargs):
        config = kwargs.copy()
        if 'user' in config or 'password' in config:
            try:
                user = config['user']
                del config['user']
            except KeyError:
                user = self._username
            try:
                password = config['password']
                del config['password']
            except KeyError:
                password = self._password
            self.set_login(user, password)

        # Configure host information
        if 'host' in config and config['host']:
            self._host = config['host']
            self.property.master_host = self._host

        if 'tenant_name' in config and config['tenant_name']:
            self.property.tenant_name = config['tenant_name']
        else:
            self.property.tenant_name = "ESGYNDB"

        # Check network locations
        if 'schema' in config and config['schema']:
            self.property.schema = config['schema'].upper()

        if 'charset' in config and config['charset']:
            self.property.charset = config['charset']

        if 'logging_path' in config and config['logging_path']:
            self.property.logging_path = config['logging_path']

        if 'loggger_name' in config and config['loggger_name']:
            self.property.loggger_name = config['loggger_name']

        if 'query_timeout' in config and config['query_timeout']:
            self.property.query_timeout = config['query_timeout']

        if 'connection_timeout' in config and config['connection_timeout']:
            self.property.connection_timeout = config['connection_timeout']

        try:
            self._port = int(config['port'])
            self.property.master_port = self._port
            del config['port']
        except KeyError:
            pass  # Missing port argument is OK

    def _get_connection(self, host='127.0.0.1', port=0):

        """
        :param host: 
        :param port: 
        :return: 
        """
        conn = None
        if self._unix_socket and os.name != 'nt':
            conn = TrafTCPSocket(self._unix_socket)
        else:
            conn = TrafTCPSocket(host=host,
                                 port=port,
                                 force_ipv6=self._force_ipv6)

        conn.set_connection_timeout(self.property.connection_timeout)
        conn.open_connection()
        return conn

    @abstractmethod
    def _connect_to_mxosrvr(self):
        """Open the connection to the mxosrvr server"""
        pass

    @abstractmethod
    def _get_objref(self):
        """
        get connection and mxosrvr info from dcsmaster
        :return: 
        """
        pass

    @abstractmethod
    def close(self):
        pass

    def connect(self, **kwargs):
        """Connect to the Trafodion server
        If no arguments are given, it will use the already configured or default
                values.

        Firstly, it will connect to dcsmaster to get info of a  free mxosrvr.
        Then connecting to mxosrvr
        """

        self._connect_to_mxosrvr()

    @abstractmethod
    def is_connected(self):
        pass

    @abstractmethod
    def ping(self, reconnect=False, attempts=1, delay=0):
        """Check availability of the mxosrvr server"""
        pass

    @abstractmethod
    def commit(self):
        """Commit current transaction"""
        pass

    @abstractmethod
    def cursor(self, buffered=None, raw=None, prepared=None, cursor_class=None,
               dictionary=None, named_tuple=None):
        """Instantiates and returns a cursor"""
        pass

    @abstractmethod
    def _execute_query(self, query):
        """Execute a query"""
        pass

    @abstractmethod
    def rollback(self):
        """Rollback current transaction"""
        pass

    def set_login(self, username=None, password=None):
        if username is not None:
            self._username = username.strip()
        else:
            self._username = ''
        if password is not None:
            self._password = password
        else:
            self._password = ''

    def _init_property(self):
        self.property = TrafProperty()

    @abstractmethod
    def _tcp_io_read(self, conn):
        """
        This function used to send or recieve data from server
        :param header: 
        :param buffer: 
        :param conn: use this connection to send
        :return: databuffer
        """
        pass

    @abstractmethod
    def _tcp_io_write(self, header, buffer, conn):
        """
        This function used to send or recieve data from server
        :param header: 
        :param buffer: 
        :param conn: use this connection to send
        :return: databuffer
        """
        pass

    @abstractmethod
    def set_auto_commit(self, auto_commit=True):
        """
            if auto commit is false , user could use connection.commit() to 
            commit any pending transaction to the database.
        :param auto_commit: boolean
        :return: None
        """
        pass

@make_abc(ABCMeta)
class TrafCursorAbstract(object):
    """Abstract cursor class

    Abstract class defining cursor class with method and members
    required by the Python Database API Specification v2.0.
    """
    def __init__(self):
        """Initialization"""
        self._description = None
        self._rowcount = -1
        self._last_insert_id = None
        self._warnings = None
        self.arraysize = 1

    @abstractmethod
    def callproc(self, procname, args=()):
        """For stored procedure with the given arguments
        """
        pass

    @abstractmethod
    def close(self):
        """Close the cursor."""
        pass

    @abstractmethod
    def execute(self, operation, params=(), multi=False):
        """Executes the given operation

        Executes the given operation substituting any markers with
        the given parameters.

        For example, getting all rows where id is 5:
          cursor.execute("SELECT * FROM t1 WHERE id = %s", (5,))

        The multi argument should be set to True when executing multiple
        statements in one operation. If not set and multiple results are
        found, an InterfaceError will be raised.

        If warnings where generated, and connection.get_warnings is True, then
        self._warnings will be a list containing these warnings.

        Returns an iterator when multi is True, otherwise None.
        """
        pass

    @abstractmethod
    def executemany(self, operation, seq_params):
        """Execute the given operation multiple times

        data = [
            ('Jane','183'),
            ('Joe', '137'),
            ('John', '187')
            ]
        stmt = "INSERT INTO employees (name, phone) VALUES ('%s','%s')"
        cursor.executemany(stmt, data)
        It is used like batch mode and will be optimized in sql.

        """
        pass

    @abstractmethod
    def fetchone(self):
        """Returns next row of a query result set

        Returns a tuple or None.
        """
        pass

    @abstractmethod
    def fetchmany(self, size=1):
        """Returns the next set of rows of a query result, returning a
        list of tuples. When no more rows are available, it returns an
        empty list.

        The number of rows returned can be specified using the size argument,
        which defaults to one
        """
        pass

    @abstractmethod
    def fetchall(self):
        """Returns all rows of a query result set

        Returns a list of tuples.
        """
        pass

    def reset(self, free=True):
        """Reset the cursor to default"""
        pass

    @property
    def description(self):
        """Returns description of columns in a result

        For catalog
        """
        return self._description

    @property
    def rowcount(self):
        """Returns the number of rows produced or affected

        Returns an integer.
        """
        return self._rowcount

    @property
    def lastrowid(self):
        """Returns the value generated for an AUTO_INCREMENT column

        Returns a long value or None.
        """
        return self._last_insert_id

    def fetchwarnings(self):
        """Returns Warnings."""
        return self._warnings
