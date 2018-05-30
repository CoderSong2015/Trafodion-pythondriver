"""Module gathering all abstract base classes"""
from abc import ABCMeta, abstractmethod, abstractproperty
from .catch23 import make_abc


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
                user = self._user
            try:
                password = config['password']
                del config['password']
            except KeyError:
                password = self._password
            self.set_login(user, password)

        # Configure host information
        if 'host' in config and config['host']:
            self._host = config['host']

        # Check network locations
        try:
            self._port = int(config['port'])
            del config['port']
        except KeyError:
            pass  # Missing port argument is OK


    @abstractmethod
    def _open_connection(self):
        """Open the connection to the mxosrvr server"""
        pass

    @abstractmethod
    def _get_Objref(self):
        """
        get connection and mxosrvr info from dcsmaster
        :return: 
        """
        pass

    @abstractmethod
    def disconnect(self):
        pass

    def connect(self, **kwargs):
        """Connect to the Trafodion server
        If no arguments are given, it will use the already configured or default
                values.

        Firstly, it will connect to dcsmaster to get info of a  free mxosrvr.
        Then connecting to mxosrvr
        """

        if kwargs:
            self.config(**kwargs)

        info = self._get_Objref()
        self._open_connection()

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
        pass

    @abstractmethod
    def _tcp_io_read(self, header, buffer,conn):
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