"""Module gathering all abstract base classes"""
from abc import ABCMeta, abstractmethod, abstractproperty
from .catch23 import make_abc


@make_abc(ABCMeta)
class TrafConnectionAbstract(object):
    """Abstract class for classes connecting to a Trafodion server"""

    def __init__(self, **kwargs):
        self.user = 0

    def config(self, **kwargs):
        pass

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