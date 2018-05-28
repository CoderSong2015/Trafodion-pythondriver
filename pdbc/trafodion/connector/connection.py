from .abstracts import TrafConnectionAbstract
import os
from .network import TrafTCPSocket, TrafUnixSocket
class TrafConnection(TrafConnectionAbstract):
    """
        Connection to a mxosrvr
    """

    def __init__(self, *args, **kwargs):
        self._user = ''
        self._password = ''
        self._catalog = ''
        self._database = ''
        self._master_host = '127.0.0.1'
        self._master_port = 23400
        self._force_ipv6 = False
        self.unix_socket = None
        super(TrafConnection, self).__init__(*args, **kwargs)

    def _get_connection(self, host = '127.0.0.1', port = 0):

        """
        :param host: 
        :param port: 
        :return: 
        """
        conn = None
        if self.unix_socket and os.name != 'nt':
            conn = TrafTCPSocket(self.unix_socket)
        else:
            conn = TrafTCPSocket(host=host,
                                 port=port,
                                 force_ipv6=self._force_ipv6)

        conn.set_connection_timeout(self._connection_timeout)
        return conn

    def _open_connection(self):
        """
        :return: 
        """

        #get conncetion from dcs master
        mxosrvr_info = self._get_Objref()

        #TODO self._get_connection() get connection from mxosrvr

        #TODO

    def _get_Objref(self):
        self._get_context()
        self._get_user_desc()

        master_conn = self._get_connection(self._master_host,self._master_port)
        if not master_conn:
            #error handle
            pass


    def _marshal(self,
                 inContext,
                 userDesc,
                 srvrType,
                 retryCount,
                 optionFlags1,
                 optionFlags2,
                 vproc,
                 ):

        wbuffer = None

        return wbuffer

    def _get_context(self):
        pass
    def _get_user_desc(self):
        userDesc = new USER_DESC_def();
        return userDesc;
