from .abstracts import TrafConnectionAbstract
import os
import sys
from .network import TrafTCPSocket, TrafUnixSocket,socket
from .struct_def import USER_DESC_def, CONNECTION_CONTEXT_def, VERSION_def, VERSION_LIST_def, Header
from .TRANSPOT import TRANSPORT,convert
import time
import getpass


class TrafConnection(TrafConnectionAbstract):
    """
        Connection to a mxosrvr
    """

    #from odbc_common.h and sql.h
    SQL_TXN_READ_UNCOMMITTED = 1
    SQL_TXN_READ_COMMITTED = 2
    SQL_TXN_REPEATABLE_READ = 4
    SQL_TXN_SERIALIZABLE = 8
    SQL_ATTR_CURRENT_CATALOG = 109
    SQL_ATTR_ACCESS_MODE = 101
    SQL_ATTR_AUTOCOMMIT = 102
    SQL_TXN_ISOLATION = 108

    # spj proxy syntax support 
    SPJ_ENABLE_PROXY = 1040
    PASSWORD_SECURITY = 0x4000000 # (2 ^ 26)
    ROWWISE_ROWSET = 0x8000000 # (2 ^ 27)

    CHARSET = 0x10000000 # (2 ^ 28)

    STREAMING_DELAYEDERROR_MODE = 0x20000000 # 2 ^ 29
    # Zbig
    JDBC_ATTR_CONN_IDLE_TIMEOUT = 3000
    RESET_IDLE_TIMER = 1070

    def __init__(self, *args, **kwargs):
        self._master_host = '127.0.0.1'
        self._master_port = 23400
        self._force_ipv6 = False
        self.unix_socket = None
        self._sessionToken = None
        self._isReadOnly = False
        self._autoCommit = True
        self._ignoreCancel = False
        super(TrafConnection, self).__init__(**kwargs)

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
        inContext = self._get_context()
        userDesc = self._get_user_desc()
        self._master_host = self.property['host']
        self._master_port = self.property['port']
        retryCount = 3
        srvrType = 2 #AS
        done = False
        try_num = 0

        #seconds
        currentTime = time.time()
        endTime =  currentTime + inContext.loginTimeoutSec * 1000 if (inContext.loginTimeoutSec > 0) else sys.maxsize

        while (done == False and try_num < retryCount and endTime > currentTime):
            rc = self._connect_master(inContext,userDesc, srvrType, retryCount)

            #in the while end
            currentTime = time.time()

    def _connect_master(self, inContext, userDesc, srvrType, retryCount):
        wbuffer = self._marshal(inContext,
                                userDesc,
                                srvrType,
                                retryCount,
                                0x10000000
                                )
        master_conn = self._get_connection(self._master_host, self._master_port)
        if not master_conn:
            #error handle
            pass
        pass


    def _marshal(self,
                 inContext,
                 userDesc,
                 srvrType,
                 retryCount,
                 optionFlags1,
                 optionFlags2 = 0,
                 vproc = "Traf_pybc_${buildId}",
                 ):
        wlength = Header.sizeOf()
        buf = b''

        vprocBytes = vproc.encode("utf-8")
        clientUserBytes = (getpass.getuser()).encode("utf-8")

        wlength += inContext.sizeOf()
        wlength += userDesc.sizeOf()

        wlength += TRANSPORT.size_int # srvrType
        wlength += TRANSPORT.size_short # retryCount
        wlength += TRANSPORT.size_int # optionFlags1
        wlength += TRANSPORT.size_int # optionFlags2
        wlength += len(vprocBytes)

        buf = bytearray(b'')

        buf.extend(bytearray(wlength))

        # use memoryview to avoid mem copy
        buf_view = memoryview(buf)
        # Read the data
        buf_view = inContext.insertIntoByteArray(buf_view)
        buf_view = userDesc.insertIntoByteArray(buf_view)

        buf_view = convert.put_int(srvrType, buf_view)
        buf_view = convert.put_short(retryCount, buf_view)
        buf_view = convert.put_int(optionFlags1, buf_view)
        buf_view = convert.put_int(optionFlags2, buf_view)
        buf_view = convert.put_string(vprocBytes, buf_view)

        # TODO: restructure all the flags and this new param
        buf_view = convert.put_string(clientUserBytes, buf_view)

        return buf

    def _get_context(self):
        inContext = CONNECTION_CONTEXT_def()
        inContext.catalog = self.property['catalog']
        inContext.schema =  self.property['schema']
        inContext.datasource = self.property['datasource']
        inContext.userRole = self.property['userRole']
        inContext.cpuToUse = self.property['cpuToUse']
        inContext.cpuToUseEnd = -1 # for future use by DBTransporter

        inContext.accessMode = 1 if self._isReadOnly else 0
        inContext.autoCommit = 1 if self._autoCommit else 0

        inContext.queryTimeoutSec = self.property['query_timeout']
        inContext.idleTimeoutSec = self.property['idleTimeout']
        inContext.loginTimeoutSec = self.property['login_timeout']
        inContext.txnIsolationLevel = self.SQL_TXN_READ_COMMITTED
        inContext.rowSetSize = self.property['fetchbuffersize']
        inContext.diagnosticFlag = 0
        inContext.processId = time.time() and 0xFFF

        try:
            inContext.computerName = socket.gethostname()
        except:
            inContext.computerName = "Unknown Client Host"

        inContext.windowText = "FASTPDBC" if not self.property['application_name'] else self.property['application_name']

        inContext.ctxDataLang = 15
        inContext.ctxErrorLang = 15

        inContext.ctxACP = 1252
        inContext.ctxCtrlInferNXHAR = -1
        inContext.clientVersionList.list = self.get_version(inContext.processId)
        return inContext

    def _get_user_desc(self):
        userDesc = USER_DESC_def()
        userDesc.userName = self._username
        userDesc.userDescType = \
            TRANSPORT.UNAUTHENTICATED_USER_TYPE if self._sessionToken == None else TRANSPORT.PASSWORD_ENCRYPTED_USER_TYPE
        userDesc.domainName = ""

        userDesc.userSid = None
        userDesc.password = None # we no longer want to send the password to the MXOAS

        return userDesc

    def get_version(self,process_id):
        majorVersion = 3
        minorVersion = 0
        buildId = 0

        version = [VERSION_def(), VERSION_def()]

        # Entry[0] is the Driver Version information
        version[0].componentId = 20
        version[0].majorVersion = majorVersion
        version[0].minorVersion = minorVersion
        version[0].buildId = buildId | self.ROWWISE_ROWSET | self.CHARSET | self.PASSWORD_SECURITY

        if (self.property['DelayedErrorMode']):
            version[0].buildId |= self.STREAMING_DELAYEDERROR_MODE

    # Entry[1] is the Application Version information
        version[1].componentId = 8
        version[1].majorVersion = 3
        version[1].minorVersion = 0
        version[1].buildId = 0

        return version
