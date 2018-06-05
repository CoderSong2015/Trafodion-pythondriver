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
        self._connection_timeout = 60
        super(TrafConnection, self).__init__(**kwargs)


        print("kwargs!?")
        if kwargs:
            print("kwargs!")
            self.connect(**kwargs)
        self._open_connection()

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
        conn.open_connection()
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
        self._master_host = self.property.master_host
        self._master_port = self.property.master_port
        retryCount = 5
        srvrType = 2 #AS
        done = False
        try_num = 0

        #seconds
        currentTime = time.time()
        endTime =  currentTime + inContext.loginTimeoutSec * 1000 if (inContext.loginTimeoutSec > 0) else sys.maxsize

        #while (done == False and try_num < retryCount and endTime > currentTime):
        rc = self._connect_master(inContext,userDesc, srvrType, retryCount)
        try_num = try_num + 1
            # in the while end
        currentTime = time.time()

    def _connect_master(self, inContext, userDesc, srvrType, retryCount):
        wbuffer = self._marshal(inContext,
                                userDesc,
                                srvrType,
                                retryCount,
                                0x10000000
                                )
        master_conn = self._get_connection(self._master_host, self._master_port)
        print(master_conn)
        ret = self._get_from_server(TRANSPORT.AS_API_GETOBJREF, wbuffer, master_conn)
        if not master_conn:
            #error handle
            pass
        pass

    def _get_from_server(self, operation_id, wbuffer, conn):

        # TODO need compress
        # ...

        totallength = len(wbuffer)
        wheader = Header(operation_id,
                         0, #m_dialogueId,
                         totallength - Header.sizeOf(), # minus the size of the Header
                         0,#cmpLength,
                         'N',#compress,
                         '0',#compType this should be modify,
                         Header.WRITE_REQUEST_FIRST,
                         Header.SIGNATURE,
                         Header.CLIENT_HEADER_VERSION_BE,
                         Header.PC,
                         Header.TCPIP,
                         Header.NO)
        data = self._tcp_io_write(wheader, wbuffer, conn)
        self._tcp_io_read(wheader, wbuffer, conn)
        return None

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

        clientUser = getpass.getuser()

        wlength += inContext.sizeOf()
        print(wlength)
        wlength += userDesc.sizeOf()
        print(wlength)
        wlength += TRANSPORT.size_int # srvrType
        wlength += TRANSPORT.size_short # retryCount
        wlength += TRANSPORT.size_int # optionFlags1
        wlength += TRANSPORT.size_int # optionFlags2
        wlength += TRANSPORT.size_bytes(vproc.encode("utf-8"))
        wlength += TRANSPORT.size_bytes(clientUser.encode("utf-8"))
        buf = bytearray(b'')

        buf.extend(bytearray(wlength))

        print(len(buf))
        # use memoryview to avoid mem copy
        # remain space for header
        buf_view = memoryview(buf)
        buf_view = buf_view[Header.sizeOf():]
        # construct bytebuffer
        buf_view = inContext.insertIntoByteArray(buf_view)
        buf_view = userDesc.insertIntoByteArray(buf_view)

        buf_view = convert.put_int(srvrType, buf_view)
        buf_view = convert.put_short(retryCount, buf_view)
        buf_view = convert.put_int(optionFlags1, buf_view)
        buf_view = convert.put_int(optionFlags2, buf_view)
        buf_view = convert.put_string(vproc, buf_view)

        # TODO: restructure all the flags and this new param
        buf_view = convert.put_string(clientUser, buf_view)

        return buf

    def _get_context(self):
        inContext = CONNECTION_CONTEXT_def()
        inContext.catalog = self.property.catalog
        inContext.schema =  self.property.schema
        inContext.datasource = self.property.datasource
        inContext.userRole = self.property.userRole
        inContext.cpuToUse = self.property.cpuToUse
        inContext.cpuToUseEnd = -1 # for future use by DBTransporter

        inContext.accessMode = 1 if self._isReadOnly else 0
        inContext.autoCommit = 1 if self._autoCommit else 0

        inContext.queryTimeoutSec = self.property.query_timeout
        inContext.idleTimeoutSec = self.property.idleTimeout
        inContext.loginTimeoutSec = self.property.login_timeout
        inContext.txnIsolationLevel = self.SQL_TXN_READ_COMMITTED
        inContext.rowSetSize = self.property.fetchbuffersize
        inContext.diagnosticFlag = 0
        inContext.processId = time.time() and 0xFFF

        try:
            inContext.computerName = socket.gethostname()
        except:
            inContext.computerName = "Unknown Client Host"

        inContext.windowText = "FASTPDBC" if not self.property.application_name else self.property.application_name

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

        userDesc.userSid = ''
        userDesc.password = '' # we no longer want to send the password to the MXOAS

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

        if (self.property.DelayedErrorMode):
            version[0].buildId |= self.STREAMING_DELAYEDERROR_MODE

    # Entry[1] is the Application Version information
        version[1].componentId = 8
        version[1].majorVersion = 3
        version[1].minorVersion = 0
        version[1].buildId = 0

        return version

    def _tcp_io_read(self, header, buffer, conn):
        num_read = 0
        retry = False
        data = conn.recv()
        print(data)

    def _tcp_io_write(self, header, buffer, conn):
        if header.hdr_type_ == Header.WRITE_REQUEST_FIRST:
            buf_view = memoryview(buffer)
            header.insertIntoByteArray(buf_view)
            conn.send(buffer)
        elif header.hdr_type_ == Header.WRITE_REQUEST_NEXT:
            conn.send(buffer)

    def _execute_query(self, query):
        pass
    def commit(self):
        pass
    def cursor(self, buffered=None, raw=None, prepared=None, cursor_class=None,
               dictionary=None, named_tuple=None):
        pass
    def disconnect(self):
        pass
    def is_connected(self):
        pass
    def ping(self, reconnect=False, attempts=1, delay=0):
        pass

    def rollback(self):
        pass