from .abstracts import TrafConnectionAbstract
import os
import sys
from .network import TrafTCPSocket, TrafUnixSocket,socket
from .struct_def import USER_DESC_def, CONNECTION_CONTEXT_def, VERSION_def, VERSION_LIST_def, Header, GetPbjRefHdlExc
from .TRANSPOT import TRANSPORT,convert
import time
import getpass


class TrafConnection(TrafConnectionAbstract):
    """
        Connection to a mxosrvr
    """

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
        self._dialogue_id = 0

        super(TrafConnection, self).__init__(**kwargs)

        print("kwargs!?")
        if kwargs:
            print("kwargs!")
            self.connect(**kwargs)

    def _connect_to_mxosrvr(self):
        """
        :return: 
        """

        # get information from dcs master
        mxosrvr_info = self._get_Objref()

        #TODO self._get_connection() get connection from mxosrvr

        #TODO

    def _get_Objref(self):
        self._in_context = self._get_context()
        self._user_desc = self._get_user_desc()
        self._master_host = self.property.master_host
        self._master_port = self.property.master_port
        retry_count = self.property.retry_count
        srvr_type = self.property.srvr_type

        wbuffer = self._marshal(self._in_context,
                                self._user_desc,
                                srvr_type,
                                retry_count,
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
                         self._dialogue_id,                              # dialogueId,
                         totallength - Header.sizeOf(),  # minus the size of the Header
                         0,                              # cmpLength,
                         'N',                            # compress,
                         '0',                            # compType this should be modify,
                         Header.WRITE_REQUEST_FIRST,
                         Header.SIGNATURE,
                         Header.CLIENT_HEADER_VERSION_BE,
                         Header.PC,
                         Header.TCPIP,
                         Header.NO)
        self._tcp_io_write(wheader, wbuffer, conn)
        data = self._tcp_io_read(conn)
        connect_reply = self._handle_master_data(data)
        # TODO handle connect_reply
        return None

    def _handle_master_data(self, data):
        try:
            buf_view = memoryview(data)
            buf_exception = GetPbjRefHdlExc()
            buf_view = buf_exception.extractFromByteArray(buf_view)

            #TODO handle error
            dialogue_id, buf_view = convert.get_int(buf_view, little=True)
            data_source, buf_view = convert.get_string(buf_view, little=True)
            user_sid, buf_view = convert.get_string(buf_view, little=True, byteoffset=True)
            version_list = VERSION_LIST_def()
            buf_view = version_list.extractFromByteArray(buf_view)
            null, buf_view = convert.get_int(buf_view, little=True) #old iso mapping
            isoMapping = 15
            server_host_name, buf_view = convert.get_string(buf_view, little=True)
            server_node_id, buf_view = convert.get_int(buf_view, little=True)
            server_process_id, buf_view = convert.get_int(buf_view, little=True)
            server_process_name, buf_view = convert.get_string(buf_view, little=True)
            server_ip_address, buf_view = convert.get_string(buf_view, little=True)
            server_port, buf_view = convert.get_int(buf_view, little=True)

            if (version_list.list[0].buildId and self.PASSWORD_SECURITY > 0):
                security_enabled = True
                timestamp, buf_view = convert.get_timestamp(buf_view)
                cluster_name, buf_view = convert.get_string(buf_view)
            else:
                security_enabled = False
        except:
            print("what?")

    def _marshal(self,
                 in_context,
                 user_desc,
                 srvr_type,
                 retry_count,
                 option_flags_1,
                 option_flags_2=0,
                 vproc="Traf_pybc_${buildId}",
                 ):
        wlength = Header.sizeOf()
        buf = b''

        clientUser = getpass.getuser()

        wlength += (in_context.sizeOf()
                    + user_desc.sizeOf()
                    + TRANSPORT.size_int    # srvr_type
                    + TRANSPORT.size_short  # retry_count
                    + TRANSPORT.size_int    # option_flags_1
                    + TRANSPORT.size_int    # option_flags_2
                    + TRANSPORT.size_bytes(vproc.encode("utf-8"))
                    + TRANSPORT.size_bytes(clientUser.encode("utf-8")))
        buf = bytearray(b'')

        buf.extend(bytearray(wlength))

        print(len(buf))
        # use memoryview to avoid mem copy
        # remain space for header
        buf_view = memoryview(buf)
        buf_view = buf_view[Header.sizeOf():]
        # construct bytebuffer
        buf_view = in_context.insertIntoByteArray(buf_view)
        buf_view = user_desc.insertIntoByteArray(buf_view)

        buf_view = convert.put_int(srvr_type, buf_view)
        buf_view = convert.put_short(retry_count, buf_view)
        buf_view = convert.put_int(option_flags_1, buf_view)
        buf_view = convert.put_int(option_flags_2, buf_view)
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
        user_desc = USER_DESC_def()
        user_desc.userName = self._username
        user_desc.userDescType = \
            TRANSPORT.UNAUTHENTICATED_USER_TYPE if self._sessionToken == '' else TRANSPORT.PASSWORD_ENCRYPTED_USER_TYPE
        user_desc.domainName = ""

        user_desc.userSid = ''
        user_desc.password = '' # we no longer want to send the password to the MXOAS

        return user_desc

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

    def _tcp_io_read(self, conn):
        data = conn.recv()
        return data

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