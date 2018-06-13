
from .transport import Transport, convert
import socket
import time
from . import errors


class CONNECTION_CONTEXT_def:
    def __init__(self, conn):
        self.datasource = ""  # string
        self.catalog = ""  # string
        self.schema = ""  # string
        self.location = ""  # string
        self.userRole = ""  # string
        self.tenantName = ""  # string

        self.accessMode = 0  # short
        self.autoCommit = 0  # short
        self.queryTimeoutSec = 0  # short
        self.idleTimeoutSec = 0  # short
        self.loginTimeoutSec = 0  # short
        self.txnIsolationLevel = 0  # short
        self.rowSetSize = 0  # short

        self.diagnosticFlag = 0  # int
        self.processId = 0  # int

        self.computerName = ""  # string
        self.windowText = ""  # string

        self.ctxACP = 0  # int
        self.ctxDataLang = 0  # int
        self.ctxErrorLang = 0  # int
        self.ctxCtrlInferNXHAR = 0  # short

        self.cpuToUse = -1  # short
        self.cpuToUseEnd = -1  # short for future use by DBTransporter

        self.connectOptions = ""  # string

        self.clientVersionList = VERSION_LIST_def()

        self.datasource_bytes = bytearray(b'')
        self.catalog_bytes = bytearray(b'')
        self.schema_bytes = bytearray(b'')
        self.location_bytes = bytearray(b'')
        self.userRole_bytes = bytearray(b'')
        self.computerName_bytes = bytearray(b'')
        self.windowText_bytes = bytearray(b'')
        self.connectOptions_bytes = bytearray(b'')

        self._init_context(conn)

    def sizeOf(self):
        self.size = 0
        self.datasource_bytes = self.datasource.encode('utf-8')
        self.catalog_bytes = self.catalog.encode('utf-8')
        self.schema_bytes = self.schema.encode('utf-8')
        self.location_bytes = self.location.encode('utf-8')
        self.userRole_bytes = self.userRole.encode('utf-8')
        self.computerName_bytes = self.computerName.encode('utf-8')
        self.windowText_bytes = self.windowText.encode('utf-8')
        self.connectOptions_bytes = self.connectOptions.encode('utf-8')

        size = Transport.size_bytes(self.datasource_bytes)
        size += Transport.size_bytes(self.catalog_bytes)
        size += Transport.size_bytes(self.schema_bytes)
        size += Transport.size_bytes(self.location_bytes)
        size += Transport.size_bytes(self.userRole_bytes)

        size += Transport.size_short  # accessMode
        size += Transport.size_short  # autoCommit
        size += Transport.size_int  # queryTimeoutSec
        size += Transport.size_int  # idleTimeoutSec
        size += Transport.size_int  # loginTimeoutSec
        size += Transport.size_short  # txnIsolationLevel
        size += Transport.size_short  # rowSetSize

        size += Transport.size_int  # diagnosticFlag
        size += Transport.size_int  # processId

        size += Transport.size_bytes(self.computerName_bytes)
        size += Transport.size_bytes(self.windowText_bytes)

        size += Transport.size_int  # ctxACP
        size += Transport.size_int  # ctxDataLang
        size += Transport.size_int  # ctxErrorLang
        size += Transport.size_short  # ctxCtrlInferNCHAR

        size += Transport.size_short  # cpuToUse
        size += Transport.size_short  # cpuToUseEnd
        size += Transport.size_bytes(self.connectOptions_bytes)

        size += self.clientVersionList.sizeOf()

        return size

    def insertIntoByteArray(self, buf_view, little=False):
        buf_view = convert.put_string(self.datasource, buf_view, little)
        buf_view = convert.put_string(self.catalog, buf_view, little) # string
        buf_view = convert.put_string(self.schema, buf_view, little)  # string
        buf_view = convert.put_string(self.location, buf_view, little)# string
        buf_view = convert.put_string(self.userRole, buf_view, little) # string
        #buf_view = convert.put_string(self.tenantName, buf_view)# string

        buf_view = convert.put_short(self.accessMode, buf_view, little) # short
        buf_view = convert.put_short(self.autoCommit, buf_view, little) # short
        buf_view = convert.put_int(self.queryTimeoutSec, buf_view, little) # int
        buf_view = convert.put_int(self.idleTimeoutSec, buf_view, little) # int
        buf_view = convert.put_int(self.loginTimeoutSec, buf_view, little) # int
        buf_view = convert.put_short(self.txnIsolationLevel, buf_view, little) # short
        buf_view = convert.put_short(self.rowSetSize, buf_view, little) # short

        buf_view = convert.put_int(self.diagnosticFlag, buf_view, little) # int
        buf_view = convert.put_int(self.processId, buf_view, little) # int

        buf_view = convert.put_string(self.computerName, buf_view, little)# string
        buf_view = convert.put_string(self.windowText, buf_view, little)  # string

        buf_view = convert.put_int(self.ctxACP, buf_view, little) # int
        buf_view = convert.put_int(self.ctxDataLang, buf_view, little) # int
        buf_view = convert.put_int(self.ctxErrorLang, buf_view, little) # int
        buf_view = convert.put_short(self.ctxCtrlInferNXHAR, buf_view, little) # short

        buf_view = convert.put_short(self.cpuToUse, buf_view, little) # short
        buf_view = convert.put_short(self.cpuToUseEnd, buf_view, little)  # short for future use by DBTransporter

        buf_view = convert.put_string(self.connectOptions, buf_view, little) # string

        buf_view = self.clientVersionList.insertIntoByteArray(buf_view, little)
        return buf_view

    def _init_context(self, conn):
        self.catalog = conn.property.catalog
        self.schema =  conn.property.schema
        self.datasource = conn.property.datasource
        self.userRole = conn.property.userRole
        self.cpuToUse = conn.property.cpuToUse
        self.cpuToUseEnd = -1 # for future use by DBTransporter

        self.accessMode = 1 if conn._isReadOnly else 0
        self.autoCommit = 1 if conn._autoCommit else 0

        self.queryTimeoutSec = conn.property.query_timeout
        self.idleTimeoutSec = conn.property.idleTimeout
        self.loginTimeoutSec = conn.property.login_timeout
        self.txnIsolationLevel = conn.SQL_TXN_READ_COMMITTED
        self.rowSetSize = conn.property.fetchbuffersize
        self.diagnosticFlag = 0
        self.processId = time.time() and 0xFFF

        try:
            self.computerName = socket.gethostname()
        except:
            self.computerName = "Unknown Client Host"

        self.windowText = "FASTPDBC" if not conn.property.application_name else conn.property.application_name

        self.ctxDataLang = 15
        self.ctxErrorLang = 15

        self.ctxACP = 1252
        self.ctxCtrlInferNXHAR = -1
        self.clientVersionList.list = conn.get_version(self.processId)

class VERSION_def:

    def __init__(self):
        self.componentId = 0  # short
        self.majorVersion = 0  # short
        self.minorVersion = 0  # short
        self.buildId = 0  # int

    @classmethod
    def sizeOf(self):
        return Transport.size_int + Transport.size_short * 3

    def insertIntoByteArray(self, buf_view, little=False):
        buf_view = convert.put_short(self.componentId, buf_view, little)
        buf_view = convert.put_short(self.majorVersion, buf_view, little)
        buf_view = convert.put_short(self.minorVersion, buf_view, little)
        buf_view = convert.put_int(self.buildId, buf_view, little)
        return buf_view

    def extractFromByteArray(self, buf_view):
        self.componentId, buf_view = convert.get_short(buf_view, little=True)
        self.majorVersion, buf_view = convert.get_short(buf_view, little=True)
        self.minorVersion, buf_view = convert.get_short(buf_view, little=True)
        self.buildId, buf_view = convert.get_int(buf_view, little=True)
        return buf_view


class VERSION_LIST_def:
    list = []

    def insertIntoByteArray(self, buf_view, little=False):
        buf_view = convert.put_int(len(self.list), buf_view, little)
        for item in self.list:
            buf_view = item.insertIntoByteArray(buf_view, little)
        return buf_view

    def sizeOf(self):
        return VERSION_def.sizeOf() * self.list.__len__() + Transport.size_int

    def extractFromByteArray(self, buf_view):

        length, buf_view = convert.get_int(buf_view, little=True)

        for i in range(length):
            version_def = VERSION_def()
            buf_view = version_def.extractFromByteArray(buf_view)
            self.list.append(version_def)

        return buf_view


class Header:
    #
    # Fixed values taken from TransportBase.h
    #
    WRITE_REQUEST_FIRST = 1
    WRITE_REQUEST_NEXT = (WRITE_REQUEST_FIRST + 1)
    READ_RESPONSE_FIRST = (WRITE_REQUEST_NEXT + 1)
    READ_RESPONSE_NEXT = (READ_RESPONSE_FIRST + 1)
    CLEANUP = (READ_RESPONSE_NEXT + 1)
    SRVR_Transport_ERROR = (CLEANUP + 1)
    CLOSE_TCPIP_SESSION = (SRVR_Transport_ERROR + 1)

    SIGNATURE = 12345  # 0x3039

    #      OLD_VERSION = 100 # pre 2.5 server
    CLIENT_HEADER_VERSION_BE = 101
    #      CLIENT_HEADER_VERSION_LE = 102 # not used in JDBC
    SERVER_HEADER_VERSION_BE = 201
    SERVER_HEADER_VERSION_LE = 202

    TCPIP = 'T'

    NSK = 'N'
    PC = 'P'

    YES = 'Y'
    NO = 'N'

    COMP_0 = 0x0
    COMP_12 = 0x12
    COMP_14 = 0x14

    """
      operation_id_  # short
      # + 2 filler
      dialogueId_    # int
      total_length_  # int
      cmp_length_    # int
      compress_ind_  # char
      compress_type_ # char
      # + 2 filler  
      hdr_type_      # int
      signature_     # int
      version_       # int
      platform_      # char
      transport_     # char
      swap_          # char
      # + 1 filler
      error_         # short
      error_detail_  # short
    """

    def __init__(self, operation_id=None,
                 dialogue_id=None,
                 total_length=None,
                 cmp_length=None,
                 compress_ind=None,
                 compress_type=None,
                 hdr_type=None,
                 signature=None,
                 version=None,
                 platform=None,
                 transport=None,
                 swap=None):
        self.operation_id_ = operation_id
        self.dialogueId_ = dialogue_id
        self.total_length_ = total_length
        self.cmp_length_ = cmp_length
        self.compress_ind_ = compress_ind
        self.compress_type_ = compress_type
        self.hdr_type_ = hdr_type
        self.signature_ = signature
        self.version_ = version
        self.platform_ = platform
        self.transport_ = transport
        self.swap_ = swap
        self.error_ = 0
        self.error_detail_ = 0

    @property
    def total_length(self):
        return self.total_length_

    def reuse_header(self, operation_id, dialogue_id):
        self.operation_id_ = operation_id
        self.dialogueId_ = dialogue_id

    @classmethod
    def sizeOf(self):
        return 40

    def insertIntoByteArray(self, buf_view):
        buf_view = convert.put_short(self.operation_id_, buf_view)  # short                  0,1
        buf_view = convert.put_short(0, buf_view)  # + 2 filler                              2,3
        buf_view = convert.put_int(self.dialogueId_, buf_view)  # int                        4-7
        buf_view = convert.put_int(self.total_length_, buf_view)  # int                      8-11
        buf_view = convert.put_int(self.cmp_length_, buf_view)  # int                        12-15
        buf_view = convert.put_char(self.compress_ind_.encode("utf-8"), buf_view)  # char    16
        buf_view = convert.put_char(self.compress_type_.encode("utf-8"), buf_view)  # char   17
        buf_view = convert.put_short(0, buf_view)  # + 2 filler                              18,19
        buf_view = convert.put_int(self.hdr_type_, buf_view)  # int                          20-23
        buf_view = convert.put_int(self.signature_, buf_view)  # int                         24-27
        buf_view = convert.put_int(self.version_, buf_view)  # int
        buf_view = convert.put_char(self.platform_.encode("utf-8"), buf_view)  # char
        buf_view = convert.put_char(self.transport_.encode("utf-8"), buf_view)  # char
        buf_view = convert.put_char(self.swap_.encode("utf-8"), buf_view)  # char
        buf_view = convert.put_char('0'.encode("utf-8"), buf_view)  # + 1 filler
        buf_view = convert.put_short(self.error_, buf_view)  # short
        buf_view = convert.put_short(self.error_detail_, buf_view)  # short
        return buf_view


    def extractFromByteArray(self, buf, little=False):
        buf_view = memoryview(buf)
        self.operation_id_, buf_view = convert.get_short(buf_view, little)
        __, buf_view = convert.get_short(buf_view, little)  # +2 fillter

        self.dialogueId_, buf_view = convert.get_int(buf_view, little)
        self.total_length_, buf_view = convert.get_int(buf_view, little)
        self.cmp_length_, buf_view = convert.get_int(buf_view, little)
        self.compress_ind_, buf_view = convert.get_char(buf_view)
        self.compress_type_, buf_view = convert.get_char(buf_view)

        __, buf_view = convert.get_short(buf_view, little)  # +2 fillter

        self.hdr_type_, buf_view = convert.get_int(buf_view, little)
        self.signature_, buf_view = convert.get_int(buf_view, little)
        self.version_, buf_view = convert.get_int(buf_view, little)
        self.platform_, buf_view = convert.get_char(buf_view)
        self.transport_, buf_view = convert.get_char(buf_view)
        self.swap_, buf_view = convert.get_char(buf_view)
        __, buf_view = convert.get_char(buf_view)  # +1 fillter
        self.error_, buf_view = convert.get_short(buf_view, little)
        self.error_detail_, buf_view = convert.get_short(buf_view, little)

class USER_DESC_def:

    def __init__(self):
        self.userDescType = 0
        self.userSid = ''
        self.domainName = ''
        self.userName = ''
        self.password = ''
        self.domainName_bytes = self.domainName.encode("utf-8")
        self.userName_bytes = self.userName.encode("utf-8")

    def sizeOf(self):
        size = 0

        size += Transport.size_int  # descType
        size += Transport.size_bytes(self.domainName_bytes)
        size += Transport.size_bytes(self.userName_bytes)
        size += Transport.size_bytes(self.userName)
        size += Transport.size_bytes(self.password)

        return size

    def insertIntoByteArray(self, buf_view, little=False):
        buf_view = convert.put_int(self.userDescType, buf_view, little)

        buf_view = convert.put_string(self.userSid, buf_view, little)
        buf_view = convert.put_string(self.domainName, buf_view, little)
        buf_view = convert.put_string(self.userName, buf_view, little)
        buf_view = convert.put_string(self.password,buf_view, little)

        return buf_view

class TrafProperty:
    def __init__(self):
        self._master_host = '127.0.0.1'
        self._master_port = 23400
        self._catalog = "TRAFODION"
        self._schema = "SEABASE"
        self._datasource = ""
        self._userRole = ""
        self._cpuToUse = 0
        self._query_timeout = 3600
        self._idleTimeout = 3600
        self._login_timeout = 60
        self._fetchbuffersize = 100
        self._application_name = ""
        self._DelayedErrorMode = False
        self._retry_count = 5
        self._srvr_type = 2  # AS
        self._fetch_ahead = ''

    @property
    def retry_count(self):
        return self._retry_count

    @retry_count.setter
    def retry_count(self, num):
        self._retry_count = num

    @property
    def fetch_ahead(self):
        return self._fetch_ahead
    @property
    def srvr_type(self):
        return self._srvr_type

    @srvr_type.setter
    def srvr_type(self, num):
        self._srvr_type = num

    @property
    def master_host(self):
        return self._master_host

    @master_host.setter
    def master_host(self, str):
        self._master_host = str

    @property
    def master_port(self):
        return self._master_port
    @master_port.setter
    def master_port(self, num):
        self._master_port = num

    @property
    def catalog(self):
        return self._catalog

    @catalog.setter
    def catalog(self, str):
        self._catalog = str

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, str):
        self._schema = str

    @property
    def datasource(self):
        return self._datasource
    @datasource.setter
    def datasource(self, str):
        self._datasource = str

    @property
    def userRole(self):
        return self._userRole
    @userRole.setter
    def userRole(self, str):
        self._userRole = str

    @property
    def cpuToUse(self):
        return self._cpuToUse
    @cpuToUse.setter
    def cpuToUse(self, num):
        self._cpuToUse = num

    @property
    def query_timeout(self):
        return self._query_timeout
    @query_timeout.setter
    def query_timeout(self, num):
        self._query_timeout = num

    @property
    def idleTimeout(self):
        return self._idleTimeout

    @idleTimeout.setter
    def idleTimeout(self, num):
        self._idleTimeout = num

    @property
    def fetchbuffersize(self):
        return self._fetchbuffersize

    @fetchbuffersize.setter
    def fetchbuffersize(self, num):
        self._fetchbuffersize = num

    @property
    def login_timeout(self):
        return self._login_timeout

    @login_timeout.setter
    def login_timeout(self, num):
        self._login_timeout = num

    @property
    def application_name(self):
        return self._application_name

    @application_name.setter
    def application_name(self, str):
        self._application_name = str

    @property
    def DelayedErrorMode(self):
        return self._DelayedErrorMode

    @DelayedErrorMode.setter
    def DelayedErrorMode(self, bool):
        self._DelayedErrorMode = bool

class GetPbjRefHdlExc:

    exception_nr = 0
    exception_detail = 0
    error_text = ''
    def extractFromByteArray(self,buf_view):
        self.exception_nr, buf_view = convert.get_int(buf_view, little=True)
        self.exception_detail, buf_view = convert.get_int(buf_view, little=True)
        self.error_text, buf_view = convert.get_string(buf_view)

        #TODO need to handle exception_nr

        return buf_view


class ConnectReply:
    def __init__(self):
        pass
    def init_reply(self, buf_view, conn):
        self.buf_exception = GetPbjRefHdlExc()
        buf_view = self.buf_exception.extractFromByteArray(buf_view)

        # TODO handle error
        self.dialogue_id, buf_view = convert.get_int(buf_view, little=True)
        self.data_source, buf_view = convert.get_string(buf_view, little=True)
        self.user_sid, buf_view = convert.get_string(buf_view, little=True, byteoffset=True)
        self.version_list = VERSION_LIST_def()
        buf_view = self.version_list.extractFromByteArray(buf_view)
        __, buf_view = convert.get_int(buf_view, little=True)  # old iso mapping
        self.isoMapping = 15 #utf-8
        self.server_host_name, buf_view = convert.get_string(buf_view, little=True)
        self.server_node_id, buf_view = convert.get_int(buf_view, little=True)
        self.server_process_id, buf_view = convert.get_int(buf_view, little=True)
        self.server_process_name, buf_view = convert.get_string(buf_view, little=True)
        self.server_ip_address, buf_view = convert.get_string(buf_view, little=True)
        self.server_port, buf_view = convert.get_int(buf_view, little=True)

        if (self.version_list.list[0].buildId and conn.PASSWORD_SECURITY > 0):
            self.security_enabled = True
            self.timestamp, buf_view = convert.get_timestamp(buf_view)
            self.cluster_name, buf_view = convert.get_string(buf_view, little=True)
        else:
            self.security_enabled = False

class OUT_CONNECTION_CONTEXT_def:
    OUTCONTEXT_OPT1_ENFORCE_ISO88591 = 1  #   (2^0)
    OUTCONTEXT_OPT1_IGNORE_SQLCANCEL = 1073741824  #   (2^30)
    OUTCONTEXT_OPT1_EXTRA_OPTIONS = 2147483648  #   (2^31)
    OUTCONTEXT_OPT1_DOWNLOAD_CERTIFICATE = 536870912  #  (2^29)

    def __init__(self):
        self.version_list = VERSION_LIST_def()
        self.node_id = 0 # short
        self.process_id = 0 # int

        self.computer_name = ''
        self.catalog = ''
        self.schema = ''
        self.option_flags1 = 0
        self.option_flags2 = 0
        self.role_name = ''
        self.enforce_iso = False
        self.ignore_cancel = True

        self.certificate = b''

    def extractFromByteArray(self, buf_view):

        self.version_list = VERSION_LIST_def()
        buf_view = self.version_list.extractFromByteArray(buf_view)

        buf_view, self.node_id = convert.get_short(buf_view, little=True)
        buf_view, self.process_id = convert.get_int(buf_view, little=True)
        buf_view, self.computer_name = convert.get_string(buf_view, little=True)
        buf_view, self.catalog = convert.get_string(buf_view, little=True)
        buf_view, self.schema = convert.get_string(buf_view, little=True)
        buf_view, self.option_flags1 = convert.get_int(buf_view, little=True)
        buf_view, self.option_flags2 = convert.get_int(buf_view, little=True)
        self.enforce_iso = (self.option_flags1 and self.OUTCONTEXT_OPT1_ENFORCE_ISO88591) > 0
        self.ignore_cancel = (self.option_flags1 and self.OUTCONTEXT_OPT1_IGNORE_SQLCANCEL) > 0

        if self.option_flags1 & self.OUTCONTEXT_OPT1_DOWNLOAD_CERTIFICATE > 0:
            buf_view, self.certificate = convert.get_string(buf_view, little=True)
        elif self.option_flags1 & self.OUTCONTEXT_OPT1_EXTRA_OPTIONS > 0:
            try:
                buf_view, buf = convert.get_string(buf_view, little=True)
                self.decodeExtraOptions(buf)
            except:
                pass

        return buf_view

    def decodeExtraOptions(self, options):
        opts = options.split(";")
        for x in opts:
            token, value = x.split("=")
            if token == "RN":
                self.role_name = value


class InitializeDialogueReply:
    odbc_SQLSvc_InitializeDialogue_ParamError_exn_ = 1
    odbc_SQLSvc_InitializeDialogue_InvalidConnection_exn_ = 2
    odbc_SQLSvc_InitializeDialogue_SQLError_exn_ = 3
    odbc_SQLSvc_InitializeDialogue_SQLInvalidHandle_exn_ = 4
    odbc_SQLSvc_InitializeDialogue_SQLNeedData_exn_ = 5
    odbc_SQLSvc_InitializeDialogue_InvalidUser_exn_ = 6

    SQL_PASSWORD_EXPIRING = 8857
    SQL_PASSWORD_GRACEPERIOD = 8837

    def __init__(self):
        self.exception_nr = 0
        self.exception_detail = 0
        self.param_error = ''
        self.SQLError = ERROR_DESC_LIST_def()
        self.InvalidUser = ERROR_DESC_LIST_def()
        self.client_error_text = ''

        self.out_context = OUT_CONNECTION_CONTEXT_def()

    def init_reply(self, buf_view, conn):
        buf_view, self.exception_nr = convert.get_int(buf_view, little=True)
        buf_view, self.exception_detail = convert.get_int(buf_view, little=True)

        if self.exception_nr == Transport.CEE_SUCCESS:
            buf_view = self.out_context.extractFromByteArray(buf_view)

        elif self.exception_nr == self.odbc_SQLSvc_InitializeDialogue_SQLError_exn_:
            buf_view = self.SQLError.extractFromByteArray(buf_view)
            if (self.exception_detail == self.SQL_PASSWORD_EXPIRING  or self.exception_detail == self.SQL_PASSWORD_GRACEPERIOD):
                self.out_context.extractFromByteArray(buf_view)

        elif self.exception_nr == self.odbc_SQLSvc_InitializeDialogue_InvalidUser_exn_:
            buf_view = self.SQLError.extractFromByteArray(buf_view)
            buf_view = self.out_context.extractFromByteArray(buf_view)

        elif self.exception_nr == self.odbc_SQLSvc_InitializeDialogue_ParamError_exn_:
            buf_view, self.param_error = convert.get_string(buf_view, little=True)
            raise errors.NotSupportedError

        elif self.exception_nr == self.odbc_SQLSvc_InitializeDialogue_InvalidConnection_exn_:
            raise errors.NotSupportedError

        else:
            self.client_error_text = "unknow error"