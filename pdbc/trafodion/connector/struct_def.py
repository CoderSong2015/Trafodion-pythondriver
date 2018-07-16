import socket
import time
from decimal import Decimal

from . import errors

from .transport import Transport, Convert



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

        self.connectOptions = "".encode()  # bytes here

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
        #self.connectOptions_bytes = self.connectOptions.encode('utf-8')

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
        size += Transport.size_bytes(self.connectOptions)

        size += self.clientVersionList.sizeOf()

        return size

    def insertIntoByteArray(self, buf_view, little=False):
        buf_view = Convert.put_string(self.datasource, buf_view, little)
        buf_view = Convert.put_string(self.catalog, buf_view, little) # string
        buf_view = Convert.put_string(self.schema, buf_view, little)  # string
        buf_view = Convert.put_string(self.location, buf_view, little)# string
        buf_view = Convert.put_string(self.userRole, buf_view, little) # string
        #buf_view = Convert.put_string(self.tenantName, buf_view)# string

        buf_view = Convert.put_short(self.accessMode, buf_view, little) # short
        buf_view = Convert.put_short(self.autoCommit, buf_view, little) # short
        buf_view = Convert.put_int(self.queryTimeoutSec, buf_view, little) # int
        buf_view = Convert.put_int(self.idleTimeoutSec, buf_view, little) # int
        buf_view = Convert.put_int(self.loginTimeoutSec, buf_view, little) # int
        buf_view = Convert.put_short(self.txnIsolationLevel, buf_view, little) # short
        buf_view = Convert.put_short(self.rowSetSize, buf_view, little) # short

        buf_view = Convert.put_int(self.diagnosticFlag, buf_view, little) # int
        buf_view = Convert.put_int(self.processId, buf_view, little) # int

        buf_view = Convert.put_string(self.computerName, buf_view, little)# string
        buf_view = Convert.put_string(self.windowText, buf_view, little)  # string

        buf_view = Convert.put_int(self.ctxACP, buf_view, little) # int
        buf_view = Convert.put_int(self.ctxDataLang, buf_view, little) # int
        buf_view = Convert.put_int(self.ctxErrorLang, buf_view, little) # int
        buf_view = Convert.put_short(self.ctxCtrlInferNXHAR, buf_view, little) # short

        buf_view = Convert.put_short(self.cpuToUse, buf_view, little) # short
        buf_view = Convert.put_short(self.cpuToUseEnd, buf_view, little)  # short for future use by DBTransporter

        buf_view = Convert.put_string(self.connectOptions.decode("utf-8"), buf_view, little)  # string

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
        buf_view = Convert.put_short(self.componentId, buf_view, little)
        buf_view = Convert.put_short(self.majorVersion, buf_view, little)
        buf_view = Convert.put_short(self.minorVersion, buf_view, little)
        buf_view = Convert.put_int(self.buildId, buf_view, little)
        return buf_view

    def extractFromByteArray(self, buf_view):
        self.componentId, buf_view = Convert.get_short(buf_view, little=True)
        self.majorVersion, buf_view = Convert.get_short(buf_view, little=True)
        self.minorVersion, buf_view = Convert.get_short(buf_view, little=True)
        self.buildId, buf_view = Convert.get_int(buf_view, little=True)
        return buf_view


class VERSION_LIST_def:
    list = []

    def insertIntoByteArray(self, buf_view, little=False):
        buf_view = Convert.put_int(len(self.list), buf_view, little)
        for item in self.list:
            buf_view = item.insertIntoByteArray(buf_view, little)
        return buf_view

    def sizeOf(self):
        return VERSION_def.sizeOf() * self.list.__len__() + Transport.size_int

    def extractFromByteArray(self, buf_view):

        length, buf_view = Convert.get_int(buf_view, little=True)

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
        buf_view = Convert.put_short(self.operation_id_, buf_view)  # short                  0,1
        buf_view = Convert.put_short(0, buf_view)  # + 2 filler                              2,3
        buf_view = Convert.put_int(self.dialogueId_, buf_view)  # int                        4-7
        buf_view = Convert.put_int(self.total_length_, buf_view)  # int                      8-11
        buf_view = Convert.put_int(self.cmp_length_, buf_view)  # int                        12-15
        buf_view = Convert.put_char(self.compress_ind_.encode("utf-8"), buf_view)  # char    16
        buf_view = Convert.put_char(self.compress_type_.encode("utf-8"), buf_view)  # char   17
        buf_view = Convert.put_short(0, buf_view)  # + 2 filler                              18,19
        buf_view = Convert.put_int(self.hdr_type_, buf_view)  # int                          20-23
        buf_view = Convert.put_int(self.signature_, buf_view)  # int                         24-27
        buf_view = Convert.put_int(self.version_, buf_view)  # int
        buf_view = Convert.put_char(self.platform_.encode("utf-8"), buf_view)  # char
        buf_view = Convert.put_char(self.transport_.encode("utf-8"), buf_view)  # char
        buf_view = Convert.put_char(self.swap_.encode("utf-8"), buf_view)  # char
        buf_view = Convert.put_char('0'.encode("utf-8"), buf_view)  # + 1 filler
        buf_view = Convert.put_short(self.error_, buf_view)  # short
        buf_view = Convert.put_short(self.error_detail_, buf_view)  # short
        return buf_view


    def extractFromByteArray(self, buf, little=False):
        buf_view = memoryview(buf)
        self.operation_id_, buf_view = Convert.get_short(buf_view, little)
        __, buf_view = Convert.get_short(buf_view, little)  # +2 fillter

        self.dialogueId_, buf_view = Convert.get_int(buf_view, little)
        self.total_length_, buf_view = Convert.get_int(buf_view, little)
        self.cmp_length_, buf_view = Convert.get_int(buf_view, little)
        self.compress_ind_, buf_view = Convert.get_char(buf_view)
        self.compress_type_, buf_view = Convert.get_char(buf_view)

        __, buf_view = Convert.get_short(buf_view, little)  # +2 fillter

        self.hdr_type_, buf_view = Convert.get_int(buf_view, little)
        self.signature_, buf_view = Convert.get_int(buf_view, little)
        self.version_, buf_view = Convert.get_int(buf_view, little)
        self.platform_, buf_view = Convert.get_char(buf_view)
        self.transport_, buf_view = Convert.get_char(buf_view)
        self.swap_, buf_view = Convert.get_char(buf_view)
        __, buf_view = Convert.get_char(buf_view)  # +1 fillter
        self.error_, buf_view = Convert.get_short(buf_view, little)
        self.error_detail_, buf_view = Convert.get_short(buf_view, little)

class USER_DESC_def:

    def __init__(self):
        self.userDescType = 0
        self.userSid = ''
        self.domainName = ''
        self.userName = ''
        self.password = ''.encode("utf-8")
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
        buf_view = Convert.put_int(self.userDescType, buf_view, little)

        buf_view = Convert.put_string(self.userSid, buf_view, little)
        buf_view = Convert.put_string(self.domainName, buf_view, little)
        buf_view = Convert.put_string(self.userName, buf_view, little)
        buf_view = Convert.put_bytes(self.password, buf_view, little)

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
        self.exception_nr, buf_view = Convert.get_int(buf_view, little=True)
        self.exception_detail, buf_view = Convert.get_int(buf_view, little=True)
        self.error_text, buf_view = Convert.get_string(buf_view)

        #TODO need to handle exception_nr

        return buf_view


class ConnectReply:
    def __init__(self):
        pass
    def init_reply(self, buf_view, conn):
        self.buf_exception = GetPbjRefHdlExc()
        buf_view = self.buf_exception.extractFromByteArray(buf_view)

        # TODO handle error
        self.dialogue_id, buf_view = Convert.get_int(buf_view, little=True)
        self.data_source, buf_view = Convert.get_string(buf_view, little=True)
        self.user_sid, buf_view = Convert.get_string(buf_view, little=True, byteoffset=True)
        self.version_list = VERSION_LIST_def()
        buf_view = self.version_list.extractFromByteArray(buf_view)
        __, buf_view = Convert.get_int(buf_view, little=True)  # old iso mapping
        self.isoMapping = 15 #utf-8
        self.server_host_name, buf_view = Convert.get_string(buf_view, little=True)
        self.server_node_id, buf_view = Convert.get_int(buf_view, little=True)
        self.server_process_id, buf_view = Convert.get_int(buf_view, little=True)
        self.server_process_name, buf_view = Convert.get_string(buf_view, little=True)
        self.server_ip_address, buf_view = Convert.get_string(buf_view, little=True)
        self.server_port, buf_view = Convert.get_int(buf_view, little=True)

        if (self.version_list.list[0].buildId and conn.PASSWORD_SECURITY > 0):
            self.security_enabled = True
            self.timestamp, buf_view = Convert.get_timestamp(buf_view)
            self.cluster_name, buf_view = Convert.get_string(buf_view, little=True)
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

        self.node_id, buf_view = Convert.get_short(buf_view, little=True)
        self.process_id, buf_view = Convert.get_int(buf_view, little=True)
        self.computer_name, buf_view = Convert.get_string(buf_view, little=True)
        self.catalog, buf_view = Convert.get_string(buf_view, little=True)
        self.schema, buf_view = Convert.get_string(buf_view, little=True)
        self.option_flags1, buf_view = Convert.get_int(buf_view, little=True)
        self.option_flags2, buf_view = Convert.get_int(buf_view, little=True)
        self.enforce_iso = (self.option_flags1 and self.OUTCONTEXT_OPT1_ENFORCE_ISO88591) > 0
        self.ignore_cancel = (self.option_flags1 and self.OUTCONTEXT_OPT1_IGNORE_SQLCANCEL) > 0

        if self.option_flags1 & self.OUTCONTEXT_OPT1_DOWNLOAD_CERTIFICATE > 0:
            self.certificate, buf_view = Convert.get_string(buf_view, little=True)
        elif self.option_flags1 & self.OUTCONTEXT_OPT1_EXTRA_OPTIONS > 0:
            try:
                buf, buf_view = Convert.get_string(buf_view, little=True)
                self.decodeExtraOptions(buf)
            except:
                pass

        return buf_view

    def decodeExtraOptions(self, options):
        opts = options.split("")
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
        self.exception_nr, buf_view = Convert.get_int(buf_view, little=True)
        self.exception_detail, buf_view = Convert.get_int(buf_view, little=True)

        if self.exception_nr == Transport.CEE_SUCCESS:
            buf_view = self.out_context.extractFromByteArray(buf_view)

        elif self.exception_nr == self.odbc_SQLSvc_InitializeDialogue_SQLError_exn_:
            buf_view = self.SQLError.extractFromByteArray(buf_view)
            if (self.exception_detail == self.SQL_PASSWORD_EXPIRING or self.exception_detail == self.SQL_PASSWORD_GRACEPERIOD):
                self.out_context.extractFromByteArray(buf_view)

            error_info = ''
            for error_desc in self.SQLError.list:
                error_info += error_desc.errorText
            raise errors.DatabaseError(error_info)

        elif self.exception_nr == self.odbc_SQLSvc_InitializeDialogue_InvalidUser_exn_:
            buf_view = self.SQLError.extractFromByteArray(buf_view)
            buf_view = self.out_context.extractFromByteArray(buf_view)

        elif self.exception_nr == self.odbc_SQLSvc_InitializeDialogue_ParamError_exn_:
            self.param_error, buf_view = Convert.get_string(buf_view, little=True)
            raise errors.ProgrammingError(self.param_error)

        elif self.exception_nr == self.odbc_SQLSvc_InitializeDialogue_InvalidConnection_exn_:
            raise errors.InternalError("invalid connection")

        else:
            self.client_error_text = "unknow error"


class ERROR_DESC_LIST_def:

    def __init__(self):
        self.length = 0
        self.list = []

    def extractFromByteArray(self, buf_view):

        length, buf_view = Convert.get_int(buf_view, little=True)

        for i in range(length):
            error_desc = ERROR_DESC_Def()
            buf_view = error_desc.extractFromByteArray(buf_view)
            self.list.append(error_desc)

        return buf_view


class ERROR_DESC_Def:

    def __int__(self):
        self.rowId = 0
        self.errorDiagnosticId = 0
        self.sqlcode  = 0
        self.sqlstate = ''
        self.errorText = ''
        self.operationAbortId = 0
        self.errorCodeType = 0
        self.Param1 = ''
        self.Param2 = ''
        self.Param3 = ''
        self.Param4 = ''
        self.Param5 = ''
        self.Param6 = ''
        self.Param7 = ''

    def extractFromByteArray(self, buf_view):
        self.rowId, buf_view= Convert.get_int(buf_view, little=True)
        self.errorDiagnosticId, buf_view = Convert.get_int(buf_view, little=True)
        self.sqlcode, buf_view = Convert.get_int(buf_view, little=True)

        # Note, SQLSTATE is logically 5 bytes, but ODBC uses 6 bytes for some reason.
        self.sqlstate, buf_view = Convert.get_bytes(buf_view, length=6)
        self.errorText, buf_view = Convert.get_string(buf_view, little=True)
        self.operationAbortId, buf_view = Convert.get_int(buf_view, little=True)
        self.errorCodeType, buf_view = Convert.get_int(buf_view, little=True)
        self.Param1, buf_view = Convert.get_string(buf_view, little=True)
        self.Param2, buf_view = Convert.get_string(buf_view, little=True)
        self.Param3, buf_view = Convert.get_string(buf_view, little=True)
        self.Param4, buf_view = Convert.get_string(buf_view, little=True)
        self.Param5, buf_view = Convert.get_string(buf_view, little=True)
        self.Param6, buf_view = Convert.get_string(buf_view, little=True)
        self.Param7, buf_view = Convert.get_string(buf_view, little=True)
        return buf_view


class SQL_DataValue_def:

    def __init__(self):
        self.buffer = None
        self.user_buffer = None

    def sizeof(self):
        return Transport.size_int if self.buffer is None else Transport.size_int + len(self.buffer) + 1

    def insertIntoByteArray(self, buf_view, little=False):
        try:
            if self.buffer is not None:
                if isinstance(self.buffer, str):
                    buf_view = Convert.put_string(self.buffer, buf_view, little)  # string
                else:
                    buf_view = Convert.put_bytes(self.buffer, buf_view, little, is_data=True)
            else:
                buf_view = Convert.put_int(0, buf_view, little)
        except:
            raise errors.InternalError("Convert buffer error")

        return buf_view

    def set_buffer(self, buffer:str):
        self.buffer = buffer

    def set_user_buffer(self, buffer:str):
        self.user_buffer = buffer

    def extractFromByteArray(self, buf_view: memoryview)->memoryview:
        self.buffer, buf_view = Convert.get_string(buf_view, little=True)
        return buf_view

    @classmethod
    def fill_in_sql_values(cls, describer, param_rowcount, param_values):
        data_value = SQL_DataValue_def()

        #TODO handle param_values
        if (param_rowcount == 0 and param_values is not None and  len(param_values) > 0):
            param_rowcount = 1 # fake a single row if we are doing inputParams
        # for an SPJ

        param_count = 0
        if param_values is not None:
            param_count = len(param_values)
        # TODO: we should really figure out WHY this could happen

        row_len = 0
        if param_count <= 0:
            data_value.buffer = ''
            data_value.length = 0
        else:  # prepare first
            row_len = describer.input_desc_list[0].row_length
            if row_len < 0:
                # TODO: we should really figure out WHY this could happen
                data_value.buffer = bytes(0)
                data_value.length = 0
            else:
                buf_len = row_len * param_rowcount
                data_value.buffer = bytearray(buf_len)
                buf_view = memoryview(data_value.buffer)
                for row in range(param_rowcount):
                    for col in range(param_count):
                        _ = cls.Convert_object_to_sql(describer.input_desc_list, param_rowcount, col,
                                                              param_values[col],
                                                              row, buf_view)

        data_value.length = row_len * param_rowcount

        return data_value

    @classmethod
    def Convert_object_to_sql(cls, input_desc_list, param_rowcount, param_count, param_values, row_num,
                              buf_view: memoryview):

        desc = input_desc_list[param_count]

        precision = desc.odbcPrecision_
        scale = desc.scale_
        sqlDatetimeCode = desc.datetimeCode_
        FSDataType = desc.dataType_
        OdbcDataType = desc.odbcDataType_
        max_len = desc.maxLen_
        dataType = desc.dataType_
        dataCharSet = desc.sqlCharset_
        # setup the offsets
        noNullValue = desc.noNullValue_
        nullValue = desc.nullValue_
        dataLength = desc.maxLen_

        dataOffset = 2
        shortLength = False

        if dataType == FetchReply.SQLTYPECODE_VARCHAR_WITH_LENGTH:
            shortLength = precision < 2**15
            dataOffset = 2 if shortLength else 4
            dataLength += dataOffset

            if dataLength % 2 != 0:
                dataLength = dataLength + 1

        elif dataType == FetchReply.SQLTYPECODE_BLOB or dataType == FetchReply.SQLTYPECODE_CLOB:
            shortLength = False
            dataOffset = 4
            dataLength += dataOffset

            if dataLength % 2 != 0:
                dataLength = dataLength + 1

        if nullValue != -1:
            nullValue = (nullValue * param_rowcount) + (row_num * 2)

        noNullValue = (noNullValue * param_rowcount) + (row_num * dataLength)
        if param_values == None :
            if nullValue == -1:
                raise errors.DataError("null_parameter_for_not_null_column")
            # values[nullValue] = -1
            _ = Convert.put_short(-1, buf_view[nullValue:], True)
            return buf_view

        if dataType == FetchReply.SQLTYPECODE_CHAR:
            if param_values is None:
                # Note for future optimization. We can probably remove the next line,
                # because the array is already initialized to 0.
                _ = Convert.put_short(0, buf_view[noNullValue:], True)
                return buf_view
            elif isinstance(param_values, (bytes, str)):
                charSet = ""

                try:
                    if dataCharSet == Transport.charset_to_value["ISO8859_1"]:
                        charSet = "UTF-8"
                    elif dataCharSet == Transport.charset_to_value["UTF-16BE"]:   # default is little endian
                        charSet = "UTF-16LE"
                    else:
                        charSet = Transport.value_to_charset[dataCharSet]
                    if isinstance(param_values, bytes):
                        param_values = param_values.decode("utf-8").encode(charSet)
                    else:
                        param_values = param_values.encode(charSet)
                except:
                    raise errors.NotSupportedError("unsupported_encoding")
            else:
                raise errors.DataError("invalid_parameter_value, data should be either bytes or String for column: %d",
                                       param_count)

            # We now have a byte array containing the parameter
            data_len = len(param_values)
            if max_len >= data_len:
                _ = Convert.put_bytes(param_values, buf_view[noNullValue:], True, nolen=True)
                # Blank pad for rest of the buffer
                if max_len > data_len:
                    if dataCharSet == Transport.charset_to_value["UTF-16BE"]:
                        # pad with Unicode spaces (0x00 0x20)
                        i2 = data_len
                        while i2 < max_len:
                            _ = Convert.put_bytes(' '.encode(), buf_view[noNullValue + i2:], little=True,
                                             nolen=True)
                            _ = Convert.put_bytes(' '.encode(), buf_view[noNullValue + i2 + 1:], little=True,
                                             nolen=True)
                            i2 = i2 + 2

                    else:
                        b = bytearray()
                        for x in range(max_len - data_len):
                            b.append(ord(' '))
                        _ = Convert.put_bytes(b, buf_view[noNullValue + data_len:], little=True, nolen=True)
            else:
                raise errors.ProgrammingError(
                        "invalid_string_parameter CHAR input data is longer than the length for column: %d",param_count)

            return None
        if dataType == FetchReply.SQLTYPECODE_VARCHAR:
            if param_values is None:
                # Note for future optimization. We can probably remove the next line,
                # because the array is already initialized to 0.
                _ = Convert.put_short(0, buf_view[noNullValue:], True)
                return None
            elif isinstance(param_values, (bytes, str)):
                charSet = ""

                try:
                    if dataCharSet == Transport.charset_to_value["ISO8859_1"]:
                        charSet = "UTF-8"
                    elif dataCharSet == Transport.charset_to_value["UTF-16BE"]:   # default is little endian
                        charSet = "UTF-16LE"
                    else:
                        charSet = Transport.value_to_charset[dataCharSet]
                    if isinstance(param_values, bytes):
                        param_values = param_values.decode("utf-8").encode(charSet)
                    else:
                        param_values = param_values.encode(charSet)
                except:
                    raise errors.NotSupportedError("unsupported_encoding")
            else:
                raise errors.DataError(
                    "invalid_parameter_value, data should be either bytes or String for column: {0}".format(
                        param_count))

            data_len = len(param_values)
            if max_len >= data_len:
                _ = Convert.put_short(data_len, buf_view[noNullValue:], little=True)
                _ = Convert.put_bytes(param_values, buf_view[noNullValue + 2:], nolen=True)
            else:
                raise errors.DataError(
                    "invalid_string_parameter input data is longer than the length for column: {0}".format(
                        param_count))
            return None
        if dataType == FetchReply.SQLTYPECODE_VARCHAR_WITH_LENGTH or dataType == FetchReply.SQLTYPECODE_VARCHAR_LONG:
            if param_values is None:
                # Note for future optimization. We can probably remove the next line,
                # because the array is already initialized to 0.
                _ = Convert.put_short(0, buf_view[noNullValue:], True)
                return buf_view
            elif isinstance(param_values, (bytes, str)):
                charSet = ""

                try:
                    if dataCharSet == Transport.charset_to_value["ISO8859_1"]:
                        charSet = "UTF-8"
                    elif dataCharSet == Transport.charset_to_value["UTF-16BE"]:   # default is little endian
                        charSet = "UTF-16LE"
                    else:
                        charSet = Transport.value_to_charset[dataCharSet]
                    if isinstance(param_values, bytes):
                        param_values = param_values.decode("utf-8").encode(charSet)
                    else:
                        param_values = param_values.encode(charSet)
                except:
                    raise errors.NotSupportedError("unsupported_encoding")
            else:
                raise errors.DataError(
                    "invalid_parameter_value, data should be either bytes or String for column: {0}".format(
                        param_count))

            data_len = len(param_values)
            if max_len > (data_len + dataOffset):
                max_len = data_len + dataOffset
                if shortLength:
                    _ = Convert.put_short(data_len, buf_view[noNullValue:], little=True)
                else:
                    _ = Convert.put_int(data_len, buf_view[noNullValue:], little=True)
                _ = Convert.put_bytes(param_values, buf_view[noNullValue + dataOffset:], nolen=True)
            else:
                raise errors.DataError(
                    "invalid_string_parameter input data is longer than the length for column: {0}".format(
                        param_count))
            return None
        if dataType == FetchReply.SQLTYPECODE_INTEGER:
            if not isinstance(param_values,(int, float)):
                raise errors.DataError(
                    "invalid_parameter_value, data should be either int or float for column: {0}".format(
                        param_count))
            if scale > 0:
                param_values = round(param_values * (10 ** scale))

            if param_values > Transport.max_int or param_values < Transport.min_int:
                raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

            # check for numeric(x, y)
            if precision > 0:
                pre = 10 ** precision
                if param_values > pre or param_values < -pre:
                    raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

            _ = Convert.put_int(param_values, buf_view[noNullValue:], little=True)
            return None

        if dataType == FetchReply.SQLTYPECODE_INTEGER_UNSIGNED:
            if not isinstance(param_values,(int, float)):
                raise errors.DataError(
                    "invalid_parameter_value, data should be either int or float for column: {0}".format(
                        param_count))
            if scale > 0:
                param_values = round(param_values * (10 ** scale))

            if param_values > Transport.max_uint or param_values < Transport.min_uint:
                raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

            # check for numeric(x, y)
            if precision > 0:
                pre = 10 ** precision
                if param_values > pre or param_values < -pre:
                    raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

            _ = Convert.put_uint(param_values, buf_view[noNullValue:], little=True)

        if dataType == FetchReply.SQLTYPECODE_TINYINT:
            # TODO have not finished
            """
                        if not isinstance(param_values,(int, float)):
                            raise errors.ProgrammingError(
                                "invalid_parameter_value, data should be either int or float for column: {0}".format(
                                    param_count))
                        if scale > 0:
                            raise errors.DataError("invalid_parameter_value: Cannot have scale for param {0}".format(param_values))

                        if param_values > Transport.max_tinyint or param_values < Transport.min_tinyint:
                            raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

                        """
            raise errors.NotSupportedError("not support tinyint")

        if dataType == FetchReply.SQLTYPECODE_TINYINT_UNSIGNED:
            raise errors.NotSupportedError("not support utinyint")

        if dataType == FetchReply.SQLTYPECODE_SMALLINT:
            if not isinstance(param_values,(int, float)):
                raise errors.DataError(
                    "invalid_parameter_value, data should be either int or float for column: {0}".format(
                        param_count))
            if scale > 0:
                param_values = round(param_values * (10 ** scale))

            if param_values > Transport.max_short or param_values < Transport.min_short:
                raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

            # check for numeric(x, y)
            if precision > 0:
                pre = 10 ** precision
                if param_values > pre or param_values < -pre:
                    raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

            _ = Convert.put_short(param_values, buf_view[noNullValue:], little=True)
            return None

        if dataType == FetchReply.SQLTYPECODE_SMALLINT_UNSIGNED:
            if not isinstance(param_values,(int, float)):
                raise errors.DataError(
                    "invalid_parameter_value, data should be either int or float for column: {0}".format(
                        param_count))
            if scale > 0:
                param_values = round(param_values * (10 ** scale))

            if param_values > Transport.max_ushort or param_values < Transport.min_ushort:
                raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

            # check for numeric(x, y)
            if precision > 0:
                pre = 10 ** precision
                if param_values > pre or param_values < -pre:
                    raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

            _ = Convert.put_ushort(param_values, buf_view[noNullValue:], little=True)
            return None

        if dataType == FetchReply.SQLTYPECODE_LARGEINT:
            if not isinstance(param_values, (int, float)):
                raise errors.DataError(
                    "invalid_parameter_value, data should be either int or float for column: {0}".format(
                        param_count))
            if scale > 0:
                param_values = round(param_values * (10 ** scale))

            if param_values > Transport.max_long or param_values < Transport.min_long:
                raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

            # check for numeric(x, y)
            if precision > 0:
                pre = 10 ** precision
                if param_values > pre or param_values < -pre:
                    raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

            _ = Convert.put_longlong(param_values, buf_view[noNullValue:], little=True)
            return None
        if dataType == FetchReply.SQLTYPECODE_LARGEINT_UNSIGNED:
            if not isinstance(param_values, (int, float)):
                raise errors.DataError(
                    "invalid_parameter_value, data should be either int or float for column: {0}".format(
                        param_count))
            if scale > 0:
                param_values = round(param_values * (10 ** scale))

            if param_values > Transport.max_ulong or param_values < Transport.min_ulong:
                raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

            # check for numeric(x, y)
            if precision > 0:
                pre = 10 ** precision
                if param_values > pre or param_values < -pre:
                    raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

            _ = Convert.put_ulonglong(param_values, buf_view[noNullValue:], little=True)
            return None

        if dataType == FetchReply.SQLTYPECODE_DECIMAL or \
                        dataType == FetchReply.SQLTYPECODE_DECIMAL_UNSIGNED:

            if not isinstance(param_values, (int, str, Decimal)):
                raise errors.DataError(
                    "invalid_parameter_value, data should be either int or str or decimal for column: {0}".format(
                        param_count))

            try:
                param_values = Decimal(param_values)
            except:
                raise errors.DataError("decimal.ConversionSyntax")
            if scale > 0:
                param_values = param_values.fma(10 ** scale, 0)

            sign = 1 if param_values.is_signed() else 0
            param_values = param_values.__abs__()
            param_values = param_values.to_integral_exact().to_eng_string()

            num_zeros = max_len - len(param_values)
            if num_zeros < 0:
                raise errors.DataError("data_truncation_exceed {0}".format(param_count))

            padding = bytes('0'.encode() * num_zeros)
            _ = Convert.put_bytes(padding, buf_view[noNullValue:], nolen=True)

            if sign:
                _ = Convert.put_bytes(param_values.encode(), buf_view[noNullValue + num_zeros:],is_data=True)

                # byte -80 : 0xFFFFFFB0
                _ = Convert.put_bytes(b'\xB0', buf_view[noNullValue:], nolen=True, is_data=True)
            else:
                _ = Convert.put_bytes(param_values.encode(), buf_view[noNullValue + num_zeros:], is_data=True)

            return buf_view
        if dataType == FetchReply.SQLTYPECODE_REAL:

            if not isinstance(param_values, float):
                raise errors.DataError(
                    "invalid_parameter_value, data should be either float for column: {0}".format(
                        param_count))
            if param_values > Transport.max_float:
                raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

            _ = Convert.put_float(param_values, buf_view[noNullValue:], little=True)

        if dataType == FetchReply.SQLTYPECODE_FLOAT or dataType == FetchReply.SQLTYPECODE_DOUBLE:
            if not isinstance(param_values, float):
                raise errors.DataError(
                    "invalid_parameter_value, data should be either float for column: {0}".format(
                        param_count))
            if param_values > Transport.max_double:
                raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

            _ = Convert.put_float(param_values, buf_view[noNullValue:], little=True)

        if dataType == FetchReply.SQLTYPECODE_NUMERIC or \
                        dataType == FetchReply.SQLTYPECODE_NUMERIC_UNSIGNED:
            pass

        if dataType == FetchReply.SQLTYPECODE_BOOLEAN:
            raise errors.NotSupportedError
        if dataType == FetchReply.SQLTYPECODE_DECIMAL_LARGE or \
                        dataType == FetchReply.SQLTYPECODE_DECIMAL_LARGE_UNSIGNED or \
                        dataType == FetchReply.SQLTYPECODE_BIT or \
                        dataType == FetchReply.SQLTYPECODE_BITVAR or \
                        dataType == FetchReply.SQLTYPECODE_BPINT_UNSIGNED:
            raise errors.NotSupportedError


class SQLValue_def:

    def __init__(self):
        self.data_type = 0                      #  int
        self.data_ind = 0                       #  short
        self.data_value = SQL_DataValue_def()
        self.data_charset = 0                   #  int

    def sizeof(self):
        return Transport.size_int * 2 + Transport.size_short + self.data_value.sizeof()

    def insertIntoByteArray(self, buf_view, little=True):
        buf_view = Convert.put_int(self.data_type, buf_view, little=little)
        buf_view = Convert.put_short(self.data_ind, buf_view, little=little)
        buf_view = self.data_value.insertIntoByteArray(buf_view, little=little)
        buf_view = Convert.put_int(self.data_charset, buf_view, little=little)

        return buf_view

    def extractFromByteArray(self, buf_view:memoryview)->memoryview:
        self.data_type, buf_view = Convert.get_int(buf_view, little=True)
        self.data_ind, buf_view = Convert.get_short(buf_view, little=True)
        buf_view = self.data_value.extractFromByteArray(buf_view)
        self.data_charset, buf_view = Convert.get_int(buf_view, little=True)
        return buf_view


class SQLValueList_def:

    def __init__(self):
        self.value_list = []

    def sizeof(self):
        size = Transport.size_int
        if (len(self.value_list)) is not 0:
            for x in self.value_list:
                size += x.sizeof()

        return size

    def insertIntoByteArray(self, buf_view, little=True):
        count = len(self.value_list)
        if count is not 0:
            Convert.put_int(count, buf_view,little)
            for x in self.value_list:
                x.insertIntoByteArray(buf_view)
        else:
            Convert.put_int(0, buf_view, little)
        return buf_view

    def extractFromByteArray(self, buf_view:memoryview)->memoryview:
        count, buf_view = Convert.get_int(buf_view, little=True)

        for x in range(count):
            temp_sql_value = SQLValue_def()
            buf_view = temp_sql_value.extractFromByteArray(buf_view)
            self.value_list.append(buf_view)

        return buf_view


class ExecuteReply:
    def __init__(self):
        self.return_code = 0
        self.total_error_length = 0
        self.errorlist = []
        self.output_desc_length = 0   #column length
        self.rows_affected = 0
        self.query_type = 0
        self.estimated_cost = 0
        self.out_values = None
        self.num_resultsets = 0
        self.output_desc_list = []
        self.stmt_labels_list = []
        self.proxy_syntax_list = []

    def init_reply(self, buf_view):
        self.return_code, buf_view = Convert.get_int(buf_view, little=True)
        self.total_error_length, buf_view = Convert.get_int(buf_view, little=True)
        if self.total_error_length > 0:
            error_count, buf_view = Convert.get_int(buf_view, little=True)
            for x in range(error_count):
                t = SQLWarningOrError()
                buf_view = t.extractFromByteArray(buf_view)
                self.errorlist.append(t)
            error_info = ''
            for item in self.errorlist:
                error_info += item.text + '\n'

            if self.return_code != 0:
                raise errors.ProgrammingError(error_info)
            else:
                raise errors.Warning(error_info)

        self.output_desc_length, buf_view = Convert.get_int(buf_view, little=True)
        if self.output_desc_length > 0:

            output_param_length, buf_view = Convert.get_int(buf_view, little=True)
            output_number_params, buf_view = Convert.get_int(buf_view, little=True)
            for x in range(output_number_params):
                t = Descriptor()
                buf_view = t.extractFromByteArray(buf_view)
                t.set_row_length(output_param_length)
                self.output_desc_list.append(t)

        self.rows_affected, buf_view = Convert.get_int(buf_view, little=True)
        self.query_type, buf_view = Convert.get_int(buf_view, little=True)
        self.estimated_cost, buf_view = Convert.get_int(buf_view, little=True)

        # 64 bit rows_affected,this is a horrible hack because we cannot change the protocol yet
        # rows_affected should be made a regular 64 bit value when possible
        self.rows_affected = self.rows_affected or (self.estimated_cost << 32)
        self.out_values, buf_view = Convert.get_bytes(buf_view)
        self.num_resultsets, buf_view = Convert.get_int(buf_view, little=True)

        if self.num_resultsets > 0:

            self.output_desc_list = []
            for x in range(self.num_resultsets):
                _, buf_view = Convert.get_int(buf_view, little=True) # stmt handle
                stmt_lable, buf_view = Convert.get_string(buf_view, little=True)
                self.stmt_labels_list.append(stmt_lable)
                _, buf_view = Convert.get_int(buf_view, little=True)  # long stmt_label_charset
                output_desc_length, buf_view = Convert.get_int(buf_view, little=True)

                temp_descriptor_list = []
                if self.output_desc_length > 0:

                    output_param_length, buf_view = Convert.get_int(buf_view, little=True)
                    output_number_params, buf_view = Convert.get_int(buf_view, little=True)

                    for y in range(output_number_params):
                        t = Descriptor()
                        t.extractFromByteArray(buf_view)
                        t.set_row_length(output_param_length)
                        temp_descriptor_list.append(t)

                self.output_desc_list.append(temp_descriptor_list)
                proxy_syntax, buf_view = Convert.get_string(buf_view, little=True)
                self.proxy_syntax_list.append(proxy_syntax)

        single_syntax, buf_view = Convert.get_string(buf_view, little=True)

        if not self.proxy_syntax_list:
            self.proxy_syntax_list.append(single_syntax)



class SQLWarningOrError:

    def __init__(self,row_id=None, sql_code=None, text=None, sql_state=None):
        self.row_id = row_id
        self.sql_code = sql_code
        self.text = text
        self.sql_state = sql_state

    def extractFromByteArray(self, buf_view: memoryview) -> memoryview:
        self.row_id, buf_view = Convert.get_int(buf_view, little=True)
        self.sql_code, buf_view = Convert.get_int(buf_view, little=True)
        self.text, buf_view = Convert.get_string(buf_view, little=True)
        self.sql_state, buf_view = Convert.get_bytes(buf_view, 5)
        _, buf_view = Convert.get_char(buf_view)
        return buf_view


class Descriptor:

    def __init__(self):
        self.noNullValue_ = 0
        self.nullValue_ = 0
        self.version_ = 0
        self.dataType_ = 0   # sql_data_type
        self.datetimeCode_ = 0
        self.maxLen_ = 0
        self.precision_ = 0
        self.scale_ = 0
        self.nullInfo_ = 0
        self.signed_ = 0
        self.odbcDataType_ = 0  # odbc_data_type
        self.odbcPrecision_ = 0
        self.sqlCharset_ = 0
        self.odbcCharset_ = 0
        self.colHeadingNm_ = None
        self.tableName_ = None
        self.catalogName_ = None
        self.schemaName_ = None
        self.headingName_ = None
        self.intLeadPrec_ = 0
        self.paramMode_ = 0
        self.row_length = 0

    def set_row_length(self, num):
        self.row_length = num

    def extractFromByteArray(self, buf_view: memoryview) -> memoryview:
        self.noNullValue_, buf_view = Convert.get_int(buf_view, little=True)
        self.nullValue_, buf_view = Convert.get_int(buf_view, little=True)
        self.version_, buf_view = Convert.get_int(buf_view, little=True)
        self.dataType_, buf_view = Convert.get_int(buf_view, little=True)
        self.datetimeCode_, buf_view = Convert.get_int(buf_view, little=True)
        self.maxLen_, buf_view = Convert.get_int(buf_view, little=True)
        self.precision_, buf_view = Convert.get_int(buf_view, little=True)
        self.scale_, buf_view = Convert.get_int(buf_view, little=True)
        self.nullInfo_, buf_view = Convert.get_int(buf_view, little=True)
        self.signed_, buf_view = Convert.get_int(buf_view, little=True)
        self.odbcDataType_, buf_view = Convert.get_int(buf_view, little=True)
        self.odbcPrecision_, buf_view = Convert.get_int(buf_view, little=True)
        self.sqlCharset_, buf_view = Convert.get_int(buf_view, little=True)
        self.odbcCharset_, buf_view = Convert.get_int(buf_view, little=True)
        self.colHeadingNm_, buf_view = Convert.get_string(buf_view, little=True)
        self.tableName_, buf_view = Convert.get_string(buf_view, little=True)
        self.catalogName_, buf_view = Convert.get_string(buf_view, little=True)
        self.schemaName_, buf_view = Convert.get_string(buf_view, little=True)
        self.headingName_, buf_view = Convert.get_string(buf_view, little=True)
        self.intLeadPrec_, buf_view = Convert.get_int(buf_view, little=True)
        self.paramMode_, buf_view = Convert.get_int(buf_view, little=True)

        return buf_view


class FetchReply:

    SQLTYPECODE_CHAR = 1
    # NUMERIC * /
    SQLTYPECODE_NUMERIC = 2
    SQLTYPECODE_NUMERIC_UNSIGNED = -201
    # DECIMAL * /
    SQLTYPECODE_DECIMAL = 3
    SQLTYPECODE_DECIMAL_UNSIGNED = -301
    SQLTYPECODE_DECIMAL_LARGE = -302
    SQLTYPECODE_DECIMAL_LARGE_UNSIGNED = -303
    # INTEGER / INT * /
    SQLTYPECODE_INTEGER = 4
    SQLTYPECODE_INTEGER_UNSIGNED = -401
    SQLTYPECODE_LARGEINT = -402
    SQLTYPECODE_LARGEINT_UNSIGNED = -405
    # SMALLINT
    SQLTYPECODE_SMALLINT = 5
    SQLTYPECODE_SMALLINT_UNSIGNED = -502
    SQLTYPECODE_BPINT_UNSIGNED = -503

        # TINYINT */
    SQLTYPECODE_TINYINT                = -403
    SQLTYPECODE_TINYINT_UNSIGNED       = -404

    # DOUBLE depending on precision
    SQLTYPECODE_FLOAT = 6
    SQLTYPECODE_REAL = 7
    SQLTYPECODE_DOUBLE = 8

    # DATE,TIME,TIMESTAMP */
    SQLTYPECODE_DATETIME = 9

    # TIMESTAMP */
    SQLTYPECODE_INTERVAL = 10

    # no ANSI value 11 */

	# VARCHAR/CHARACTER VARYING */
    SQLTYPECODE_VARCHAR = 12

    # SQL/MP stype VARCHAR with length prefix:

    SQLTYPECODE_VARCHAR_WITH_LENGTH = -601
    SQLTYPECODE_BLOB = -602
    SQLTYPECODE_CLOB = -603
    # LONG VARCHAR/ODBC CHARACTER VARYING */
    SQLTYPECODE_VARCHAR_LONG = -1 # ## NEGATIVE??? */

    # no ANSI value 13 */

	# BIT */
    SQLTYPECODE_BIT = 14 # not supported */

    # BIT VARYING */
    SQLTYPECODE_BITVAR = 15 # not supported */

    # NCHAR -- CHAR(n) CHARACTER SET s -- where s uses two bytes per char */
    SQLTYPECODE_CHAR_DBLBYTE = 16
    # NCHAR VARYING -- VARCHAR(n) CHARACTER SET s -- s uses 2 bytes per char */
    SQLTYPECODE_VARCHAR_DBLBYTE = 17
    # BOOLEAN TYPE */
    SQLTYPECODE_BOOLEAN = -701

    # Date/Time/TimeStamp related constants */
    SQLDTCODE_DATE = 1
    SQLDTCODE_TIME = 2
    SQLDTCODE_TIMESTAMP = 3
    SQLDTCODE_MPDATETIME = 4

    def __init__(self):
        self.return_code = 0
        self.errorlist = []
        self.out_values_format = 0
        self.out_values = None
        self.rows_affected = 0
        self.total_error_length = 0
        self.result_set = []
        self.end_of_data = False

    def init_reply(self, buf_view: memoryview, execute_desc):
        self.return_code, buf_view = Convert.get_int(buf_view, little=True)
        if self.return_code != Transport.SQL_SUCCESS and self.return_code != Transport.NO_DATA_FOUND:
            self.total_error_length, buf_view = Convert.get_int(buf_view, little=True)
            if self.total_error_length > 0:
                error_count, buf_view = Convert.get_int(buf_view, little=True)
                for x in range(error_count):
                    t = SQLWarningOrError()
                    buf_view = t.extractFromByteArray(buf_view)
                    self.errorlist.append(t)

                error_info = ''
                for item in self.errorlist:
                    error_info += item.text + '\n'
                raise errors.ProgrammingError(error_info)

        self.rows_affected, buf_view = Convert.get_int(buf_view, little=True)
        self.out_values_format, buf_view = Convert.get_int(buf_view, little=True)

        if self.return_code == Transport.SQL_SUCCESS or self.return_code == Transport.SQL_SUCCESS_WITH_INFO:
            self.out_values, buf_view = Convert.get_bytes(buf_view, little=True)

            if len(self.errorlist) != 0:
                pass
            self._set_out_puts(execute_desc)
            self.end_of_data = False

        if self.return_code == Transport.NO_DATA_FOUND:
            self.end_of_data = True

    def _set_out_puts(self, execute_desc):

        buf_view = memoryview(self.out_values)

        data_len = 0
        out_desc_list = execute_desc.output_desc_list
        if len(out_desc_list) != 0:
            data_len = out_desc_list[0].row_length

        column_count = len(execute_desc.output_desc_list)
        for rows_x in range(self.rows_affected):
            row_offset = rows_x * data_len

            column_result_list = []
            for column_x in range(column_count):
                nonull_value_offset = out_desc_list[column_x].noNullValue_
                null_value_offset = out_desc_list[column_x].nullValue_

                if null_value_offset != -1:
                    null_value_offset += row_offset
                if nonull_value_offset != -1:
                    nonull_value_offset += row_offset

                null_value = 0
                if null_value_offset != -1:
                    null_value, _ = Convert.get_short(buf_view[null_value_offset:], little=True)

                column_value = None
                if null_value_offset != -1 and null_value == -1:
                    column_value = None
                else:
                    column_value = self._get_execute_to_fetch_string(nonull_value_offset, out_desc_list[column_x])
                    if not column_value:
                        raise errors.InternalError("column value is null")

                column_result_list.append(column_value)

            self.result_set.append(column_result_list)

        self._clear_and_set()

    def _clear_and_set(self):

        # save result set and clear out_values
        self.out_values = None
        self.rows_fetched = len(self.result_set)

    def _get_execute_to_fetch_string(self, nonull_value_offset, column_desc: Descriptor):
        ret_obj = None
        buf_view = memoryview(self.out_values)
        sql_data_type = column_desc.dataType_
        if sql_data_type == self.SQLTYPECODE_CHAR:
            length = column_desc.maxLen_
            ret_obj, _ = Convert.get_bytes(buf_view[nonull_value_offset:], length=length)

        if sql_data_type == self.SQLTYPECODE_VARCHAR or \
                        sql_data_type == self.SQLTYPECODE_VARCHAR_WITH_LENGTH or \
                        sql_data_type == self.SQLTYPECODE_VARCHAR_LONG or \
                        sql_data_type == self.SQLTYPECODE_BLOB or \
                        sql_data_type == self.SQLTYPECODE_CLOB:

            short_length = 2 if column_desc.precision_ < 2**15 else 4
            data_offset = nonull_value_offset + short_length

            data_len = 0
            if short_length == 2:
                data_len, _ = Convert.get_short(buf_view[nonull_value_offset:], little=True)
            else:
                data_len, _ = Convert.get_int(buf_view[nonull_value_offset:], little=True)

            length_left = len(self.out_values) - data_offset

            data_len = length_left if length_left < data_len else data_len

            ret_obj = buf_view[data_offset:data_offset + data_len].tobytes()

        if sql_data_type == self.SQLTYPECODE_INTERVAL:
            pass

        if sql_data_type == self.SQLTYPECODE_INTEGER:
            ret_obj, _ = Convert.get_int(buf_view[nonull_value_offset:], little=True)
            # TODO scale of big decimal

        if sql_data_type == self.SQLTYPECODE_TINYINT_UNSIGNED:
            pass
        if sql_data_type == self.SQLTYPECODE_TINYINT:
            pass
        if sql_data_type == self.SQLTYPECODE_SMALLINT:
            pass
        if sql_data_type == self.SQLTYPECODE_SMALLINT_UNSIGNED:
            pass
        if sql_data_type == self.SQLTYPECODE_INTEGER:
            pass
        if sql_data_type == self.SQLTYPECODE_INTEGER_UNSIGNED:
            pass
        if sql_data_type == self.SQLTYPECODE_LARGEINT:
            pass
        if sql_data_type == self.SQLTYPECODE_LARGEINT_UNSIGNED:
            pass
        if sql_data_type == self.SQLTYPECODE_NUMERIC or \
            sql_data_type == self.SQLTYPECODE_NUMERIC_UNSIGNED:
            pass
        if sql_data_type == self.SQLTYPECODE_DECIMAL or \
                        sql_data_type == self.SQLTYPECODE_DECIMAL_UNSIGNED or \
                        sql_data_type == self.SQLTYPECODE_DECIMAL_LARGE or \
                        sql_data_type == self.SQLTYPECODE_DECIMAL_LARGE_UNSIGNED:
            pass
        if sql_data_type == self.SQLTYPECODE_REAL:
            pass
        if sql_data_type == self.SQLTYPECODE_DOUBLE or sql_data_type == self.SQLTYPECODE_FLOAT:
            pass
        if sql_data_type == self.SQLTYPECODE_BIT or \
                        sql_data_type == self.SQLTYPECODE_BITVAR or \
                        sql_data_type == self.SQLTYPECODE_BPINT_UNSIGNED:
            pass
        return ret_obj


class PrepareReply:
    def __init__(self):
        self.errorlist = []
        self.return_code = 0
        self.sql_query_type = 0
        self.stmt_handle = 0
        self.estimated_cost = 0
        self.input_desc_length = 0
        self.input_desc_list = []
        self.output_desc_list = []
        self.output_desc_length = 0
        self.total_error_length = 0

    def init_reply(self, buf_view: memoryview):
        self.return_code, buf_view = Convert.get_int(buf_view, little=True)

        if self.return_code == Transport.SQL_SUCCESS or self.return_code == Transport.SQL_SUCCESS_WITH_INFO:
            if self.return_code == Transport.SQL_SUCCESS_WITH_INFO:
                self.total_error_length, buf_view = Convert.get_int(buf_view, little=True)
                if self.total_error_length > 0:
                    error_count, buf_view = Convert.get_int(buf_view, little=True)
                    for x in error_count:
                        t = SQLWarningOrError()
                        buf_view = t.extractFromByteArray(buf_view)
                        self.errorlist.append(t)
                    error_info = ''
                    for item in self.errorlist:
                        error_info += item.text + '\n'
                    raise errors.Warning(error_info)
            self.sql_query_type, buf_view = Convert.get_int(buf_view, little=True)
            self.stmt_handle, buf_view = Convert.get_int(buf_view, little=True)
            self.estimated_cost, buf_view = Convert.get_int(buf_view, little=True)
            self.input_desc_length, buf_view = Convert.get_int(buf_view, little=True)

            if self.input_desc_length > 0:
                input_param_length, buf_view = Convert.get_int(buf_view, little=True)
                input_number_params, buf_view = Convert.get_int(buf_view, little=True)
                for x in range(input_number_params):
                    t = Descriptor()
                    buf_view = t.extractFromByteArray(buf_view)
                    t.set_row_length(input_param_length)
                    self.input_desc_list.append(t)

            self.output_desc_length, buf_view = Convert.get_int(buf_view, little=True)
            if self.output_desc_length > 0:
                output_param_length, buf_view = Convert.get_int(buf_view, little=True)
                output_number_params, buf_view = Convert.get_int(buf_view, little=True)
                for x in range(output_number_params):
                    t = Descriptor()
                    buf_view = t.extractFromByteArray(buf_view)
                    t.set_row_length(output_param_length)
                    self.output_desc_list.append(t)

        else:
            self.total_error_length, buf_view = Convert.get_int(buf_view, little=True)
            if self.total_error_length > 0:
                error_count, buf_view = Convert.get_int(buf_view, little=True)
                for x in range(error_count):
                    t = SQLWarningOrError()
                    buf_view = t.extractFromByteArray(buf_view)
                    self.errorlist.append(t)
                error_info = ''
                for item in self.errorlist:
                    error_info += item.text + '\n'
                raise errors.ProgrammingError(error_info)
