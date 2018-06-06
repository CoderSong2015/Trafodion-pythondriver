
from .TRANSPOT import TRANSPORT, convert
class CONNECTION_CONTEXT_def:
    def __init__(self):
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

        size = TRANSPORT.size_bytes(self.datasource_bytes)
        size += TRANSPORT.size_bytes(self.catalog_bytes)
        size += TRANSPORT.size_bytes(self.schema_bytes)
        size += TRANSPORT.size_bytes(self.location_bytes)
        size += TRANSPORT.size_bytes(self.userRole_bytes)

        size += TRANSPORT.size_short  # accessMode
        size += TRANSPORT.size_short  # autoCommit
        size += TRANSPORT.size_int  # queryTimeoutSec
        size += TRANSPORT.size_int  # idleTimeoutSec
        size += TRANSPORT.size_int  # loginTimeoutSec
        size += TRANSPORT.size_short  # txnIsolationLevel
        size += TRANSPORT.size_short  # rowSetSize

        size += TRANSPORT.size_int  # diagnosticFlag
        size += TRANSPORT.size_int  # processId

        size += TRANSPORT.size_bytes(self.computerName_bytes)
        size += TRANSPORT.size_bytes(self.windowText_bytes)

        size += TRANSPORT.size_int  # ctxACP
        size += TRANSPORT.size_int  # ctxDataLang
        size += TRANSPORT.size_int  # ctxErrorLang
        size += TRANSPORT.size_short  # ctxCtrlInferNCHAR

        size += TRANSPORT.size_short  # cpuToUse
        size += TRANSPORT.size_short  # cpuToUseEnd
        size += TRANSPORT.size_bytes(self.connectOptions_bytes)

        size += self.clientVersionList.sizeOf()

        return size

    def insertIntoByteArray(self, buf_view):
        buf_view = convert.put_string(self.datasource, buf_view)
        buf_view = convert.put_string(self.catalog, buf_view) # string
        buf_view = convert.put_string(self.schema, buf_view)  # string
        buf_view = convert.put_string(self.location, buf_view)# string
        buf_view = convert.put_string(self.userRole, buf_view) # string
        #buf_view = convert.put_string(self.tenantName, buf_view)# string

        buf_view = convert.put_short(self.accessMode, buf_view) # short
        buf_view = convert.put_short(self.autoCommit, buf_view) # short
        buf_view = convert.put_int(self.queryTimeoutSec, buf_view) # int
        buf_view = convert.put_int(self.idleTimeoutSec, buf_view) # int
        buf_view = convert.put_int(self.loginTimeoutSec, buf_view) # int
        buf_view = convert.put_short(self.txnIsolationLevel, buf_view) # short
        buf_view = convert.put_short(self.rowSetSize, buf_view) # short

        buf_view = convert.put_int(self.diagnosticFlag, buf_view) # int
        buf_view = convert.put_int(self.processId, buf_view) # int

        buf_view = convert.put_string(self.computerName, buf_view)# string
        buf_view = convert.put_string(self.windowText, buf_view)  # string

        buf_view = convert.put_int(self.ctxACP, buf_view) # int
        buf_view = convert.put_int(self.ctxDataLang, buf_view) # int
        buf_view = convert.put_int(self.ctxErrorLang, buf_view) # int
        buf_view = convert.put_short(self.ctxCtrlInferNXHAR, buf_view) # short

        buf_view = convert.put_short(self.cpuToUse, buf_view) # short
        buf_view = convert.put_short(self.cpuToUseEnd, buf_view)  # short for future use by DBTransporter

        buf_view = convert.put_string(self.connectOptions, buf_view) # string

        buf_view = self.clientVersionList.insertIntoByteArray(buf_view)
        return buf_view


class VERSION_def:

    def __init__(self):
        self.componentId = 0  # short
        self.majorVersion = 0  # short
        self.minorVersion = 0  # short
        self.buildId = 0  # int

    @classmethod
    def sizeOf(self):
        return TRANSPORT.size_int + TRANSPORT.size_short * 3

    def insertIntoByteArray(self, buf_view):
        buf_view = convert.put_short(self.componentId, buf_view)
        buf_view = convert.put_short(self.majorVersion, buf_view)
        buf_view = convert.put_short(self.minorVersion, buf_view)
        buf_view = convert.put_int(self.buildId, buf_view)
        return buf_view

    def extractFromByteArray(self, buf_view):
        self.componentId, buf_view = convert.get_short(buf_view, little=True)
        self.majorVersion, buf_view = convert.get_short(buf_view, little=True)
        self.minorVersion, buf_view = convert.get_short(buf_view, little=True)
        self.buildId, buf_view = convert.get_int(buf_view, little=True)
        return buf_view


class VERSION_LIST_def:
    list = []

    def insertIntoByteArray(self, buf_view):
        buf_view = convert.put_int(len(self.list), buf_view)
        for item in self.list:
            buf_view = item.insertIntoByteArray(buf_view)
        return buf_view

    def sizeOf(self):
        return VERSION_def.sizeOf() * self.list.__len__() + TRANSPORT.size_int

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
    SRVR_TRANSPORT_ERROR = (CLEANUP + 1)
    CLOSE_TCPIP_SESSION = (SRVR_TRANSPORT_ERROR + 1)

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


    def extractFromByteArray(self, buf):
        buf_view = memoryview(buf)
        self.operation_id_, buf_view = convert.get_short(buf_view)
        null, buf_view = convert.get_short(buf_view)  # +2 fillter

        self.dialogueId_, buf_view = convert.get_int(buf_view)
        self.total_length_, buf_view = convert.get_int(buf_view)
        self.cmp_length_, buf_view = convert.get_int(buf_view)
        self.compress_ind_, buf_view = convert.get_char(buf_view)
        self.compress_type_, buf_view = convert.get_char(buf_view)

        null, buf_view = convert.get_short(buf_view)  # +2 fillter

        self.hdr_type_, buf_view = convert.get_int(buf_view)
        self.signature_, buf_view = convert.get_int(buf_view)
        self.version_, buf_view = convert.get_int(buf_view)
        self.platform_, buf_view = convert.get_char(buf_view)
        self.transport_, buf_view = convert.get_char(buf_view)
        self.swap_, buf_view = convert.get_char(buf_view)
        null, buf_view = convert.get_char(buf_view)  # +1 fillter
        self.error_, buf_view = convert.get_short(buf_view)
        self.error_detail_, buf_view = convert.get_short(buf_view)

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

        size += TRANSPORT.size_int  # descType
        size += TRANSPORT.size_bytes(self.domainName_bytes)
        size += TRANSPORT.size_bytes(self.userName_bytes)
        size += TRANSPORT.size_bytes(self.userName)
        size += TRANSPORT.size_bytes(self.password)

        return size

    def insertIntoByteArray(self, buf_view):
        buf_view = convert.put_int(self.userDescType, buf_view)

        buf_view = convert.put_string(self.userSid, buf_view)
        buf_view = convert.put_string(self.domainName, buf_view)
        buf_view = convert.put_string(self.userName, buf_view)
        buf_view = convert.put_string(self.password,buf_view)

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

    @property
    def retry_count(self):
        return self._retry_count

    @retry_count.setter
    def retry_count(self, num):
        self._retry_count = num

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


