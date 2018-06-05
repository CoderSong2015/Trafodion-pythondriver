
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

        self.datasourceBytes = bytearray(b'')
        self.catalogBytes = bytearray(b'')
        self.schemaBytes = bytearray(b'')
        self.locationBytes = bytearray(b'')
        self.userRoleBytes = bytearray(b'')
        self.computerNameBytes = bytearray(b'')
        self.windowTextBytes = bytearray(b'')
        self.connectOptionsBytes = bytearray(b'')

    def sizeOf(self):
        self.size = 0
        self.datasourceBytes = self.datasource.encode('utf-8')
        self.catalogBytes = self.catalog.encode('utf-8')
        self.schemaBytes = self.schema.encode('utf-8')
        self.locationBytes = self.location.encode('utf-8')
        self.userRoleBytes = self.userRole.encode('utf-8')
        self.computerNameBytes = self.computerName.encode('utf-8')
        self.windowTextBytes = self.windowText.encode('utf-8')
        self.connectOptionsBytes = self.connectOptions.encode('utf-8')

        size = TRANSPORT.size_bytes(self.datasourceBytes)
        size += TRANSPORT.size_bytes(self.catalogBytes)
        size += TRANSPORT.size_bytes(self.schemaBytes)
        size += TRANSPORT.size_bytes(self.locationBytes)
        size += TRANSPORT.size_bytes(self.userRoleBytes)

        size += TRANSPORT.size_short  # accessMode
        size += TRANSPORT.size_short  # autoCommit
        size += TRANSPORT.size_int  # queryTimeoutSec
        size += TRANSPORT.size_int  # idleTimeoutSec
        size += TRANSPORT.size_int  # loginTimeoutSec
        size += TRANSPORT.size_short  # txnIsolationLevel
        size += TRANSPORT.size_short  # rowSetSize

        size += TRANSPORT.size_int  # diagnosticFlag
        size += TRANSPORT.size_int  # processId

        size += TRANSPORT.size_bytes(self.computerNameBytes)
        size += TRANSPORT.size_bytes(self.windowTextBytes)

        size += TRANSPORT.size_int  # ctxACP
        size += TRANSPORT.size_int  # ctxDataLang
        size += TRANSPORT.size_int  # ctxErrorLang
        size += TRANSPORT.size_short  # ctxCtrlInferNCHAR

        size += TRANSPORT.size_short  # cpuToUse
        size += TRANSPORT.size_short  # cpuToUseEnd
        size += TRANSPORT.size_bytes(self.connectOptionsBytes)

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
    componentId = 0  # short
    majorVersion = 0  # short
    minorVersion = 0  # short
    buildId = 0  # int

    @classmethod
    def sizeOf(self):
        return TRANSPORT.size_int + TRANSPORT.size_short * 3

    def insertIntoByteArray(self, buf_view):
        buf_view = convert.put_short(self.componentId, buf_view)
        buf_view = convert.put_short(self.majorVersion, buf_view)
        buf_view = convert.put_short(self.minorVersion, buf_view)
        buf_view = convert.put_int(self.buildId, buf_view)
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
        pass


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

    #
    # The Java version of the HEADER structure taken from TransportBase.h
    #
    operation_id_ = 0  # short
    # + 2 filler
    dialogueId_ = 0  # int
    total_length_ = 0 # int
    cmp_length_ = 0   #int
    compress_ind_ = '' #char
    compress_type_ = '' #char
    # + 2 filler  = 0
    hdr_type_ = 0  #int
    signature_ = 0 #int
    version_ = 0   #int
    platform_ = '' #char
    transport_ ='' #char
    swap_      = '' #char
    # + 1 filler
    error_     = 0 #short
    error_detail_ = 0 #short


    def __init__(self,  operation_id = None,
                        dialogueId = None,
                        total_length = None,
                        cmp_length = None,
                        compress_ind = None,
                        compress_type = None,
                        hdr_type = None,
                        signature = None,
                        version = None,
                        platform = None,
                        transport = None,
                        swap = None):
        self.operation_id_ = operation_id
        self.dialogueId_ = dialogueId
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
    @property
    def total_length(self):
        return self.total_length_

    def reuseHeader(self,
                    operation_id,
                    dialogueId):
        self.operation_id_ = operation_id
        self.dialogueId_ = dialogueId


    @classmethod
    def sizeOf(self):
        return 40

    def insertIntoByteArray(self, buf_view):
        buf_view = convert.put_short(self.operation_id_, buf_view)  # short                  0,1
        buf_view = convert.put_short(0, buf_view)# + 2 filler                                2,3
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
        buf_view = convert.put_short(self.error_, buf_view) # short
        buf_view = convert.put_short(self.error_detail_, buf_view)  # short
        return buf_view


    def extractFromByteArray(self, buf):
        buf_view = memoryview(buf)
        self.operation_id_, buf_view= convert.get_short(buf_view)
        null, buf_view = convert.get_short(buf_view) # +2 fillter

        self.dialogueId_, buf_view= convert.get_int(buf_view)
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
    userDescType = 0
    userSid = ''
    domainName = ''
    userName = ''
    password = ''
    domainNameBytes = ''
    userNameBytes = ''

    def __init__(self):
        self.userDescType = 0
        self.userSid = ''
        self.domainName = ''
        self.userName = ''
        self.password = ''
        self.domainNameBytes = ''
        self.userNameBytes = ''

    def sizeOf(self):
        size = 0

        Tr = TRANSPORT()
        size += Tr.size_int # descType

        size += TRANSPORT.size_bytes(self.userSid)
        size += TRANSPORT.size_bytes(self.domainName)
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

