
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

        size = len(self.datasourceBytes)
        size += len(self.catalogBytes)
        size += len(self.schemaBytes)
        size += len(self.locationBytes)
        size += len(self.userRoleBytes)

        size += TRANSPORT.size_short  # accessMode
        size += TRANSPORT.size_short  # autoCommit
        size += TRANSPORT.size_int  # queryTimeoutSec
        size += TRANSPORT.size_int  # idleTimeoutSec
        size += TRANSPORT.size_int  # loginTimeoutSec
        size += TRANSPORT.size_short  # txnIsolationLevel
        size += TRANSPORT.size_short  # rowSetSize

        size += TRANSPORT.size_short  # diagnosticFlag
        size += TRANSPORT.size_int  # processId

        size += len(self.computerNameBytes)
        size += len(self.windowTextBytes)

        size += TRANSPORT.size_int  # ctxACP
        size += TRANSPORT.size_int  # ctxDataLang
        size += TRANSPORT.size_int  # ctxErrorLang
        size += TRANSPORT.size_short  # ctxCtrlInferNCHAR

        size += TRANSPORT.size_short  # cpuToUse
        size += TRANSPORT.size_short  # cpuToUseEnd
        size += len(self.connectOptionsBytes)

        size += self.clientVersionList.sizeOf()

        return size

    def insertIntoByteArray(self, buf_view):
        temp = (
        self.datasource,
        self.catalog,
        self.schema,
        self.location,
        self.userRole,
        self.tenantName,

        self.accessMode,
        self.autoCommit,
        self.queryTimeoutSec,
        self.idleTimeoutSec,
        self.loginTimeoutSec,
        self.txnIsolationLevel,
        self.rowSetSize,
        self.diagnosticFlag,
        self.processId,
        self.computerName,
        self.windowText,

        self.ctxACP,
        self.ctxDataLang,
        self.ctxErrorLang,
        self.ctxCtrlInferNXHAR,

        self.cpuToUse,
        self.cpuToUseEnd,

        self.connectOptions
            #need clientVersionList
        )
        data = convert.convert_buf()
        for index, byte in enumerate(data):
            buf_view[index] = byte
        return buf_view + self.sizeOf()


class VERSION_def:
    componentId = 0  # short
    majorVersion = 0  # short
    minorVersion = 0  # short
    buildId = 0  # int

    @classmethod
    def sizeOf(self):
        return TRANSPORT.size_int + TRANSPORT.size_short * 3


class VERSION_LIST_def:
    list = []

    def sizeOf(self):
        return VERSION_def.sizeOf() * self.list.__len__() + TRANSPORT.size_int


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


    def __init__(self,  operation_id,
                        dialogueId,
                        total_length,
                        cmp_length,
                        compress_ind,
                        compress_type,
                        hdr_type,
                        signature,
                        version,
                        platform,
                        transport,
                        swap):
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


    def reuseHeader(self,
                    operation_id,
                    dialogueId):
        self.operation_id_ = operation_id
        self.dialogueId_ = dialogueId


    @classmethod
    def sizeOf(self):
        return 40

    def insertIntoByteArray(self, buffer1):
        pass


    def extractFromByteArray(self, LogicalByteArray):
        pass

class USER_DESC_def:
    userDescType = 0
    userSid = b''
    domainName = ''
    userName = ''
    password = b''
    domainNameBytes = b''
    userNameBytes = b''

    def sizeOf(self):
        size = 0

        domainNameBytes = len(self.domainName)
        userNameBytes = len(self.userName)
        Tr = TRANSPORT()
        size += Tr.size_int # descType

        size += Tr.size_bytes(self.userSid)
        size += Tr.size_bytes(self.domainNameBytes)
        size += Tr.size_bytes(self.userNameBytes)
        size += Tr.size_bytes(self.password)

        return size

    def return_bytearray(self):
        pass
