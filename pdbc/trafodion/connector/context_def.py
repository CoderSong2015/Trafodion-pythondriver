
from .version_def import  VERSION_LIST_def
class CONNECTION_CONTEXT_def:
    def __init__(self):
        self.datasource = ""    # string
        self.catalog    = ""    # string
        self.schema     = ""    # string
        self.location   = ""    # string
        self.userRole   = ""    # string
        self.tenantName = ""    # string

        self.accessMode = 0        # short
        self.autoCommit = 0        # short
        self.queryTimeoutSec = 0   # short
        self.idleTimeoutSec  = 0   # short
        self.loginTimeoutSec = 0   # short
        self.txnIsolationLevel = 0 # short
        self.rowSetSize        = 0 # short

        self.diagnosticFlag = 0    # int
        self.processId      = 0    # int

        self.computerName = ""     # string
        self.windowText   = ""     # string

        self.ctxACP       = 0      # int
        self.ctxDataLang  = 0      # int
        self.ctxErrorLang = 0      # int
        self.ctxCtrlInferNXHAR = 0    # short


        self.cpuToUse = -1    #short
        self.cpuToUseEnd = -1 # short for future use by DBTransporter

        self.connectOptions = "" #string

        self.clientVersionList = VERSION_LIST_def()

        self.datasourceBytes = bytearray(b'')
        self.catalogBytes = bytearray(b'')
        self.schemaBytes = bytearray(b'')
        self.locationBytes = bytearray(b'')
        self.userRoleBytes = bytearray(b'')
        self.computerNameBytes = bytearray(b'')
        self.windowTextBytes = bytearray(b'')
        self.connectOptionsBytes = bytearray(b'')


    @property
    def sizeOf(self):
        self.size = 0
        self.datasourceBytes =  self.datasource.encode('utf-8')
        self.catalogBytes =     self.catalog.encode('utf-8')
        self.schemaBytes =      self.schema.encode('utf-8')
        self.locationBytes =    self.location.encode('utf-8')
        self.userRoleBytes =    self.userRole.encode('utf-8')
        self.computerNameBytes =self.computerName.encode('utf-8')
        self.windowTextBytes =  self.windowText.encode('utf-8')
        self.connectOptionsBytes = self.connectOptions.encode('utf-8')

        size =  len(self.datasourceBytes)
        size += len(self.catalogBytes)
        size += len(self.schemaBytes)
        size += len(self.locationBytes)
        size += len(self.userRoleBytes)

        size += 2 # accessMode
        size += 2 # autoCommit
        size += 4 # queryTimeoutSec
        size += 4 # idleTimeoutSec
        size += 4 # loginTimeoutSec
        size += 2 # txnIsolationLevel
        size += 2 # rowSetSize

        size += 2 # diagnosticFlag
        size += 4 # processId

        size += len(self.computerNameBytes)
        size += len(self.windowTextBytes)

        size += 4 # ctxACP
        size += 4 # ctxDataLang
        size += 4 # ctxErrorLang
        size += 2 # ctxCtrlInferNCHAR

        size += 2 # cpuToUse
        size += 2 # cpuToUseEnd
        size += len(self.connectOptionsBytes)

        size += self.clientVersionList.sizeOf()

        return size


