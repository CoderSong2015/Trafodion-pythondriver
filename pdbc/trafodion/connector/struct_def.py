import socket
import time

from . import errors

from .constants.TRANSPORT import Transport
from .converters import sql_to_py_convert_dict, py_to_sql_convert_dict, Convert
from .constants import CONNECTION, STRUCTDEF, FIELD_TYPE
from .logmodule import PyLog

class ConnectionContextDef:
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

    def insert_into_bytearray(self, buf_view, little=False):
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

        buf_view = self.clientVersionList.insert_into_bytearray(buf_view, little)
        return buf_view

    def _init_context(self, conn):
        self.catalog = conn.property.catalog
        self.schema =  conn.property.schema
        self.datasource = conn.property.datasource
        self.userRole = conn.property.userRole
        self.cpuToUse = conn.property.cpuToUse
        self.cpuToUseEnd = -1 # for future use by DBTransporter

        self.accessMode = 1 if conn._is_read_only else 0
        self.autoCommit = 1 if conn._auto_commit else 0

        self.queryTimeoutSec = conn.property.query_timeout
        self.idleTimeoutSec = conn.property.idleTimeout
        self.loginTimeoutSec = conn.property.login_timeout
        self.txnIsolationLevel = CONNECTION.SQL_TXN_READ_COMMITTED
        self.rowSetSize = conn.property.fetchbuffersize
        self.diagnosticFlag = 0
        self.processId = time.time() and 0xFFF

        try:
            self.computerName = socket.gethostname()
        except:
            self.computerName = "Unknown Client Host"

        self.windowText = "Python-Driver" if not conn.property.application_name else conn.property.application_name

        self.ctxDataLang = 15
        self.ctxErrorLang = 15

        self.ctxACP = 1252
        self.ctxCtrlInferNXHAR = -1
        self.clientVersionList.list = conn.get_version(self.processId)


class VersionDef:

    def __init__(self):
        self.componentId = 0  # short
        self.majorVersion = 0  # short
        self.minorVersion = 0  # short
        self.buildId = 0  # int

    @classmethod
    def sizeOf(self):
        return Transport.size_int + Transport.size_short * 3

    def insert_into_bytearray(self, buf_view, little=False):
        buf_view = Convert.put_short(self.componentId, buf_view, little)
        buf_view = Convert.put_short(self.majorVersion, buf_view, little)
        buf_view = Convert.put_short(self.minorVersion, buf_view, little)
        buf_view = Convert.put_int(self.buildId, buf_view, little)
        return buf_view

    def extract_from_bytearray(self, buf_view):
        self.componentId, buf_view = Convert.get_short(buf_view, little=True)
        self.majorVersion, buf_view = Convert.get_short(buf_view, little=True)
        self.minorVersion, buf_view = Convert.get_short(buf_view, little=True)
        self.buildId, buf_view = Convert.get_int(buf_view, little=True)
        return buf_view


class VERSION_LIST_def:
    list = []

    def insert_into_bytearray(self, buf_view, little=False):
        buf_view = Convert.put_int(len(self.list), buf_view, little)
        for item in self.list:
            buf_view = item.insert_into_bytearray(buf_view, little)
        return buf_view

    def sizeOf(self):
        return VersionDef.sizeOf() * self.list.__len__() + Transport.size_int

    def extract_from_bytearray(self, buf_view):

        length, buf_view = Convert.get_int(buf_view, little=True)

        for i in range(length):
            versiondef = VersionDef()
            buf_view = versiondef.extract_from_bytearray(buf_view)
            self.list.append(versiondef)

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
      version       # int
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
        self.version = version
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

    def insert_into_bytearray(self, buf_view):
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
        buf_view = Convert.put_int(self.version, buf_view)  # int
        buf_view = Convert.put_char(self.platform_.encode("utf-8"), buf_view)  # char
        buf_view = Convert.put_char(self.transport_.encode("utf-8"), buf_view)  # char
        buf_view = Convert.put_char(self.swap_.encode("utf-8"), buf_view)  # char
        buf_view = Convert.put_char('0'.encode("utf-8"), buf_view)  # + 1 filler
        buf_view = Convert.put_short(self.error_, buf_view)  # short
        buf_view = Convert.put_short(self.error_detail_, buf_view)  # short
        return buf_view

    def extract_from_bytearray(self, buf, little=False):
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
        self.version, buf_view = Convert.get_int(buf_view, little)
        self.platform_, buf_view = Convert.get_char(buf_view)
        self.transport_, buf_view = Convert.get_char(buf_view)
        self.swap_, buf_view = Convert.get_char(buf_view)
        __, buf_view = Convert.get_char(buf_view)  # +1 fillter
        self.error_, buf_view = Convert.get_short(buf_view, little)
        self.error_detail_, buf_view = Convert.get_short(buf_view, little)


class UserDescDef:

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

    def insert_into_bytearray(self, buf_view, little=False):
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
        self._connection_timeout = 300
        self._idleTimeout = 3600
        self._login_timeout = 60
        self._fetchbuffersize = 100
        self._application_name = ""
        self._DelayedErrorMode = False
        self._retry_count = 5
        self._srvr_type = 2  # AS
        self._fetch_ahead = ''
        self._tenant_name = None
        self._charset = "UTF-8"
        self._logging_file_path = None
        self._loggger_name = "Trafodion"

    @property
    def loggger_name(self):
        return self._loggger_name

    @loggger_name.setter
    def loggger_name(self, logger_name):
        self._loggger_name = logger_name

    @property
    def logging_path(self):
        return self._logging_file_path

    @logging_path.setter
    def logging_path(self, logging_path):
        self._logging_file_path = logging_path

    @property
    def charset(self):
        return self._charset

    @charset.setter
    def charset(self, charset):
        charset = charset.upper()
        if charset not in Transport.charset_to_value:
            raise errors.ProgrammingError("unsupport charset: {0}".format(charset))
        self._charset = charset

    @property
    def tenant_name(self):
        return self._tenant_name

    @tenant_name.setter
    def tenant_name(self, tenant):
        self._tenant_name = tenant

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
    def connection_timeout(self):
        return self._connection_timeout

    @connection_timeout.setter
    def connection_timeout(self, num):
        self._connection_timeout = num

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
    def extract_from_bytearray(self,buf_view):
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
        buf_view = self.buf_exception.extract_from_bytearray(buf_view)

        # TODO handle error
        if self.buf_exception.exception_nr == Transport.SQL_SUCCESS:
            self.dialogue_id, buf_view = Convert.get_int(buf_view, little=True)
            self.data_source, buf_view = Convert.get_string(buf_view, little=True)
            self.user_sid, buf_view = Convert.get_string(buf_view, little=True, byteoffset=True)
            self.versionlist = VERSION_LIST_def()
            buf_view = self.versionlist.extract_from_bytearray(buf_view)
            __, buf_view = Convert.get_int(buf_view, little=True)  # old iso mapping
            self.isoMapping = 15 #utf-8
            self.server_host_name, buf_view = Convert.get_string(buf_view, little=True)
            self.server_node_id, buf_view = Convert.get_int(buf_view, little=True)
            self.server_process_id, buf_view = Convert.get_int(buf_view, little=True)
            self.server_process_name, buf_view = Convert.get_string(buf_view, little=True)
            self.server_ip_address, buf_view = Convert.get_string(buf_view, little=True)
            self.server_port, buf_view = Convert.get_int(buf_view, little=True)

            if self.versionlist.list[0].buildId and CONNECTION.PASSWORD_SECURITY > 0:
                self.security_enabled = True
                self.timestamp, buf_view = Convert.get_timestamp(buf_view)
                self.cluster_name, buf_view = Convert.get_string(buf_view, little=True)
            else:
                self.security_enabled = False
        else:
            if self.buf_exception.exception_nr == STRUCTDEF.odbc_Dcs_GetObjRefHdl_ASParamError_exn_:
                PyLog.global_logger.set_error("odbc_Dcs_GetObjRefHdl_ASParamError_exn_")
                raise errors.DatabaseError("odbc_Dcs_GetObjRefHdl_ASParamError_exn_")
            if self.buf_exception.exception_nr == STRUCTDEF.odbc_Dcs_GetObjRefHdl_ASTimeout_exn_:
                PyLog.global_logger.set_error("odbc_Dcs_GetObjRefHdl_ASTimeout_exn_")
                raise errors.DatabaseError("odbc_Dcs_GetObjRefHdl_ASTimeout_exn_")
            if self.buf_exception.exception_nr == STRUCTDEF.odbc_Dcs_GetObjRefHdl_ASNoSrvrHdl_exn_:
                PyLog.global_logger.set_error("odbc_Dcs_GetObjRefHdl_ASNoSrvrHdl_exn_")
                raise errors.DatabaseError("odbc_Dcs_GetObjRefHdl_ASNoSrvrHdl_exn_")
            if self.buf_exception.exception_nr == STRUCTDEF.odbc_Dcs_GetObjRefHdl_ASTryAgain_exn_:
                PyLog.global_logger.set_error("odbc_Dcs_GetObjRefHdl_ASTryAgain_exn_")
                raise errors.DatabaseError("odbc_Dcs_GetObjRefHdl_ASTryAgain_exn_")
            if self.buf_exception.exception_nr == STRUCTDEF.odbc_Dcs_GetObjRefHdl_ASNotAvailable_exn_:
                PyLog.global_logger.set_error("odbc_Dcs_GetObjRefHdl_ASNotAvailable_exn_")
                raise errors.DatabaseError("odbc_Dcs_GetObjRefHdl_ASNotAvailable_exn_")
            if self.buf_exception.exception_nr == STRUCTDEF.odbc_Dcs_GetObjRefHdl_DSNotAvailable_exn_:
                PyLog.global_logger.set_error("odbc_Dcs_GetObjRefHdl_DSNotAvailable_exn_")
                raise errors.DatabaseError("odbc_Dcs_GetObjRefHdl_DSNotAvailable_exn_")
            if self.buf_exception.exception_nr == STRUCTDEF.odbc_Dcs_GetObjRefHdl_PortNotAvailable_exn_:
                PyLog.global_logger.set_error("odbc_Dcs_GetObjRefHdl_PortNotAvailable_exn_")
                raise errors.DatabaseError("odbc_Dcs_GetObjRefHdl_PortNotAvailable_exn_")
            if self.buf_exception.exception_nr == STRUCTDEF.odbc_Dcs_GetObjRefHdl_InvalidUser_exn_:
                PyLog.global_logger.set_error("odbc_Dcs_GetObjRefHdl_ASParamError_exn_")
                raise errors.DatabaseError("odbc_Dcs_GetObjRefHdl_ASParamError_exn_")
            if self.buf_exception.exception_nr == STRUCTDEF.odbc_Dcs_GetObjRefHdl_LogonUserFailure_exn_:
                PyLog.global_logger.set_error("odbc_Dcs_GetObjRefHdl_LogonUserFailure_exn_")
                raise errors.DatabaseError("odbc_Dcs_GetObjRefHdl_LogonUserFailure_exn_")
            if self.buf_exception.exception_nr == STRUCTDEF.odbc_Dcs_GetObjRefHdl_TenantName_exn_:
                PyLog.global_logger.set_error("wrong tenant name")
                raise errors.DatabaseError("wrong tenant name")


class OutConnectionContextDef:

    def __init__(self):
        self.versionlist = VERSION_LIST_def()
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

    def extract_from_bytearray(self, buf_view):

        self.versionlist = VERSION_LIST_def()
        buf_view = self.versionlist.extract_from_bytearray(buf_view)

        self.node_id, buf_view = Convert.get_short(buf_view, little=True)
        self.process_id, buf_view = Convert.get_int(buf_view, little=True)
        self.computer_name, buf_view = Convert.get_string(buf_view, little=True)
        self.catalog, buf_view = Convert.get_string(buf_view, little=True)
        self.schema, buf_view = Convert.get_string(buf_view, little=True)
        self.option_flags1, buf_view = Convert.get_int(buf_view, little=True)
        self.option_flags2, buf_view = Convert.get_int(buf_view, little=True)
        self.enforce_iso = (self.option_flags1 and STRUCTDEF.OUTCONTEXT_OPT1_ENFORCE_ISO88591) > 0
        self.ignore_cancel = (self.option_flags1 and STRUCTDEF.OUTCONTEXT_OPT1_IGNORE_SQLCANCEL) > 0

        if self.option_flags1 & STRUCTDEF.OUTCONTEXT_OPT1_DOWNLOAD_CERTIFICATE > 0:
            self.certificate, buf_view = Convert.get_string(buf_view, little=True)
        elif self.option_flags1 & STRUCTDEF.OUTCONTEXT_OPT1_EXTRA_OPTIONS > 0:
            try:
                buf, buf_view = Convert.get_string(buf_view, little=True)
                self.decode_extra_options(buf)
            except:
                pass

        return buf_view

    def decode_extra_options(self, options):
        opts = options.split("")
        for x in opts:
            token, value = x.split("=")
            if token == "RN":
                self.role_name = value


class InitializeDialogueReply:

    def __init__(self):
        self.exception_nr = 0
        self.exception_detail = 0
        self.param_error = ''
        self.SQLError = ErrorDescListDef()
        self.InvalidUser = ErrorDescListDef()
        self.client_error_text = ''

        self.out_context = OutConnectionContextDef()

    def init_reply(self, buf_view, conn):
        self.exception_nr, buf_view = Convert.get_int(buf_view, little=True)
        self.exception_detail, buf_view = Convert.get_int(buf_view, little=True)

        if self.exception_nr == Transport.CEE_SUCCESS:
            buf_view = self.out_context.extract_from_bytearray(buf_view)

        elif self.exception_nr == STRUCTDEF.odbc_SQLSvc_InitializeDialogue_SQLError_exn_:
            buf_view = self.SQLError.extract_from_bytearray(buf_view)
            if self.exception_detail == STRUCTDEF.SQL_PASSWORD_EXPIRING \
                or self.exception_detail == STRUCTDEF.SQL_PASSWORD_GRACEPERIOD:
                self.out_context.extract_from_bytearray(buf_view)

            PyLog.global_logger.set_error(self.SQLError.get_error_info())
            raise errors.DatabaseError(self.SQLError.get_error_info())

        elif self.exception_nr == STRUCTDEF.odbc_SQLSvc_InitializeDialogue_InvalidUser_exn_:
            buf_view = self.SQLError.extract_from_bytearray(buf_view)
            buf_view = self.out_context.extract_from_bytearray(buf_view)

        elif self.exception_nr == STRUCTDEF.odbc_SQLSvc_InitializeDialogue_ParamError_exn_:
            self.param_error, buf_view = Convert.get_string(buf_view, little=True)
            PyLog.global_logger.set_error(self.param_error)
            raise errors.ProgrammingError(self.param_error)

        elif self.exception_nr == STRUCTDEF.odbc_SQLSvc_InitializeDialogue_InvalidConnection_exn_:
            PyLog.global_logger.set_error("invalid connection")
            raise errors.InternalError("invalid connection")

        else:
            PyLog.global_logger.set_error("unknow error")
            self.client_error_text = "unknow error"


class ErrorDescListDef:

    def __init__(self):
        self.length = 0
        self.list = []

    def extract_from_bytearray(self, buf_view):

        length, buf_view = Convert.get_int(buf_view, little=True)

        for i in range(length):
            error_desc = ErrorDescDef()
            buf_view = error_desc.extract_from_bytearray(buf_view)
            self.list.append(error_desc)

        return buf_view

    def get_error_info(self):
        return '\n'.join([error_desc.errorText for error_desc in self.list])


class ErrorDescDef:

    def __init__(self):
        self.rowId = 0
        self.errorDiagnosticId = 0
        self.sqlcode = 0
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

    def extract_from_bytearray(self, buf_view):
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


class SQLDataValueDef:

    def __init__(self):
        self.buffer = None
        self.user_buffer = None

    def sizeof(self):
        return Transport.size_int if self.buffer is None else Transport.size_int + len(self.buffer) + 1

    def insert_into_bytearray(self, buf_view, little=False):
        try:
            if self.buffer is not None:
                if isinstance(self.buffer, str):
                    buf_view = Convert.put_string(self.buffer, buf_view, little)  # string
                else:
                    buf_view = Convert.put_bytes(self.buffer, buf_view, little, is_data=True)
            else:
                buf_view = Convert.put_int(0, buf_view, little)
        except:
            PyLog.global_logger.set_error("Convert buffer error")
            raise errors.InternalError("Convert buffer error")

        return buf_view

    def set_buffer(self, buffer:str):
        self.buffer = buffer

    def set_user_buffer(self, buffer:str):
        self.user_buffer = buffer

    def extract_from_bytearray(self, buf_view: memoryview)->memoryview:
        self.buffer, buf_view = Convert.get_string(buf_view, little=True)
        return buf_view

    @classmethod
    def fill_in_sql_values(cls, describer, param_rowcount, param_values, is_executemany=False):
        data_value = SQLDataValueDef()

        #TODO handle param_values
        if param_rowcount == 0 and param_values is not None and len(param_values) > 0:
            param_rowcount = 1  # fake a single row if we are doing inputParams
        # for an SPJ

        param_count = 0
        if param_values is not None:
            param_count = len(param_values[0]) if is_executemany else len(param_values)

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
                    row_value = param_values[row] if is_executemany else param_values
                    for col in range(param_count):
                        _ = cls.convert_object_to_sql(describer.input_desc_list, param_rowcount, col,
                                                      row_value[col], row, buf_view)

        data_value.length = row_len * param_rowcount

        return data_value

    @classmethod
    def convert_object_to_sql(cls, input_desc_list, param_rowcount, param_count, param_values, row_num,
                              buf_view: memoryview):

        desc = input_desc_list[param_count]

        precision = desc.odbc_precision
        dataType = desc.data_type
        # setup the offsets
        no_nullvalue = desc.no_null_value
        null_value = desc.null_value
        data_length = desc.max_len

        short_length = False

        if dataType == FIELD_TYPE.SQLTYPECODE_VARCHAR_WITH_LENGTH:
            short_length = precision < 2**15
            data_offset = 2 if short_length else 4
            data_length += data_offset

            if data_length % 2 != 0:
                data_length = data_length + 1

        elif dataType == FIELD_TYPE.SQLTYPECODE_BLOB or dataType == FIELD_TYPE.SQLTYPECODE_CLOB:
            short_length = False
            data_offset = 4
            data_length += data_offset

            if data_length % 2 != 0:
                data_length = data_length + 1

        if null_value != -1:
            null_value = (null_value * param_rowcount) + (row_num * 2)

        no_nullvalue = (no_nullvalue * param_rowcount) + (row_num * data_length)
        if param_values is None :
            if null_value == -1:
                PyLog.global_logger.set_error("null_parameter_for_not_null_column")
                raise errors.DataError("null_parameter_for_not_null_column")
            # values[null_value] = -1
            _ = Convert.put_short(-1, buf_view[null_value:], True)
            return buf_view

        py_to_sql_convert_dict[dataType](buf_view, no_nullvalue, param_values, desc, param_count, short_length)



class SQLValueDef:

    def __init__(self):
        self.data_type = 0                      #  int
        self.data_ind = 0                       #  short
        self.data_value = SQLDataValueDef()
        self.data_charset = 0                   #  int

    def sizeof(self):
        return Transport.size_int * 2 + Transport.size_short + self.data_value.sizeof()

    def insert_into_bytearray(self, buf_view, little=True):
        buf_view = Convert.put_int(self.data_type, buf_view, little=little)
        buf_view = Convert.put_short(self.data_ind, buf_view, little=little)
        buf_view = self.data_value.insert_into_bytearray(buf_view, little=little)
        buf_view = Convert.put_int(self.data_charset, buf_view, little=little)

        return buf_view

    def extract_from_bytearray(self, buf_view:memoryview)->memoryview:
        self.data_type, buf_view = Convert.get_int(buf_view, little=True)
        self.data_ind, buf_view = Convert.get_short(buf_view, little=True)
        buf_view = self.data_value.extract_from_bytearray(buf_view)
        self.data_charset, buf_view = Convert.get_int(buf_view, little=True)
        return buf_view


class SQLValueListDef:

    def __init__(self):
        self.value_list = []

    def sizeof(self):
        size = Transport.size_int
        if (len(self.value_list)) is not 0:
            for x in self.value_list:
                size += x.sizeof()

        return size

    def insert_into_bytearray(self, buf_view, little=True):
        count = len(self.value_list)
        if count is not 0:
            Convert.put_int(count, buf_view,little)
            for x in self.value_list:
                x.insert_into_bytearray(buf_view)
        else:
            Convert.put_int(0, buf_view, little)
        return buf_view

    def extract_from_bytearray(self, buf_view:memoryview)->memoryview:
        count, buf_view = Convert.get_int(buf_view, little=True)

        for x in range(count):
            temp_sql_value = SQLValueDef()
            buf_view = temp_sql_value.extract_from_bytearray(buf_view)
            self.value_list.append(buf_view)

        return buf_view


class ExecuteReply:
    def __init__(self):
        self.return_code = 0
        self.total_error_length = 0
        self.output_desc_length = 0   #column length
        self.rows_affected = 0
        self.query_type = 0
        self.estimated_cost = 0
        self.out_values = None
        self.num_resultsets = 0
        self.output_desc_list = []
        self.stmt_labels_list = []
        self.proxy_syntax_list = []
        self._out_values_used = False
        self.errorlist = []

    def init_reply(self, buf_view):
        self.return_code, buf_view = Convert.get_int(buf_view, little=True)
        self.total_error_length, buf_view = Convert.get_int(buf_view, little=True)
        if self.total_error_length > 0:
            error_count, buf_view = Convert.get_int(buf_view, little=True)

            for x in range(error_count):
                t = SQLWarningOrError()
                buf_view = t.extract_from_bytearray(buf_view)
                self.errorlist.append(t)
            error_info = ''
            for item in self.errorlist:
                error_info += item.text + '\n'

            if self.return_code != 0:
                PyLog.global_logger.set_error(error_info)
                raise errors.ProgrammingError(error_info, msg_list=self.errorlist)
            else:
                PyLog.global_logger.set_warn(error_info)

        self.output_desc_length, buf_view = Convert.get_int(buf_view, little=True)
        if self.output_desc_length > 0:

            output_param_length, buf_view = Convert.get_int(buf_view, little=True)
            output_number_params, buf_view = Convert.get_int(buf_view, little=True)
            for x in range(output_number_params):
                t = Descriptor()
                buf_view = t.extract_from_bytearray(buf_view)
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
                _, buf_view = Convert.get_int(buf_view, little=True)  # stmt handle
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
                        t.extract_from_bytearray(buf_view)
                        t.set_row_length(output_param_length)
                        temp_descriptor_list.append(t)

                self.output_desc_list.append(temp_descriptor_list)
                proxy_syntax, buf_view = Convert.get_string(buf_view, little=True)
                self.proxy_syntax_list.append(proxy_syntax)

        single_syntax, buf_view = Convert.get_string(buf_view, little=True)

        if not self.proxy_syntax_list:
            self.proxy_syntax_list.append(single_syntax)

    # When sql has condition and the key is primary key, it will return values directly by execute
    def has_outvalues(self):
        if self.out_values:
            return True
        else:
            return False

    def get_outvalues(self):
        return self.out_values

    # called after outvalues used
    def clear_outvalues(self):
        self.out_values = None
        self._set_out_values_used()

    def _set_out_values_used(self):
        self._out_values_used = True

    def is_out_values_used(self):
        return self._out_values_used


class SQLWarningOrError:

    def __init__(self,row_id=None, sql_code=None, text=None, sql_state=None):
        self.row_id = row_id
        self.sql_code = sql_code
        self.text = text
        self.sql_state = sql_state

    def extract_from_bytearray(self, buf_view: memoryview) -> memoryview:
        self.row_id, buf_view = Convert.get_int(buf_view, little=True)
        self.sql_code, buf_view = Convert.get_int(buf_view, little=True)
        self.text, buf_view = Convert.get_string(buf_view, little=True)
        self.sql_state, buf_view = Convert.get_bytes(buf_view, 5)
        _, buf_view = Convert.get_char(buf_view)
        return buf_view


class Descriptor:

    def __init__(self):
        self.no_null_value = 0
        self.null_value = 0
        self.version = 0
        self.data_type = 0   # sql_data_type
        self.datetime_code = 0
        self.max_len = 0    # sqlOctetLength_
        self.precision = 0
        self.scale = 0
        self.null_info = 0
        self.signed = 0
        self.odbc_data_type = 0  # odbc_data_type
        self.odbc_precision = 0
        self.sql_charset = 0
        self.odbc_charset = 0
        self.col_heading_name = None
        self.table_name = None
        self.catalog_name = None
        self.schema_name = None
        self.heading_name = None
        self.int_lead_prec = 0
        self.param_mode = 0
        self.row_length = 0

    def set_row_length(self, num):
        self.row_length = num

    def extract_from_bytearray(self, buf_view: memoryview) -> memoryview:
        self.no_null_value, buf_view = Convert.get_int(buf_view, little=True)
        self.null_value, buf_view = Convert.get_int(buf_view, little=True)
        self.version, buf_view = Convert.get_int(buf_view, little=True)
        self.data_type, buf_view = Convert.get_int(buf_view, little=True)
        self.datetime_code, buf_view = Convert.get_int(buf_view, little=True)
        self.max_len, buf_view = Convert.get_int(buf_view, little=True)
        self.precision, buf_view = Convert.get_int(buf_view, little=True)
        self.scale, buf_view = Convert.get_int(buf_view, little=True)
        self.null_info, buf_view = Convert.get_int(buf_view, little=True)
        self.signed, buf_view = Convert.get_int(buf_view, little=True)
        self.odbc_data_type, buf_view = Convert.get_int(buf_view, little=True)
        self.odbc_precision, buf_view = Convert.get_int(buf_view, little=True)
        self.sql_charset, buf_view = Convert.get_int(buf_view, little=True)
        self.odbc_charset, buf_view = Convert.get_int(buf_view, little=True)
        self.col_heading_name, buf_view = Convert.get_string(buf_view, little=True)
        self.table_name, buf_view = Convert.get_string(buf_view, little=True)
        self.catalog_name, buf_view = Convert.get_string(buf_view, little=True)
        self.schema_name, buf_view = Convert.get_string(buf_view, little=True)
        self.heading_name, buf_view = Convert.get_string(buf_view, little=True)
        self.int_lead_prec, buf_view = Convert.get_int(buf_view, little=True)
        self.param_mode, buf_view = Convert.get_int(buf_view, little=True)

        return buf_view


class FetchReply:

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
                    buf_view = t.extract_from_bytearray(buf_view)
                    self.errorlist.append(t)

                error_info = '\n'.join([item.text for item in self.errorlist])
                PyLog.global_logger.set_warn(error_info)
                raise errors.ProgrammingError(error_info, msg_list=self.errorlist)

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

    def init_from_values(self, execute_desc, values, max_row):
        self.out_values = values
        self.rows_affected = execute_desc.rows_affected
        self._set_out_puts(execute_desc)

        self.end_of_data = False

        # TODO if rows_affected < max_row
        # then we should set a flag to prevent another fetch to get
        # NO_DATA_FOUND
        #
        #if self.rows_affected < max_row:
        #    self.end_of_data = True
        #else:
        #    self.end_of_data = False

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
                nonull_value_offset = out_desc_list[column_x].no_null_value
                null_value_offset = out_desc_list[column_x].null_value

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
                    if column_value is None:
                        PyLog.global_logger.set_warn("column value is null")
                        raise errors.InternalError("column value is null")

                column_result_list.append(column_value)

            self.result_set.append(tuple(column_result_list))

        self._clear_and_set()

    def _clear_and_set(self):

        # save result set and clear out_values
        self.out_values = None
        self.rows_fetched = len(self.result_set)

    def _get_execute_to_fetch_string(self, nonull_value_offset, column_desc: Descriptor):
        ret_obj = None
        buf_view = memoryview(self.out_values)
        sql_data_type = column_desc.data_type
        return sql_to_py_convert_dict[sql_data_type](buf_view, column_desc, nonull_value_offset)


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
        self.rows_affected = 0

    def init_reply(self, buf_view: memoryview):
        self.return_code, buf_view = Convert.get_int(buf_view, little=True)

        if self.return_code == Transport.SQL_SUCCESS or self.return_code == Transport.SQL_SUCCESS_WITH_INFO:
            if self.return_code == Transport.SQL_SUCCESS_WITH_INFO:
                self.total_error_length, buf_view = Convert.get_int(buf_view, little=True)
                if self.total_error_length > 0:
                    error_count, buf_view = Convert.get_int(buf_view, little=True)
                    for x in error_count:
                        t = SQLWarningOrError()
                        buf_view = t.extract_from_bytearray(buf_view)
                        self.errorlist.append(t)
                    error_info = '\n'.join([item.text for item in self.errorlist])
                    PyLog.global_logger.set_warn(error_info)
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
                    buf_view = t.extract_from_bytearray(buf_view)
                    t.set_row_length(input_param_length)
                    self.input_desc_list.append(t)

            self.output_desc_length, buf_view = Convert.get_int(buf_view, little=True)
            if self.output_desc_length > 0:
                output_param_length, buf_view = Convert.get_int(buf_view, little=True)
                output_number_params, buf_view = Convert.get_int(buf_view, little=True)
                for x in range(output_number_params):
                    t = Descriptor()
                    buf_view = t.extract_from_bytearray(buf_view)
                    t.set_row_length(output_param_length)
                    self.output_desc_list.append(t)

        else:
            self.total_error_length, buf_view = Convert.get_int(buf_view, little=True)
            if self.total_error_length > 0:
                error_count, buf_view = Convert.get_int(buf_view, little=True)
                for x in range(error_count):
                    t = SQLWarningOrError()
                    buf_view = t.extract_from_bytearray(buf_view)
                    self.errorlist.append(t)
                error_info = '\n'.join([item.text for item in self.errorlist])
                PyLog.global_logger.set_error(error_info)
                raise errors.ProgrammingError(error_info, msg_list=self.errorlist)


class TerminateReply:

    def __init__(self):
        self.return_code = 0
        self.exception_detail = 0
        self.SQLError = ErrorDescListDef()
        self.error_text = ''

    def init_reply(self, buf_view: memoryview):
        self.return_code, buf_view = Convert.get_int(buf_view, little=True)
        self.exception_detail, buf_view = Convert.get_int(buf_view, little=True)
        if self.return_code == Transport.SQL_SUCCESS:
            return True
        if self.return_code == STRUCTDEF.odbc_SQLSvc_TerminateDialogue_SQLError_exn_:
            if self.exception_detail == 25000:
                PyLog.global_logger.set_error("ids_25_000")
                raise errors.DatabaseError("ids_25_000")
            buf_view = self.SQLError.extract_from_bytearray(buf_view)
            PyLog.global_logger.set_error(self.SQLError.get_error_info())
            raise errors.DatabaseError(self.SQLError.get_error_info())
        if self.return_code == STRUCTDEF.odbc_SQLSvc_TerminateDialogue_ParamError_exn_:
            self.error_text, buf_view = Convert.get_string(buf_view)
            PyLog.global_logger.set_error(self.error_text)
            raise errors.DatabaseError(self.error_text)
        if self.return_code == STRUCTDEF.odbc_SQLSvc_TerminateDialogue_InvalidConnection_exn_:
            PyLog.global_logger.set_error("ids_08_s01")
            raise errors.DatabaseError("ids_08_s01")
        PyLog.global_logger.set_error("ids_unknown_reply_error")
        raise errors.DatabaseError("ids_unknown_reply_error")


class SetConnectionOptionReply:

    def __init__(self):
        self.return_code = 0
        self.exception_detail = 0
        self.SQLError = ErrorDescListDef()
        self.error_text = ''

    def init_reply(self, buf_view: memoryview):
        self.return_code, buf_view = Convert.get_int(buf_view, little=True)
        self.exception_detail, buf_view = Convert.get_int(buf_view, little=True)
        if self.return_code == Transport.SQL_SUCCESS:
            buf_view = self.SQLError.extract_from_bytearray(buf_view)
            return None
        if self.return_code == STRUCTDEF.odbc_SQLSvc_SetConnectionOption_SQLError_exn_:
            buf_view = self.SQLError.extract_from_bytearray(buf_view)
            PyLog.global_logger.set_error(self.SQLError.get_error_info())
            raise errors.DatabaseError(self.SQLError.get_error_info())
        if self.return_code == STRUCTDEF.odbc_SQLSvc_SetConnectionOption_ParamError_exn_:
            self.error_text, buf_view = Convert.get_string(buf_view)
            PyLog.global_logger.set_error(self.error_text)
            raise errors.DatabaseError(self.error_text)
        if self.return_code == STRUCTDEF.odbc_SQLSvc_SetConnectionOption_InvalidConnection_exn_:
            PyLog.global_logger.set_error("Invalid connection:" + "ids_program_error")
            raise errors.DatabaseError("Invalid connection:" + "ids_program_error")
        if self.return_code == STRUCTDEF.odbc_SQLSvc_SetConnectionOption_SQLInvalidHandle_exn_:
            PyLog.global_logger.set_error("autocommit_txn_in_progress")
            raise errors.DatabaseError("autocommit_txn_in_progress")


class EndTransactionReply:

    def __init__(self):
        self.return_code = 0
        self.exception_detail = 0
        self.SQLError = ErrorDescListDef()
        self.error_text = ''

    def init_reply(self, buf_view: memoryview):
        self.return_code, buf_view = Convert.get_int(buf_view, little=True)
        self.exception_detail, buf_view = Convert.get_int(buf_view, little=True)
        if self.return_code == Transport.SQL_SUCCESS:
            buf_view = self.SQLError.extract_from_bytearray(buf_view)
            return None
        if self.return_code == STRUCTDEF.odbc_SQLSvc_EndTransaction_SQLError_exn_:
            buf_view = self.SQLError.extract_from_bytearray(buf_view)
            PyLog.global_logger.set_error(self.SQLError.get_error_info())
            raise errors.DatabaseError(self.SQLError.get_error_info())
        if self.return_code == STRUCTDEF.odbc_SQLSvc_EndTransaction_ParamError_exn_:
            self.error_text, buf_view = Convert.get_string(buf_view)
            PyLog.global_logger.set_error(self.error_text)
            raise errors.DatabaseError(self.error_text)
        if self.return_code == STRUCTDEF.odbc_SQLSvc_EndTransaction_InvalidConnection_exn_:
            PyLog.global_logger.set_error("Invalid connection:" + "ids_transaction_error")
            raise errors.DatabaseError("Invalid connection:" + "ids_transaction_error")
        if self.return_code == STRUCTDEF.odbc_SQLSvc_EndTransaction_SQLInvalidHandle_exn_:
            PyLog.global_logger.set_error("autocommit_txn_in_progress")
            raise errors.DatabaseError("autocommit_txn_in_progress")

        PyLog.global_logger.set_error("ids_unknown_reply_error")
        raise errors.DatabaseError("ids_unknown_reply_error")
