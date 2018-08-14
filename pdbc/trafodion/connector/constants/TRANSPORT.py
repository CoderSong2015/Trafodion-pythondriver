class Transport:
    size_long = 8
    size_int = 4
    size_short = 2
    sql_identifier = 129

    max_int = 2147483647
    min_int = -2147483648
    max_uint = 4294967295
    min_uint = 0
    max_short = 32767
    min_short = -32768
    max_ushort = 65535
    min_ushort = 0
    max_tinyint = 127
    min_tinyint = -128
    max_utinyint = 255
    min_utinyint = 0
    max_long = 9223372036854775807
    min_long = -9223372036854775808
    max_ulong = 18446744073709551616
    min_ulong = 0
    max_float = 3.40282347e+38
    min_float = -3.40282347e+38
    max_double = 1.7976931348623157e+308
    min_double = -1.7976931348623157e+308

    # password types
    UNAUTHENTICATED_USER_TYPE = 2  # regular password
    PASSWORD_ENCRYPTED_USER_TYPE = 3  # used for SAS

    #
    # IO_BUFFER_LENGTH is used to determin the
    # size of physical buffers as they are
    # created and used by the transport layer
    # for doing ernal buffering as well
    # as I/O.
    #
    # The static value here corresponds to the
    # expected buffer size by the ODBC server.
    # It's a fixed value here, but could
    # easly be changed to a System properties
    # value, or gotten programmatically from
    # the ODBC server.
    #

    IO_BUFFER_LENGTH = 4096
    IO_BUFFER_BLOCKS = 8

    # ============== ERRORS ====================
    COMM_LINK_FAIL_EXCEPTION = 98
    TIMEOUT_EXCEPTION = 99
    WRONG_SIGNATURE = 100
    MEMORY_ALLOC_FAILED = 101
    DRVR_ERR_INCORRECT_LENGTH = 102
    DRVR_ERR_ERROR_FROM_SERVER = 103
    UNKNOWN_HEADER_TYPE = 104
    UNKNOWN_API = 0
    AS_API_START = 1000
    CFG_API_START = 2000
    SRVR_API_START = 3000
    ALL_API_MXCS_END = 9999 # end of all APIs that MXCS
# understands.
    AS_API_INIT = AS_API_START
    AS_API_GETOBJREF_OLD = AS_API_INIT + 1 # OK
    # NSKDRVR/CFGDRVR
    AS_API_REGPROCESS = AS_API_GETOBJREF_OLD + 1 # OK
    # NSKSRVR/CFGSRVR
    AS_API_UPDATESRVRSTATE = AS_API_REGPROCESS + 1 # OK
    # NSKSRVR
    AS_API_WOULDLIKETOLIVE = AS_API_UPDATESRVRSTATE + 1 # OK
    # NSKSRVR
    AS_API_STARTAS = AS_API_WOULDLIKETOLIVE + 1 # OK
    # CFGDRVR
    AS_API_STOPAS = AS_API_STARTAS + 1 # OK CFGDRVR
    AS_API_STARTDS = AS_API_STOPAS + 1 # OK CFGDRVR
    AS_API_STOPDS = AS_API_STARTDS + 1 # OK CFGDRVR
    AS_API_STATUSAS = AS_API_STOPDS + 1 # OK CFGDRVR
    AS_API_STATUSDS = AS_API_STATUSAS + 1 # OK CFGDRVR
    AS_API_STATUSDSDETAIL = AS_API_STATUSDS + 1 # OK
    # CFGDRVR
    AS_API_STATUSSRVRALL = AS_API_STATUSDSDETAIL + 1 # OK
    # CFGDRVR
    AS_API_STOPSRVR = AS_API_STATUSSRVRALL + 1 # OK
    # CFGDRVR
    AS_API_STATUSDSALL = AS_API_STOPSRVR + 1 # OK
    # CFGDRVR
    AS_API_DATASOURCECONFIGCHANGED = AS_API_STATUSDSALL + 1 # OK
    # CFGSRVR
    AS_API_ENABLETRACE = AS_API_DATASOURCECONFIGCHANGED + 1 # OK
    # CFGDRVR
    AS_API_DISABLETRACE = AS_API_ENABLETRACE + 1 # OK
    # CFGDRVR
    AS_API_GETVERSIONAS = AS_API_DISABLETRACE + 1 # OK
    # CFGDRVR
    AS_API_GETOBJREF = AS_API_GETVERSIONAS + 1 # OK
    # NSKDRVR/CFGDRVR

    SRVR_API_INIT = SRVR_API_START
    SRVR_API_SQLCONNECT = SRVR_API_INIT + 1 # OK NSKDRVR
    SRVR_API_SQLDISCONNECT = SRVR_API_SQLCONNECT + 1 # OK
    # NSKDRVR
    SRVR_API_SQLSETCONNECTATTR = SRVR_API_SQLDISCONNECT + 1 # OK
    # NSKDRVR
    SRVR_API_SQLENDTRAN = SRVR_API_SQLSETCONNECTATTR + 1 # OK
    # NSKDRVR
    SRVR_API_SQLPREPARE = SRVR_API_SQLENDTRAN + 1 # OK
    # NSKDRVR
    SRVR_API_SQLPREPARE_ROWSET = SRVR_API_SQLPREPARE + 1 # OK
    # NSKDRVR
    SRVR_API_SQLEXECUTE_ROWSET = SRVR_API_SQLPREPARE_ROWSET + 1 # OK
    # NSKDRVR
    SRVR_API_SQLEXECDIRECT_ROWSET = SRVR_API_SQLEXECUTE_ROWSET + 1 # OK
    # NSKDRVR
    SRVR_API_SQLFETCH = SRVR_API_SQLEXECDIRECT_ROWSET + 1
    SRVR_API_SQLFETCH_ROWSET = SRVR_API_SQLFETCH + 1 # OK
    # NSKDRVR
    SRVR_API_SQLEXECUTE = SRVR_API_SQLFETCH_ROWSET + 1 # OK
    # NSKDRVR
    SRVR_API_SQLEXECDIRECT = SRVR_API_SQLEXECUTE + 1 # OK
    # NSKDRVR
    SRVR_API_SQLEXECUTECALL = SRVR_API_SQLEXECDIRECT + 1 # OK
    # NSKDRVR
    SRVR_API_SQLFETCH_PERF = SRVR_API_SQLEXECUTECALL + 1 # OK
    # NSKDRVR
    SRVR_API_SQLFREESTMT = SRVR_API_SQLFETCH_PERF + 1 # OK
    # NSKDRVR
    SRVR_API_GETCATALOGS = SRVR_API_SQLFREESTMT + 1 # OK
    # NSKDRVR
    SRVR_API_STOPSRVR = SRVR_API_GETCATALOGS + 1 # OK AS
    SRVR_API_ENABLETRACE = SRVR_API_STOPSRVR + 1 # OK AS
    SRVR_API_DISABLETRACE = SRVR_API_ENABLETRACE + 1 # OK
    # AS
    SRVR_API_ENABLE_SERVER_STATISTICS = SRVR_API_DISABLETRACE + 1 # OK
    # AS
    SRVR_API_DISABLE_SERVER_STATISTICS = SRVR_API_ENABLE_SERVER_STATISTICS + 1 # OK
    # AS
    SRVR_API_UPDATE_SERVER_CONTEXT = SRVR_API_DISABLE_SERVER_STATISTICS + 1 # OK
    # AS
    SRVR_API_MONITORCALL = SRVR_API_UPDATE_SERVER_CONTEXT + 1 # OK
    # PCDRIVER
    SRVR_API_SQLPREPARE2 = SRVR_API_MONITORCALL + 1 # OK
    # PCDRIVER
    SRVR_API_SQLEXECUTE2 = SRVR_API_SQLPREPARE2 + 1 # OK
    # PCDRIVER
    SRVR_API_SQLFETCH2 = SRVR_API_SQLEXECUTE2 + 1 # OK
    # PCDRIVER

    # extent API used to extract lob data
    SRVR_API_EXTRACTLOB = 3030
    SRVR_API_UPDATELOB = 3031

    SQL_ATTR_ROWSET_RECOVERY = 2000

    MAX_REQUEST = 300
    MAX_BUFFER_LENGTH = 32000
    MAX_PROCESS_NAME = 50
    MAX_OBJECT_REF = 129
    SIGNATURE = 12345 # 0x3039
    VERSION = 100

    FILE_SYSTEM = 70 # 'F'
    TCPIP = 84 # 'T'
    UNKNOWN_TRANSPORT = 78 # 'N'

    NSK = 78 # 'N'
    PC = 80 # 'P'

    SWAP_YES = 89 # 'Y'
    SWAP_NO = 78 # 'N'

    WRITE_REQUEST_FIRST = 1
    WRITE_REQUEST_NEXT = WRITE_REQUEST_FIRST + 1
    READ_RESPONSE_FIRST = WRITE_REQUEST_NEXT + 1
    READ_RESPONSE_NEXT = READ_RESPONSE_FIRST + 1
    CLEANUP = READ_RESPONSE_NEXT + 1
    SRVR_TRANSPORT_ERROR = CLEANUP + 1
    CLOSE_TCPIP_SESSION = SRVR_TRANSPORT_ERROR + 1

    # ================ SQL Statement type ====================

    TYPE_UNKNOWN = 0
    TYPE_SELECT = 0x0001
    TYPE_UPDATE = 0x0002
    TYPE_DELETE = 0x0004
    TYPE_INSERT = 0x0008
    TYPE_EXPLAIN = 0x0010
    TYPE_CREATE = 0x0020
    TYPE_GRANT = 0x0040
    TYPE_DROP = 0x0080
    TYPE_INSERT_PARAM = 0x0100
    TYPE_SELECT_CATALOG = 0x0200
    TYPE_SMD = 0x0400
    TYPE_CALL = 0x0800
    TYPE_STATS = 0x1000
    TYPE_CONFIG = 0x2000
    # qs_erface support
    TYPE_QS = 0x4000
    TYPE_QS_OPEN = 0x4001
    TYPE_QS_CLOSE = 0x4002
    TYPE_CMD = 0x03000
    TYPE_CMD_OPEN = 0x03001
    TYPE_CMD_CLOSE = 0x03002
    TYPE_BEGIN_TRANSACTION = 0x05001
    TYPE_END_TRANSACTION = 0x05002

    # ================ SQL Query type ====================
    #
    # These values are taken from "Performace Updates External Specification,
    # Database Software"
    # document Version 0.4 Created on May 10, 2005.
    #
    SQL_OTHER = -1
    SQL_UNKNOWN = 0
    SQL_SELECT_UNIQUE = 1
    SQL_SELECT_NON_UNIQUE = 2
    SQL_INSERT_UNIQUE = 3
    SQL_INSERT_NON_UNIQUE = 4
    SQL_UPDATE_UNIQUE = 5
    SQL_UPDATE_NON_UNIQUE = 6
    SQL_DELETE_UNIQUE = 7
    SQL_DELETE_NON_UNIQUE = 8
    SQL_CONTROL = 9
    SQL_SET_TRANSACTION = 10
    SQL_SET_CATALOG = 11
    SQL_SET_SCHEMA = 12

    # ================ Execute2 return values ====================

    NO_DATA_FOUND = 100

    # =========================== NCS versions ==========

    NCS_VERSION_3_3 = 3.3
    NCS_VERSION_3_4 = 3.4

    # From CEE class
    CEE_SUCCESS = 0
    # Added by SB 7/5/2005 for handling SUCCESS_WITH_INFO for Prepare2
    SQL_SUCCESS = 0 # ODBC Standard
    SQL_SUCCESS_WITH_INFO = 1 # ODBC Standard

    # From Global.h
    ESTIMATEDCOSTRGERRWARN = 2

    charset_to_value = {
        "ISO8859_1": 1,  # ISO
        "MS932": 10,  # SJIS
        "UTF-16BE": 11,  # UCS2
        "EUCJP": 12,  # EUCJP
        "MS950": 13,  # BIG5
        "GB18030": 14,  # GB18030
        "UTF-8": 15,  # UTF8
        "MS949": 16,  # MB_KSC5601
        "GB2312": 17,  # GB2312
    }

    value_to_charset = {
        1: "ISO8859_1",
        10: "MS932",
        11: "UTF-16LE",    # server needs little endian
        12: "EUCJP",
        13: "MS950",
        14: "GB18030",
        15: "UTF-8",
        16: "MS949",
        17: "GB2312",
    }

    @classmethod
    def size_bytes(cls, buf, fixForServer = None):
        return cls.size_int + len(buf) + 1 if (buf and (len(buf) > 0)) else cls.size_int + 1

    @classmethod
    def size_bytes_with_charset(cls, buf):
        return cls.size_int + len(buf) + 1 + cls.size_int if buf and len(buf) > 0 else cls.size_int

    # end class TRANSPORT
