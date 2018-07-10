import struct
from . import errors


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
    max_float = 3.40282e+038
    max_double = 1.79769e+308

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
        11: "UTF-16BE",
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


class convert:

    @classmethod
    def convert_buf(cls, values):
        pass

    @classmethod
    def int_to_byteshort(cls, num, little=False):
        if not little:
            return struct.pack('!h', num)
        else:
            return struct.pack('<h', num)

    @classmethod
    def int_to_byteushort(cls, num, little=False):
        if not little:
            return struct.pack('!H', num)
        else:
            return struct.pack('<H', num)

    @classmethod
    def float_to_bytefloat(cls, num, little=False):
        if not little:
            return struct.pack('!f', num)
        else:
            return struct.pack('<f', num)

    @classmethod
    def int_to_byteint(cls, num, little=False):
        if not little:
            return struct.pack('!i', num)
        else:
            return struct.pack('<i', num)

    @classmethod
    def int_to_byteuint(cls, num, little=False):
        if not little:
            return struct.pack('!I', num)
        else:
            return struct.pack('<I', num)

    @classmethod
    def int_to_bytelonglong(cls, num, little=False):
        if not little:
            return struct.pack('!q', num)
        else:
            return struct.pack('<q', num)

    @classmethod
    def int_to_byteulonglong(cls, num, little=False):
        if not little:
            return struct.pack('!Q', num)
        else:
            return struct.pack('<Q', num)

    @classmethod
    def char_to_bytechar(cls, char):
        return struct.pack('!c', char)

    @classmethod
    def put_data_memview(cls, mem, buf):
        """
        :param mem: memoryview
        :param buf: 
        :return: 
        """

        #TODO It should to make sure that the length of buf is long enough
        for index, byte in enumerate(buf):
                mem[index] = byte

    @classmethod
    def put_string(self, string, buf_view: memoryview, little=False, charset="utf-8"):
        if not isinstance(string, str):
            raise errors.InternalError("function needs input type is string")

        tmp_len = len(string) + 1  # server need to handle the '\0'
        buf_view = self.put_int(tmp_len, buf_view, little)  #
        if tmp_len is not 0:
            self.put_data_memview(buf_view, string.encode(charset))  # string
            buf_view = buf_view[tmp_len:]
        return buf_view

    @classmethod
    def put_bytes(cls, data, buf_view: memoryview, little=False, nolen=False, is_data=False):
        """
        :param data: 
        :param buf_view: Python memoryview
        :param little:
        :param nolen: if nolen, put bytes directly without length
        :return: buf_view in current position
        """

        if not nolen:
            # server need to handle the '\0', when used in data value , not '\0'
            tmp_len = len(data) + 1 if not is_data else len(data)
            buf_view = cls.put_int(tmp_len, buf_view, little)  #
            if tmp_len is not 0:
                cls.put_data_memview(buf_view, data)
                buf_view = buf_view[tmp_len:]
            return buf_view
        else:
            cls.put_data_memview(buf_view, data)
            buf_view = buf_view[len(data):]
            return buf_view

    @classmethod
    def put_short(cls, num, buf_view: memoryview, little=False):
        """
        :param num: short 
        :param buf_view: Python memoryview
        :return: buf_view in current position
        """
        if not isinstance(num, int):
            raise errors.InternalError("function needs input type is int")
        data = cls.int_to_byteshort(num, little)
        cls.put_data_memview(buf_view, data)
        return buf_view[2:]

    @classmethod
    def put_ushort(cls, num, buf_view: memoryview, little=False):
        """
        :param num: short 
        :param buf_view: Python memoryview
        :return: buf_view in current position
        """
        if not isinstance(num, int):
            raise errors.InternalError("function needs input type is int")
        data = cls.int_to_byteushort(num, little)
        cls.put_data_memview(buf_view, data)
        return buf_view[2:]

    @classmethod
    def put_float(cls, num, buf_view: memoryview, little=False):
        """
        :param num: float
        :param buf_view: Python memoryview
        :return: buf_view in current position
        """
        if not isinstance(num, float):
            raise errors.InternalError("function needs input type is float")

        data = cls.int_to_byteushort(num, little)
        cls.put_data_memview(buf_view, data)
        return buf_view[4:]

    @classmethod
    def put_int(self, num, buf_view: memoryview, little=False):
        if not isinstance(num, int):
            raise errors.InternalError("function needs input type is int")
        data = self.int_to_byteint(num, little)
        self.put_data_memview(buf_view, data)
        return buf_view[4:]

    @classmethod
    def put_uint(self, num, buf_view: memoryview, little=False):
        if not isinstance(num, int):
            raise errors.InternalError("function needs input type is int")
        data = self.int_to_byteuint(num, little)
        self.put_data_memview(buf_view, data)
        return buf_view[4:]

    @classmethod
    def put_longlong(self, num, buf_view: memoryview, little=False):
        if not isinstance(num, int):
            raise errors.InternalError("function needs input type is int")
        data = self.int_to_bytelonglong(num, little)
        self.put_data_memview(buf_view, data)
        return buf_view[8:]

    @classmethod
    def put_ulonglong(self, num, buf_view: memoryview, little=False):
        if not isinstance(num, int):
            raise errors.InternalError("function needs input type is int")
        data = self.int_to_bytelonglong(num, little)
        self.put_data_memview(buf_view, data)
        return buf_view[8:]

    @classmethod
    def put_char(self, char, buf_view: memoryview):

        #TODO need exception
        data = self.char_to_bytechar(char)
        self.put_data_memview(buf_view, data)
        return buf_view[1:]

    @staticmethod
    def get_short(buf_view: memoryview, little=False):
        if not little:
            return (struct.unpack('!h', buf_view[0:2])[0], buf_view[2:])
        else:
            return (struct.unpack('<h', buf_view[0:2])[0], buf_view[2:])

    @staticmethod
    def get_int(buf_view: memoryview, little=False):
        if not little:
            return (struct.unpack('!i', buf_view[0:4])[0], buf_view[4:])
        else:
            return (struct.unpack('<i', buf_view[0:4])[0], buf_view[4:])

    @classmethod
    def get_string(cls, buf_view: memoryview, little=False, byteoffset=False):
        length, buf_view = cls.get_int(buf_view, little=little)

        # In server there are two function:put_string and put_bytestring,
        # they are different in calculating length of string
        offset = 1 if not byteoffset else 0
        if length is not 0:
            to_bytes = buf_view[0:length - offset].tobytes()
            return (to_bytes.decode("utf-8"), buf_view[length + (1 - offset):])
        else:
            return ('', buf_view)

    @staticmethod
    def get_bytes(buf_view: memoryview, length=0, little=True):

        if length is not 0:
            to_bytes = buf_view[0:length].tobytes()
            return (to_bytes, buf_view[length:])
        else:
            length, buf_view = convert.get_int(buf_view, little=little)
            to_bytes = buf_view[0:length].tobytes()
            return (to_bytes, buf_view[length:])

    @classmethod
    def get_char(self, buf_view: memoryview):
        return (struct.unpack('!c', buf_view[0:1])[0], buf_view[1:])

    @classmethod
    def get_timestamp(self, buf_view: memoryview):
        time = buf_view[0:8].tobytes()
        return (time, buf_view[8:])

    @staticmethod
    def turple_to_bytes(tur:tuple)-> bytes:
        s = ''
        for x in tur:
            s = s + chr(x)
        return s.encode("utf-8")

