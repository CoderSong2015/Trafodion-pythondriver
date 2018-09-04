from .constants.TRANSPORT import Transport
from . import errors
from .constants import FIELD_TYPE
from decimal import Decimal
import datetime
import re
import struct


class Convert:

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
    def float_to_bytedouble(cls, num, little=False):
        if not little:
            return struct.pack('!d', num)
        else:
            return struct.pack('<d', num)

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
    def put_data_memview(cls, mem: memoryview, buf: bytes):
        """
        :param mem: memoryview
        :param buf: 
        :return: 
        """

        if len(buf) > 0:
            mem[0:len(buf)] = buf
        #TODO It should to make sure that the length of buf is long enough
        #for index, byte in enumerate(buf):
        #        mem[index] = byte

    @classmethod
    def put_string(cls, string, buf_view: memoryview, little=False, charset="utf-8"):
        if not isinstance(string, str):
            raise errors.InternalError("function needs input type is string")

        tmp_len = len(string) + 1  # server need to handle the '\0'
        buf_view = cls.put_int(tmp_len, buf_view, little)  #
        if tmp_len is not 0:
            cls.put_data_memview(buf_view, string.encode(charset))  # string
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

        data = cls.float_to_bytefloat(num, little)
        cls.put_data_memview(buf_view, data)
        return buf_view[4:]

    @classmethod
    def put_double(cls, num, buf_view: memoryview, little=False):
        """
        :param num: double
        :param buf_view: Python memoryview
        :return: buf_view in current position
        """
        if not isinstance(num, float):
            raise errors.InternalError("function needs input type is float")

        data = cls.float_to_bytedouble(num, little)
        cls.put_data_memview(buf_view, data)
        return buf_view[8:]

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

    @classmethod
    def put_numeric(cls, num, buf_view: memoryview, scale, max_len, precision, little=False):

        data_lits, sign = cls.convert_bigdecimal_to_sqlbignum(num, scale, max_len, precision)

        for x in data_lits:
            buf_view = cls.put_ushort(x, buf_view, little=True)

        return sign

    @staticmethod
    def get_short(buf_view: memoryview, little=False):
        if not little:
            return struct.unpack('!h', buf_view[0:2].tobytes())[0], buf_view[2:]
        else:
            return struct.unpack('<h', buf_view[0:2].tobytes())[0], buf_view[2:]

    @staticmethod
    def get_ushort(buf_view: memoryview, little=False):
        if not little:
            return struct.unpack('!H', buf_view[0:2].tobytes())[0], buf_view[2:]
        else:
            return struct.unpack('<H', buf_view[0:2].tobytes())[0], buf_view[2:]

    @staticmethod
    def get_int(buf_view: memoryview, little=False):
        if not little:
            return struct.unpack('!i', buf_view[0:4].tobytes())[0], buf_view[4:]
        else:
            return struct.unpack('<i', buf_view[0:4].tobytes())[0], buf_view[4:]

    @staticmethod
    def get_uint(buf_view: memoryview, little=False):
        if not little:
            return struct.unpack('!I', buf_view[0:4].tobytes())[0], buf_view[4:]
        else:
            return struct.unpack('<I', buf_view[0:4].tobytes())[0], buf_view[4:]

    @staticmethod
    def get_longlong(buf_view: memoryview, little=False):
        if not little:
            return struct.unpack('!q', buf_view[0:8].tobytes())[0], buf_view[8:]
        else:
            return struct.unpack('<q', buf_view[0:8].tobytes())[0], buf_view[8:]

    @staticmethod
    def get_ulonglong(buf_view: memoryview, little=False):
        if not little:
            return struct.unpack('!Q', buf_view[0:8].tobytes())[0], buf_view[8:]
        else:
            return struct.unpack('<Q', buf_view[0:8].tobytes())[0], buf_view[8:]

    @staticmethod
    def get_float(buf_view: memoryview, little=False):
        if not little:
            return struct.unpack('!f', buf_view[0:4].tobytes())[0], buf_view[4:]
        else:
            return struct.unpack('<f', buf_view[0:4].tobytes())[0], buf_view[4:]

    @staticmethod
    def get_double(buf_view: memoryview, little=False):
        if not little:
            return struct.unpack('!d', buf_view[0:8].tobytes())[0], buf_view[8:]
        else:
            return struct.unpack('<d', buf_view[0:8].tobytes())[0], buf_view[8:]

    @classmethod
    def get_string(cls, buf_view: memoryview, little=False, byteoffset=False):
        length, buf_view = cls.get_int(buf_view, little=little)

        # In server there are two function:put_string and put_bytestring,
        # they are different in calculating length of string
        offset = 1 if not byteoffset else 0
        if length is not 0:
            to_bytes = buf_view[0:length - offset].tobytes()
            return to_bytes.decode("utf-8"), buf_view[length + (1 - offset):]
        else:
            return '', buf_view

    @staticmethod
    def get_bytes(buf_view: memoryview, length=0, little=True):

        if length is not 0:
            to_bytes = buf_view[0:length].tobytes()
            return to_bytes, buf_view[length:]
        else:
            length, buf_view = Convert.get_int(buf_view, little=little)
            to_bytes = buf_view[0:length].tobytes()
            return to_bytes, buf_view[length:]

    @classmethod
    def get_char(cls, buf_view: memoryview, to_python_int=False, little=True):
        little_str = "little" if little else "big"

        if to_python_int:
            char = struct.unpack('!c', buf_view[0:1].tobytes())[0]
            return int.from_bytes(char, little_str), buf_view[1:]
        else:
            return struct.unpack('!c', buf_view[0:1].tobytes())[0], buf_view[1:]

    @classmethod
    def get_timestamp(cls, buf_view: memoryview):
        time = buf_view[0:8].tobytes()
        return time, buf_view[8:]

    @classmethod
    def get_numeric(cls, buf_view: memoryview, max_len, scale):
        numeric_bytes = buf_view[0:max_len].tobytes()
        data = cls.convert_sqlbignum_to_bigdecimal(numeric_bytes, scale)
        return data

    @staticmethod
    def turple_to_bytes(tur: tuple)-> bytes:
        s = ''
        for x in tur:
            s = s + chr(x)
        return s.encode("utf-8")

    @classmethod
    def convert_sqlbignum_to_bigdecimal(cls, numeric_bytes, scale, is_unsigned=False):
        result = 0
        data_shorts = []
        data_shorts_len = len(numeric_bytes) // 2
        buf_view = memoryview(numeric_bytes)
        for i in range(data_shorts_len):
            # copy data
            data, _= cls.get_ushort(buf_view[i*2:], little=True)
            data_shorts.append(data)
        negative = False
        if not is_unsigned:
            negative = (data_shorts[data_shorts_len - 1] & 0x8000) > 0
            data_shorts[data_shorts_len - 1] &= 0x0FFF  # force sign to 0, continue normally

        cur_pos = data_shorts_len - 1  # start at the end
        while cur_pos >= 0 and data_shorts[cur_pos] == 0:
            cur_pos -= 1
        remainder = 0
        digit = 0
        while cur_pos >= 0 or data_shorts[0] >= 10000:

            for j in range(cur_pos, -1, -1):

                temp = remainder & 0xFFFF
                temp = temp << 16
                temp += data_shorts[j]

                data_shorts[j] = temp // 10000
                remainder = temp % 10000
            #  if we are done with the current 16bits, move on
            if data_shorts[cur_pos] == 0:
                cur_pos -= 1
            # go through the remainder and add each digit to the final String

            result += remainder * 10**digit
            digit += 4
            remainder = 0
        remainder = data_shorts[0]
        result += remainder * 10 ** digit
        result = str(result)
        if scale > 0:
            result = result[:-scale] + '.' + result[len(result) - scale:]
        if negative:
            result = '-' + result
        return Decimal(result)

    @classmethod
    def convert_bigdecimal_to_sqlbignum(cls, numeric_bytes, scale, max_len, precision, is_unsigned=False):

        try:
            param_values = Decimal(numeric_bytes)
            sign = 1 if param_values.is_signed() else 0
            param_values = param_values.to_eng_string()
            if sign:
                param_values = param_values.lstrip('-')
        except:
            raise errors.DataError("decimal.ConversionSyntax")
        if scale > 0:
            pos_point = param_values.find('.')
            if pos_point == -1:
                param_values = ''.join((param_values, '0' * scale))
            else:
                remove_point = param_values.replace('.', '')
                if pos_point + scale > len(remove_point):
                    param_values = ''.join((remove_point, '0' * (pos_point + scale - len(remove_point))))
                else:
                    param_values = remove_point[:pos_point + scale]

        #keep precision in driver
        param_values = param_values[:precision]
        # iterate through 4 bytes at a time
        val_len = len(param_values)
        i = 0
        ar = []
        target_len = max_len // 2
        target_list = [0] * target_len
        tar_pos = 1
        while i < val_len:
            str_num = param_values[i: i + 4]
            power = len(str_num)
            num = int(str_num)
            i += 4

            temp = target_list[0] * 10 ** power + num
            target_list[0] = temp & 0xFFFF  # we save only up to 16bits -- the rest gets carried over

            # we do the same thing for the rest of the digits now that we have # an upper bound
            for x in range(1, target_len, 1):
                t = (temp & 0xFFFF0000) >> 16
                temp = target_list[x] * 10 ** power + t
                target_list[x] = temp & 0xFFFF

            carry = (temp & 0xFFFF0000) >> 16
            if carry > 0:
                target_list[tar_pos] = carry
                tar_pos += 1

        return target_list, sign


def get_sqltype_char(buf_view, column_desc, nonull_value_offset):
    charset = Transport.value_to_charset[column_desc.odbc_charset]
    length = column_desc.max_len
    ret_obj, _ = Convert.get_bytes(buf_view[nonull_value_offset:], length=length)
    ret_obj = ret_obj.decode(charset)
    ret_obj = ret_obj.rstrip('\x00').rstrip(' ')

    return ret_obj


def get_sqltype_varchar(buf_view, column_desc, nonull_value_offset):
    short_length = 2 if column_desc.precision < 2 ** 15 else 4
    data_offset = nonull_value_offset + short_length
    data_len = 0
    if short_length == 2:
        data_len, _ = Convert.get_short(buf_view[nonull_value_offset:], little=True)
    else:
        data_len, _ = Convert.get_int(buf_view[nonull_value_offset:], little=True)
    length_left = len(buf_view) - data_offset
    data_len = length_left if length_left < data_len else data_len
    ret_obj = buf_view[data_offset:data_offset + data_len].tobytes()
    charset = Transport.value_to_charset[column_desc.odbc_charset]
    ret_obj = ret_obj.decode(charset)
    return ret_obj


def get_sqltype_interval(buf_view, column_desc, nonull_value_offset):
    pass


def get_sqltype_tiny_unsigned(buf_view, column_desc, nonull_value_offset):
    ret_obj, _ = Convert.get_char(buf_view[nonull_value_offset:])
    return ret_obj


def get_sqltype_tiny(buf_view, column_desc, nonull_value_offset):
    ret_obj, _ = Convert.get_char(buf_view[nonull_value_offset:])
    return ret_obj


def get_sqltype_smallint(buf_view, column_desc, nonull_value_offset):
    ret_obj, _ = Convert.get_short(buf_view[nonull_value_offset:], little=True)
    return ret_obj


def get_sqltype_smallint_unsigned(buf_view, column_desc, nonull_value_offset):
    ret_obj, _ = Convert.get_ushort(buf_view[nonull_value_offset:], little=True)
    return ret_obj


def get_sqltype_int(buf_view, column_desc, nonull_value_offset):
    ret_obj, _ = Convert.get_int(buf_view[nonull_value_offset:], little=True)
    # TODO scale of big decimal

    if column_desc.scale > 0:
        ret_obj = ret_obj / 10**column_desc.scale
    return ret_obj


def get_sqltype_int_unsigned(buf_view, column_desc, nonull_value_offset):
    ret_obj, _ = Convert.get_uint(buf_view[nonull_value_offset:], little=True)
    # TODO scale of big decimal
    if column_desc.scale > 0:
        ret_obj = ret_obj / 10**column_desc.scale
    return ret_obj


def get_sqltype_largeint(buf_view, column_desc, nonull_value_offset):
    ret_obj, _ = Convert.get_longlong(buf_view[nonull_value_offset:], little=True)
    if column_desc.scale > 0:
        ret_obj = ret_obj / 10**column_desc.scale
    return ret_obj


def get_sqltype_largeint_unsigned(buf_view, column_desc, nonull_value_offset):
    ret_obj, _ = Convert.get_ulonglong(buf_view[nonull_value_offset:], little=True)
    return ret_obj


def get_sqltype_numeric(buf_view, column_desc, nonull_value_offset):
    ret_obj = Convert.get_numeric(buf_view[nonull_value_offset:], column_desc.max_len, column_desc.scale)
    return ret_obj


def get_sqltype_decimal(buf_view, column_desc, nonull_value_offset):
    first_byte, _ = Convert.get_bytes(buf_view[nonull_value_offset:], length=1, little=True)
    int_first_byte = int.from_bytes(first_byte, byteorder='little')
    if int_first_byte & 0x80:
        ret_obj, _ = Convert.get_bytes(buf_view[nonull_value_offset:], length=column_desc.max_len,
                                       little=True)
        ret_obj = '-' + (bytes([ret_obj[0] & 0x7F]) + ret_obj[1:]).decode()
        ret_obj = Decimal(ret_obj) / (10 ** column_desc.scale)
    else:
        ret_obj, _ = Convert.get_bytes(buf_view[nonull_value_offset:], length=column_desc.max_len, little=True)
        ret_obj = Decimal(ret_obj.decode()) / (10 ** column_desc.scale)
    return ret_obj


def get_sqltype_real(buf_view, column_desc, nonull_value_offset):
    ret_obj, _ = Convert.get_float(buf_view[nonull_value_offset:], little=True)
    return ret_obj


def get_sqltype_double(buf_view, column_desc, nonull_value_offset):
    ret_obj, _ = Convert.get_double(buf_view[nonull_value_offset:], little=True)
    return ret_obj


def get_sqltype_bit(buf_view, column_desc, nonull_value_offset):
    pass


def get_sqltype_datetime(buf_view, column_desc, nonull_value_offset):

    ret_obj = None
    if column_desc.datetime_code == FIELD_TYPE.SQLDTCODE_DATE:
        # "yyyy-mm-dd"
        year, _ = Convert.get_ushort(buf_view[nonull_value_offset:], little=True)
        month, _ = Convert.get_char(buf_view[nonull_value_offset + 2:], to_python_int=True)
        day, _ = Convert.get_char(buf_view[nonull_value_offset + 3:], to_python_int=True)
        ret_obj = datetime.date(year, month, day)
    if column_desc.datetime_code == FIELD_TYPE.SQLDTCODE_TIMESTAMP:
        # yyyy - mm - dd hh: mm:ss.fffffffff
        year, _ = Convert.get_ushort(buf_view[nonull_value_offset:], little=True)
        month, _ = Convert.get_char(buf_view[nonull_value_offset + 2:], to_python_int=True)
        day, _ = Convert.get_char(buf_view[nonull_value_offset + 3:], to_python_int=True)
        hour, _ = Convert.get_char(buf_view[nonull_value_offset + 4:], to_python_int=True)
        min, _ = Convert.get_char(buf_view[nonull_value_offset + 5:], to_python_int=True)
        sec, _ = Convert.get_char(buf_view[nonull_value_offset + 6:], to_python_int=True)

        nano_seconds = 0
        if column_desc.precision > 0:
            nano_seconds, _ = Convert.get_uint(buf_view[nonull_value_offset + 7:], little=True)
            if nano_seconds > 999999:  # returned in microseconds
                nano_seconds = 0

        nano_seconds = nano_seconds * 10 ** (6 - column_desc.precision)
        ret_obj = datetime.datetime(year, month, day, hour, min, sec, microsecond=nano_seconds)
    if column_desc.datetime_code == FIELD_TYPE.SQLDTCODE_TIME:
        # "hh:mm:ss"
        hour, _ = Convert.get_char(buf_view[nonull_value_offset:], to_python_int=True)
        minute, _ = Convert.get_char(buf_view[nonull_value_offset + 1:], to_python_int=True)
        second, _ = Convert.get_char(buf_view[nonull_value_offset + 2:], to_python_int=True)

        if column_desc.precision > 0:
            nano_seconds, _ = Convert.get_uint(buf_view[nonull_value_offset + 3:], little=True)
            if nano_seconds > 999999:  # returned in microseconds
                nano_seconds = 0

            digit_num = len(str(nano_seconds))
            # fractional seconds  to microsecond
            # nano_seconds = nano_seconds * (10 ** (column_desc.precision - digit_num)) * 10 ** (6 - digit_num)
            nano_seconds = nano_seconds * 10 ** (6 - column_desc.precision)
            ret_obj = datetime.time(hour=hour, minute=minute, second=second, microsecond=nano_seconds)
        else:
            ret_obj = datetime.time(hour=hour, minute=minute, second=second)

    return ret_obj


sql_to_py_convert_dict = {
    FIELD_TYPE.SQLTYPECODE_CHAR:             get_sqltype_char,
    FIELD_TYPE.SQLTYPECODE_VARCHAR:          get_sqltype_varchar,
    FIELD_TYPE.SQLTYPECODE_VARCHAR_WITH_LENGTH:get_sqltype_varchar,
    FIELD_TYPE.SQLTYPECODE_VARCHAR_LONG:     get_sqltype_varchar,
    FIELD_TYPE.SQLTYPECODE_BLOB :            get_sqltype_varchar,
    FIELD_TYPE.SQLTYPECODE_CLOB:             get_sqltype_varchar,
    FIELD_TYPE.SQLTYPECODE_INTERVAL:         get_sqltype_interval,
    FIELD_TYPE.SQLTYPECODE_TINYINT_UNSIGNED: get_sqltype_tiny_unsigned,
    FIELD_TYPE.SQLTYPECODE_TINYINT:          get_sqltype_tiny,
    FIELD_TYPE.SQLTYPECODE_SMALLINT:         get_sqltype_smallint,
    FIELD_TYPE.SQLTYPECODE_SMALLINT_UNSIGNED:get_sqltype_smallint_unsigned,
    FIELD_TYPE.SQLTYPECODE_INTEGER:          get_sqltype_int,
    FIELD_TYPE.SQLTYPECODE_INTEGER_UNSIGNED: get_sqltype_int_unsigned,
    FIELD_TYPE.SQLTYPECODE_LARGEINT:         get_sqltype_largeint,
    FIELD_TYPE.SQLTYPECODE_LARGEINT_UNSIGNED:get_sqltype_largeint_unsigned,
    FIELD_TYPE.SQLTYPECODE_NUMERIC:          get_sqltype_numeric,
    FIELD_TYPE.SQLTYPECODE_NUMERIC_UNSIGNED: get_sqltype_numeric,
    FIELD_TYPE.SQLTYPECODE_DECIMAL:          get_sqltype_decimal,
    FIELD_TYPE.SQLTYPECODE_DECIMAL_UNSIGNED: get_sqltype_decimal,
    FIELD_TYPE.SQLTYPECODE_DECIMAL_LARGE:    get_sqltype_decimal,
    FIELD_TYPE.SQLTYPECODE_DECIMAL_LARGE_UNSIGNED: get_sqltype_decimal,
    FIELD_TYPE.SQLTYPECODE_REAL:             get_sqltype_real,
    FIELD_TYPE.SQLTYPECODE_DOUBLE:           get_sqltype_double,
    FIELD_TYPE.SQLTYPECODE_FLOAT:            get_sqltype_double,
    FIELD_TYPE.SQLTYPECODE_BIT:              get_sqltype_bit,
    FIELD_TYPE.SQLTYPECODE_BITVAR:           get_sqltype_bit,
    FIELD_TYPE.SQLTYPECODE_BPINT_UNSIGNED:   get_sqltype_bit,
    FIELD_TYPE.SQLTYPECODE_DATETIME:         get_sqltype_datetime,
}


def put_sqltype_char(buf_view, no_null_value, param_values, desc, param_count, short_length):
    max_len = desc.max_len
    sql_charset = desc.sql_charset
    target_charset = "utf-8"
    character_len = 0
    if param_values is None:
        # Note for future optimization. We can probably remove the next line,
        # because the array is already initialized to 0.
        _ = Convert.put_short(0, buf_view[no_null_value:], True)
        return buf_view
    elif isinstance(param_values, (bytes, str)):

        try:
            if sql_charset == Transport.charset_to_value["ISO8859_1"]:
                target_charset = "UTF-8"
            elif sql_charset == Transport.charset_to_value["UTF-16BE"]:  # default is little endian
                target_charset = "UTF-16LE"
            else:
                target_charset = Transport.value_to_charset[sql_charset]
                character_len = len(param_values)
            if isinstance(param_values, bytes):
                param_values = param_values.decode("utf-8").encode(target_charset)
            else:
                param_values = param_values.encode(target_charset)
        except:
            raise errors.NotSupportedError("unsupported charset: {0}".format(target_charset))
    else:
        raise errors.DataError(
            "invalid_parameter_value, data should be either bytes or String for column number {0}".format(
                param_count))

    # We now have a byte array containing the parameter
    data_len = len(param_values)
    # if column is utf-8, length is the number of character while other charset is the number of bytes
    # max len will be length * 4 if charset is utf-8

    max_char_len = max_len
    if sql_charset != Transport.charset_to_value["UTF-8"]:
        character_len = data_len
    else:
        max_char_len = max_len // 4
    if max_char_len >= character_len:
        _ = Convert.put_bytes(param_values, buf_view[no_null_value:], True, nolen=True)
        # Blank pad for rest of the buffer
        if max_len > data_len:
            if sql_charset == Transport.charset_to_value["UTF-16BE"]:
                # pad with Unicode spaces (0x00 0x20)
                i2 = data_len
                while i2 < max_len:
                    _ = Convert.put_bytes(' '.encode("UTF-16BE"), buf_view[no_null_value + i2:], little=True,
                                          nolen=True)
                    _ = Convert.put_bytes(' '.encode("UTF-16BE"), buf_view[no_null_value + i2 + 1:], little=True,
                                          nolen=True)
                    i2 = i2 + 2

            else:
                b = bytearray()
                if sql_charset != Transport.charset_to_value["UTF-8"]:
                    for x in range(max_len - data_len):
                        b.append(0x20)
                else:
                    for x in range(max_len - data_len):
                        b.append(0x20)
                _ = Convert.put_bytes(b, buf_view[no_null_value + data_len:], little=True, nolen=True)
    else:
        raise errors.ProgrammingError(
            "invalid_string_parameter CHAR input data is longer than the length for column number {0}".format(
                param_count))

    return None


def put_sqltype_varchar(buf_view, no_null_value, param_values, desc, param_count, short_length):
    max_len = desc.max_len
    sql_charset = desc.sql_charset
    if param_values is None:
        # Note for future optimization. We can probably remove the next line,
        # because the array is already initialized to 0.
        _ = Convert.put_short(0, buf_view[no_null_value:], True)
        return None
    elif isinstance(param_values, (bytes, str)):
        target_charset = ""

        try:
            if sql_charset == Transport.charset_to_value["ISO8859_1"]:
                target_charset = "UTF-8"
            elif sql_charset == Transport.charset_to_value["UTF-16BE"]:  # default is little endian
                target_charset = "UTF-16LE"
            else:
                target_charset = Transport.value_to_charset[sql_charset]
            if isinstance(param_values, bytes):
                param_values = param_values.decode("utf-8").encode(target_charset)
            else:
                param_values = param_values.encode(target_charset)
        except:
            raise errors.NotSupportedError("unsupported charset: {0}".format(target_charset))
    else:
        raise errors.DataError(
            "invalid_parameter_value, data should be either bytes or String for column number: {0}".format(
                param_count))

    data_len = len(param_values)
    if max_len >= data_len:
        _ = Convert.put_short(data_len, buf_view[no_null_value:], little=True)
        _ = Convert.put_bytes(param_values, buf_view[no_null_value + 2:], nolen=True)
    else:
        raise errors.DataError(
            "invalid_string_parameter input data is longer than the length for column number: {0}".format(
                param_count))
    return None


def put_sqltype_varchar_with_length(buf_view, no_null_value, param_values, desc, param_count, short_length):
    max_len = desc.max_len
    sql_charset = desc.sql_charset
    character_len = 0
    if param_values is None:
        # Note for future optimization. We can probably remove the next line,
        # because the array is already initialized to 0.
        _ = Convert.put_short(0, buf_view[no_null_value:], True)
        return buf_view
    elif isinstance(param_values, (bytes, str)):
        target_charset = "utf-8"

        try:
            if sql_charset == Transport.charset_to_value["ISO8859_1"]:
                target_charset = "UTF-8"
            elif sql_charset == Transport.charset_to_value["UTF-16BE"]:  # default is little endian
                target_charset = "UTF-16LE"
            else:
                target_charset = Transport.value_to_charset[sql_charset]
                character_len = len(param_values)
            if isinstance(param_values, bytes):
                param_values = param_values.decode().encode(target_charset)
            else:
                param_values = param_values.encode(target_charset)
        except:
            raise errors.NotSupportedError("unsupported charset: {0}".format(target_charset))
    else:
        raise errors.DataError(
            "invalid_parameter_value, data should be either bytes or String for column number: {0}".format(
                param_count))

    data_len = len(param_values)
    # if column is utf-8, length is the number of character while other charset is the number of bytes
    # max len will be length * 4 if charset is utf-8
    if sql_charset != Transport.charset_to_value["UTF-8"]:
        character_len = data_len
    else:
        max_len = max_len // 4
    if max_len >= character_len:
        if short_length:
            _ = Convert.put_short(data_len, buf_view[no_null_value:], little=True)
            _ = Convert.put_bytes(param_values, buf_view[no_null_value + 2:], nolen=True)
        else:
            _ = Convert.put_int(data_len, buf_view[no_null_value:], little=True)
            _ = Convert.put_bytes(param_values, buf_view[no_null_value + 4:], nolen=True)

    else:
        raise errors.DataError(
            "invalid_string_parameter input data is longer than the length for column number: {0}".format(
                param_count))
    return None


def put_sqltype_int(buf_view, no_null_value, param_values, desc, param_count, short_length):
    scale = desc.scale
    precision = desc.precision
    if not isinstance(param_values, (int, float)):
        raise errors.DataError(
            "invalid_parameter_value, data should be either int or float for column number: {0}".format(
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

    _ = Convert.put_int(param_values, buf_view[no_null_value:], little=True)
    return None


def put_sqltype_int_unsigned(buf_view, no_null_value, param_values, desc, param_count, short_length):
    scale = desc.scale
    precision = desc.precision
    if not isinstance(param_values, (int, float)):
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

    _ = Convert.put_uint(param_values, buf_view[no_null_value:], little=True)


def put_sqltype_tinyint(buf_view, no_null_value, param_values, desc, param_count, short_length):
    # TODO have not finished

    raise errors.NotSupportedError("not support tinyint")


def put_sqltype_tinyint_unsigned(buf_view, no_null_value, param_values, desc, param_count, short_length):
    raise errors.NotSupportedError("not support utinyint")


def put_sqltype_smallint(buf_view, no_null_value, param_values, desc, param_count, short_length):
    scale = desc.scale
    precision = desc.precision
    if not isinstance(param_values, (int, float)):
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

    _ = Convert.put_short(param_values, buf_view[no_null_value:], little=True)
    return None


def put_sqltype_smallint_unsigned(buf_view, no_null_value, param_values, desc, param_count, short_length):
    scale = desc.scale
    precision = desc.precision
    if not isinstance(param_values, (int, float)):
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

    _ = Convert.put_ushort(param_values, buf_view[no_null_value:], little=True)
    return None


def put_sqltype_largeint(buf_view, no_null_value, param_values, desc, param_count, short_length):
    scale = desc.scale
    precision = desc.precision
    if not isinstance(param_values, (int, float)):
        raise errors.DataError(
            "invalid_parameter_value, data should be either int or float for column: {0}".format(
                param_count))
    if scale > 0:
        param_values = int(param_values * (10 ** scale))

    if param_values > Transport.max_long or param_values < Transport.min_long:
        raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

    # check for numeric(x, y)
    if precision > 0:
        pre = 10 ** precision
        if param_values > pre or param_values < -pre:
            raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

    _ = Convert.put_longlong(param_values, buf_view[no_null_value:], little=True)
    return None


def put_sqltype_largeint_unsigned(buf_view, no_null_value, param_values, desc, param_count, short_length):
    scale = desc.scale
    if not isinstance(param_values, (int, float)):
        raise errors.DataError(
            "invalid_parameter_value, data should be either int or float for column: {0}".format(
                param_count))
    if scale > 0:
        param_values = int(param_values * (10 ** scale))

    if param_values > Transport.max_ulong or param_values < Transport.min_ulong:
        raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

    _ = Convert.put_ulonglong(param_values, buf_view[no_null_value:], little=True)
    return None


def put_sqltype_decimal(buf_view, no_null_value, param_values, desc, param_count, short_length):
    scale = desc.scale
    max_len = desc.max_len
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
    param_values = param_values.to_integral_exact(rounding='ROUND_DOWN').to_eng_string()

    num_zeros = max_len - len(param_values)
    if num_zeros < 0:
        raise errors.DataError("data_truncation_exceed {0}".format(param_count))

    padding = bytes('0'.encode() * num_zeros)
    _ = Convert.put_bytes(padding, buf_view[no_null_value:], nolen=True)

    if sign:
        _ = Convert.put_bytes(param_values.encode(), buf_view[no_null_value + num_zeros:], nolen=True, is_data=True)

        # byte -80 : 0xFFFFFFB0
        num, _ = Convert.get_bytes(buf_view[no_null_value:], length=1)

        _ = Convert.put_bytes(bytes([int(num) | 0xB0]), buf_view[no_null_value:], nolen=True, is_data=True)
        # _ = Convert.put_bytes(num | 0xB0, buf_view[no_null_value:], nolen=True, is_data=True)
    else:
        _ = Convert.put_bytes(param_values.encode(), buf_view[no_null_value + num_zeros:], nolen=True,
                              is_data=True)

    return buf_view


def put_sqltype_real(buf_view, no_null_value, param_values, desc, param_count, short_length):

    if not isinstance(param_values, (float, int)):
        raise errors.DataError(
            "invalid_parameter_value, data should be either float for column: {0}".format(
                param_count))

    if isinstance(param_values, int):
        param_values = float(param_values)
    if param_values > Transport.max_float or param_values < Transport.min_float:
        raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

    _ = Convert.put_float(param_values, buf_view[no_null_value:], little=True)


def put_sqltype_double(buf_view, no_null_value, param_values, desc, param_count, short_length):
    if not isinstance(param_values, (float, int)):
        raise errors.DataError(
            "invalid_parameter_value, data should be either float for column: {0}".format(
                param_count))

    if isinstance(param_values, int):
        param_values = float(param_values)
    if param_values > Transport.max_double or param_values < Transport.min_double:
        raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

    _ = Convert.put_double(param_values, buf_view[no_null_value:], little=True)


def put_sqltype_numeric(buf_view, no_null_value, param_values, desc, param_count, short_length):
    scale = desc.scale
    precision = desc.precision
    max_len = desc.max_len
    if not isinstance(param_values, (int, str, Decimal)):
        raise errors.DataError(
            "invalid_parameter_value, data should be either int or str or decimal for value: {0}".format(
                param_values))

    temp_num = Decimal(param_values)
    if temp_num >= 10 ** (precision - scale):
        raise errors.DataError(
            "invalid_parameter_value: number larger than the max valid value, number: {0}, precision:{1}, scale:{2}".format(
                param_values, precision, scale))

    sign = Convert.put_numeric(param_values, buf_view[no_null_value:], scale, max_len, precision)
    if sign:
        # byte -80 : 0xFFFFFFB0
        num, _ = Convert.get_bytes(buf_view[no_null_value + max_len - 1:], length=1)

        plus_sign = int.from_bytes(num, 'little') | 0x80

        _ = Convert.put_bytes(plus_sign.to_bytes(1, 'little'), buf_view[no_null_value + max_len - 1:], nolen=True,
                              is_data=True)


def put_sqltype_boolean(buf_view, no_null_value, param_values, desc, param_count, short_length):
    raise errors.NotSupportedError


def put_sqltype_other(buf_view, no_null_value, param_values, desc, param_count, short_length):
    raise errors.NotSupportedError


def put_sqltype_datetime(buf_view, no_null_value, param_values, desc, param_count, short_length):
    precision = desc.precision
    max_len = desc.max_len
    datetime_code = desc.datetime_code
    if datetime_code == FIELD_TYPE.SQLDTCODE_DATE:
        if not isinstance(param_values, (datetime.date, str)):
            raise errors.DataError(
                "invalid_parameter_value: data should be either datetime.date or string for value: {0}".format(
                    param_values))
        if isinstance(param_values, datetime.date):
            param_values = str(param_values)
        if isinstance(param_values, str):
            if not re.fullmatch('[\d]{4}-[\d]{2}-[\d]{2}', param_values):
                raise errors.DataError("invalid_parameter_value: string date should be YYYY-MM-DD")
            _ = Convert.put_bytes(param_values.encode(), buf_view[no_null_value:], nolen=True, is_data=True)

    if datetime_code == FIELD_TYPE.SQLDTCODE_TIMESTAMP:
        if not isinstance(param_values, datetime.datetime):
            raise errors.DataError(
                "invalid_parameter_value: data should be either datetime.time for value: {0}".format(
                    param_values))

        param_values = str(param_values) + '.0' if param_values.microsecond == 0 else str(param_values)

        # ODBC precision is nano secs. PDBC precision is micro secs
        # so substract 3 from ODBC precision.
        # max_len = max_len

        param_values = param_values.encode()
        length = len(param_values)

        if max_len >= length:
            padding = bytes(' '.encode() * (max_len - length))
            _ = Convert.put_bytes(param_values + padding, buf_view[no_null_value:], nolen=True, is_data=True)
        else:
            _ = Convert.put_bytes(param_values[0:max_len], buf_view[no_null_value:], nolen=True, is_data=True)
    if datetime_code == FIELD_TYPE.SQLDTCODE_TIME:

        if isinstance(param_values, datetime.time):
            param_values = str(param_values) + '.0' if precision > 0 and param_values.microsecond == 0 else str(param_values)
        elif isinstance(param_values, str):
            if not re.fullmatch('[\d]{2}:[\d]{2}:[\d]{2}(\.\d{1,6})?', param_values):
                raise errors.DataError("invalid_parameter_value: string date should be HH:MM:ss or HH:MM:ss.xxxxxx")
        else:
            raise errors.DataError(
                "invalid_parameter_value: data should be either datetime.time or string for value: {0}".format(
                    param_values))

        param_values = param_values.encode()
        length = len(param_values)
        if max_len >= length:
            padding = bytes(' '.encode() * (max_len - length))
            _ = Convert.put_bytes(param_values + padding, buf_view[no_null_value:], nolen=True, is_data=True)
        else:
            _ = Convert.put_bytes(param_values[0:max_len], buf_view[no_null_value:], nolen=True, is_data=True)

py_to_sql_convert_dict = {
    FIELD_TYPE.SQLTYPECODE_CHAR:                   put_sqltype_char,
    FIELD_TYPE.SQLTYPECODE_VARCHAR:                put_sqltype_varchar,
    FIELD_TYPE.SQLTYPECODE_VARCHAR_WITH_LENGTH:    put_sqltype_varchar_with_length,
    FIELD_TYPE.SQLTYPECODE_VARCHAR_LONG:           put_sqltype_varchar_with_length,
    FIELD_TYPE.SQLTYPECODE_INTEGER:                put_sqltype_int,
    FIELD_TYPE.SQLTYPECODE_INTEGER_UNSIGNED:       put_sqltype_int_unsigned,
    FIELD_TYPE.SQLTYPECODE_TINYINT:                put_sqltype_tinyint,
    FIELD_TYPE.SQLTYPECODE_SMALLINT:               put_sqltype_smallint,
    FIELD_TYPE.SQLTYPECODE_SMALLINT_UNSIGNED:      put_sqltype_smallint_unsigned,
    FIELD_TYPE.SQLTYPECODE_LARGEINT:               put_sqltype_largeint,
    FIELD_TYPE.SQLTYPECODE_LARGEINT_UNSIGNED:      put_sqltype_largeint_unsigned,
    FIELD_TYPE.SQLTYPECODE_DECIMAL:                put_sqltype_decimal,
    FIELD_TYPE.SQLTYPECODE_DECIMAL_UNSIGNED:       put_sqltype_decimal,
    FIELD_TYPE.SQLTYPECODE_REAL:                   put_sqltype_real,
    FIELD_TYPE.SQLTYPECODE_FLOAT:                  put_sqltype_double,
    FIELD_TYPE.SQLTYPECODE_DOUBLE:                 put_sqltype_double,
    FIELD_TYPE.SQLTYPECODE_NUMERIC:                put_sqltype_numeric,
    FIELD_TYPE.SQLTYPECODE_NUMERIC_UNSIGNED:       put_sqltype_numeric,
    FIELD_TYPE.SQLTYPECODE_BOOLEAN:                put_sqltype_boolean,
    FIELD_TYPE.SQLTYPECODE_DECIMAL_LARGE:          put_sqltype_other,
    FIELD_TYPE.SQLTYPECODE_DECIMAL_LARGE_UNSIGNED: put_sqltype_other,
    FIELD_TYPE.SQLTYPECODE_BIT:                    put_sqltype_other,
    FIELD_TYPE.SQLTYPECODE_BITVAR:                 put_sqltype_other,
    FIELD_TYPE.SQLTYPECODE_BPINT_UNSIGNED:         put_sqltype_other,
    FIELD_TYPE.SQLTYPECODE_DATETIME:               put_sqltype_datetime,
}
