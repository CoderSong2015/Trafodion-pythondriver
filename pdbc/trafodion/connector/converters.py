from .transport import Transport, Convert
from . import errors
from .constants import FIELD_TYPE
from decimal import Decimal
import datetime
import re


def get_sqltype_char(buf_view, column_desc, nonull_value_offset):
    charset = Transport.value_to_charset[column_desc.odbc_charset]
    length = column_desc.max_len
    ret_obj, _ = Convert.get_bytes(buf_view[nonull_value_offset:], length=length)
    ret_obj = ret_obj.decode(charset)
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

        nano_seconds = 123
        if column_desc.precision > 0:
            nano_seconds, _ = Convert.get_uint(buf_view[nonull_value_offset + 7:], little=True)
            if nano_seconds > 999999:  # returned in microseconds
                nano_seconds = 0
        ret_obj = datetime.datetime(year, month, day, hour, min, sec, microsecond=nano_seconds)
    if column_desc.datetime_code == FIELD_TYPE.SQLDTCODE_TIME:
        # "hh:mm:ss"
        hour, _ = Convert.get_char(buf_view[nonull_value_offset:], to_python_int=True)
        minute, _ = Convert.get_char(buf_view[nonull_value_offset + 1:], to_python_int=True)
        second, _ = Convert.get_char(buf_view[nonull_value_offset + 2:], to_python_int=True)
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
    if sql_charset != Transport.charset_to_value["UTF-8"]:
        character_len = data_len
    else:
        max_len = max_len // 4
    if max_len >= data_len:
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
                for x in range(max_len - data_len):
                    b.append(ord(' '))
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
        param_values = round(param_values * (10 ** scale))

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
        param_values = round(param_values * (10 ** scale))

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
    param_values = param_values.to_integral_exact().to_eng_string()

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

    if not isinstance(param_values, float):
        raise errors.DataError(
            "invalid_parameter_value, data should be either float for column: {0}".format(
                param_count))
    if param_values > Transport.max_float:
        raise errors.DataError("numeric_out_of_range: {0}".format(param_values))

    _ = Convert.put_float(param_values, buf_view[no_null_value:], little=True)


def put_sqltype_double(buf_view, no_null_value, param_values, desc, param_count, short_length):
    if not isinstance(param_values, float):
        raise errors.DataError(
            "invalid_parameter_value, data should be either float for column: {0}".format(
                param_count))
    if param_values > Transport.max_double:
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

        if max_len > length:
            padding = bytes(' '.encode() * (max_len - length))
            _ = Convert.put_bytes(param_values + padding, buf_view[no_null_value:], nolen=True, is_data=True)
        else:
            _ = Convert.put_bytes(param_values[0:precision], buf_view[no_null_value:], nolen=True, is_data=True)
    if datetime_code == FIELD_TYPE.SQLDTCODE_TIME:
        if not isinstance(param_values, (datetime.time, str)):
            raise errors.DataError(
                "invalid_parameter_value: data should be either datetime.time or string for value: {0}".format(
                    param_values))
        if isinstance(param_values, datetime.time):
            param_values = str(param_values)
        if isinstance(param_values, str):
            if not re.fullmatch('[\d]{2}:[\d]{2}:[\d]{2}', param_values):
                raise errors.DataError("invalid_parameter_value: string date should be HH:MM:ss")
            _ = Convert.put_bytes(param_values.encode(), buf_view[no_null_value:], nolen=True, is_data=True)

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
