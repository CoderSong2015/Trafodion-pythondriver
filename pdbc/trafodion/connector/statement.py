from . import errors
from .struct_def import (SQLDataValueDef, SQLValueListDef, Header, ExecuteReply, FetchReply, PrepareReply)
from .constants.TRANSPORT import Transport

from .converters import Convert
from .logmodule import PyLog

class Statement:

    def __init__(self, conn, cursor):
        self._connection = conn
        #self._log_obj = conn.get_logger_obj()
        self._cursor = cursor
        self._outputDesc_ = None
        self._stmt_handle_ = 0
        self.sql_stmt_type_ = 0
        self.stmt_explain_label = ""
        self._rowcount = 0
        self._last_count = 0
        self._batch_row_count = []
        self.stmt_label = cursor._stmt_name
        self._descriptor = None
        self._max_row_count = 100
        self._sql_async_enable = 1
        self._stmt_label_charset = self._cursor._stmt_name_charset
        self._cursor_name = self._cursor._cursor_name
        self._cursor_name_charset = 1
        self._stmt_options = ""
        self._max_rowset_size = self._cursor._max_rowset_size
        self._module_name = ""
        self._module_name_charset = 1
        self._module_timestamp = 0
        self._is_prepared = False
        self.stmt_type = 0  # EXTERNAL_STMT
        self._error_list = []
        pass

    def execute(self, query: bytes, execute_api, params=None, is_executemany=False):
        ## default:
        # resultSetType  ResultSet.TYPE_FORWARD_ONLY, \
        # resultSetConcurrency ResultSet.CONCUR_READ_ONLY,
        # resultSetHoldability TrafT4ResultSet.CLOSE_CURSORS_AT_COMMIT

        self._cursor_name = self._cursor._cursor_name
        sql_async_enable = 0 #self._sql_async_enable  # sqlAsyncEnable = 1 if stmt.getResultSetHoldability() == TrafT4ResultSet.HOLD_CURSORS_OVER_COMMIT else 0
        input_row_count = self._handle_params(params, is_executemany)  #used for batch insert
        max_rowset_size = self._max_rowset_size
        sqlstring = query
        sqlstring_charset = 1

        stmt_label_charset = self._cursor._stmt_name_charset
        tx_id = 0
        param_count = 0
        client_errors_list = []

        """
         if (stmt.transactionToJoin != null)
            txId = stmt.transactionToJoin;
        elif (stmt.connection_.transactionToJoin != null)
            txId = stmt.connection_.transactionToJoin;
        """

        input_value_list = SQLValueListDef()

        self.sql_stmt_type_ = self._get_statement_type(sqlstring, params)
        if execute_api == Transport.SRVR_API_SQLEXECDIRECT:

            # self.set_transaction_status(stmt.connection_, sql)
            self._outputDesc_ = None  # clear the output descriptors
            PyLog.global_logger.set_debug("Execute Type: SQLEXECDIRECT" + "\n"
                                          + "Query: " + str(sqlstring))
        else:
            PyLog.global_logger.set_debug("Execute Type: SQLEXECUTE2" + "\n"
                                          + "Query: " + str(sqlstring))

    #if (.usingRawRowset_):
    #else:
        input_data_value = SQLDataValueDef.fill_in_sql_values(self._descriptor, input_row_count, params,
                                                              is_executemany=is_executemany)


        temp_descriptor = self._to_send(execute_api, sql_async_enable, input_row_count,
                                         max_rowset_size, self.sql_stmt_type_, self._stmt_handle_, sqlstring,
                                         sqlstring_charset,
                                         self._cursor_name,
                                         self._cursor_name_charset, self.stmt_label, stmt_label_charset,
                                         input_data_value,
                                         input_value_list, tx_id,
                                         self._cursor._using_rawrowset)

        if execute_api == Transport.SRVR_API_SQLEXECDIRECT:
            self._descriptor = temp_descriptor
        else:
            self._descriptor.rows_affected = temp_descriptor.rows_affected

        if execute_api == Transport.SRVR_API_SQLEXECDIRECT:

            # self.set_transaction_status(stmt.connection_, sql)
            self._outputDesc_ = None  # clear the output descriptors
            PyLog.global_logger.set_debug("EXECUTE SUCCESS" + "\n" + "Execute Type: SQLEXECDIRECT" + "\n"
                                          + "Query: " + str(sqlstring))
        else:
            PyLog.global_logger.set_debug("EXECUTE SUCCESS" + "\n" + "Execute Type: SQLEXECUTE2" + "\n"
                                          + "Query: " + str(sqlstring))
        return self._descriptor
        # TODO now there is no need to make a resultset
        #self._handle_recv_data(recv_reply, execute_api, client_errors_list, input_row_count)

    def _handle_params(self, params, is_executemany):
        if not params:
            return 0
        if not isinstance(params, (list, tuple)):
            raise errors.DataError("Parameters should be list or tuple")
        if is_executemany:
            for x in params:
                if not isinstance(x, tuple):
                    raise errors.DataError("Parameters contained in params of executemany should be tuple")
            return len(params)
        else:
            # TODO check parameters
            return 1

    def fetch(self, row_count=None):
        max_row_count = row_count if row_count else self._max_row_count

        # When sql has condition and the key is primary key, it will return values directly by execute
        # Here check if the result has returned by execute
        if self._descriptor.has_outvalues() and not self._descriptor.is_out_values_used():
            t = FetchReply()
            t.init_from_values(self._descriptor, self._descriptor.get_outvalues(), max_row_count)

            # clear the outvalues and set used flag
            self._descriptor.clear_outvalues()
            return t

        # fetch from server
        wbuffer = self._marshal_fetch(self._connection.dialogue_id, self._sql_async_enable,
                                      self._connection.property.query_timeout,
                                      self._stmt_handle_, self.stmt_label, self._stmt_label_charset, max_row_count, 0,
                                      self._cursor_name, self._cursor_name_charset, self._stmt_options)

        data = self._connection.get_from_server(Transport.SRVR_API_SQLFETCH, wbuffer, self._connection._mxosrvr_conn)

        buf_view = memoryview(data)
        t = FetchReply()
        t.init_reply(buf_view, self._descriptor)
        return t

    def _marshal_fetch(self, dialogue_id, sql_async_enable, query_timeout, stmt_handle,
                       stmt_label, stmt_label_charset, max_row_count, max_row_len, cursor_name, cursor_name_charset,
                       stmt_options):
        try:
            wlength = Header.sizeOf()

            wlength += Transport.size_int  # dialogue_id
            wlength += Transport.size_int  # sql_async_enable
            wlength += Transport.size_int  # query_timeout
            wlength += Transport.size_int  # stmt_handle
            wlength += Transport.size_bytes(stmt_label.encode("utf-8"))
            wlength += Transport.size_int  # stmt_label_charset
            wlength += Transport.size_long  # max_row_count
            wlength += Transport.size_long  # max_row_len
            wlength += Transport.size_bytes(cursor_name.encode("utf-8"))
            wlength += Transport.size_int  # cursor_name_charset
            wlength += Transport.size_bytes(stmt_options.encode("utf-8"))
            buf = bytearray(b'')

            buf.extend(bytearray(wlength))
            buf_view = memoryview(buf)
            buf_view = buf_view[Header.sizeOf():]

            buf_view = Convert.put_int(dialogue_id, buf_view, little=True)
            buf_view = Convert.put_int(sql_async_enable, buf_view, little=True)
            buf_view = Convert.put_int(query_timeout, buf_view, little=True)
            buf_view = Convert.put_int(stmt_handle, buf_view, little=True)
            buf_view = Convert.put_string(stmt_label, buf_view, little=True)
            buf_view = Convert.put_int(stmt_label_charset, buf_view, little=True)
            buf_view = Convert.put_longlong(max_row_count, buf_view, little=True)
            buf_view = Convert.put_longlong(max_row_len, buf_view, little=True)
            buf_view = Convert.put_string(cursor_name, buf_view, little=True)
            buf_view = Convert.put_int(cursor_name_charset, buf_view, little=True)
            buf_view = Convert.put_string(stmt_options, buf_view, little=True)
        except:
            raise errors.InternalError("fetch data marshalling error")
        return buf

    def _to_send(self, execute_api, sql_async_enable, input_row_count, max_rowset_size, sql_stmt_type,
                 stmt_handle, sqlstring, sqlstring_charset, cursor_name, cursor_name_charset,
                 stmt_label, stmt_label_charset, input_data_value, input_value_list,
                 tx_id, user_buffer: bool):

        wbuffer = self._marshal_statement(self._connection.dialogue_id, sql_async_enable,
                                          self._connection.property.query_timeout, input_row_count, max_rowset_size,
                                          sql_stmt_type,
                                          stmt_handle, 0, sqlstring, sqlstring_charset, cursor_name,
                                          cursor_name_charset,
                                          stmt_label, stmt_label_charset, self.stmt_explain_label, input_data_value,
                                          input_value_list, tx_id, user_buffer)

        data = self._connection.get_from_server(execute_api, wbuffer, self._connection._mxosrvr_conn)
        buf_view = memoryview(data)
        c = ExecuteReply()
        c.init_reply(buf_view)
        return c

    def _handle_recv_data(self, recv_reply, execute_api, client_errors_list, input_row_count):
        if execute_api == Transport.SRVR_API_SQLEXECDIRECT:
            self.sql_query_type = recv_reply.query_type

        if len(client_errors_list) > 0:
            if not recv_reply.errorlist:
                recv_reply.errorlist = client_errors_list
            else:
                pass
                # TODO recv_reply.errorlist = mergeerror(recv_reply.errorlist, client_errors_list)
        self._result_set_offset = 0
        self._rowcount = recv_reply.rows_affected

        num_status = 0
        delay_error_mode = self._connection.property.DelayedErrorMode
        if delay_error_mode:
            if self._last_count > 0:
                num_status = self._last_count

            else:
                num_status = input_row_count
        else:
            num_status = input_row_count

        if num_status < 1:
            num_status = 1

        batch_exception = False

        if delay_error_mode and self._last_count < 1:
            for x in range(num_status):
                self._batch_row_count.append(-2)  # fill with success
        elif (recv_reply.return_code == Transport.SQL_SUCCESS or
              recv_reply.return_code == Transport.SQL_SUCCESS_WITH_INFO or
              recv_reply.return_code == Transport.NO_DATA_FOUND):
            for x in range(num_status):
                self._batch_row_count.append(-2)  # fill with success

            if recv_reply.errorlist:
                for item in recv_reply.errorlist:
                    row = item.row_id -1
                    if 0 <= row < len(self._batch_row_count):
                        self._batch_row_count[row] = -3
                        batch_exception = True

            #if self.sql_stmt_type_ == Transport.TYPE_QS_OPEN:
            #TODO  set the statement mode as the command succeeded
            #if (self.sql_stmt_type_ == Transport.TYPE_QS_OPEN):
            #elif (self.sql_stmt_type_ == Transport.TYPE_QS_CLOSE):
            #elif (self.sql_stmt_type_ == Transport.TYPE_CMD_OPEN):
            #elif(self.sql_stmt_type_ == Transport.TYPE_CMD_CLOSE):

            # set the statement label if we didnt get one back.
            if len(recv_reply.stmt_labels_list):
                recv_reply.stmt_labels_list = []
                recv_reply.stmt_labels_list.append(self.stmt_label)

        else:
            for x in range(num_status):
                self._batch_row_count.append(-3)  # fill with failed

    def _marshal_statement(self, dialogue_id, sql_async_enable, query_timeout, input_row_count,
                           max_rowset_size, sql_stmt_type, stmt_handle, stmt_type, sqlstring, sqlstring_charset,
                           cursor_name: str, cursor_name_charset, stmt_label: str, stmt_label_charset, stmtExplainLabel,
                           input_data_value, input_value_list, tx_id, user_buffer):
        try:
            wlength = Header.sizeOf()
            wlength += Transport.size_int        # dialogue_id
            wlength += Transport.size_int        # sqlAsyncEnable
            wlength += Transport.size_int        # queryTimeout
            wlength += Transport.size_int        # inputRowCnt
            wlength += Transport.size_int        # maxRowsetSize
            wlength += Transport.size_int        # sqlStmtType
            wlength += Transport.size_int        # stmtHandle
            wlength += Transport.size_int        # stmtType#
            wlength += Transport.size_bytes(sqlstring)
            wlength += Transport.size_int  # sqlStringCharset
            wlength += Transport.size_bytes(cursor_name.encode("utf-8"))       # cursorNameCharset
            wlength += Transport.size_int
            wlength += Transport.size_bytes(stmt_label.encode("utf-8"))        # stmtLabelCharset
            wlength += Transport.size_int
            wlength += Transport.size_bytes(stmtExplainLabel.encode("utf-8"))

            if not user_buffer:
                wlength += input_data_value.sizeof()
                wlength += Transport.size_int + Transport.size_int + 1  # transId

            buf = bytearray(b'')

            buf.extend(bytearray(wlength))

            buf_view = memoryview(buf)
            buf_view = buf_view[Header.sizeOf():]

            buf_view = Convert.put_int(dialogue_id, buf_view, little=True)
            buf_view = Convert.put_int(sql_async_enable, buf_view, little=True)
            buf_view = Convert.put_int(query_timeout, buf_view, little=True)
            buf_view = Convert.put_int(input_row_count, buf_view, little=True)
            buf_view = Convert.put_int(max_rowset_size, buf_view, little=True)
            buf_view = Convert.put_int(sql_stmt_type, buf_view, little=True)
            buf_view = Convert.put_int(stmt_handle, buf_view, little=True)
            buf_view = Convert.put_int(stmt_type, buf_view, little=True)

            buf_view = Convert.put_bytes(sqlstring, buf_view, little=True)
            buf_view = Convert.put_int(sqlstring_charset, buf_view, little=True)
            buf_view = Convert.put_string(cursor_name, buf_view, little=True)
            buf_view = Convert.put_int(cursor_name_charset, buf_view, little=True)
            buf_view = Convert.put_string(stmt_label, buf_view, little=True)
            buf_view = Convert.put_int(stmt_label_charset, buf_view, little=True)

            buf_view = Convert.put_string(stmtExplainLabel, buf_view, little=True)

            if user_buffer:
                pass
            else:
                buf_view = input_data_value.insert_into_bytearray(buf_view, little=True)
                buf_view = Convert.put_int(4 + 1, buf_view, little=True)
                buf_view = Convert.put_int(tx_id, buf_view, little=True)
                buf_view = Convert.put_bytes(b'\x00', buf_view, nolen=True)
        except:
            raise errors.InternalError("marshal error")
        return buf

    def set_transaction_status(self, conn, sql):
        tran_status = self.get_transaction_status(sql)
        if tran_status == Transport.TYPE_BEGIN_TRANSACTION:
            conn.setBeginTransaction(True)
        elif tran_status == Transport.TYPE_END_TRANSACTION:
            conn.setBeginTransaction(False)

    def get_transaction_status(self, sql):
        return ""
        pass

    @staticmethod
    def _get_statement_type(query, params):

        # TODO There are different mode in trafodion
        # MODE_SQL\MODE_WMS\MODE_CMD
        # default is MODE_SQL
        type_dict = {
            "JOINXATXN": Transport.TYPE_SELECT,
            "WMSOPEN": Transport.TYPE_QS_OPEN,
            "WMSCLOSE": Transport.TYPE_QS_CLOSE,
            "CMDOPEN": Transport.TYPE_CMD_OPEN,
            "CMDCLOSE": Transport.TYPE_CMD_CLOSE,
            "SELECT": Transport.TYPE_SELECT,
            "WITH": Transport.TYPE_SELECT,
            "SHOWSHAPE": Transport.TYPE_SELECT,
            "INVOKE": Transport.TYPE_SELECT,
            "SHOWCONTROL": Transport.TYPE_SELECT,
            "SHOWDDL": Transport.TYPE_SELECT,
            "EXPLAIN": Transport.TYPE_SELECT,
            "SHOWPLAN": Transport.TYPE_SELECT,
            "REORGANIZE": Transport.TYPE_SELECT,
            "MAINTAIN": Transport.TYPE_SELECT,
            "SHOWLABEL": Transport.TYPE_SELECT,
            "VALUES": Transport.TYPE_SELECT,
            "REORG": Transport.TYPE_SELECT,
            "SEL": Transport.TYPE_SELECT,
            "GET": Transport.TYPE_SELECT,
            "SHOWSTATS": Transport.TYPE_SELECT,
            "GIVE": Transport.TYPE_SELECT,
            "STATUS": Transport.TYPE_SELECT,
            "INFO": Transport.TYPE_SELECT,
            "LIST": Transport.TYPE_SELECT,
            "UPDATE": Transport.TYPE_UPDATE,
            "MERGE": Transport.TYPE_UPDATE,
            "DELETE": Transport.TYPE_DELETE,
            "STOP": Transport.TYPE_DELETE,
            "START": Transport.TYPE_DELETE,
            "INSERT": Transport.TYPE_INSERT,
            "INS": Transport.TYPE_INSERT,
            "UPSERT": Transport.TYPE_INSERT,
            "CREATE": Transport.TYPE_CREATE,
            "GRANT": Transport.TYPE_GRANT,
            "DROP": Transport.TYPE_DROP,
            "CALL": Transport.TYPE_CALL,
            "INFOSTATS": Transport.TYPE_STATS,
            #  "EXPLAIN": Transport.TYPE_EXPLAIN,
        }

        if isinstance(query, (bytearray, bytes)):
            first_word = query.split(" ".encode())[0].decode().upper()
        else:
            first_word = query.split(" ")[0].upper()

        ans = type_dict.setdefault(first_word, Transport.TYPE_UNKNOWN)
        if ans == Transport.TYPE_INSERT and params:
            return Transport.TYPE_INSERT_PARAM
        else:
            return ans

    def execute_all(self, operation, execute_type, params, is_executemany=False):
        # make for prepared statement
        pass

    def set_max_row_count(self, size=100):
        self._max_row_count = size

    def get_error_list(self):
        return self._error_list

    #def get_warning_list(self):
    #    return self._warning_list

class PreparedStatement(Statement):

    def __init__(self, conn, cursor):
        super(PreparedStatement, self).__init__(conn, cursor)
        self._descriptor = None
        pass

    def set_is_prepare(self, val=False):
        self._is_prepared = val

    def execute_all(self, operation, execute_type, params, is_executemany=False):

        # first: prepare
        PyLog.global_logger.set_debug("prepare info:"
                                      + "\n" + "is prepared:" + str(self._is_prepared))
        if not self._is_prepared:
            self._prepare(operation)
            self.set_is_prepare(True)
        # second: execute
        self.execute(operation, execute_type, params, is_executemany=is_executemany)
        return self._descriptor

    def _prepare(self, operation):

        cursor_name_charset = self._cursor_name_charset
        module_name = self._module_name
        module_name_charset = self._module_name_charset
        module_timestamp = self._module_timestamp
        stmt_options = ""
        stmt_type = self.stmt_type = 0  # EXTERNAL_STMT
        sql_async_enable = self._sql_async_enable
        input_row_count = 0
        max_rowset_size = self._max_rowset_size
        sql_string = operation
        sql_string_charset = 1
        tx_id = 0
        stmt_label_charset = self._stmt_label_charset

        PyLog.global_logger.set_debug("prepare info:"
                                      + "\n" + "sql string:" + str(sql_string))

        self._descriptor = self._to_send_prepare(sql_async_enable, stmt_type, self.sql_stmt_type_,
                                                 self.stmt_label, stmt_label_charset,
                                                 self._cursor_name, cursor_name_charset, module_name,
                                                 module_name_charset,
                                                 module_timestamp,
                                                 sql_string, sql_string_charset, stmt_options, max_rowset_size,
                                                 tx_id)
        self._stmt_handle_ = self._descriptor.stmt_handle

    def _to_send_prepare(self, sql_async_enable, stmt_type, sql_stmt_type, stmt_label, stmt_label_charset,
                         cursor_name, cursor_name_charset, module_name, module_name_charset, module_timestamp,
                         sql_string, sql_string_charset, stmt_options, max_rowset_size, tx_id):

        wbuffer = self._marshal_prepare_statement(self._connection.dialogue_id, sql_async_enable,
                                                  self._connection.property.query_timeout,
                                                  stmt_type, sql_stmt_type,
                                                  stmt_label, stmt_label_charset,
                                                  cursor_name, cursor_name_charset, module_name, module_name_charset,
                                                  module_timestamp,
                                                  sql_string, sql_string_charset, stmt_options, self.stmt_explain_label,
                                                  max_rowset_size,
                                                  tx_id)
        data = self._connection.get_from_server(Transport.SRVR_API_SQLPREPARE, wbuffer, self._connection._mxosrvr_conn)
        buf_view = memoryview(data)
        t = PrepareReply()
        t.init_reply(buf_view)

        PyLog.global_logger.set_debug("prepare success")
        return t

    def _marshal_prepare_statement(self, dialogue_id, sql_async_enable, query_timeout, stmt_type, sql_stmt_type,
                                   stmt_label, stmt_label_charset,
                                   cursor_name, cursor_name_charset, module_name, module_name_charset, module_timestamp,
                                   sql_string, sql_string_charset, stmt_options, stmt_explain_label, max_rowset_size,
                                   tx_id):

        try:
            wlength = Header.sizeOf()
            wlength += Transport.size_int        # dialogue_id
            wlength += Transport.size_int        # sqlAsyncEnable
            wlength += Transport.size_int        # queryTimeout
            wlength += Transport.size_short      # stmt_type
            wlength += Transport.size_int        # sqlStmtType

            wlength += Transport.size_bytes(stmt_label.encode("utf-8"))
            wlength += Transport.size_int        # stmtLabelCharset
            wlength += Transport.size_bytes(cursor_name.encode("utf-8"))
            wlength += Transport.size_int        # cursorNameCharset
            wlength += Transport.size_bytes(module_name.encode("utf-8"))
            wlength += Transport.size_int        # moduleName charset

            #if len(module_name) > 0:
            wlength += Transport.size_long  # in fact server will read moduleTimestamp anyway

            wlength += Transport.size_bytes(sql_string)
            wlength += Transport.size_int  # sql_string charset
            wlength += Transport.size_bytes(stmt_options.encode("utf-8"))
            wlength += Transport.size_bytes(stmt_explain_label.encode("utf-8"))
            wlength += Transport.size_int  # maxRowsetSize
            wlength += Transport.size_int  # tx_id

            buf = bytearray(b'')

            buf.extend(bytearray(wlength))

            buf_view = memoryview(buf)
            buf_view = buf_view[Header.sizeOf():]

            buf_view = Convert.put_int(dialogue_id, buf_view, little=True)
            buf_view = Convert.put_int(sql_async_enable, buf_view, little=True)
            buf_view = Convert.put_int(query_timeout, buf_view, little=True)
            buf_view = Convert.put_short(stmt_type, buf_view, little=True)
            buf_view = Convert.put_int(sql_stmt_type, buf_view, little=True)
            buf_view = Convert.put_string(stmt_label, buf_view, little=True)
            buf_view = Convert.put_int(stmt_label_charset, buf_view, little=True)
            buf_view = Convert.put_string(cursor_name, buf_view, little=True)
            buf_view = Convert.put_int(cursor_name_charset, buf_view, little=True)
            buf_view = Convert.put_string(module_name, buf_view, little=True)
            buf_view = Convert.put_int(module_name_charset, buf_view, little=True)
            #if len(module_name) > 0:
            buf_view = Convert.put_longlong(module_timestamp, buf_view, little=True)
            buf_view = Convert.put_bytes(sql_string, buf_view, little=True)
            buf_view = Convert.put_int(sql_string_charset, buf_view, little=True)
            buf_view = Convert.put_string(stmt_options, buf_view, little=True)
            buf_view = Convert.put_string(stmt_explain_label, buf_view, little=True)
            buf_view = Convert.put_int(max_rowset_size, buf_view, little=True)
            buf_view = Convert.put_int(tx_id, buf_view, little=True)
        except:
            raise errors.InternalError("marshal error")
        return buf

