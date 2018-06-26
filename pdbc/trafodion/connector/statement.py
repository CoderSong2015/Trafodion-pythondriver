from .transport import Transport
from .struct_def import (SQL_DataValue_def, SQLValueList_def, SQLValue_def,
                         Header, ExecuteReply)
from .transport import convert
from . import errors

class Statement:

    def __init__(self, conn, cursor):
        self._connection = conn
        self._cursor = cursor
        self._outputDesc_ = None
        self._stmt_handle_ = 0
        self.sql_stmt_type_ = 0
        self.stmt_explain_label = ""
        pass

    def execute(self, query: bytes, execute_api):
        #sqlAsyncEnable = 1 if stmt.getResultSetHoldability() == TrafT4ResultSet.HOLD_CURSORS_OVER_COMMIT else 0
        cursor_name = self._cursor._cursor_name
        rowCount_ = 0
        sql_async_enable  = 1
        input_row_count = 0
        max_rowset_size = self._cursor._max_rows
        sqlstring = query
        sqlstring_charset = 1
        cursor_name_charset = 1
        stmt_label_charset = 1
        tx_id = 0
        param_count = 0
        client_errors = []

        """
         if (stmt.transactionToJoin != null)
            txId = stmt.transactionToJoin;
        elif (stmt.connection_.transactionToJoin != null)
            txId = stmt.connection_.transactionToJoin;
        """

        input_value_list = SQLValueList_def()
        input_params = None

        if (execute_api == Transport.SRVR_API_SQLEXECDIRECT):
            self.sql_stmt_type_ = self._get_statement_type(sqlstring)
            #self.set_transaction_status(stmt.connection_, sql)
            self._outputDesc_ = None  # clear the output descriptors

    #if (.usingRawRowset_):
    #else:
        input_data_value = SQL_DataValue_def.fill_in_sql_values("zh", self._cursor, input_row_count, param_count, None, client_errors)

        er = self._to_send(execute_api, sql_async_enable, input_row_count - len(client_errors),
                           max_rowset_size, self.sql_stmt_type_, self._stmt_handle_, sqlstring, sqlstring_charset,
                           cursor_name,
                           cursor_name_charset, self._cursor._stmt_name, stmt_label_charset, input_data_value,
                           input_value_list, tx_id,
                           self._cursor._using_rawrowset)

    def _to_send(self, execute_api, sql_async_enable, input_row_count, max_rowset_size, sql_stmt_type,
                 stmt_handle, sqlstring, sqlstring_charset, cursor_name, cursor_name_charset,
                 stmt_label, stmt_label_charset, input_data_value, input_value_list,
                 tx_id, user_buffer: bool):

        wbuffer = self._marshal_statement(self._connection._dialogue_id, sql_async_enable,
                                          self._connection.property.query_timeout, input_row_count, max_rowset_size,
                                          sql_stmt_type,
                                          stmt_handle, 0, sqlstring, sqlstring_charset, cursor_name,
                                          cursor_name_charset,
                                          stmt_label, stmt_label_charset, self.stmt_explain_label, input_data_value,
                                          input_value_list, tx_id, user_buffer)

        data = self._connection._get_from_server(Transport.SRVR_API_SQLEXECDIRECT, wbuffer, self._connection._mxosrvr_conn)
        recv_reply = self._handle_recv_data(data)
        return 0

    def _handle_recv_data(self, data):
        try:
            buf_view = memoryview(data)
            c = ExecuteReply()
            c.init_reply(buf_view)
        except:
            raise errors.InternalError("handle mxosrvr data error")
        return c
    def _marshal_statement(self, dialogueId, sql_async_enable, queryTimeout, input_row_count,
                           max_rowset_size, sql_stmt_type, stmt_handle, stmt_type, sqlstring, sqlstring_charset,
                           cursor_name: str, cursor_name_charset, stmt_label: str, stmt_label_charset, stmtExplainLabel,
                           input_data_value, input_value_list, tx_id, user_buffer):
        try:
            wlength = Header.sizeOf()
            wlength += Transport.size_int        # dialogueId
            wlength += Transport.size_int        # sqlAsyncEnable
            wlength += Transport.size_int        # queryTimeout
            wlength += Transport.size_int        # inputRowCnt
            wlength += Transport.size_int        # maxRowsetSize
            wlength += Transport.size_int        # sqlStmtType
            wlength += Transport.size_int        # stmtHandle
            wlength += Transport.size_int        # stmtType#
            wlength += Transport.size_bytes(sqlstring)
            wlength += Transport.size_int        # sqlStringCharset
            wlength += Transport.size_bytes(cursor_name.encode("utf-8"))
            wlength += Transport.size_int        # cursorNameCharset
            wlength += Transport.size_bytes(stmt_label.encode("utf-8"))
            wlength += Transport.size_int        # stmtLabelCharset
            wlength += Transport.size_bytes(stmtExplainLabel.encode("utf-8"))

            if not user_buffer:
                wlength += input_data_value.sizeof()
                wlength += Transport.size_int # transId

            buf = bytearray(b'')

            buf.extend(bytearray(wlength))

            buf_view = memoryview(buf)
            buf_view = buf_view[Header.sizeOf():]

            buf_view = convert.put_int(dialogueId, buf_view, little=True)
            buf_view = convert.put_int(sql_async_enable, buf_view, little=True)
            buf_view = convert.put_int(queryTimeout, buf_view, little=True)
            buf_view = convert.put_int(input_row_count, buf_view, little=True)
            buf_view = convert.put_int(max_rowset_size, buf_view, little=True)
            buf_view = convert.put_int(sql_stmt_type, buf_view, little=True)
            buf_view = convert.put_int(stmt_handle, buf_view, little=True)
            buf_view = convert.put_int(stmt_type, buf_view, little=True)

            buf_view = convert.put_bytes(sqlstring, buf_view, little=True)
            buf_view = convert.put_int(sqlstring_charset, buf_view, little=True)
            buf_view = convert.put_string(cursor_name, buf_view, little=True)
            buf_view = convert.put_int(cursor_name_charset, buf_view, little=True)
            buf_view = convert.put_string(stmt_label, buf_view, little=True)
            buf_view = convert.put_int(stmt_label_charset, buf_view, little=True)

            buf_view = convert.put_string(stmtExplainLabel, buf_view, little=True)

            if user_buffer:
                pass
            else:
                input_data_value.insertIntoByteArray(buf_view, little=True)
                buf_view = convert.put_int(tx_id, buf_view, little=True)
        except:
            raise errors.InternalError("marshal error")
        return buf


    def set_transaction_status(self, conn, sql):
        tran_status = self.get_transaction_status(sql)
        if (tran_status == Transport.TYPE_BEGIN_TRANSACTION):
            conn.setBeginTransaction(True)
        elif (tran_status == Transport.TYPE_END_TRANSACTION):
            conn.setBeginTransaction(False)

    def get_transaction_status(self, sql):
        return ""
        pass

    @staticmethod
    def _get_statement_type(query):

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

        return type_dict[first_word]

class PreparedStatement(Statement):
    def __init__(self, conn):
        super(PreparedStatement, self).__init__(conn)
        pass