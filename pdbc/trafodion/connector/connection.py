import getpass
import threading
import socket

from . import authentication
from . import errors
from .constants import CONNECTION
from .abstracts import TrafConnectionAbstract
from .cursor import CursorBase, TrafCursor
from .struct_def import (
    ConnectReply, UserDescDef, ConnectionContextDef,
    VersionDef, Header, InitializeDialogueReply, TerminateReply, EndTransactionReply
)

from .transport import Transport, Convert


class TrafConnection(TrafConnectionAbstract):
    """
        Connection to a mxosrvr
    """

    def __init__(self, *args, **kwargs):
        self._force_ipv6 = False
        self._unix_socket = None
        self._sessionToken = None
        self._is_read_only = False
        self._auto_commit = True
        self._ignoreCancel = False
        self._connection_timeout = 60
        self._dialogue_id = 0
        self._session_name = ''
        self._mxosrvr_conn = None
        self._seq_num = 0
        self._stmt_name_lock = threading.Lock()
        self._buffered = None
        self._raw = None
        self._connecte_status = 0
        super(TrafConnection, self).__init__(**kwargs)

        if kwargs:
            self.connect(**kwargs)

    def _connect_to_mxosrvr(self):
        """
        :return: 
        """

        # get information from dcs master
        self.mxosrvr_info = self._get_objref()
        self._dialogue_id = self.mxosrvr_info.dialogue_id
        
        if self.mxosrvr_info.security_enabled:
            return_code = self._secure_login()
            if return_code == Transport.SQL_SUCCESS:
                self._connecte_status = 1
        else:
            self._old_encrypt_password()
            self.init_diag(True, False)
        #TODO self._get_connection() get connection from mxosrvr

        #TODO

    def _secure_login(self):
        #  if there is no certificate in local system, downloading
        #  if from mxosrvr first.
        #  now download certificate every time

        proc_info = self._create_proc_info(self.mxosrvr_info.server_process_id,
                                         self.mxosrvr_info.server_node_id,
                                         self.mxosrvr_info.timestamp)

        #  spj_mode = this.t4props_.getSPJEnv() & & this.t4props_.getTokenAuth();

        # TODO init directory and filename in property
        self.secpwd = authentication.SecPwd('', '', False, self.mxosrvr_info.cluster_name, proc_info)
        init_reply = self._download_cer()
        #  TODO save certificate into file
        try:
            self._user_desc.password = self._encrypt_password(init_reply)
        except:
            raise errors.DataError("encrypt error")
        init_reply = self._init_diag(True, False)
        return init_reply.exception_nr

    def _create_proc_info(self, server_process_id,
                         server_node_id,
                         timestamp: bytes,
                         )-> bytes:
        proc = server_process_id.to_bytes(length=4, byteorder='little') \
               + server_node_id.to_bytes(length=4, byteorder='little') \
               + timestamp

        return proc

    def _encrypt_password(self, init_reply):

        self.secpwd.open_certificate(certificate=init_reply.out_context.certificate)
        #out_context = init_reply.out_context
        role_name = self.property.userRole
        try:
            pwd = self.secpwd.encrypt_pwd(self._password, role_name)
            return pwd
        except:
            raise errors.DataError

    def _download_cer(self):
        # attempt download
        self._in_context.connectOptions = ''
        init_reply = self._init_diag(True, True)
        return init_reply

    def _old_encrypt_password(self):
        pass

    def _init_diag(self, set_timestamp, download_cert: bool):

        #  get connection

        option_flags1 = CONNECTION.INCONTEXT_OPT1_CLIENT_USERNAME
        option_flags2 = 0
        if set_timestamp:
            option_flags1 |= CONNECTION.INCONTEXT_OPT1_CERTIFICATE_TIMESTAMP

        if self._session_name is not None and len(self._session_name) > 0:
            option_flags1 |= self.INCONTEXT_OPT1_SESSIONNAME

        if self.property.fetch_ahead:
            option_flags1 |= self.INCONTEXT_OPT1_FETCHAHEAD

        self._in_context.connectOptions = ''.encode("utf-8") if download_cert else Convert.turple_to_bytes(self.secpwd.get_cer_exp_date())
        wbuffer = self._marshal_initdialog(self._user_desc,
                                           self._in_context,
                                           self._dialogue_id,
                                           option_flags1,
                                           option_flags2,
                                           "")

        if self._mxosrvr_conn is None:
            self._mxosrvr_conn = self._get_connection(self.mxosrvr_info.server_ip_address, self.mxosrvr_info.server_port)
        data = self.get_from_server(Transport.SRVR_API_SQLCONNECT, wbuffer, self._mxosrvr_conn)

        init_reply = self._extract_mxosrvr_data(data)
        # TODO init connection information to property
        return init_reply

    def _extract_mxosrvr_data(self, data):
        buf_view = memoryview(data)
        c = InitializeDialogueReply()
        c.init_reply(buf_view, self)
        return c

    def _marshal_initdialog(self, _user_desc,
                            _in_context,
                            dialogue_id,
                            option_flags1,
                            option_flags2,
                            session_name,
                            ):
        try:
            wlength = Header.sizeOf()

            client_user = getpass.getuser()
            try:
                wlength += (_in_context.sizeOf()
                            + _user_desc.sizeOf()
                            + Transport.size_int  # dialogueId
                            + Transport.size_int  # option_flags_1
                            + Transport.size_int  # option_flags_2
                            + Transport.size_bytes(session_name.encode("utf-8"))  # sessionBytes
                            + Transport.size_bytes(client_user.encode("utf-8")))
            except:
                raise errors.InternalError("sizeOf error")

            buf = bytearray(b'')

            buf.extend(bytearray(wlength))

            # use memoryview to avoid mem copy
            # remain space for header
            buf_view = memoryview(buf)
            buf_view = buf_view[Header.sizeOf():]
            # construct bytebuffer
            buf_view = _user_desc.insert_into_bytearray(buf_view, little=True)
            buf_view = _in_context.insert_into_bytearray(buf_view, little=True)

            buf_view = Convert.put_int(dialogue_id, buf_view, little=True)
            buf_view = Convert.put_int(option_flags1, buf_view, little=True)
            buf_view = Convert.put_int(option_flags2, buf_view, little=True)

            if (option_flags1 & CONNECTION.INCONTEXT_OPT1_SESSIONNAME) is not 0:
                buf_view = Convert.put_string(session_name, buf_view, little=True)
            if (option_flags1 & CONNECTION.INCONTEXT_OPT1_CLIENT_USERNAME) is not 0:
                buf_view = Convert.put_string(client_user, buf_view, little=True)
            return buf
        except:
            raise errors.InternalError("marshal init dialog error")

    def _get_objref(self):
        self._in_context = self._get_context()
        self._user_desc = self._get_user_desc()
        master_host = self.property.master_host
        master_port = self.property.master_port
        retry_count = self.property.retry_count
        srvr_type = self.property.srvr_type

        wbuffer = self._marshal_getobjref(self._in_context,
                                          self._user_desc,
                                          srvr_type,
                                          retry_count,
                                          0x10000000
                                          )
        master_conn = self._get_connection(master_host, master_port)
        data = self.get_from_server(Transport.AS_API_GETOBJREF, wbuffer, master_conn)
        connect_reply = self._extract_master_data(data)
        if not master_conn:
            #error handle
            pass
        return connect_reply

    def get_from_server(self, operation_id, wbuffer, conn):

        # TODO need compress
        # ...

        totallength = len(wbuffer)
        wheader = Header(operation_id,
                         self._dialogue_id,              # dialogueId,
                         totallength - Header.sizeOf(),  # minus the size of the Header
                         0,                              # cmpLength,
                         'N',                            # compress,
                         '0',                            # compType this should be modify,
                         Header.WRITE_REQUEST_FIRST,
                         Header.SIGNATURE,
                         Header.CLIENT_HEADER_VERSION_BE,
                         Header.PC,
                         Header.TCPIP,
                         Header.NO)

        self._tcp_io_write(wheader, wbuffer, conn)

        #  master return header with Big-Endian while other is Little-Endian
        little = False if operation_id == Transport.AS_API_GETOBJREF else True
        data = self._tcp_io_read(conn, little)

        # TODO handle connect_reply
        return data

    def _extract_master_data(self, data):
        try:
            buf_view = memoryview(data)
            c = ConnectReply()
            c.init_reply(buf_view, self)
        except:
            raise errors.DataError("extract_master_data")
        return c

    def _marshal_getobjref(self,
                           in_context,
                           user_desc,
                           srvr_type,
                           retry_count,
                           option_flags_1,
                           option_flags_2=0,
                           vproc="Traf_pybc_${buildId}",
                           ):
        wlength = Header.sizeOf()

        cc_extention = "\"sessionName\":\"{0}\"," \
                       "\"clientIpAddress\":\"{1}\"," \
                       "\"clientHostName\":\"{2}\"," \
                       "\"userName\":\"{3}\"," \
                       "\"roleName\":\"{4}\"," \
                       "\"applicationName\":\"{5}\"," \
                       "\"tenantName\":\"{6}\"".format(self._session_name,
                                                       "",  # clientIpAddress,
                                                       "",  # clientHostName,
                                                       self.property.userRole,
                                                       self.property.application_name,
                                                       in_context.computerName,
                                                       self.property.tenant_name,
                                                       )

        cc_extention = "{" + cc_extention + "}"
        clientUser = getpass.getuser()

        wlength += (in_context.sizeOf()
                    + user_desc.sizeOf()
                    + Transport.size_int    # srvr_type
                    + Transport.size_short  # retry_count
                    + Transport.size_int    # option_flags_1
                    + Transport.size_int    # option_flags_2
                    + Transport.size_bytes(vproc.encode("utf-8"))
                    + Transport.size_bytes(clientUser.encode("utf-8"))
                    + Transport.size_bytes(cc_extention.encode("utf-8")))
        buf = bytearray(b'')

        buf.extend(bytearray(wlength))

        # use memoryview to avoid mem copy
        # remain space for header
        buf_view = memoryview(buf)
        buf_view = buf_view[Header.sizeOf():]
        # construct bytebuffer
        buf_view = in_context.insert_into_bytearray(buf_view)
        buf_view = user_desc.insert_into_bytearray(buf_view)

        buf_view = Convert.put_int(srvr_type, buf_view)
        buf_view = Convert.put_short(retry_count, buf_view)
        buf_view = Convert.put_int(option_flags_1, buf_view)
        buf_view = Convert.put_int(option_flags_2, buf_view)
        buf_view = Convert.put_string(vproc, buf_view)

        # TODO: restructure all the flags and this new param
        buf_view = Convert.put_string(clientUser, buf_view)
        buf_view = Convert.put_string(cc_extention, buf_view)
        return buf

    def _get_context(self):
        in_context = ConnectionContextDef(self)
        return in_context

    def _get_user_desc(self):
        user_desc = UserDescDef()
        user_desc.userName = self._username
        user_desc.userDescType = \
            Transport.UNAUTHENTICATED_USER_TYPE if self._sessionToken == '' else Transport.PASSWORD_ENCRYPTED_USER_TYPE
        user_desc.domainName = ""

        user_desc.userSid = ''
        user_desc.password = '' # we no longer want to send the password to the MXOAS

        return user_desc

    def get_version(self, process_id):
        majorVersion = 3
        minorVersion = 0
        buildId = 0

        version = [VersionDef(), VersionDef()]

        # Entry[0] is the Driver Version information
        version[0].componentId = 20
        version[0].majorVersion = majorVersion
        version[0].minorVersion = minorVersion
        version[0].buildId = buildId | CONNECTION.ROWWISE_ROWSET | CONNECTION.CHARSET | CONNECTION.PASSWORD_SECURITY

        if self.property.DelayedErrorMode:
            version[0].buildId |= CONNECTION.STREAMING_DELAYEDERROR_MODE

    # Entry[1] is the Application Version information
        version[1].componentId = 8
        version[1].majorVersion = 3
        version[1].minorVersion = 0
        version[1].buildId = 0

        return version

    def _tcp_io_read(self, conn, little=False):
        data = conn.recv(little=little)
        return data

    def _tcp_io_write(self, header, buffer, conn):
        if header.hdr_type_ == Header.WRITE_REQUEST_FIRST:
            buf_view = memoryview(buffer)
            header.insert_into_bytearray(buf_view)
            conn.send(buffer)
        elif header.hdr_type_ == Header.WRITE_REQUEST_NEXT:
            conn.send(buffer)

    def _execute_query(self, query):
        pass

    def commit(self):
        if not self.is_connected():
            raise errors.DatabaseError("Connection not available.")

        if self._auto_commit:
            raise errors.DatabaseError("auto commit has opened")

        self._end_transaction(CONNECTION.SQL_COMMIT)

    def cursor(self, buffered=None, raw=None, prepared=None, cursor_class=None,
               dictionary=None, named_tuple=None):
        """Instantiates and returns a cursor
           By default, TrafCursor is returned. 
           Returns a cursor-object
        """

        if not self.is_connected():
            raise errors.OperationalError("Connection not available.")

        if cursor_class is not None:
            if not issubclass(cursor_class, CursorBase):
                raise errors.ProgrammingError(
                    "Cursor class needs be to subclass of cursor.CursorBase")
            return (cursor_class)(self)

        buffered = buffered if buffered is not None else self._buffered
        raw = raw if raw is not None else self._raw

        cursor_type = 0
        if buffered is True:
            cursor_type |= 1
        if raw is True:
            cursor_type |= 2
        if dictionary is True:
            cursor_type |= 4
        if named_tuple is True:
            cursor_type |= 8
        if prepared is True:
            cursor_type |= 16

        #0: TrafCursor,  # 0
        #1: TrafCursorBuffered,
        #2: TrafCursorRaw,
        #3: TrafCursorBufferedRaw,
        #4: TrafCursorDict,
        #5: TrafCursorBufferedDict,
        #8: TrafCursorNamedTuple,
        #9: TrafCursorBufferedNamedTuple,
        #16:TrafCursorPrepared#
        types = {
            0: TrafCursor,  # 0
        }
        try:
            return (types[cursor_type])(self)
        except KeyError:
            args = ('buffered', 'raw', 'dictionary', 'named_tuple', 'prepared')
            raise ValueError('Cursor not available with given criteria: ' +
                             ', '.join([args[i] for i in range(5)
                                        if cursor_type & (1 << i) != 0]))
        pass

    def is_connected(self):
        """Reports whether the connection to mxosrvr Server is available

           Returns True or False.
        """
        # TODO it is not enough only using connection, ping is needed
        #self.ping()

        if self._connecte_status == 1:
            return True
        return False

    def ping(self, reconnect=False, attempts=1, delay=0):
        pass

    def rollback(self):
        if not self.is_connected():
            raise errors.DatabaseError("Connection not available.")

        self._end_transaction(CONNECTION.SQL_ROLLBACK)

    def get_seq(self):
        self._stmt_name_lock.acquire()
        self._seq_num = self._seq_num + 1
        self._stmt_name_lock.release()
        return self._seq_num

    def close(self):
        if not self.is_connected():
            raise errors.DatabaseError("Connection not available.")

        if not self._auto_commit:
            self.rollback()

        wbuffer = self._marshal_close(self._dialogue_id)

        data = self.get_from_server(Transport.SRVR_API_SQLDISCONNECT, wbuffer, self._mxosrvr_conn)

        buf_view = memoryview(data)
        c = TerminateReply()
        c.init_reply(buf_view)
        if c.return_code == Transport.SQL_SUCCESS:
            self._mxosrvr_conn.close_connection()
            self._connecte_status = 0

    def _marshal_close(self, dialogue_id):
        wlength = Header.sizeOf()
        wlength += Transport.size_int  # dialogue_id

        buf = bytearray(b'')

        buf.extend(bytearray(wlength))

        # use memoryview to avoid mem copy
        # remain space for header
        buf_view = memoryview(buf)
        buf_view = buf_view[Header.sizeOf():]

        buf_view = Convert.put_int(dialogue_id, buf_view, little=True)

        return buf

    def _set_connection_attr(self, connection_option, value_num, value_num_str: str):

        wbuffer = self._marshal_set_connection_attr(self._dialogue_id, connection_option, value_num, value_num_str)
        data = self.get_from_server(Transport.SRVR_API_SQLSETCONNECTATTR, wbuffer, self._mxosrvr_conn)

        buf_view = memoryview(data)
        c = TerminateReply()
        c.init_reply(buf_view)

        # TODO 3196 - NDCS transaction for SPJ
        if connection_option == CONNECTION.SQL_ATTR_JOIN_UDR_TRANSACTION:
            transId_ = int(value_num_str)
            suspendRequest_ = True
        elif connection_option == CONNECTION.SQL_ATTR_SUSPEND_UDR_TRANSACTION:
            transId_ = int(value_num_str)
            suspendRequest_ = True

        return c.return_code

    def _marshal_set_connection_attr(self, dialogue_id, connection_option,
                                     option_value_num, option_value_str: str):
        wlength = Header.sizeOf()

        wlength += Transport.size_int  # dialogue_id
        wlength += Transport.size_short  # connection_option
        wlength += Transport.size_int  # option_value_num
        wlength += Transport.size_bytes(option_value_str.encode())  # option_value_str

        buf = bytearray(b'')

        buf.extend(bytearray(wlength))

        # use memoryview to avoid mem copy
        # remain space for header
        buf_view = memoryview(buf)
        buf_view = buf_view[Header.sizeOf():]

        buf_view = Convert.put_int(dialogue_id, buf_view, little=True)
        buf_view = Convert.put_short(connection_option, buf_view, little=True)
        buf_view = Convert.put_int(option_value_num, buf_view, little=True)
        buf_view = Convert.put_string(option_value_str, buf_view, little=True)

        return buf

    def set_auto_commit(self, auto_commit=True):

        if not self.is_connected():
            raise errors.DatabaseError("Connection not available.")

        save_commit = self._auto_commit
        self._auto_commit = auto_commit

        self._in_context.autoCommit = 1 if auto_commit else 0
        try:
            self._set_connection_attr(CONNECTION.SQL_ATTR_AUTOCOMMIT, self._in_context.autoCommit,
                                      str(self._in_context.autoCommit))
        except Exception:
            self._auto_commit = save_commit
            return False

        return False

    def _end_transaction(self, transaction_opt):
        wbuffer = self._marshal_end_transaction(self._dialogue_id, transaction_opt)
        data = self.get_from_server(Transport.SRVR_API_SQLENDTRAN, wbuffer, self._mxosrvr_conn)

        buf_view = memoryview(data)
        c = EndTransactionReply()
        c.init_reply(buf_view)

    def _marshal_end_transaction(self, dialogue_id, transaction_opt):
        wlength = Header.sizeOf()

        wlength += Transport.size_int  # dialogue_id
        wlength += Transport.size_short  # connection_option

        buf = bytearray(b'')

        buf.extend(bytearray(wlength))

        # use memoryview to avoid mem copy
        # remain space for header
        buf_view = memoryview(buf)
        buf_view = buf_view[Header.sizeOf():]

        buf_view = Convert.put_int(dialogue_id, buf_view, little=True)
        buf_view = Convert.put_short(transaction_opt, buf_view, little=True)

        return buf

    @property
    def dialogue_id(self):
        return self._dialogue_id

    def set_charset(self, charset:str):
        pass
