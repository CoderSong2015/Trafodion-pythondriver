import getpass
import threading

from .abstracts import TrafConnectionAbstract
from .struct_def import (
    ConnectReply, USER_DESC_def, CONNECTION_CONTEXT_def,
    VERSION_def, Header, InitializeDialogueReply
)

from .transport import Transport, convert
from . import errors
from . import authentication
from .cursor import CursorBase, TrafCursor


class TrafConnection(TrafConnectionAbstract):
    """
        Connection to a mxosrvr
    """

    def __init__(self, *args, **kwargs):
        self._master_host = '127.0.0.1'
        self._master_port = 23400
        self._force_ipv6 = False
        self.unix_socket = None
        self._sessionToken = None
        self._isReadOnly = False
        self._autoCommit = True
        self._ignoreCancel = False
        self._connection_timeout = 60
        self._dialogue_id = 0
        self._session_name = ''
        self._mxosrvr_conn = None
        self._seq_num = 0
        self._stmt_name_lock = threading.Lock()
        self._buffered = None
        self._raw = None
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
            self.secure_login()
        else:
            self.old_encrypt_password()
            self.init_diag(True , False)
        #TODO self._get_connection() get connection from mxosrvr

        #TODO

    def secure_login(self):
        #  if there is no certificate in local system, downloading
        #  if from mxosrvr first.
        #  now download certificate every time

        proc_info = self.create_proc_info(self.mxosrvr_info.server_process_id,
                                         self.mxosrvr_info.server_node_id,
                                         self.mxosrvr_info.timestamp)

        #  spj_mode = this.t4props_.getSPJEnv() & & this.t4props_.getTokenAuth();

        # TODO init directory and filename in property
        self.secpwd = authentication.SecPwd('', '', False, self.mxosrvr_info.cluster_name, proc_info)
        init_reply = self.download_cer()
        #  TODO save certificate into file
        try:
            self._user_desc.password = self.encrypt_password(init_reply)
        except:
            raise errors.DataError("encrypt error")
        init_reply = self.init_diag(True, False)
        return init_reply

    def create_proc_info(self, server_process_id,
                         server_node_id,
                         timestamp: bytes,
                         )-> bytes:
        proc = server_process_id.to_bytes(length=4, byteorder='little') \
               + server_node_id.to_bytes(length=4, byteorder='little') \
               + timestamp

        return proc

    def encrypt_password(self, init_reply):

        self.secpwd.open_certificate(certificate=init_reply.out_context.certificate)
        #out_context = init_reply.out_context
        role_name = self.property.userRole
        try:
            pwd = self.secpwd.encrypt_pwd(self._password, role_name)
            return pwd
        except:
            raise errors.DataError

    def download_cer(self):
        # attempt download
        self._in_context.connectOptions = ''
        init_reply = self.init_diag(True, True)
        return init_reply

    def old_encrypt_password(self):
        pass

    def init_diag(self, set_timestamp, download_cert:bool):

        #  get connection

        option_flags1 = self.INCONTEXT_OPT1_CLIENT_USERNAME
        option_flags2 = 0
        if set_timestamp:
            option_flags1 |= self.INCONTEXT_OPT1_CERTIFICATE_TIMESTAMP

        if self._session_name is not None and len(self._session_name) > 0:
            option_flags1 |= self.INCONTEXT_OPT1_SESSIONNAME

        if self.property.fetch_ahead:
            option_flags1 |= self.INCONTEXT_OPT1_FETCHAHEAD

        self._in_context.connectOptions = ''.encode("utf-8") if download_cert else convert.turple_to_bytes(self.secpwd.get_cer_exp_date())
        wbuffer = self._marshal_initdialog(self._user_desc,
                                           self._in_context,
                                           self._dialogue_id,
                                           option_flags1,
                                           option_flags2,
                                           "")

        if self._mxosrvr_conn is None:
            self._mxosrvr_conn = self._get_connection(self.mxosrvr_info.server_ip_address, self.mxosrvr_info.server_port)
        data = self._get_from_server(Transport.SRVR_API_SQLCONNECT, wbuffer, self._mxosrvr_conn)
        try:
            init_reply = self._extract_mxosrvr_data(data)
            # TODO init connection information to property
            if init_reply.exception_nr == Transport.CEE_SUCCESS:
                # TODO
                pass
            else:
                pass
            return init_reply
        except:
            raise errors.InternalError("init dialog error")

    def _extract_mxosrvr_data(self, data):
        try:
            buf_view = memoryview(data)
            c = InitializeDialogueReply()
            c.init_reply(buf_view, self)
        except:
            raise errors.InternalError("handle mxosrvr data error")
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
                        + Transport.size_bytes(session_name.encode("utf-8")) #sessionBytes
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
            buf_view = _user_desc.insertIntoByteArray(buf_view, little=True)
            buf_view = _in_context.insertIntoByteArray(buf_view, little=True)

            buf_view = convert.put_int(dialogue_id, buf_view, little=True)
            buf_view = convert.put_int(option_flags1, buf_view, little=True)
            buf_view = convert.put_int(option_flags2, buf_view, little=True)

            if (option_flags1 & self.INCONTEXT_OPT1_SESSIONNAME) is not 0:
                buf_view = convert.put_string(session_name, buf_view, little=True)
            if (option_flags1 & self.INCONTEXT_OPT1_CLIENT_USERNAME) is not 0:
                buf_view = convert.put_string(client_user, buf_view, little=True)
            return buf
        except:
            raise errors.InternalError("marshal init dialog error")

    def _get_objref(self):
        self._in_context = self._get_context()
        self._user_desc = self._get_user_desc()
        self._master_host = self.property.master_host
        self._master_port = self.property.master_port
        retry_count = self.property.retry_count
        srvr_type = self.property.srvr_type

        wbuffer = self._marshal_getobjref(self._in_context,
                                          self._user_desc,
                                          srvr_type,
                                          retry_count,
                                          0x10000000
                                          )
        master_conn = self._get_connection(self._master_host, self._master_port)
        print(master_conn)
        data = self._get_from_server(Transport.AS_API_GETOBJREF, wbuffer, master_conn)
        connect_reply = self._extract_master_data(data)
        if not master_conn:
            #error handle
            pass
        return connect_reply

    def _get_from_server(self, operation_id, wbuffer, conn):

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
            raise errors.DataError(2345)
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
        buf = b''

        clientUser = getpass.getuser()

        wlength += (in_context.sizeOf()
                    + user_desc.sizeOf()
                    + Transport.size_int    # srvr_type
                    + Transport.size_short  # retry_count
                    + Transport.size_int    # option_flags_1
                    + Transport.size_int    # option_flags_2
                    + Transport.size_bytes(vproc.encode("utf-8"))
                    + Transport.size_bytes(clientUser.encode("utf-8")))
        buf = bytearray(b'')

        buf.extend(bytearray(wlength))

        # use memoryview to avoid mem copy
        # remain space for header
        buf_view = memoryview(buf)
        buf_view = buf_view[Header.sizeOf():]
        # construct bytebuffer
        buf_view = in_context.insertIntoByteArray(buf_view)
        buf_view = user_desc.insertIntoByteArray(buf_view)

        buf_view = convert.put_int(srvr_type, buf_view)
        buf_view = convert.put_short(retry_count, buf_view)
        buf_view = convert.put_int(option_flags_1, buf_view)
        buf_view = convert.put_int(option_flags_2, buf_view)
        buf_view = convert.put_string(vproc, buf_view)

        # TODO: restructure all the flags and this new param
        buf_view = convert.put_string(clientUser, buf_view)

        return buf

    def _get_context(self):
        in_context = CONNECTION_CONTEXT_def(self)
        return in_context

    def _get_user_desc(self):
        user_desc = USER_DESC_def()
        user_desc.userName = self._username
        user_desc.userDescType = \
            Transport.UNAUTHENTICATED_USER_TYPE if self._sessionToken == '' else Transport.PASSWORD_ENCRYPTED_USER_TYPE
        user_desc.domainName = ""

        user_desc.userSid = ''
        user_desc.password = '' # we no longer want to send the password to the MXOAS

        return user_desc

    def get_version(self,process_id):
        majorVersion = 3
        minorVersion = 0
        buildId = 0

        version = [VERSION_def(), VERSION_def()]

        # Entry[0] is the Driver Version information
        version[0].componentId = 20
        version[0].majorVersion = majorVersion
        version[0].minorVersion = minorVersion
        version[0].buildId = buildId | self.ROWWISE_ROWSET | self.CHARSET | self.PASSWORD_SECURITY

        if (self.property.DelayedErrorMode):
            version[0].buildId |= self.STREAMING_DELAYEDERROR_MODE

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
            header.insertIntoByteArray(buf_view)
            conn.send(buffer)
        elif header.hdr_type_ == Header.WRITE_REQUEST_NEXT:
            conn.send(buffer)

    def _execute_query(self, query):
        pass

    def commit(self):
        pass

    def cursor(self, buffered=None, raw=None, prepared=None, cursor_class=None,
               dictionary=None, named_tuple=None):
        """Instantiates and returns a cursor

                By default, TrafCursor is returned. 

                Returns a cursor-object
                """
        #self.handle_unread_result()

        if not self.is_connected():
            raise errors.OperationalError("MySQL Connection not available.")
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

    def disconnect(self):
        pass

    def is_connected(self):
        """Reports whether the connection to mxosrvr Server is available

           Returns True or False.
        """
        # TODO it is not enough only using connection, ping is needed
        self.ping()
        return True if self._mxosrvr_conn is not None else False

    def ping(self, reconnect=False, attempts=1, delay=0):
        pass

    def rollback(self):
        pass

    def cmd_query(self, query, execute_type, raw=False, buffered=False, raw_as_string=False):
        """Send a query to the mxosrvr server

        This method send the query to the mxosrvr server and returns the result.

        If there was a text result, a tuple will be returned consisting of
        the number of columns and a list containing information about these
        columns.

        When the query doesn't return a text result, the OK or EOF packet
        information as dictionary will be returned. In case the result was
        an error, exception errors.Error will be raised.

        Returns a tuple()
        """
        if not isinstance(query, bytes):
            query = query.encode('utf-8')

        statement_type = self._get_statement_type(query)
        result = self._send_cmd(statement_type, execute_type, query)
        result = self._handle_result()

        if self._have_next_result:
            raise errors.InterfaceError(
                'Use cmd_query_iter for statements with multiple queries.')

        return result

    def _send_cmd(self, command, argument=None, packet_number=0, packet=None,
                  expect_response=True, compressed_packet_number=0):
        """Send a command to the mxosrvr server

        Returns a mxosrvr packet or None.
        """
        self.handle_unread_result()

        try:
            self._socket.send(
                self._protocol.make_command(command, packet or argument),
                packet_number, compressed_packet_number)
        except AttributeError:
            raise errors.OperationalError("mxosrvr Connection not available.")

        if not expect_response:
            return None
        return self._socket.recv()

    def get_seq(self):
        self._stmt_name_lock.acquire()
        self._seq_num = self._seq_num + 1
        self._stmt_name_lock.release()
        return self._seq_num
