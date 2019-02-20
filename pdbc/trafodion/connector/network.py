import socket

from .struct_def import Header
from . import errors
from .logmodule import PyLog
class BaseTrafSocket(object):
    """Base class for Trafodion socket communication

       TrafSQLTCPSocket
       TrafUnixSocket
    """
    def __init__(self):
        self.sock = None
        self._connection_timeout = None #Set a timeout on blocking socket operations.

    def open_connection(self):
        """ Open connection"""
        raise NotImplementedError

    def close_connection(self):
        try:
            self.sock.shutdown(2)
            self.sock.close()
        except (socket.error, AttributeError):
            raise

    def send(self, buf, *, isCompressed = False):
        if isCompressed:
            self._send_compressed(buf)
        else:
            self._send_all(buf)

    def _send_all(self, buf):
        try:
            self.sock.sendall(buf)
        except:
            raise errors.InternalError("sock error")

    def _send_compressed(self, buf):
        """
            used when buffer need to be compressed
        :param buf: 
        :return: 
        """
        pass

    def recv(self, isCompressed = False, little=False):
        """Receive packets from the mxosrvr server"""
        try:
            # Read the header of the mxosrvr packet, 40 bytes
            packet = bytearray(b'')
            packet_len = 0
            while packet_len < Header.sizeOf():
                chunk = self.sock.recv(Header.sizeOf() - packet_len)
                if not chunk:
                    # needs errors
                    pass
                packet += chunk
                packet_len = len(packet)

            # Get the data length from packet

            hdr = Header()
            hdr.extract_from_bytearray(packet, little)
            datalen = hdr.total_length
            rest = datalen
            packet.extend(bytearray(datalen))

            #use memoryview to avoid mem copy
            packet_view = memoryview(packet)
            # Read the data
            while rest:
                read = self.sock.recv_into(packet_view, rest)
                if read == 0 and rest > 0:
                    pass
                    #need errors
                    #raise errors.InterfaceError(errno=2013)
                packet_view = packet_view[read:]
                rest -= read

            if isCompressed:
                return self._uncompress(packet)
            else:
                return packet
        except IOError as err:
            raise

    def _get_data_len(self, packet):
        return len(packet)

    def _uncompress(self, packet):
        pass

    def _compress(self, packet):
        pass

    def set_connection_timeout(self, connection_timeout):
        self._connection_timeout = connection_timeout


class TrafTCPSocket(BaseTrafSocket):
    """
    Open a TCP/IP connection to the trafodion server
    """

    def __init__(self, host='127.0.0.1', port=23400, force_ipv6 = False):
        super(TrafTCPSocket, self).__init__()
        self.master_host = host
        self.master_port = port
        self.force_ipv6 = force_ipv6

    def open_connection(self):

        #Get address info
        addrinfo = [None] * 5
        try:
            addrinfos = socket.getaddrinfo(self.master_host,
                                           self.master_port,
                                           0,
                                           socket.SOCK_STREAM,
                                           socket.SOL_TCP)
            # If multiple results we favor IPv4, unless IPv6 was forced.
            for info in addrinfos:
                if self.force_ipv6 and info[0] == socket.AF_INET6:
                    addrinfo = info
                    break
                elif info[0] == socket.AF_INET:
                    addrinfo = info
                    break
            if self.force_ipv6 and addrinfo[0] is None:
                #need error
                pass
            if addrinfo[0] is None:
                addrinfo = addrinfos[0]
        except IOError as err:
            raise errors.Error("invalid socket host or port: " + err.strerror)
        else:
            (self._family, self.socktype, self.proto, _, self.sockaddr) = addrinfo

        # Instanciate the socket and connect
        try:
            self.sock = socket.socket(self._family, self.socktype, self.proto)
            self.sock.settimeout(self._connection_timeout)
            self.sock.connect(self.sockaddr)
        except IOError as err:
            PyLog.global_logger.set_error("invalid socket host or port: " + err.strerror)
            raise errors.Error("invalid socket host or port: " + err.strerror)
        except Exception as err:
            raise


class TrafUnixSocket(BaseTrafSocket):
    """
    
    """
    def __init__(self, unix_socket):
        self.unix_socket = unix_socket
    pass
