class test1:
    class test2:
        def __init__(self):
            self.id = bytearray(10)



def _send_cmd(command, argument=None, packet_number=0, packet=None,expect_response=True, compressed_packet_number=0):
     print(command)
     print(argument)
class ServerCmd():
    """MySQL Server Commands
    """
    _prefix = 'COM_'
    SLEEP = 0
    QUIT = 1
    INIT_DB = 2
    QUERY = 3
    FIELD_LIST = 4
    CREATE_DB = 5
    DROP_DB = 6
    REFRESH = 7
    SHUTDOWN = 8
    STATISTICS = 9
    PROCESS_INFO = 10
    CONNECT = 11
    PROCESS_KILL = 12
    DEBUG = 13
    PING = 14
    TIME = 15
    DELAYED_INSERT = 16
    CHANGE_USER = 17
    BINLOG_DUMP = 18
    TABLE_DUMP = 19
    CONNECT_OUT = 20
    REGISTER_SLAVE = 21
    STMT_PREPARE = 22
    STMT_EXECUTE = 23
    STMT_SEND_LONG_DATA = 24
    STMT_CLOSE = 25
    STMT_RESET = 26
    SET_OPTION = 27
    STMT_FETCH = 28
    DAEMON = 29
    BINLOG_DUMP_GTID = 30
    RESET_CONNECTION = 31

    desc = {
        'SLEEP': (0, 'SLEEP'),
        'QUIT': (1, 'QUIT'),
        'INIT_DB': (2, 'INIT_DB'),
        'QUERY': (3, 'QUERY'),
        'FIELD_LIST': (4, 'FIELD_LIST'),
        'CREATE_DB': (5, 'CREATE_DB'),
        'DROP_DB': (6, 'DROP_DB'),
        'REFRESH': (7, 'REFRESH'),
        'SHUTDOWN': (8, 'SHUTDOWN'),
        'STATISTICS': (9, 'STATISTICS'),
        'PROCESS_INFO': (10, 'PROCESS_INFO'),
        'CONNECT': (11, 'CONNECT'),
        'PROCESS_KILL': (12, 'PROCESS_KILL'),
        'DEBUG': (13, 'DEBUG'),
        'PING': (14, 'PING'),
        'TIME': (15, 'TIME'),
        'DELAYED_INSERT': (16, 'DELAYED_INSERT'),
        'CHANGE_USER': (17, 'CHANGE_USER'),
        'BINLOG_DUMP': (18, 'BINLOG_DUMP'),
        'TABLE_DUMP': (19, 'TABLE_DUMP'),
        'CONNECT_OUT': (20, 'CONNECT_OUT'),
        'REGISTER_SLAVE': (21, 'REGISTER_SLAVE'),
        'STMT_PREPARE': (22, 'STMT_PREPARE'),
        'STMT_EXECUTE': (23, 'STMT_EXECUTE'),
        'STMT_SEND_LONG_DATA': (24, 'STMT_SEND_LONG_DATA'),
        'STMT_CLOSE': (25, 'STMT_CLOSE'),
        'STMT_RESET': (26, 'STMT_RESET'),
        'SET_OPTION': (27, 'SET_OPTION'),
        'STMT_FETCH': (28, 'STMT_FETCH'),
        'DAEMON': (29, 'DAEMON'),
        'BINLOG_DUMP_GTID': (30, 'BINLOG_DUMP_GTID'),
        'RESET_CONNECTION': (31, 'RESET_CONNECTION'),
    }


class testa:
    nihaoma = 123
    def __init__(self):
        dqwdf =3
        self.song = 1
        self.hao = '123'

    def print(self):
        for name, value in vars(self).items():
            print('%s=%s' % (name, value))

def testlist(mlist):
    mlist.append(7778)

if __name__ == '__main__':
    ss = test1.test2()
    ss.id[1] = 1
    print(ss.id)
    tt = (48, 50, 54)
    s = ''
    for x in tt:
        s = s + chr(x)
    print(s.encode())

    ss = "select *".encode()
    qq = "select *"
    first_name = ss.split(" ".encode())
    ff = qq.split(" ")
    print(ff[0].upper())
    type_dict = {
        "SELECT": 4,
        "WITH": 3,
        "SHOWSHAPE": 2,
    }
    print(type_dict[first_name[0].decode().upper()])

    _send_cmd(ServerCmd.QUERY,"queryddd")

    ss = 3
    tt = type(ss)
    print(tt)

    s = testa()
    s.print()

    rr = []
    testlist(rr)
    print(isinstance(s, int))
    print(rr)