import sys
sys.path.append("..")
from pdbc.trafodion.connector.TRANSPOT import convert
def mem(mem):
    mem[3] = 22
    mem = mem[5:]
    return mem

if __name__ == '__main__':
    bb = b'haolin'
    buf = bytearray(20)
    pack = memoryview(buf)

    print(pack)
    pack = mem(pack)
    print()
    print(len(buf))
    print(len(pack))
    con = convert()
    con.put_string("cccd",pack)
    print(buf)