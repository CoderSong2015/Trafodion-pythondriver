import sys
sys.path.append("..")
from pdbc.trafodion.connector.TRANSPOT import convert
def mem(mem):
    mem[3] = 22
    mem = mem[5:]
    return mem
def kk(value,x):
    result = {
        'a': lambda x: x * 5,
        'b': lambda x: x + 7,
        'c': lambda x: x - 2
    }[value](x)

    return result
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

    print(kk('b', 3))

