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

    print(pack.ndim)
    print(pack)
    pack = pack[3:4]
    print(pack)
    pack[0] = 3
    print(buf)
