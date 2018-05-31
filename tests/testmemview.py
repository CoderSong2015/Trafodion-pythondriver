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

    wlength = 40
    buf = bytearray(b'')

    buf.extend(bytearray(wlength))

    print(len(buf))
    # use memoryview to avoid mem copy
    # remain space for header
    buf_view = memoryview(buf)
    buf_view[5] = 23
    print(buf)
    buf_view = buf_view[5:]
    buf_view[5] = 23
    print(buf)