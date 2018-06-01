import struct
import  sys
def int_to_byteint( num):
    return struct.pack('>i', num)


if __name__ == '__main__':
    for b in int_to_byteint(12345):
        print(b)


    print(0x3930)

    print(sys.byteorder)