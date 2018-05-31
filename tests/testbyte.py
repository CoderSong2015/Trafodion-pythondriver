import struct
def int_to_byteint( num):
    return struct.pack('!i', num)


if __name__ == '__main__':
    print(int_to_byteint(12345).hex())
    print(0x3930)