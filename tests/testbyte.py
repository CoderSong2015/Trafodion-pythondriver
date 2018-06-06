import struct
import  sys
def int_to_byteint( num):
    return struct.pack('>iii', num,7,11)


if __name__ == '__main__':
    num = int_to_byteint(12345)

    print(len(num))
    bv = memoryview(num)

    for i in num:
        print(i)
    print(num[0:4])
    print(struct.unpack('>i',bv[4:8]))

    print(sys.byteorder)
    str = b"\x00\x01\x02\x03\x04\x05\x06\x07\x089"
    print(str[0:4])
    print(str[4:8])

    ss = b'\x03\x00\x00\x00'

    bv = memoryview(ss)
    vv = bv[0:5].tobytes()
    print(vv.decode("utf-8"))


    print(struct.unpack('<i', bv[0:4])[0])
