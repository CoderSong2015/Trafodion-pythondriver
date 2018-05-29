class dd:
    asd = 123
    def pr(self):
        print("aASD=" + str(self.asd))


config = {
    'songhaolin':123,
    'hao':556,
    'lin':'883'
}
dict = {
    'songhaolin':0,
    'lin':''
}


class VERSION_def:
    componentId = 0  # short
    majorVersion = 0  # short
    minorVersion = 0  # short
    buildId = 0  # int

    @classmethod
    def sizeOf(self):
        return 5


class VERSION_LIST_def:
    list = []

    def sizeOf(self):
        print(self.list.__len__())
        print(self.list)
        return VERSION_def.sizeOf() * self.list.__len__() + 6


def get_version(process_id):
        majorVersion = 3
        minorVersion = 0
        buildId = 0

        version = [VERSION_def(), VERSION_def()]

        #Entry[0] is the Driver Version information
        version[0].componentId = 20
        version[0].majorVersion = majorVersion
        version[0].minorVersion = minorVersion
        version[0].buildId = buildId


    # Entry[1] is the Application Version information
        version[1].componentId = 8
        version[1].majorVersion = 3
        version[1].minorVersion = 0
        version[1].buildId = 0

        return version

def mem(mem):
    mem[3] = 22
if __name__ == '__main__':
    xx = dd()
    #xx.asd = 44
    xx.pr()

    for key in config:
        if key in dict:
            dict[key] = config[key]
    print(dict)
    import time
    print(time.time() )
    print(time.localtime())
    print(time.time() + 1000)

    list = [1,3]
    print(list.__len__())
    print(list[0])
    tt = VERSION_LIST_def()
    tt.list.append(VERSION_def())
    print(tt.sizeOf())

    tt = get_version(1)
    print(tt[1].componentId)
    import getpass
    print(getpass.getuser().encode("utf-8"))


    bb = b'haolin'
    buf = bytearray(20)
    pack = memoryview(buf)

    print(len(buf))


    for index, byte in enumerate(bb):
        pack[index + 2] = byte
    print(len(buf))

    import struct
    tt = 3
    print(struct.pack('h',tt))

    mem(pack)
    print(buf)