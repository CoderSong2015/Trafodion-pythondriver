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
    mem += 2
if __name__ == '__main__':
    """
     from tests import dicttest
    for k, v in dicttest.__dict__.items():
        if 9 == v:
            print(k)
    """
    import os
    print(os.environ['HOMEDRIVE'])
    env = os.environ
    if "HOMEDRVE" in env:
        print(env['HOMEDRIVE'])
    elif "xx" in env:
        print("ok")
    else:
        print("nihao")
    tt = ''
    if not tt:
        print("yes")
"""
    print(env['HOMEPATH'])
    print(os.sep)
    dic = env['HOMEDRIVE'] + os.sep + env['HOMEPATH'] + os.sep +"xxxxxxxxxx"
    print(dic)
    with open(dic, 'w'):
        pass


    print("ddd")
"""



