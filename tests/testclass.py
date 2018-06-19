class test1:
    class test2:
        def __init__(self):
            self.id = bytearray(10)








if __name__ == '__main__':
    ss = test1.test2()
    ss.id[1] = 1
    print(ss.id)
    tt = (48, 50, 54)
    s = ''
    for x in tt:
        s = s + chr(x)
    print(s.encode())
