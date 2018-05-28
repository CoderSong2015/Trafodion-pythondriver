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