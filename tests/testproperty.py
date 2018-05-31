class property:


    def __init__(self):
        self._catalog = 'buhao'
        self.testnull = ''
    @property
    def catalog(self):
        return self._catalog

    @catalog.setter
    def catalog(self, str):
        self._catalog = str


if __name__ == '__main__':
    #property.catalog = 'nihao'
    myproperty = property()
    print(myproperty.catalog)
    if not myproperty.testnull:
        print("null")
    else:
        print("not null")

    tt = "dwqdwefcqew3我"
    print(len(tt))
    print(len(tt.encode("utf-8")))