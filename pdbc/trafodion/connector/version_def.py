class VERSION_def:
    componentId = 0   #short
    majorVersion= 0   #short
    minorVersion= 0   #short
    buildId     = 0   #int

class VERSION_LIST_def:
    versionList = [VERSION_def()];

    def sizeof(self):
        pass
