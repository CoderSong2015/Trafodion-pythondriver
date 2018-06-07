

FGREG = 9
ZXCVF = 1
VFDA = 34

import sys
sys.path.append("..")
from pdbc.trafodion.connector import errors

if True:
    raise  errors.NotSupportedError(errno=2009)