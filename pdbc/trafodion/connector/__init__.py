from .connection import TrafConnection
def connect(*args, **kwargs):
    return TrafConnection(*args, **kwargs)

Connect = connect
