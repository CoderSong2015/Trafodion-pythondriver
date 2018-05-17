
"""Module gathering all abstract base classes"""
from abc import ABCMeta, abstractmethod, abstractproperty
from .catch23 import make_abc




@make_abc(ABCMeta)
class TrafConnectionAbstract(object):
    """Abstract class for classes connecting to a Trafodion server"""

    def __init__(self, **kwargs):
        self.user