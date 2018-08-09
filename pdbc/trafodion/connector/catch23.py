import sys

PY2 = sys.version_info[0] == 2


def make_abc(base_class):
    """Decorator used to create a abstract base class

    We use this decorator to create abstract base classes instead of
    using the abc-module. The decorator makes it possible to do the
    same in both Python v2 and v3 code.
    This is from 
    """

    def wrapper(class_):
        """Wrapper"""
        attrs = class_.__dict__.copy()
        for attr in '__dict__', '__weakref__':
            attrs.pop(attr, None)  # ignore missing attributes

        bases = class_.__bases__
        if PY2:
            attrs['__metaclass__'] = class_
        else:
            bases = (class_,) + bases
        return base_class(class_.__name__, bases, attrs)
    return wrapper