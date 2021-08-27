# -*- coding: utf-8 -*-

import functools


class abstractclassmethod(classmethod):  # https://stackoverflow.com/a/11218474

    __isabstractmethod__ = True

    def __init__(self, callable):
        callable.__isabstractmethod__ = True
        super(abstractclassmethod, self).__init__(callable)


def new_entity(blocked_params=None):
    """Decorator to validate arguments to local subclient's new_*() methods.

    Arguments for these methods are passed directly to the initializers for
    their respective entity objects, but some arguments are supplied
    internally and wouldn't make sense to be overridden by the user.

    """
    if blocked_params is None:
        blocked_params = set()
    blocked_params.update({
        "conn",
        "id",
    })

    def decorator(f):
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            for param_name in kwargs:
                if ((param_name in blocked_params)
                        or param_name.endswith("_name")):
                    raise TypeError(
                        "`{}` cannot be provided as an argument".format(param_name)
                    )
            return f(self, *args, **kwargs)
        return wrapper
    return decorator


def open_entity(blocked_params=None):
    """Decorator to validate arguments to local subclient's open_*() methods.

    Arguments for these methods are passed directly to the initializers for
    their respective entity objects, but some arguments wouldn't make sense
    for an existing entity.

    """
    if blocked_params is None:
        blocked_params = set()
    blocked_params = set(blocked_params)
    blocked_params.add("conn")

    def decorator(f):
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            for param_name in kwargs:
                if param_name in blocked_params:
                    raise TypeError(
                        "`{}` cannot be provided as an argument".format(param_name)
                    )
            return f(self, *args, **kwargs)
        return wrapper
    return decorator
