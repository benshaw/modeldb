# -*- coding: utf-8 -*-

import functools

from verta._internal_utils import connection

from . import tracking


def new_entity(f):
    """Decorator to validate arguments to new_*() methods.

    Arguments for these methods are passed directly to the initializers for
    their respective entity objects, but some arguments are supplied
    internally and wouldn't make sense to be overridden by the user.

    """
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        for param_name in kwargs:
            if (param_name == "conn"
                    or param_name.endswith("_name")):
                raise TypeError(
                    "`{}` cannot be provided as an argument".format(param_name)
                )
        return f(self, *args, **kwargs)
    return wrapper


class Client(object):
    def __init__(self, conn=None):
        self._conn = conn or connection.Connection.from_env()

    @new_entity
    def new_project(self, *args, **kwargs):
        return tracking.Project(*args, conn=self._conn, **kwargs)
