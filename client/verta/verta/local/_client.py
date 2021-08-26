# -*- coding: utf-8 -*-

import functools

from verta._internal_utils import connection

from . import registry, tracking


def new_entity(blocked_params=None):
    """Decorator to validate arguments to local subclient's new_*() methods.

    Arguments for these methods are passed directly to the initializers for
    their respective entity objects, but some arguments are supplied
    internally and wouldn't make sense to be overridden by the user.

    """
    if blocked_params is None:
        blocked_params = []
    blocked_params.append("conn")

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


class Client(object):
    def __init__(self, conn=None):
        self._conn = conn or connection.Connection.from_env()

    @new_entity()
    def new_project(self, *args, **kwargs):
        return tracking.Project(*args, conn=self._conn, **kwargs)

    @new_entity()
    def new_registered_model(self, *args, **kwargs):
        return registry.RegisteredModel(*args, conn=self._conn, **kwargs)
