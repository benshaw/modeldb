# -*- coding: utf-8 -*-

import functools


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
