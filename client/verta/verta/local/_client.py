# -*- coding: utf-8 -*-

from verta._internal_utils import connection

from . import registry, tracking
from . import _decorators


class Client(object):
    def __init__(self, conn=None):
        self._conn = conn or connection.Connection.from_env()

    @_decorators.new_entity()
    def new_project(self, *args, **kwargs):
        return tracking.Project(*args, conn=self._conn, **kwargs)

    @_decorators.new_entity()
    def new_registered_model(self, *args, **kwargs):
        return registry.RegisteredModel(*args, conn=self._conn, **kwargs)
