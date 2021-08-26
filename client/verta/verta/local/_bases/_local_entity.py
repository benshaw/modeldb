# -*- coding: utf-8 -*-

import abc

from verta.external import six

from verta._internal_utils import connection


@six.add_metaclass(abc.ABCMeta)
class _LocalEntity(object):
    def __init__(self, conn=None):
        self._conn = conn or connection.Connection.from_env()
        self._msg = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if all(arg is None for arg in [exc_type, exc_value, traceback]):
            self.save()  # TODO: flag to save on exception

    @abc.abstractmethod
    def __repr__(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _create(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _update(self):
        raise NotImplementedError

    @property
    def id(self):
        return self._msg.id

    @abc.abstractproperty
    def workspace(self):
        raise NotImplementedError

    def save(self):
        if self.id:
            self._update()
        else:
            self._create()
