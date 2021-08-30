# -*- coding: utf-8 -*-

import abc

from verta.external import six


@six.add_metaclass(abc.ABCMeta)
class UndoRedoMixin(object):
    def __init__(self):
        self._command_history = []
        self._command_cursor = -1

    def command_history(self):
        return tuple(self._command_history)

    @abc.abstractmethod
    def _apply_commands(self):
        raise NotImplementedError

    def undo(self):
        if self._command_cursor == -1:
            raise RuntimeError("no commands to undo")

        self._command_cursor -= 1

    def redo(self):
        if self._command_cursor == len(self._command_history) - 1:
            raise RuntimeError("no commands to redo")

        self._command_cursor += 1


@six.add_metaclass(abc.ABCMeta)
class Action(object):
    def __init__(self, field, key):
        self._field = field
        self._key = key

    def __repr__(self):
        return "<{} {} {}>".format(
            type(self).__name__,
            self._field,
            self._key,
        )


class Add(Action):
    def __init__(self, field, key, value):
        super(Add, self).__init__(field, key)
        self._value = value


class Delete(Action):
    pass
