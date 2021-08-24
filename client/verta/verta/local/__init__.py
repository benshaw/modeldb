# -*- coding: utf-8 -*-

"""Experimental API for local iteration of entities.

.. versionadded:: 0.20.0

.. note::

    This API is only compatible with Verta Enterprise.

"""

from verta._internal_utils import documentation

from ._client import Client


documentation.reassign_module(
    [Client],
    module_name=__name__,
)
