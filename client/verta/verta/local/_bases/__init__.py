# -*- coding: utf-8 -*-

"""Cross-module base classes."""

from verta._internal_utils import documentation

from ._local_entity import _LocalEntity


documentation.reassign_module(
    [_LocalEntity],
    module_name=__name__,
)
