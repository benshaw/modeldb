# -*- coding: utf-8 -*-

"""ModelDB experiment management."""

from verta._internal_utils import documentation

from ._project import LocalProject as Project


documentation.reassign_module(
    [Project],
    module_name=__name__,
)
