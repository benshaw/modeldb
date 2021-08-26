# -*- coding: utf-8 -*-

from verta._internal_utils import documentation

from ._attributes import AttributesMixin
from ._tags import TagsMixin


documentation.reassign_module(
    [AttributesMixin, TagsMixin],
    module_name=__name__,
)
