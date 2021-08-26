# -*- coding: utf-8 -*-

from verta._internal_utils import documentation

from ._attributes import AttributesMixin
from ._registry_labels import RegistryLabelsMixin
from ._tags import TagsMixin


documentation.reassign_module(
    [AttributesMixin, RegistryLabelsMixin, TagsMixin],
    module_name=__name__,
)
