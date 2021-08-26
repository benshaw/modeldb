# -*- coding: utf-8 -*-

"""Registry model management."""

from verta._internal_utils import documentation

from ._model_version import LocalModelVersion as ModelVersion
from ._registered_model import LocalRegisteredModel as RegisteredModel


documentation.reassign_module(
    [ModelVersion, RegisteredModel],
    module_name=__name__,
)
