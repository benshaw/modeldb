# -*- coding: utf-8 -*-

"""Cross-module base classes."""

from verta._internal_utils import documentation

from ._local_artifact_entity import _LocalArtifactEntity
from ._local_entity import _LocalEntity


documentation.reassign_module(
    [_LocalArtifactEntity, _LocalEntity],
    module_name=__name__,
)
