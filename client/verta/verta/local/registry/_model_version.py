# -*- coding: utf-8 -*-

import logging

from verta._protos.public.registry import RegistryService_pb2

from verta._internal_utils import _utils, artifact_manager
from verta.local import _bases, _mixins

from . import _registered_model


logger = logging.getLogger(__name__)


class LocalModelVersion(_mixins.AttributesMixin, _bases._LocalArtifactEntity):
    def __init__(self, conn=None, registered_model_name=None, workspace=None, version=None):
        super(LocalModelVersion, self).__init__(conn=conn)

        if registered_model_name is None:
            reg_model = _registered_model.LocalRegisteredModel(
                conn=self._conn,
                workspace=workspace,
            )
            reg_model.save()
        else:
            raise NotImplementedError("TODO: fetch existing reg model")

        self._msg = RegistryService_pb2.ModelVersion(
            registered_model_id=reg_model.id,
            version=version,
        )

    def __repr__(self):
        lines = [type(self).__name__]

        if self._msg.version:
            lines.append("version: {}".format(self._msg.version))

        if self.id:
            lines.append("id: {}".format(self.id))

        if self._msg.attributes:
            lines.append(
                "attributes: {}".format(self.get_attributes())
            )

        if self._msg.artifacts:
            lines.append(
                "artifacts: {}".format(self.get_artifact_keys())
            )

        return "\n    ".join(lines)

    def _create(self):
        endpoint = "/api/v1/registry/registered_models/{}/model_versions".format(
            self._msg.registered_model_id,
        )
        response = self._conn.make_proto_request("POST", endpoint, body=self._msg)
        self._msg = self._conn.must_proto_response(
            response,
            RegistryService_pb2.SetModelVersion.Response,
        ).model_version
        logger.info(
            'created model version "%s"',
            self._msg.version,
        )

    def _update(self):
        endpoint = "/api/v1/registry/registered_models/{}/model_versions/{}".format(
            self._msg.registered_model_id,
            self.id,
        )
        response = self._conn.make_proto_request("PUT", endpoint, body=self._msg)
        self._msg = self._conn.must_proto_response(
            response,
            RegistryService_pb2.SetModelVersion.Response,
        ).model_version
        logger.info(
            'updated model version "%s"',
            self._msg.version,
        )

    @property
    def workspace(self):
        raise NotImplementedError("TODO: fetch existing reg model")
        return reg_model.workspace

    def _get_artifact_resolver(self, key):
        return _ModelVersionArtifactResolver(self._conn, self.id, key)


class _ModelVersionArtifactResolver(artifact_manager.ArtifactResolver):
    @property
    def _get_url_endpoint(self):
        return "/api/v1/registry/model_versions/{}/getUrlForArtifact".format(
            self._entity_id,
        )

    @property
    def _get_url_msg(self):
        return RegistryService_pb2.GetUrlForArtifact(
            model_version_id=self._entity_id,
            key=self._artifact_key,
        )

    @property
    def _commit_part_endpoint(self):
        return "/api/v1/registry/model_versions/{}/commitArtifactPart".format(
            self._entity_id,
        )

    @property
    def _commit_part_msg(self):
        return RegistryService_pb2.CommitArtifactPart(
            model_version_id=self._entity_id,
            key=self._artifact_key,
        )

    @property
    def _commit_artifact_endpoint(self):
        return "/api/v1/registry/model_versions/{}/commitMultipartArtifact".format(
            self._entity_id,
        )

    @property
    def _commit_artifact_msg(self):
        return RegistryService_pb2.CommitMultipartArtifact(
            model_version_id=self._entity_id,
            key=self._artifact_key,
        )
