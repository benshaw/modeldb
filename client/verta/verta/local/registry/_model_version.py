# -*- coding: utf-8 -*-

import logging

from google.protobuf import struct_pb2
from verta._protos.public.common import CommonService_pb2
from verta._protos.public.registry import RegistryService_pb2

from verta._internal_utils import artifact_manager
from verta.local import _bases, _mixins

from . import _registered_model


logger = logging.getLogger(__name__)


class LocalModelVersion(_mixins.AttributesMixin, _bases._LocalArtifactEntity):
    def __init__(
        self,
        conn=None,
        registered_model_name=None,
        workspace=None,
        version=None,
        id=None,
    ):
        if id and (set(locals().keys()) - {"conn", "id"}):
            raise ValueError("cannot provide other arguments alongside `id`")

        super(LocalModelVersion, self).__init__(conn=conn)

        if id:
            self._msg = self._get_proto_by_id(self._conn, id)
        else:
            if registered_model_name:
                _reg_model = _registered_model.LocalRegisteredModel._get_proto_by_name(
                    self._conn,
                    registered_model_name,
                    workspace,
                )
            else:
                _reg_model = _registered_model.LocalRegisteredModel(
                    conn=self._conn,
                    workspace=workspace,
                )
                _reg_model.save()
            reg_model_id = _reg_model.id

            if version:
                self._msg = self._get_proto_by_name(self._conn, version, reg_model_id)
        if self._msg:
            logger.info(
                'opened model version "%s"',
                self._msg.version,
            )
        else:
            self._msg = RegistryService_pb2.ModelVersion(
                registered_model_id=reg_model_id,
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

    @classmethod
    def _get_proto_by_id(cls, conn, id):
        endpoint = "/api/v1/registry/model_versions/{}".format(id)
        response = conn.make_proto_request("GET", endpoint)

        return conn.maybe_proto_response(
            response,
            RegistryService_pb2.GetModelVersionRequest.Response,
        ).model_version

    @classmethod
    def _get_proto_by_name(cls, conn, version, registered_model_id):
        endpoint = "/api/v1/registry/registered_models/{}/model_versions/find".format(
            registered_model_id
        )
        msg = RegistryService_pb2.FindModelVersionRequest(
            predicates=[
                CommonService_pb2.KeyValueQuery(
                    key="version",
                    value=struct_pb2.Value(string_value=version),
                    operator=CommonService_pb2.OperatorEnum.EQ,
                ),
            ],
        )
        proto_response = conn.make_proto_request("POST", endpoint, body=msg)
        response = conn.maybe_proto_response(proto_response, msg.Response)

        if not response.model_versions:
            return None
        # should only have 1 entry here, as name/version is unique
        return response.model_versions[0]

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
        workspace_id = _registered_model.LocalRegisteredModel._get_proto_by_id(
            self._conn,
            self._msg.registered_model_id,
        ).workspace_id
        return self._conn.get_workspace_name_from_id(workspace_id)

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
