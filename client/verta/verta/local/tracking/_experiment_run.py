# -*- coding: utf-8 -*-

import logging

from verta._protos.public.modeldb import (
    CommonService_pb2,
    ExperimentRunService_pb2,
)

from verta._internal_utils import _utils, artifact_manager
from verta.local import _bases, _mixins

from . import _experiment


logger = logging.getLogger(__name__)


class LocalExperimentRun(_mixins.AttributesMixin, _mixins.TagsMixin, _bases._LocalArtifactEntity):
    def __init__(self, conn=None, project_name=None, experiment_name=None, workspace=None, name=None):
        super(LocalExperimentRun, self).__init__(conn=conn)

        if experiment_name is None:
            expt = _experiment.LocalExperiment(
                conn=conn,
                project_name=project_name,
                workspace=workspace,
            )
            expt.save()
        else:
            raise NotImplementedError("TODO: fetch existing expt")
            raise NotImplementedError("TODO: validate under proj")

        self._msg = ExperimentRunService_pb2.ExperimentRun(
            project_id=expt._msg.project_id,
            experiment_id=expt.id,
            name=name,
        )

    def __repr__(self):
        lines = [type(self).__name__]

        if self._msg.name:
            lines.append("name: {}".format(self._msg.name))

        if self.id:
            lines.append("id: {}".format(self.id))

        if self._msg.attributes:
            lines.append(
                "attributes: {}".format(self.get_attributes())
            )

        if self._msg.tags:
            lines.append(
                "tags: {}".format(self.get_tags())
            )

        if self._msg.artifacts:
            lines.append(
                "artifacts: {}".format(self.get_artifact_keys())
            )

        return "\n    ".join(lines)

    def _create(self):
        endpoint = "/api/v1/modeldb/experiment-run/createExperimentRun"
        body = ExperimentRunService_pb2.CreateExperimentRun(
            project_id=self._msg.project_id,
            experiment_id=self._msg.experiment_id,
            name=self._msg.name,
            tags=self._msg.tags,
            attributes=self._msg.attributes,
            artifacts=self._msg.artifacts,
        )
        response = self._conn.make_proto_request("POST", endpoint, body=body)
        self._msg = self._conn.must_proto_response(response, body.Response).experiment_run
        logger.info(
            'created experiment run "%s"',
            self._msg.name,
        )

    def _update(self):
        raise NotImplementedError

    @property
    def workspace(self):
        raise NotImplementedError("TODO: fetch existing proj")
        return proj.workspace

    def _get_artifact_resolver(self, key):
        return _ExperimentRunArtifactResolver(self._conn, self.id, key)


class _ExperimentRunArtifactResolver(artifact_manager.ArtifactResolver):
    @property
    def _get_url_endpoint(self):
        return "/api/v1/modeldb/experiment-run/getUrlForArtifact"

    @property
    def _get_url_msg(self):
        return CommonService_pb2.GetUrlForArtifact(
            id=self._entity_id,
            key=self._artifact_key,
        )

    @property
    def _commit_part_endpoint(self):
        return "/api/v1/modeldb/experiment-run/commitArtifactPart"

    @property
    def _commit_part_msg(self):
        return CommonService_pb2.CommitArtifactPart(
            id=self._entity_id,
            key=self._artifact_key,
        )

    @property
    def _commit_artifact_endpoint(self):
        return "/api/v1/modeldb/experiment-run/commitMultipartArtifact"

    @property
    def _commit_artifact_msg(self):
        return CommonService_pb2.CommitMultipartArtifact(
            id=self._entity_id,
            key=self._artifact_key,
        )
