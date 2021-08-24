# -*- coding: utf-8 -*-

import logging

from verta._protos.public.modeldb import ExperimentService_pb2

from verta._internal_utils import _utils
from verta.local import _bases, _mixins

from . import _project


logger = logging.getLogger(__name__)


class LocalExperiment(_bases._LocalEntity, _mixins.AttributesMixin):
    def __init__(self, conn=None, project_name=None, workspace=None, name=None):
        super(LocalExperiment, self).__init__(conn=conn)

        if project_name is None:
            proj = _project.LocalProject(conn=conn, workspace=workspace)
            proj.save()
        else:
            raise NotImplementedError("TODO: fetch existing proj")

        self._msg = ExperimentService_pb2.Experiment(
            project_id=proj.id,
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
                "attributes: {}".format(_utils.unravel_key_values(self._msg.attributes))
            )

        return "\n    ".join(lines)

    @property
    def workspace(self):
        raise NotImplementedError("TODO: fetch existing proj")
        return proj.workspace

    def save(self):
        endpoint = "/api/v1/modeldb/experiment/createExperiment"
        body = ExperimentService_pb2.CreateExperiment(
            project_id=self._msg.project_id,
            name=self._msg.name,
            attributes=self._msg.attributes,
        )
        response = self._conn.make_proto_request("POST", endpoint, body=body)
        self._msg = self._conn.must_proto_response(response, body.Response).experiment
        logger.info(
            'saved experiment "%s"',
            self._msg.name,
        )
