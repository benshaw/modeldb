# -*- coding: utf-8 -*-

import logging

from verta._protos.public.modeldb import ProjectService_pb2

from verta._internal_utils import _utils
from verta.local import _bases, _mixins


logger = logging.getLogger(__name__)


class LocalProject(_bases._LocalEntity, _mixins.AttributesMixin):
    def __init__(self, conn=None, workspace=None, name=None):
        super(LocalProject, self).__init__(conn=conn)
        self._msg = ProjectService_pb2.Project(name=name)
        self._workspace = workspace

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
        if self._msg.workspace_id:
            return self._conn.get_workspace_name_from_legacy_id(
                self._msg.workspace_id,
            )
        elif self._workspace:
            return self._workspace
        else:
            return self._conn.get_default_workspace()

    def save(self):
        endpoint = "/api/v1/modeldb/project/createProject"
        body = ProjectService_pb2.CreateProject(
            name=self._msg.name,
            attributes=self._msg.attributes,
            workspace_name=self._workspace,
        )
        response = self._conn.make_proto_request("POST", endpoint, body=body)
        self._msg = self._conn.must_proto_response(response, body.Response).project
        logger.info(
            'saved project "%s" to workspace "%s"',
            self._msg.name,
            self.workspace,
        )
