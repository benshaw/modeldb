# -*- coding: utf-8 -*-

import logging

from verta._protos.public.registry import RegistryService_pb2

from verta._internal_utils import _utils
from verta.local import _bases, _mixins


logger = logging.getLogger(__name__)


class LocalRegisteredModel(_mixins.AttributesMixin, _bases._LocalEntity):
    def __init__(self, conn=None, workspace=None, name=None):
        super(LocalRegisteredModel, self).__init__(conn=conn)

        self._msg = RegistryService_pb2.RegisteredModel(name=name)
        self._workspace = workspace

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

        return "\n    ".join(lines)

    def _create(self):
        endpoint = "/api/v1/registry/workspaces/{}/registered_models".format(
            self.workspace,
        )
        response = self._conn.make_proto_request("POST", endpoint, body=self._msg)
        self._msg = self._conn.must_proto_response(
            response,
            RegistryService_pb2.SetRegisteredModel.Response,
        ).registered_model
        logger.info(
            'created registered model "%s" in workspace "%s"',
            self._msg.name,
            self.workspace,
        )

    def _update(self):
        endpoint = "/api/v1/registry/registered_models/{}".format(
            self.id,
        )
        response = self._conn.make_proto_request("PUT", endpoint, body=self._msg)
        self._msg = self._conn.must_proto_response(
            response,
            RegistryService_pb2.SetRegisteredModel.Response,
        ).registered_model
        logger.info(
            'updated registered model "%s" in workspace "%s"',
            self._msg.name,
            self.workspace,
        )

    @property
    def workspace(self):
        if self._msg.workspace_id:
            return self._conn.get_workspace_name_from_id(
                self._msg.workspace_id,
            )
        elif self._workspace:
            return self._workspace
        else:
            return self._conn.get_default_workspace()
