# -*- coding: utf-8 -*-

import logging

from verta._protos.public.registry import RegistryService_pb2

from verta.local import _bases, _decorators, _mixins
from . import _model_version


logger = logging.getLogger(__name__)


class LocalRegisteredModel(_mixins.AttributesMixin, _bases._LocalEntity):
    def __init__(self, conn=None, workspace=None, name=None, id=None):
        if id and (set(locals().keys()) - {"conn", "id"}):
            raise ValueError("cannot provide other arguments alongside `id`")

        super(LocalRegisteredModel, self).__init__(conn=conn)
        self._workspace = workspace

        if id:
            self._msg = self._get_proto_by_id(self._conn, id)
        else:
            if name:
                self._msg = self._get_proto_by_name(self._conn, name, workspace)
        if self._msg:
            logger.info(
                'opened registered model "%s" in workspace "%s"',
                self._msg.name,
                self.workspace,
            )
        else:
            self._msg = RegistryService_pb2.RegisteredModel(name=name)

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

    @classmethod
    def _get_proto_by_id(cls, conn, id):
        endpoint = "/api/v1/registry/registered_models/{}".format(id)
        response = conn.make_proto_request("GET", endpoint)

        return conn.maybe_proto_response(
            response,
            RegistryService_pb2.GetRegisteredModelRequest.Response,
        ).registered_model

    @classmethod
    def _get_proto_by_name(cls, conn, name, workspace=None):
        endpoint = "/api/v1/registry/workspaces/{}/registered_models/{}".format(
            workspace or conn.get_default_workspace(),
            name,
        )
        response = conn.make_proto_request("GET", endpoint)

        return conn.maybe_proto_response(
            response,
            RegistryService_pb2.GetRegisteredModelRequest.Response,
        ).registered_model

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

    @_decorators.new_entity(blocked_params={"workspace"})
    def new_version(self, *args, **kwargs):
        return _model_version.LocalModelVersion(
            *args,
            conn=self._conn,
            registered_model_name=self._msg.name,
            workspace=self.workspace,
            **kwargs
        )

    @_decorators.open_entity(blocked_params={"registered_model_name", "workspace"})
    def open_version(self, *args, **kwargs):
        # TODO: this whole method is a hack
        if "id" in kwargs:
            msg = _model_version.LocalModelVersion._get_proto_by_id(
                self._conn,
                kwargs["id"],
            )
        else:
            msg = _model_version.LocalModelVersion._get_proto_by_name(
                *args,
                conn=self._conn,
                **kwargs
            )

        if msg:
            return _model_version.LocalModelVersion(
                *args,
                conn=self._conn,
                **kwargs
            )
        raise ValueError("registered model not found")
