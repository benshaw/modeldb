# -*- coding: utf-8 -*-

import logging
import os
import re

import requests
from urllib3.util.retry import Retry

from verta.external.six.moves.urllib.parse import urlparse  # pylint: disable=import-error, no-name-in-module

from verta._protos.public.uac import Organization_pb2, UACService_pb2, Workspace_pb2

from . import _utils


logger = logging.getLogger(__name__)


_VERTA_HOST_ENV_VAR = "VERTA_HOST"
_VERTA_EMAIL_ENV_VAR = "VERTA_EMAIL"
_VERTA_DEV_KEY_ENV_VAR = "VERTA_DEV_KEY"
_GRPC_PREFIX = "Grpc-Metadata-"


class Connection:
    _OSS_DEFAULT_WORKSPACE = "personal"

    def __init__(
        self,
        scheme=None,
        socket=None,
        auth=None,
        max_retries=5,
        ignore_conn_err=False,
    ):
        """
        HTTP connection configuration utility struct.

        Parameters
        ----------
        scheme : {'http', 'https'}, optional
            HTTP authentication scheme.
        socket : str, optional
            Hostname and port.
        auth : dict, optional
            Verta authentication headers.
        max_retries : int, default 0
            Maximum number of times to retry a request on a connection failure. This only attempts retries
            on HTTP codes {502, 503, 504} which commonly occur during back end connection lapses.
        ignore_conn_err : bool, default False
            Whether to ignore connection errors and instead return successes with empty contents.

        """
        self.scheme = scheme
        self.socket = socket
        self.auth = auth
        # TODO: retry on 404s, but only if we're sure it's not legitimate e.g. from a GET
        self.retry = Retry(
            total=max_retries,
            backoff_factor=1,  # each retry waits (2**retry_num) seconds
            method_whitelist=False,  # retry on all HTTP methods
            status_forcelist=(502, 503, 504),  # only retry on these status codes
            raise_on_redirect=False,  # return Response instead of raising after max retries
            raise_on_status=False,  # return Response instead of raising after max retries
        )
        self.ignore_conn_err = ignore_conn_err

    @classmethod
    def from_env(cls, verify_connection=True):
        # TODO: consolidate with Client.__init__()
        # TODO: reconcile config file
        verta_url = urlparse(os.environ[_VERTA_HOST_ENV_VAR])
        logger.info("set host from environment")
        socket = verta_url.netloc + verta_url.path.rstrip('/')
        scheme = verta_url.scheme or ("https" if ".verta.ai" in socket else "http")
        logger.debug("using Verta URL: %s://%s/", scheme, socket)

        auth = {
            _GRPC_PREFIX + "source": "PythonClient",
            _GRPC_PREFIX + "scheme": scheme,
        }
        email = os.environ.get(_VERTA_EMAIL_ENV_VAR)
        if email:
            auth[_GRPC_PREFIX + "email"] = email
            logger.info("set email from environment")
            logger.debug("using email: %s", email)
        dev_key = os.environ.get(_VERTA_DEV_KEY_ENV_VAR)
        if dev_key:
            auth[_GRPC_PREFIX + "developer-key"] = dev_key
            logger.info("set developer key from environment")
            logger.debug(
                "using developer key: %s",
                dev_key[:8] + re.sub(r"[^-]", '*', dev_key[8:]),
            )

        conn = cls(scheme, socket, auth)
        if verify_connection:
            conn.verify_connection()
        return conn

    def verify_connection(self):
        # TODO: call this in Client.__init__()
        url = "{}://{}/api/v1/modeldb/project/verifyConnection".format(
            self.scheme,
            self.socket,
        )
        try:
            response = _utils.make_request("GET", url, self)
        except requests.ConnectionError as e:
            e_msg = (
                "connection failed; please check `host` and `port`\n\n"
                + e.args[0]
            )
            e.args = (e_msg,) + e.args[1:]
            raise e

        try:
            self.must_response(response)
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                e_msg = (
                    "authentication failed; please check `VERTA_EMAIL` and `VERTA_DEV_KEY`\n\n"
                    + e.args[0]
                )
                e.args = (e_msg,) + e.args[1:]
            raise e

        logger.info("connection successfully established")

    def make_proto_request(
        self,
        method,
        path,
        params=None,
        body=None,
        include_default=True,
    ):
        if params is not None:
            params = _utils.proto_to_json(params)
        if body is not None:
            body = _utils.proto_to_json(body, include_default)
        response = _utils.make_request(
            method,
            "{}://{}{}".format(self.scheme, self.socket, path),
            self,
            params=params,
            json=body,
        )

        return response

    @staticmethod
    def maybe_proto_response(response, response_type):
        if response.ok:
            response_msg = _utils.json_to_proto(
                _utils.body_to_json(response),
                response_type,
            )
            return response_msg
        else:
            if (
                response.status_code == 403
                and _utils.body_to_json(response)["code"] == 7
            ) or (
                response.status_code == 404
                and _utils.body_to_json(response)["code"] == 5
            ):
                return _utils.NoneProtoResponse()
            else:
                _utils.raise_for_http_error(response)

    @staticmethod
    def must_proto_response(response, response_type):
        if response.ok:
            response_msg = _utils.json_to_proto(
                _utils.body_to_json(response),
                response_type,
            )
            return response_msg
        else:
            _utils.raise_for_http_error(response)

    @staticmethod
    def must_response(response):
        _utils.raise_for_http_error(response)

    @staticmethod
    def _request_to_curl(request):
        """
        Prints a cURL to reproduce `request`.

        Parameters
        ----------
        request : :class:`requests.PreparedRequest`

        Examples
        --------
        From a :class:`~requests.Response`:

        .. code-block:: python

            response = _utils.make_request("GET", "https://www.google.com/", conn)
            conn._request_to_curl(response.request)

        From a :class:`~requests.HTTPError`:

        .. code-block:: python

            try:
                pass  # insert bad call here
            except Exception as e:
                client._conn._request_to_curl(e.request)
                raise

        """
        lines = []
        lines.append('curl -X {} "{}"'.format(request.method, request.url))
        if request.headers:
            lines.extend(
                '-H "{}: {}"'.format(key, val) for key, val in request.headers.items()
            )
        if request.body:
            lines.append("-d '{}'".format(request.body.decode()))

        curl = " \\\n    ".join(lines)
        print(curl)

    @staticmethod
    def is_html_response(response):
        return response.text.strip().endswith("</html>")

    @property
    def email(self):
        return self.auth.get(_GRPC_PREFIX + "email")

    def _get_visible_orgs(self):
        response = self.make_proto_request(
            "GET",
            "/api/v1/uac-proxy/workspace/getVisibleWorkspaces",
        )
        response = self.must_proto_response(response, Workspace_pb2.Workspaces)

        org_names = map(lambda workspace: workspace.org_name, response.workspace)
        org_names = filter(None, org_names)
        return list(org_names)

    def _set_default_workspace(self, name):
        msg = Workspace_pb2.GetWorkspaceByName(name=name)
        response = self.make_proto_request(
            "GET",
            "/api/v1/uac-proxy/workspace/getWorkspaceByName",
            params=msg,
        )
        workspace = self.must_proto_response(response, Workspace_pb2.Workspace)

        response = self.make_proto_request(
            "GET",
            "/api/v1/uac-proxy/uac/getCurrentUser",
        )
        user_info = self.must_proto_response(response, UACService_pb2.UserInfo)

        msg = UACService_pb2.UpdateUser(
            info=user_info,
            default_workspace_id=workspace.id,
        )
        response = self.make_proto_request(
            "POST",
            "/api/v1/uac-proxy/uac/updateUser",
            body=msg,
        )
        _utils.raise_for_http_error(response)

    def is_workspace(self, workspace_name):
        msg = Workspace_pb2.GetWorkspaceByName(name=workspace_name)
        response = self.make_proto_request(
            "GET",
            "/api/v1/uac-proxy/workspace/getWorkspaceByName",
            params=msg,
        )

        return response.ok

    def get_workspace_name_from_legacy_id(self, workspace_id):
        """For project, dataset, and repository, which were pre-workspace service."""
        # try getting organization
        msg = Organization_pb2.GetOrganizationById(org_id=workspace_id)
        response = self.make_proto_request(
            "GET",
            "/api/v1/uac-proxy/organization/getOrganizationById",
            params=msg,
        )
        if not response.ok:
            # try getting user
            msg = UACService_pb2.GetUser(user_id=workspace_id)
            response = self.make_proto_request(
                "GET",
                "/api/v1/uac-proxy/uac/getUser",
                params=msg,
            )
            # workspace is user
            return self.must_proto_response(
                response,
                UACService_pb2.UserInfo,
            ).verta_info.username
        else:
            # workspace is organization
            return self.must_proto_response(response, msg.Response).organization.name

    def get_workspace_name_from_id(self, workspace_id):
        """For registry, which uses workspace service."""
        msg = Workspace_pb2.GetWorkspaceById(id=int(workspace_id))
        response = self.make_proto_request(
            "GET",
            "/api/v1/uac-proxy/workspace/getWorkspaceById",
            params=msg,
        )

        workspace = self.must_proto_response(response, Workspace_pb2.Workspace)
        return workspace.username or workspace.org_name

    def get_personal_workspace(self):
        email = self.auth.get("Grpc-Metadata-email")
        if email is not None:
            msg = UACService_pb2.GetUser(email=email)
            response = self.make_proto_request(
                "GET",
                "/api/v1/uac-proxy/uac/getUser",
                params=msg,
            )

            if (
                response.ok and self.is_html_response(response)  # fetched webapp
            ) or response.status_code == 404:  # UAC not found
                pass  # fall through to OSS default workspace
            else:
                return self.must_proto_response(
                    response,
                    UACService_pb2.UserInfo,
                ).verta_info.username
        return self._OSS_DEFAULT_WORKSPACE

    def get_default_workspace(self):
        response = self.make_proto_request(
            "GET",
            "/api/v1/uac-proxy/uac/getCurrentUser",
        )

        if (
            response.ok and self.is_html_response(response)  # fetched webapp
        ) or response.status_code == 404:  # UAC not found
            return self._OSS_DEFAULT_WORKSPACE

        user_info = self.must_proto_response(response, UACService_pb2.UserInfo)
        workspace_id = user_info.verta_info.default_workspace_id
        if workspace_id:
            return self.get_workspace_name_from_id(workspace_id)
        else:  # old backend
            return self.get_personal_workspace()
