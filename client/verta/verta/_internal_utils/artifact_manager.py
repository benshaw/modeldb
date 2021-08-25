# -*- coding: utf-8 -*-

import abc
import logging

from verta.external import six

from . import _artifact_utils, _utils


logger = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class ArtifactResolver(object):
    def __init__(self, conn, entity_id, artifact_key):
        self._conn = conn
        self._entity_id = entity_id
        self._artifact_key = artifact_key

        # cached metadata, set in get_upload_url()
        self._is_multipart_upload_ok = None
        self._post_fields = None

    @abc.abstractproperty
    def _get_url_endpoint(self):
        raise NotImplementedError

    @abc.abstractproperty
    def _get_url_msg(self):
        raise NotImplementedError

    @abc.abstractproperty
    def _commit_part_endpoint(self):
        raise NotImplementedError

    @abc.abstractproperty
    def _commit_part_msg(self):
        raise NotImplementedError

    @abc.abstractproperty
    def _commit_artifact_endpoint(self):
        raise NotImplementedError

    @abc.abstractproperty
    def _commit_artifact_msg(self):
        raise NotImplementedError

    def get_download_url(self):
        msg = type(self._get_url_msg)()
        msg.CopyFrom(self._get_url_msg)
        msg.method = "GET"

        response = self._conn.make_proto_request(
            "POST",
            self._get_url_endpoint,
            body=msg,
        )
        url_for_artifact = self._conn.must_proto_response(
            response,
            msg.Response,
        )

        return url_for_artifact.url

    def get_upload_url(self, part_num=1):
        msg = type(self._get_url_msg)()
        msg.CopyFrom(self._get_url_msg)
        msg.method = "PUT"
        msg.part_number = part_num

        response = self._conn.make_proto_request(
            "POST",
            self._get_url_endpoint,
            body=msg,
        )
        url_for_artifact = self._conn.must_proto_response(
            response,
            msg.Response,
        )

        self._is_multipart_upload_ok = url_for_artifact.multipart_upload_ok
        self._post_fields = url_for_artifact.fields

        return url_for_artifact.url

    @property
    def is_multipart_upload_ok(self):
        if self._is_multipart_upload_ok is None:
            self.get_upload_url()

        return self._is_multipart_upload_ok

    @property
    def post_fields(self):
        if self._post_fields is None:
            self.get_upload_url()

        return self._post_fields

    def commit_part(self, part_num, etag):
        msg = type(self._commit_part_msg)()
        msg.CopyFrom(self._commit_part_msg)
        msg.artifact_part.part_number = part_num
        msg.artifact_part.etag = etag

        response = self._conn.make_proto_request(
            "POST",
            self._commit_part_endpoint,
            body=msg,
        )
        self._conn.must_response(response)

    def commit_artifact(self):
        response = self._conn.make_proto_request(
            "POST",
            self._commit_artifact_endpoint,
            body=self._commit_artifact_msg,
        )
        self._conn.must_response(response)


class ArtifactManager(object):
    def __init__(self, conn, artifact_resolver):
        self._conn = conn
        self._resolver = artifact_resolver

    def upload(self, artifact_stream, part_size=_artifact_utils._64MB):
        artifact_stream.seek(0)

        if self._resolver.is_multipart_upload_ok:
            # TODO: parallelize this
            file_parts = iter(lambda: artifact_stream.read(part_size), b"")
            for part_num, file_part in enumerate(file_parts, start=1):
                logger.info("uploading part %s", part_num)

                # get presigned URL
                url = self._resolver.get_upload_url(part_num=part_num)

                # wrap file part into bytestream to avoid OverflowError
                #     Passing a bytestring >2 GB (num bytes > max val of int32) directly to
                #     ``requests`` will overwhelm CPython's SSL lib when it tries to sign the
                #     payload. But passing a buffered bytestream instead of the raw bytestring
                #     indicates to ``requests`` that it should perform a streaming upload via
                #     HTTP/1.1 chunked transfer encoding and avoid this issue.
                #     https://github.com/psf/requests/issues/2717
                part_stream = six.BytesIO(file_part)

                # upload part
                # TODO: what happened to retrying ConnectionError (e.g. broken pipe)?
                response = _utils.make_request("PUT", url, self._conn, data=part_stream)
                self._conn.must_response(response)

                # commit part
                self._resolver.commit_part(part_num, response.headers["ETag"])

            # complete upload
            self._resolver.commit_artifact()
        else:
            url = self._resolver.get_upload_url(part_num=0)

            # upload full artifact
            if self._resolver.post_fields:
                # if fields were returned by backend, make a POST request and supply them as form fields
                response = _utils.make_request(
                    "POST",
                    url,
                    self._conn,
                    # requests uses the `files` parameter for sending multipart/form-data POSTs.
                    #     https://stackoverflow.com/a/12385661/8651995
                    # the file contents must be the final form field
                    #     https://docs.aws.amazon.com/AmazonS3/latest/dev/HTTPPOSTForms.html#HTTPPOSTFormFields
                    files=list(self._resolver.post_fields.items())
                    + [("file", artifact_stream)],
                )
            else:
                response = _utils.make_request("PUT", url, self._conn, data=artifact_stream)
            self._conn.must_response(response)

        logger.info("upload complete")

    def download(self):
        url = self._resolver.get_download_url()

        response = _utils.make_request("GET", url, self._conn)
        self._conn.must_response(response)
        return response.content
