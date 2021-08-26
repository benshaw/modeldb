# -*- coding: utf-8 -*-

import abc
import io
import os
import pickle
import shutil
import tempfile

import cloudpickle

from verta.external import six

from verta._protos.public.common import CommonService_pb2

from verta._internal_utils import _artifact_utils, artifact_manager
from . import _local_entity


@six.add_metaclass(abc.ABCMeta)
class _LocalArtifactEntity(_local_entity._LocalEntity):
    """Local Entity capable of having artifacts logged.

    Implementing classes must manually call :meth:`_upload_pending_artifacts`,
    likely near the end of :meth:`save`.

    """
    def __init__(self, conn=None):
        super(_LocalArtifactEntity, self).__init__(conn=conn)
        self._pending_artifacts = []

    @abc.abstractmethod
    def _get_artifact_resolver(self, key):
        raise NotImplementedError

    def _upload_pending_artifacts(self):
        for pending_artifact in self._pending_artifacts:
            manager = artifact_manager.ArtifactManager(
                self._conn,
                self._get_artifact_resolver(pending_artifact._msg.key),
            )
            with open(pending_artifact.filepath, "rb") as f:
                manager.upload(f)
            os.remove(pending_artifact.filepath)

    def _build_artifact_store_path(self, artifact_filepath, key, ext):
        # calculate checksum
        with open(artifact_filepath, "rb") as f:
            artifact_hash = _artifact_utils.calc_sha256(f)

        # determine basename
        #     The key might already contain the file extension, thanks to our hard-coded deployment
        #     keys e.g. "model.pkl" and "model_api.json".
        if ext is None or key.endswith("." + ext):
            basename = key
        else:
            basename = key + "." + ext

        return artifact_hash + "/" + basename

    def add_artifact(self, key, artifact):
        # TODO: centralized dir for pending artifacts
        if isinstance(artifact, io.IOBase) and hasattr(artifact, "read"):
            # preserve file extension
            if hasattr(artifact, "name"):  # probably `open()` object
                ext = os.path.splitext(artifact.name)[-1]
            else:
                ext = "bin"
            f = shutil.copyfileobj
        else:
            # TODO: more ways of serializing artifacts
            #       (e.g. models, keeping bytestrings instead of pickling)
            ext = "pkl"
            f = cloudpickle.dump
        with tempfile.NamedTemporaryFile(mode="wb", suffix="." + ext, delete=False) as tempf:
            f(artifact, tempf)

        msg = CommonService_pb2.Artifact(
            key=key,
            path=self._build_artifact_store_path(tempf.name, key, ext),
            artifact_type=CommonService_pb2.ArtifactTypeEnum.BLOB,
            filename_extension=os.path.splitext(tempf.name)[-1],
        )
        self._msg.artifacts.append(msg)
        self._pending_artifacts.append(_PendingArtifact(msg, tempf.name))

    def del_artifact(self, key):
        for i, artifact in enumerate(self._msg.artifacts):
            if artifact.key == key:
                del self._msg.artifact[i]
                break

        for i, pending_artifact in enumerate(self._pending_artifacts):
            if pending_artifact.msg.key == key:
                del self._pending_artifacts[i]
                break

    def get_artifact(self, key):
        manager = artifact_manager.ArtifactManager(
            self._conn,
            self._get_artifact_resolver(key),
        )
        artifact = manager.download()

        try:
            return pickle.loads(artifact)
        except pickle.UnpicklingError:
            return six.BytesIO(artifact)

    def get_artifact_keys(self):
        return sorted(artifact.key for artifact in self._msg.artifacts)

    def save(self):
        super(_LocalArtifactEntity, self).save()

        self._upload_pending_artifacts()
        del self._pending_artifacts[:]


class _PendingArtifact(object):
    def __init__(self, msg, filepath):
        self._msg = msg
        self._filepath = filepath

    @property
    def msg(self):
        return self._msg

    @property
    def filepath(self):
        return self._filepath
