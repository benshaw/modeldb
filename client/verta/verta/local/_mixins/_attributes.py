# -*- coding: utf-8 -*-

from verta._protos.public.common import CommonService_pb2

from verta._internal_utils import _utils
from verta import data_types


class AttributesMixin(object):
    def add_attribute(self, key, value):
        self.add_attributes({key: value})

    def add_attributes(self, attributes):
        for key in attributes.keys():
            _utils.validate_flat_key(key)

        for key, value in attributes.items():
            val_msg = _utils.python_to_val_proto(value, allow_collection=True)
            attr_msg = CommonService_pb2.KeyValue(key=key, value=val_msg)
            self._msg.attributes.append(attr_msg)

    def del_attribute(self, key):
        for i, attribute in enumerate(self._msg.attributes):
            if attribute.key == key:
                del self._msg.attributes[i]
                break

    def get_attribute(self, key):
        return self.get_attributes()[key]

    def get_attributes(self):
        attributes = _utils.unravel_key_values(self._msg.attributes)
        for key, attribute in attributes.items():
            try:
                attributes[key] = data_types._VertaDataType._from_dict(attribute)
            except (KeyError, TypeError, ValueError):
                pass
        return attributes
