# -*- coding: utf-8 -*-

class RegistryLabelsMixin(object):
    def add_label(self, label):
        self.add_labels([label])

    def add_labels(self, labels):
        self._msg.labels.extend(
            set(labels) - set(self._msg.labels)
        )

    def del_label(self, label):
        if label in self._msg.labels:
            self._msg.labels.remove(label)

    def get_labels(self):
        return sorted(self._msg.labels)
