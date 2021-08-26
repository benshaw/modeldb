# -*- coding: utf-8 -*-

class TagsMixin(object):
    def add_tag(self, tag):
        self.add_tags([tag])

    def add_tags(self, tags):
        self._msg.tags.extend(
            set(tags) - set(self._msg.tags)
        )

    def del_tag(self, tag):
        if tag in self._msg.tags:
            self._msg.tags.remove(tag)

    def get_tags(self):
        return sorted(self._msg.tags)
