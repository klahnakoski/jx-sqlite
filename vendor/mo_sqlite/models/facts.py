# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from mo_imports import export

from jx_base.models.facts import Facts as _Facts
from mo_json import entype


class Facts(_Facts):

    @property
    def nested_path(self):
        return self.container.get_table(self.name).nested_path

    def add(self, documents):
        documents = entype(documents)
        self.container.add(self.name, documents)


export("mo_sqlite.models.container", Facts)

