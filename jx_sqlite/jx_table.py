# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.models.container import type2container
from jx_base.models.facts import Facts

from mo_imports import export
from mo_kwargs import override


class QueryTable(Facts):
    @override
    def __init__(self, name, container):
        Facts.__init__(self, name, container)

    @property
    def nested_path(self):
        return self.container.get_table(self.name).nested_path


# TODO: use dependency injection
type2container["sqlite"] = QueryTable

export("jx_sqlite.models.container", QueryTable)
export("jx_sqlite.models.table", QueryTable)
export("jx_sqlite.expressions.nested_op", QueryTable)
