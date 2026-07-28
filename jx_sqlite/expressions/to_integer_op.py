# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import ToIntegerOp as _IntegerOp
from mo_json import base_type, JX_INTEGER
from mo_sqlite import sql_cast
from mo_sqlite.expressions import SqlScript
from jx_sqlite.expressions._utils import check


class ToIntegerOp(_IntegerOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        value = self.term.to_sql(schema)

        if base_type(value.jx_type) == JX_INTEGER:
            return value
        # CAST AS INTEGER truncates a float (2.9 -> 2) and parses leading text ("5" -> 5)
        return SqlScript(
            jx_type=JX_INTEGER, expr=sql_cast(value.expr, "INTEGER"), frum=self, miss=value.miss, schema=schema,
        )
