# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import StrictSubstringOp as StrictSubstringOp_, FALSE, SqlScript, ONE
from jx_sqlite.expressions.add_op import AddOp
from jx_sqlite.expressions.sub_op import SubOp
from mo_json import JX_TEXT
from mo_sqlite import SQLang, check, SqlScript
from mo_sqlite.expressions import SqlSubstrOp


class StrictSubstringOp(StrictSubstringOp_):
    @check
    def to_sql(self, schema) -> SqlScript:
        value = self.value.partial_eval(SQLang).to_sql(schema)
        start = AddOp(self.start, ONE, nulls=False).partial_eval(SQLang).to_sql(schema)
        length = SubOp(self.end, self.start).partial_eval(SQLang).to_sql(schema)
        sql = SqlSubstrOp(value.expr, start.expr, length.expr)
        return SqlScript(jx_type=JX_TEXT, expr=sql, frum=self, miss=FALSE, schema=schema)
