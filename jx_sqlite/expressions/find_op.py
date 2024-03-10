# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base import simplified
from jx_base.expressions import FindOp as _FindOp, BasicEqOp
from jx_base.expressions.literal import NEG_ONE, ZERO
from jx_sqlite.expressions._utils import with_var, JxSql
from jx_sqlite.expressions.not_left_op import NotLeftOp
from jx_sqlite.expressions.or_op import OrOp
from mo_json import JX_INTEGER
from mo_sqlite import SQLang, check
from mo_sqlite.expressions import SqlInstrOp, SqlWhenOp, SqlCaseOp, SqlVariable, SqlScript, SqlAddOp


class FindOp(_FindOp):
    @simplified
    def partial_eval(self, lang):
        return FindOp(self.value.partial_eval(SQLang), self.find.partial_eval(SQLang), self.start.partial_eval(SQLang))

    @check
    def to_sql(self, schema) -> SqlScript:
        find = self.find.partial_eval(SQLang).to_sql(schema)
        start = self.start.partial_eval(SQLang).to_sql(schema)
        value = NotLeftOp(self.value, self.start).partial_eval(SQLang).to_sql(schema)

        index = SqlInstrOp(value.expr, find.expr)
        i = SqlVariable("i", jx_type=JX_INTEGER)
        sql = with_var(i, index, SqlCaseOp(SqlWhenOp(i, then=SqlAddOp(i, NEG_ONE, start))).partial_eval(SQLang))
        return SqlScript(jx_type=JX_INTEGER, expr=sql, frum=self, schema=schema, miss=self.missing(JxSql))

    def missing(self, lang):
        not_found = BasicEqOp(SqlInstrOp(NotLeftOp(self.value, self.start), self.find), ZERO)

        output = OrOp(self.value.missing(lang), self.find.missing(lang), not_found).partial_eval(self.lang)
        return output
