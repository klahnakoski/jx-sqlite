# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base import is_op, TRUE
from jx_base.expressions import AndOp as _AndOp, ToBooleanOp, CoalesceOp, SqlScript
from mo_json.types import JX_BOOLEAN
from mo_sqlite.expressions import SqlAndOp, SqlScript, SqlCoalesceOp
from mo_sqlite import SQLang, check


class AndOp(_AndOp):
    @check
    def to_sql(self, schema) -> SqlScript:

        for t in self.terms:
            b = ToBooleanOp(t)
            s = b.to_sql(schema)
            SqlCoalesceOp(s.expr, TRUE)

        this = SqlAndOp(
            *(SqlCoalesceOp(ToBooleanOp(t).to_sql(schema).expr, TRUE) for t in self.terms)
        ).partial_eval(SQLang)
        if not is_op(this, AndOp):
            return this.to_sql(schema)
        terms = this.terms
        if not terms:
            return SqlScript(jx_type=JX_BOOLEAN, expr=TRUE.to_sql(), frum=self, schema=schema)
        return SqlScript(
            jx_type=JX_BOOLEAN, expr=SqlAndOp(*(t.to_sql(schema) for t in self.terms)), frum=self, schema=schema,
        )
