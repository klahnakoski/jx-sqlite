# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import WhenOp as _WhenOp, TRUE, SqlScript
from jx_sqlite.expressions.and_op import AndOp
from jx_sqlite.expressions.not_op import NotOp
from jx_sqlite.expressions.or_op import OrOp
from jx_sqlite.expressions.to_boolean_op import ToBooleanOp
from mo_sqlite import SQLang, check, SqlScript
from mo_sqlite.expressions import SqlCaseOp, SqlWhenOp

class WhenOp(_WhenOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        when = ToBooleanOp(self.when).partial_eval(SQLang)
        then = self.then.partial_eval(SQLang)
        _else = self.els_.partial_eval(SQLang)

        if then.missing(SQLang) is TRUE:
            return SqlScript(
                jx_type=_else.jx_type, frum=self, expr=_else.to_sql(schema).expr, miss=OrOp(when, _else.missing(SQLang)), schema=schema,
            )
        elif _else.missing(SQLang) is TRUE:
            return SqlScript(
                jx_type=then.jx_type, frum=self, expr=then.to_sql(schema).expr, miss=OrOp(NotOp(when), then.missing(SQLang)), schema=schema,
            )

        return SqlScript(
            jx_type=then.jx_type | _else.jx_type,
            frum=self,
            expr=SqlCaseOp(SqlWhenOp(when.to_sql(schema).expr, then.to_sql(schema).expr), _else=_else.to_sql(schema).expr),
            miss=OrOp(AndOp(when, then.missing(SQLang)), AndOp(OrOp(when.missing(SQLang), NotOp(when)), _else.missing(SQLang))),
            schema=schema,
        )
