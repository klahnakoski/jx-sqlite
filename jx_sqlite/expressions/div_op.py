# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import (
    SqlScript,
    DivOp as _DivOp,
    MissingOp,
    OrOp,
    ToNumberOp,
)
from mo_sqlite import SQLang
from jx_sqlite.expressions._utils import check
from jx_sqlite.expressions._utils import SqlScript
from mo_json import JX_NUMBER
from mo_sqlite import sql_iso, ConcatSQL, SQL_DIV


class DivOp(_DivOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        lhs = ToNumberOp(self.lhs).partial_eval(SQLang).to_sql(schema)
        rhs = ToNumberOp(self.rhs).partial_eval(SQLang).to_sql(schema)

        return SqlScript(
            jx_type=JX_NUMBER,
            expr=ConcatSQL(sql_iso(lhs), SQL_DIV, sql_iso(rhs)),
            frum=self,
            miss=OrOp(MissingOp(self.lhs), MissingOp(self.rhs)),
            schema=schema,
        )
