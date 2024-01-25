# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import FloorOp as _FloorOp, OrOp
from jx_sqlite.expressions._utils import SQLang, check
from jx_sqlite.expressions.sql_script import SqlScript
from mo_sqlite import (
    sql_iso,
    SQL_DIV,
    SQL_STAR,
    ConcatSQL,
    SQL,
    SQL_CAST,
    SQL_SUB,
    SQL_LT,
    SQL_ZERO, sql_cast,
)
from mo_future import text
from mo_json import JX_NUMBER


class FloorOp(_FloorOp):
    @check
    def to_sql(self, schema):
        lhs = self.lhs.partial_eval(SQLang).to_sql(schema)
        rhs = self.rhs.partial_eval(SQLang).to_sql(schema)
        modifier = sql_iso(lhs.expr, SQL_LT, SQL_ZERO)

        if text(rhs).strip() != "1":
            floor = sql_cast(
                ConcatSQL(sql_iso(lhs.expr), SQL_DIV, sql_iso(rhs.expr)),
                "INTEGER"
            )
            sql = ConcatSQL(sql_iso(floor, SQL_SUB, modifier), SQL_STAR, rhs)
        else:
            floor = sql_cast(lhs.expr, "INTEGER")
            sql = ConcatSQL(floor, SQL_SUB, modifier)

        return SqlScript(
            jx_type=JX_NUMBER,
            expr=sql,
            frum=self,
            miss=OrOp(self.lhs.missing(SQLang), self.rhs.missing(SQLang)),
            schema=schema,
        )