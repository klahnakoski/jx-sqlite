# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import (
    SubOp as _SubOp,
    TRUE,
    OrOp,
    MissingOp,
    AndOp,
    IsNumberOp,
    NULL,
)
from jx_sqlite.expressions._utils import _binaryop_to_sql, check, SQLang
from jx_sqlite.expressions.sql_script import SqlScript
from mo_json import JX_NUMBER
from mo_sqlite.sqlite import ConcatSQL, sql_iso, SQL_SUB, sql_call


class SubOp(_SubOp):
    to_sql = _binaryop_to_sql

    @check
    def to_sql(self, schema):
        lhs = IsNumberOp(self.lhs).partial_eval(SQLang).to_sql(schema)
        rhs = self.rhs.partial_eval(SQLang).to_sql(schema)

        if lhs.miss is TRUE or rhs.miss is TRUE:
            return NULL.to_sql(schema)

        sql = ConcatSQL(sql_iso(lhs.expr), SQL_SUB, sql_iso(rhs.expr))

        return SqlScript(
            jx_type=JX_NUMBER,
            expr=sql,
            frum=self,
            miss=OrOp(MissingOp(self.lhs), MissingOp(self.rhs)),
            schema=schema
        )
