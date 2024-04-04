# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import MissingOp as MissingOp_, FALSE, SqlScript
from jx_base.language import is_op
from mo_json.types import JX_BOOLEAN, JX_TEXT
from mo_sql import SQL_EMPTY_STRING
from mo_sqlite import SQLang, check
from mo_sqlite.expressions import SqlOrOp, SqlIsNullOp, SqlEqOp, SqlScript


class MissingOp(MissingOp_):
    @check
    def to_sql(self, schema) -> SqlScript:
        sql = self.expr.partial_eval(SQLang).to_sql(schema)

        if is_op(sql.miss, MissingOp):
            if sql.jx_type == JX_TEXT:
                return SqlScript(
                    jx_type=JX_BOOLEAN,
                    miss=FALSE,
                    expr=SqlOrOp(SqlIsNullOp(sql.expr), SqlEqOp(sql.expr, SQL_EMPTY_STRING)),
                    frum=self,
                    schema=schema,
                )

            return SqlScript(
                jx_type=JX_BOOLEAN, miss=FALSE, expr=SqlIsNullOp(sql.expr), frum=self, schema=schema
            )

        expr = sql.miss.to_sql(schema).expr
        return SqlScript(jx_type=JX_BOOLEAN, miss=FALSE, expr=expr, frum=sql.miss, schema=schema)
