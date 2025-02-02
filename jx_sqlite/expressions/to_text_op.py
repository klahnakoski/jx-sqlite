# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base import NULL
from jx_base.expressions import ToTextOp as ToTextOp_, SelectOp, CoalesceOp, SqlScript, Literal
from jx_base.language import is_op
from mo_json import JX_TEXT, JX_BOOLEAN, JX_NUMBER_TYPES, split_field, base_type
from mo_sqlite import SQLang, check, SqlScript
from mo_sqlite import quote_value, sql_call
from mo_sqlite import (
    sql_cast,
)
from mo_sqlite.expressions import SqlCaseOp, SqlWhenOp
from mo_sqlite.expressions.sql_cast_op import SqlCastOp


class ToTextOp(ToTextOp_):
    @check
    def to_sql(self, schema) -> SqlScript:
        sql_script = self.term.to_sql(schema)
        if is_op(sql_script.expr, CoalesceOp):
            return SqlScript(
                jx_type=JX_TEXT,
                expr=CoalesceOp(*(ToTextOp(t) for t in sql_script.expr.terms)).partial_eval(SQLang).to_sql(schema).expr,
                frum=self,
                schema=schema,
            )
        elif sql_script.expr is NULL:
            return sql_script

        type = base_type(sql_script.jx_type)
        if type == JX_TEXT:
            return sql_script
        elif type == JX_BOOLEAN:
            return SqlScript(
                jx_type=JX_TEXT,
                expr=SqlCaseOp(SqlWhenOp(sql_script.expr, Literal("true")), _else=Literal("false")),
                frum=self,
                schema=schema,
            )
        elif type in JX_NUMBER_TYPES:
            return SqlScript(
                jx_type=JX_TEXT,
                expr=sql_call(
                    "RTRIM", sql_call("RTRIM", SqlCastOp(sql_script.expr, "TEXT"), quote_value("0"),), quote_value("."),
                ),
                frum=self,
                schema=schema,
            )
        elif is_op(sql_script.frum, SelectOp) and len(sql_script.frum.terms) > 1:
            return (
                CoalesceOp(*(ToTextOp(t.value) for t in sql_script.frum.terms if len(split_field(t.name)) == 1))
                .partial_eval(SQLang)
                .to_sql(schema)
            )
        else:
            return SqlScript(jx_type=JX_TEXT, expr=sql_cast(sql_script.expr, "TEXT"), frum=self, schema=schema,)
