# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import InOp as _InOp, FALSE, SqlLiteral
from jx_base.expressions.variable import is_variable
from jx_base.language import is_op
from jx_sqlite.expressions._utils import value2boolean
from jx_sqlite.expressions.literal import Literal
from jx_sqlite.expressions.sql_select_all_from_op import SqlSelectAllFromOp
from mo_json import JX_BOOLEAN
from mo_logs import Log
from mo_sqlite import SQLang, check, SqlScript
from mo_sqlite.expressions import SqlInOp, SqlAliasOp, SqlCoalesceOp


class InOp(_InOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        value = self.value.partial_eval(SQLang).to_sql(schema).expr
        if is_op(value, SqlAliasOp):
            value = value.value
        superset = self.superset.partial_eval(SQLang)
        if is_op(superset, Literal):
            if value.jx_type == JX_BOOLEAN:
                superset = SqlLiteral([value2boolean(v) for v in superset.value])
                return SqlScript(jx_type=JX_BOOLEAN, expr=SqlInOp(value, superset), frum=self, miss=FALSE, schema=schema)
            sql = SqlCoalesceOp(SqlInOp(value, superset), FALSE)
            return SqlScript(jx_type=JX_BOOLEAN, expr=sql, frum=self, miss=FALSE, schema=schema)

        if not is_variable(superset):
            Log.error("Do not know how to hanlde")

        return SqlScript(
            jx_type=JX_BOOLEAN,
            expr=SqlInOp(value, SqlSelectAllFromOp(superset.to_sql())),
            frum=self,
            miss=FALSE,
            schema=schema
        )