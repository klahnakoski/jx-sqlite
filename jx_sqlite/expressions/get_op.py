# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base import NULL, TRUE
from jx_base.expressions import GetOp as _GetOp
from jx_base.expressions.variable import is_variable
from jx_sqlite.expressions._utils import SqlScript
from mo_dots import concat_field
from mo_future import first
from mo_json import to_jx_type, JX_ANY
from mo_logs import Log
from mo_sqlite.expressions import SqlVariable, SqlSelectOp, SqlAliasOp, SqlCoalesceOp
from mo_sqlite import SQLang, check


class GetOp(_GetOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        if not is_variable(self):
            Log.error("Can only handle Variable")
        var_name = self.var
        leaves = list(schema.leaves(var_name))
        unique = set(r for r, _ in leaves)

        if len(leaves) == 1:
            rr, c = leaves[0]
            term = SqlVariable(c.es_index, c.es_column, jx_type=to_jx_type(c.es_type))
            return SqlScript(
                jx_type=term.jx_type,
                expr=term,
                frum=self,
                schema=schema
            )

        if not unique:
            return SqlScript(
                jx_type=JX_ANY,
                expr=NULL,
                frum=self,
                miss=TRUE,
                schema=schema
            )
        if len(unique) == 1:
            r = first(unique)
            term = SqlCoalesceOp(*(
                SqlVariable(c.es_index, c.es_column, jx_type=to_jx_type(c.es_type))
                for rr, c in leaves
                if rr == r
            )).partial_eval(SQLang)
            return SqlScript(
                jx_type=term.jx_type,
                expr=term,
                frum=self,
                schema=schema
            )

        flat = SqlSelectOp(
            SqlVariable(schema.table),
            *(
                SqlAliasOp(
                    SqlCoalesceOp(*(SqlVariable(c.es_index, c.es_column, jx_type=to_jx_type(c.es_type)) for rr, c in leaves if rr == r)).partial_eval(SQLang),
                    concat_field(var_name, r)
                )
                for r in unique
            )
        )
        return SqlScript(jx_type=flat.jx_type, expr=flat, frum=self, schema=schema)
