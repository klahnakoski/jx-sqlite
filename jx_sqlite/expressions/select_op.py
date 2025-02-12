# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base import FALSE
from jx_base.expressions import SelectOp as SelectOp_, LeavesOp, NULL, SqlScript
from jx_base.expressions.variable import is_variable
from jx_base.language import is_op
from jx_sqlite.expressions._utils import check
from mo_sqlite.expressions.sql_script import SqlScript
from mo_dots import concat_field, literal_field, startswith_field, tail_field
from mo_json.types import JX_IS_NULL, to_jx_type
from mo_sqlite.expressions import SqlVariable, SqlSelectOp, SqlAliasOp


class SelectOp(SelectOp_):
    @check
    def to_sql(self, schema) -> SqlScript:
        jx_type = JX_IS_NULL
        sql_terms = []
        for term in self.terms:
            name, expr, agg, default = term.name, term.value, term.aggregate, term.default
            if is_variable(expr):
                var_name = expr.var
                if startswith_field(var_name, "row"):
                    _, var_name = tail_field(var_name)
                cols = list(schema.leaves(var_name))
                if len(cols) == 0:
                    sql_terms.append(SqlAliasOp(NULL, name))
                    continue
                elif len(cols) == 1:
                    rel_name0, col0 = cols[0]
                    if col0.es_column == var_name:
                        # WHEN WE REQUEST AN ES_COLUMN DIRECTLY, BREAK THE RECURSIVE LOOP
                        full_name = concat_field(name, rel_name0)
                        jx_type |= full_name + to_jx_type(col0.json_type)
                        sql_terms.append(SqlAliasOp(SqlVariable(None, expr.var, jx_type= to_jx_type(col0.json_type)), full_name))
                        continue

                for rel_name, col in cols:
                    full_name = concat_field(name, rel_name)
                    jx_type |= full_name + to_jx_type(col.json_type)
                    sql_terms.append(SqlAliasOp(SqlVariable(col.es_index, col.es_column, jx_type=to_jx_type(col.json_type)), full_name))
            elif is_op(expr, LeavesOp):
                var_names = expr.vars()
                for var_name in var_names:
                    cols = schema.leaves(var_name)
                    for rel_name, col in cols:
                        full_name = concat_field(name, literal_field(rel_name))
                        jx_type |= full_name + to_jx_type(col.json_type)
                        sql_terms.append(SqlAliasOp(SqlVariable(col.es_index, col.es_column, jx_type=to_jx_type(col.json_type)), full_name))
            else:
                sql_script = expr.to_sql(schema)
                jx_type |= name + to_jx_type(sql_script.jx_type)
                sql_terms.append(SqlAliasOp(sql_script, name))

        return SqlScript(
            jx_type=jx_type,
            expr=SqlSelectOp(self.frum.to_sql(schema), *sql_terms),
            miss=FALSE,
            frum=self,
            schema=schema,
        )


