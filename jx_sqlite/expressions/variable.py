# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import NULL, Variable as Variable_, SelectOp, FALSE
from jx_base.expressions._utils import simplified
from jx_base.expressions.select_op import SelectOne
from jx_base.expressions.variable import is_variable
from jx_sqlite.expressions._utils import SqlScript
from jx_sqlite.utils import GUID
from mo_dots import concat_field, tail_field, startswith_field
from mo_json.types import JX_INTEGER, JxType, to_jx_type, STRING, union_type, JX_TEXT, base_type
from mo_logs import logger
from jx_sqlite.expressions._utils import check
from mo_sqlite import json_type_to_sqlite_type
from mo_sqlite.expressions import SqlVariable, SqlCoalesceOp


class Variable(Variable_):
    @simplified
    def partial_eval(self, lang):
        first, rest = tail_field(self.var)
        if first == "row":
            return Variable(rest)
        return Variable(self.var)

    @check
    def to_sql(self, schema) -> SqlScript:
        var_name = self.var
        if startswith_field(var_name, "row"):
            _, var_name = tail_field(var_name)

        if var_name == GUID:
            output = SqlScript(
                jx_type=JX_INTEGER,
                expr=SqlVariable(schema.nested_path[0], GUID, jx_type=JX_TEXT),
                frum=self,
                miss=FALSE,
                schema=schema,
            )
            return output
        cols = list(schema.leaves(var_name))
        select = []

        if len(cols) == 0:
            return NULL.to_sql(schema)
        elif len(cols) == 1:
            _, col = cols[0]
            return SqlScript(
                jx_type=to_jx_type(col.es_type),
                expr=SqlVariable(col.es_index, col.es_column, jx_type=to_jx_type(col.es_type)),
                frum=self,
                schema=schema,
            )
        elif len(set(n for n, _ in cols)) == 1:
            return SqlScript(
                jx_type=union_type(*(to_jx_type(c.es_type) for _, c in cols)),
                expr=SqlCoalesceOp(
                    *(SqlVariable(c.es_index, c.es_column, jx_type=to_jx_type(c.es_type)) for _, c in cols)
                ),
                frum=self,
                schema=schema,
            )

        logger.warning("not expected")
        for rel_name, col in cols:
            select.append(SelectOne(
                concat_field(var_name, rel_name),
                SqlVariable(col.es_index, col.es_column, jx_type=to_jx_type(col.es_type)),
            ))
        return SelectOp(schema, *select).to_sql(schema)


def typed_leaf(term, schema, target_types) -> SqlScript:
    """
    Resolve `term` to only its leaf column(s) whose type is in `target_types`, NULL otherwise.

    A union column maps to several typed columns ($N/$B/$S/...) and `Variable.to_sql`
    COALESCEs across them, erasing the per-type split. A type predicate needs the opposite:
    the single matching leaf, so a non-matching row reads NULL. `frum` is the resolved leaf,
    so `miss` is that column's null-check (not the whole union's).
    """
    value = term.to_sql(schema)
    if base_type(value.jx_type) in target_types:
        return value
    if is_variable(term):  # a Variable, or a GetOp resolving to one (post partial_eval)
        name = term.var
        if startswith_field(name, "row"):
            _, name = tail_field(name)
        cols = [c for _, c in schema.leaves(name) if base_type(to_jx_type(c.es_type)) in target_types]
        if cols:
            exprs = [SqlVariable(c.es_index, c.es_column, jx_type=to_jx_type(c.es_type)) for c in cols]
            expr = exprs[0] if len(exprs) == 1 else SqlCoalesceOp(*exprs)
            return SqlScript(
                jx_type=union_type(*(to_jx_type(c.es_type) for c in cols)), expr=expr, frum=expr, schema=schema,
            )
    return NULL.to_sql(schema)
