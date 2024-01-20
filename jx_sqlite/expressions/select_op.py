# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import SelectOp as SelectOp_, LeavesOp, Variable, AndOp, NULL
from jx_base.expressions.select_op import SelectOne
from jx_base.expressions.variable import is_variable
from jx_base.language import is_op, is_expression
from jx_sqlite.expressions._utils import check, SQLang
from jx_sqlite.expressions.sql_script import SqlScript
from mo_sqlite.sqlite import (
    quote_column,
    SQL_COMMA,
    SQL_AS,
    SQL_SELECT,
    SQL,
    Log,
    ENABLE_TYPE_CHECKING,
    SQL_CR,
)
from mo_dots import concat_field, literal_field
from mo_json.types import JX_IS_NULL, to_jx_type


class SelectOp(SelectOp_):
    @check
    def to_sql(self, schema):
        jx_type = JX_IS_NULL
        sql_terms = []
        diff = False
        for term in self.terms:
            name, expr, agg, default = term.name, term.value, term.aggregate, term.default
            if is_variable(expr):
                var_name = expr.var
                cols = list(schema.leaves(var_name))
                if len(cols) == 0:
                    sql_terms.append(SelectOne(name, NULL, agg, default))
                    continue
                elif len(cols) == 1:
                    rel_name0, col0 = cols[0]
                    if col0.es_column == var_name:
                        # WHEN WE REQUEST AN ES_COLUMN DIRECTLY, BREAK THE RECURSIVE LOOP
                        full_name = concat_field(name, rel_name0)
                        jx_type |= full_name + to_jx_type(col0.json_type)
                        sql_terms.append(SelectOne(full_name, expr, agg, default))
                        continue

                diff = True
                for rel_name, col in cols:
                    full_name = concat_field(name, rel_name)
                    jx_type |= full_name + to_jx_type(col.json_type)
                    sql_terms.append(SelectOne(full_name, Variable(col.es_column, col.json_type), agg, default))
            elif is_op(expr, LeavesOp):
                var_names = expr.vars()
                for var_name in var_names:
                    cols = schema.leaves(var_name)
                    diff = True
                    for rel_name, col in cols:
                        full_name = concat_field(name, literal_field(rel_name))
                        jx_type |= full_name + to_jx_type(col.json_type)
                        sql_terms.append(SelectOne(full_name, Variable(col.es_column, col.json_type), agg, default))
            else:
                jx_type |= name + to_jx_type(expr.jx_type)
                sql_terms.append(SelectOne(name, expr, agg, default))

        if diff:
            return SelectOp(self.frum, *sql_terms).partial_eval(SQLang).to_sql(schema)

        return SqlScript(
            jx_type=jx_type,
            expr=SelectSQL([{"name":t.name, "value":t.expr} for t in sql_terms], schema),
            miss=AndOp(*(t.expr.missing(SQLang) for t in sql_terms)),  # TODO: should be False?
            frum=self,
            schema=schema,
        )


class SelectSQL(SQL):
    __slots__ = ["terms", "schema"]

    def __init__(self, terms, schema):
        if ENABLE_TYPE_CHECKING:
            if not isinstance(terms, list) or not all(isinstance(term, dict) for term in terms):
                Log.error("expecting list of dicts")
            if not all(is_expression(term["value"]) for term in terms):
                Log.error("expecting list of dicts with expressions")
        self.terms = terms
        self.schema = schema

    def __iter__(self):
        yield from SQL_SELECT
        comma = SQL_CR
        for term in self.terms:
            name, value = term["name"], term["value"]
            yield from comma
            comma = SQL_COMMA
            yield from value.to_sql(self.schema)
            yield from SQL_AS
            yield from quote_column(name)
