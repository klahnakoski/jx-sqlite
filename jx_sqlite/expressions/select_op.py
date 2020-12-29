# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from jx_base.expressions import SelectOp as SelectOp_, FALSE, LeavesOp, Variable, AndOp
from jx_base.language import is_op
from jx_sqlite.expressions._utils import check, SQLang
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import quote_column, SQL_COMMA, SQL_AS, SQL_SELECT, SQL, Log
from mo_dots import concat_field, relative_field, literal_field
from mo_json.types import ToJsonType, T_IS_NULL


class SelectOp(SelectOp_):
    @check
    def to_sql(self, schema):
        type = T_IS_NULL
        sql_terms = []
        diff = False
        for name, expr in self:
            if is_op(expr, Variable):
                var_name = expr.var
                cols = list(schema.leaves(var_name))
                if len(cols) == 1:
                    col0 = cols[0]
                    if col0.es_column == var_name:
                        type |= col0.es_column + ToJsonType(col0.jx_type)
                        sql_terms.append({"name": name, "value": expr})
                        continue

                diff = True
                for col in cols:
                    abs_name = concat_field(name, relative_field(col.name, var_name))
                    type |= abs_name + ToJsonType(col.jx_type)
                    sql_terms.append({
                        "name": abs_name,
                        "value": Variable(col.es_column, col.jx_type),
                    })
            elif is_op(expr, LeavesOp):
                # SAME AS Variable EXCEPT PATHS ARE LITERAL
                var_name = expr.var
                cols = list(schema.leaves(var_name))
                diff = True
                for col in cols:
                    abs_name = concat_field(
                        name, literal_field(relative_field(col.name, var_name))
                    )
                    type |= abs_name + ToJsonType(col.jx_type)
                    sql_terms.append({
                        "name": abs_name,
                        "value": Variable(col.es_column, col.jx_type),
                    })
            else:
                type |= name + ToJsonType(expr.type)
                sql_terms.append({"name": name, "value": expr})

        if diff:
            return SelectOp(sql_terms).partial_eval(SQLang).to_sql(schema)

        return SQLScript(
            data_type=type,
            expr=LazySelectClause(sql_terms, schema),
            miss=AndOp([t['value'].missing(SQLang) for t in sql_terms]),
            frum=self,
            schema=schema,
        )


class LazySelectClause(SQL):
    __slots__ = ["terms", "schema"]

    def __init__(self, terms, schema):
        if not isinstance(terms, list) or not all(
            isinstance(term, dict) for term in terms
        ):
            Log.error("expecting list of dicts")
        self.terms = terms
        self.schema = schema

    def __iter__(self):
        for s in SQL_SELECT:
            yield s
        comma = SQL(" ")
        for term in self.terms:
            name, value = term["name"], term["value"]
            for s in comma:
                yield s
            comma = SQL_COMMA
            for s in quote_column(name):
                yield s
            for s in SQL_AS:
                yield s
            for s in value.to_sql(self.schema):
                yield s
