# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import SelectOp, SqlScript, SqlSelectOp
from jx_base.language import is_op
from mo_sqlite import Facts
from mo_sqlite import SQLang
from jx_sqlite.aggregates import aggregates
from jx_sqlite.utils import (
    ColumnMapping,
    _make_column_name,
    get_column,
    sql_join_chain,
)
from jx_sqlite.window import _window_op
from mo_dots import relative_field, unliteral_field, tail_field
from mo_future import extend
from mo_json import jx_type_to_json_type, JX_INTEGER
from mo_sql.utils import sql_aggs
from mo_sqlite import (
    SQL_FROM,
    SQL_GROUPBY,
    SQL_IS_NULL,
    SQL_ONE,
    SQL_ORDERBY,
    SQL_SELECT,
    SQL_WHERE,
    sql_count,
    sql_iso,
    sql_list,
    sql_coalesce,
    ConcatSQL,
    SQL_ASC,
    SQL_DESC,
    SQL_COMMA,
)
from mo_sqlite import sql_alias, sql_call


@extend(Facts)
def _groupby_op(self, query, schema):
    index_to_column = {}
    # TABLES ALIAS AS THEMSELVES (SEE sql_join_chain): NO SCHEMA RENAME NEEDED
    required_tables = {c.nested_path[0] for v in query.vars() for _, c in schema.leaves(v)}
    nest_to_alias, from_sql = sql_join_chain(self.schema.snowflake, schema.nested_path[0], required_tables)
    inner_schema = schema

    selects = []
    groupby = []
    column_index = 0
    for edge in query.groupby:
        top = edge["name"] != "."
        edge_sql = edge.value.partial_eval(SQLang).to_sql(inner_schema)
        if is_op(edge_sql.expr, SqlSelectOp):
            for t in edge_sql.expr.terms:
                name, value = t.name, t.value
                if top:
                    top_name = edge["name"]
                    end_name = relative_field(name, edge["name"])
                else:
                    top_name, end_name = tail_field(name)
                column_number = len(selects)

                part_edge_sql = value.to_sql(inner_schema)
                json_type = jx_type_to_json_type(part_edge_sql.jx_type)

                column_alias = _make_column_name(column_number)
                groupby.append(part_edge_sql)
                selects.append(sql_alias(part_edge_sql, column_alias))
                index_to_column[column_number] = ColumnMapping(
                    is_edge=True,
                    push_list_name=top_name,
                    push_column_name=unliteral_field(top_name),
                    push_column_index=column_index,
                    push_column_child=end_name,
                    pull=get_column(column_number, json_type),
                    sql=part_edge_sql,
                    column_alias=column_alias,
                    type=json_type,
                )
                if not top:
                    column_index += 1
            if top:
                column_index += 1
        else:
            column_number = len(selects)
            json_type = jx_type_to_json_type(edge_sql.jx_type)

            column_alias = _make_column_name(column_number)
            groupby.append(edge_sql)
            selects.append(sql_alias(edge_sql, column_alias))
            index_to_column[column_number] = ColumnMapping(
                is_edge=True,
                push_list_name=edge["name"],
                push_column_name=unliteral_field(edge["name"]),
                push_column_index=column_index,
                push_column_child=".",
                pull=get_column(column_number, json_type),
                sql=edge_sql,
                column_alias=column_alias,
                type=json_type,
            )
            column_index += 1

    # THE SAME AGGREGATE RULES AS THE EDGES PATH: ONE RULE PER AGGREGATE SHAPE (aggregates.py).
    # A groupby IS AN OPTIMIZATION OF edges - NO DOMAIN SUBQUERY, NO OUTER JOIN TO PAD COORDINATES
    # THAT NO DOCUMENT REACHED - SO IT MUST NOT HAVE ITS OWN OPINION ABOUT HOW AN AGGREGATE
    # COMPILES.  THE HAND-ROLLED COPY HERE KNEW ONLY sql_aggs[op] AND COUNT(1), SO percentile,
    # stats, cardinality, or/and/union AND A TUPLE VALUE ALL WENT MISSING ON THIS PATH
    aggregates(self, index_to_column, column_index, selects, query, inner_schema)

    for w in query.window:
        selects.append(_window_op(w, schema))

    where = query.where.partial_eval(SQLang).to_sql(inner_schema)

    command = [ConcatSQL(
        SQL_SELECT,
        sql_list(selects),
        SQL_FROM,
        ConcatSQL(*from_sql),
        SQL_WHERE,
        where,
        SQL_GROUPBY,
        sql_list(groupby),
    )]

    if query.sort:
        command.append(ConcatSQL(
            SQL_ORDERBY,
            sql_list(
                ConcatSQL(sql_iso(sql), SQL_IS_NULL, SQL_COMMA, sql_iso(sql), SQL_DESC if s.sort == -1 else SQL_ASC,)
                for s in query.sort
                for sql in [s.value.partial_eval(SQLang).to_sql(schema)]
            ),
        ))

    return ConcatSQL(*command), index_to_column
