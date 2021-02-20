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

from jx_base.expressions import TRUE, Variable
from jx_base.language import is_op
from jx_python import jx
from jx_sqlite.edges_table import EdgesTable
from jx_sqlite.expressions._utils import SQLang
from jx_sqlite.sqlite import (
    SQL_FROM,
    SQL_GROUPBY,
    SQL_IS_NULL,
    SQL_LEFT_JOIN,
    SQL_ON,
    SQL_ONE,
    SQL_ORDERBY,
    SQL_SELECT,
    SQL_WHERE,
    sql_count,
    sql_iso,
    sql_list,
    SQL_EQ,
    sql_coalesce, ConcatSQL, SQL_ASC, SQL_DESC, SQL_COMMA,
)
from jx_sqlite.sqlite import quote_column, sql_alias, sql_call
from jx_sqlite.utils import (
    ColumnMapping,
    _make_column_name,
    get_column,
    sql_aggs,
    PARENT,
    UID, table_alias,
)
from mo_dots import listwrap, split_field, startswith_field
from mo_json import FromJsonType


class GroupbyTable(EdgesTable):
    def _groupby_op(self, query, schema):
        base_table = schema.snowflake.fact_name
        path = schema.nested_path
        # base_table, path = tail_field(frum)
        # schema = self.snowflake.tables[path].schema
        index_to_column = {}
        nest_to_alias = {
            nested_path: table_alias(i)
            for i, nested_path in enumerate(self.schema.snowflake.query_paths)
        }
        tables = []
        for n, a in nest_to_alias.items():
            if startswith_field(path, n):
                tables.append({"nest": n, "alias": a})
        tables = jx.sort(tables, {"value": {"length": "nest"}})

        from_sql = [sql_alias(
            quote_column(base_table, *split_field(tables[0].nest)),
            tables[0].alias
        )]
        previous = tables[0]
        for t in tables[1::]:
            from_sql.append(ConcatSQL(
                SQL_LEFT_JOIN,
                quote_column(base_table, *split_field(t.nest)),
                t.alias,
                SQL_ON,
                quote_column(t.alias, PARENT),
                SQL_EQ,
                quote_column(previous.alias, UID)
            ))

        selects = []
        groupby = []
        for i, e in enumerate(query.groupby):
            edge_sql = e.value.partial_eval(SQLang).to_sql(schema)
            column_number = len(selects)
            sql = edge_sql.expr
            data_type = edge_sql.data_type

            column_alias = _make_column_name(column_number)
            groupby.append(sql)
            selects.append(sql_alias(sql, column_alias))
            index_to_column[column_number] = ColumnMapping(
                is_edge=True,
                push_name=e.name,
                push_column_name=e.name.replace("\\.", "."),
                push_column_index=i,
                push_child=".",
                pull=get_column(column_number),
                sql=sql,
                column_alias=column_alias,
                type=FromJsonType(data_type),
            )

        for i, select in enumerate(listwrap(query.select)):
            column_number = len(selects)
            sql = select.value.partial_eval(SQLang).to_sql(schema)
            data_type = sql.data_type

            # AGGREGATE
            if is_op(select.value, Variable) and select.value.var == "." and select.aggregate == "count":
                sql = sql_count(SQL_ONE)
            else:
                sql = sql_call(sql_aggs[select.aggregate], sql)

            if select.default.missing(SQLang) != TRUE and select.aggregate != "count":
                sql = sql_coalesce([sql, select.default.partial_eval(SQLang).to_sql(schema)])

            selects.append(sql_alias(sql, select.name))

            index_to_column[column_number] = ColumnMapping(
                push_name=select.name,
                push_column_name=select.name,
                push_column_index=i + len(query.groupby),
                push_child=".",
                pull=get_column(column_number),
                sql=sql,
                column_alias=select.name,
                type=FromJsonType(data_type),
            )

        for w in query.window:
            selects.append(self._window_op(self, query, w))

        where = query.where.partial_eval(SQLang).to_sql(schema)

        command = [ConcatSQL(
            SQL_SELECT,
            sql_list(selects),
            SQL_FROM,
            ConcatSQL(*from_sql),
            SQL_WHERE,
            where,
            SQL_GROUPBY,
            sql_list(groupby)
        )]

        if query.sort:
            command.append(ConcatSQL(
                SQL_ORDERBY,
                sql_list(
                    ConcatSQL(sql_iso(sql), SQL_IS_NULL, SQL_COMMA, sql_iso(sql), SQL_DESC if s.sort == -1 else SQL_ASC)
                    for s in query.sort
                    for sql in [s.value.partial_eval(SQLang).to_sql(schema)]
                )
            ))

        return ConcatSQL(*command), index_to_column
