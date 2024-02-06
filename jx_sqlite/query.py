# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import mo_json
from jx_base import Column
from jx_base.domains import SimpleSetDomain
from jx_base.expressions import TupleOp, jx_expression, QueryOp, SelectOp, NULL
from jx_base.expressions.select_op import normalize_one
from jx_base.language import is_op
from jx_python import jx
from jx_sqlite.expressions._utils import SQLang
from jx_sqlite.jx_table import QueryTable
from jx_sqlite.utils import GUID, unique_name, untyped_column
from mo_collections.matrix import Matrix, index_to_coordinate
from mo_dots import (
    Data,
    Null,
    to_data,
    coalesce,
    concat_field,
    listwrap,
    relative_field,
    startswith_field,
    unwraplist,
    wrap,
    list_to_data,
    from_data,
)
from mo_future import text, transpose, is_text, extend
from mo_json import STRING, STRUCT
from mo_logs import Log
from mo_sql.utils import sql_aggs
from mo_sqlite import (
    SQL_FROM,
    SQL_ORDERBY,
    SQL_SELECT,
    SQL_WHERE,
    sql_count,
    sql_iso,
    sql_list,
    SQL_CREATE,
    SQL_AS,
    SQL_DELETE,
    ConcatSQL,
    JoinSQL,
    SQL_COMMA,
)
from mo_sqlite import quote_column, sql_alias
from mo_threads import register_thread


@extend(QueryTable)
def get_column_name(self, column):
    return relative_field(column.name, self.snowflake.fact_name)


@extend(QueryTable)
@register_thread
def __len__(self):
    counter = self.container.db.query(ConcatSQL(
        SQL_SELECT, sql_count("*"), SQL_FROM, quote_column(self.snowflake.fact_name)
    ))[0][0]
    return counter


@extend(QueryTable)
def __nonzero__(self):
    return bool(self.__len__())


@extend(QueryTable)
def delete(self, where):
    filter = jx_expression(where).partial_eval(SQLang).to_sql(self.schema)
    with self.container.db.transaction() as t:
        t.execute(ConcatSQL(SQL_DELETE, SQL_FROM, quote_column(self.snowflake.fact_name), SQL_WHERE, filter,))


@extend(QueryTable)
def vars(self):
    return set(self.schema.columns.keys())


@extend(QueryTable)
def map(self, map_):
    return self


@extend(QueryTable)
def where(self, filter):
    """
    WILL NOT PULL WHOLE OBJECT, JUST TOP-LEVEL PROPERTIES
    :param filter:  jx_expression filter
    :return: list of objects that match
    """
    select = []
    column_names = []
    for c in self.schema.columns:
        if c.json_type in STRUCT:
            continue
        if len(c.nested_path) != 1:
            continue
        column_names.append(c.name)
        select.append(sql_alias(quote_column(c.es_column), c.name))

    where_sql = jx_expression(filter).partial_eval(SQLang).to_sql(self.schema)
    result = self.container.db.query(ConcatSQL(
        SQL_SELECT, JoinSQL(SQL_COMMA, select), SQL_FROM, quote_column(self.snowflake.fact_name), SQL_WHERE, where_sql,
    ))

    return list_to_data([{c: v for c, v in zip(column_names, r)} for r in result.data])


@extend(QueryTable)
def query(self, query=None):
    """
    :param query:  JSON Query Expression, SET `format="container"` TO MAKE NEW TABLE OF RESULT
    :return:
    """
    if not query:
        query = {}

    if not query.get("from"):
        query["from"] = self.name

    if is_text(query["from"]) and not startswith_field(query["from"], self.name):
        Log.error("Expecting table, or some nested table")
    normalized_query = QueryOp.wrap(query, self, SQLang)

    if normalized_query.groupby and normalized_query.format != "cube":
        command, index_to_columns = self._groupby_op(normalized_query, self.schema)
    elif normalized_query.groupby:
        normalized_query.edges, normalized_query.groupby = (
            normalized_query.groupby,
            normalized_query.edges,
        )
        command, index_to_columns = self._edges_op(normalized_query, self.schema)
        normalized_query.edges, normalized_query.groupby = (
            normalized_query.groupby,
            normalized_query.edges,
        )
    elif normalized_query.edges or any(t.aggregate is not NULL for t in listwrap(normalized_query.select.terms)):
        command, index_to_columns = self._edges_op(normalized_query, normalized_query.frum.schema)
    else:
        return self._set_op(normalized_query)

    return self.format_flat(normalized_query, command, index_to_columns)



@extend(QueryTable)
def get_table(self, table_name):
    if startswith_field(table_name, self.name):
        return QueryTable(table_name, self.container)
    Log.error("programmer error")


@extend(QueryTable)
def query_metadata(self, query):
    frum, query["from"] = query["from"], self
    # schema = self.snowflake.get_table(".").schema
    query = QueryOp.wrap(query, schema)
    columns = self.snowflake.columns
    where = query.where
    table_name = None
    column_name = None

    if query.edges or query.groupby:
        raise Log.error("Aggregates(groupby or edge) are not supported")

    if where.op == "eq" and where.lhs.var == "table":
        table_name = mo_json.json2value(where.rhs.json)
    elif where.op == "eq" and where.lhs.var == "name":
        column_name = mo_json.json2value(where.rhs.json)
    else:
        raise Log.error('Only simple filters are expected like: "eq" on table and column name')

    tables = [concat_field(self.snowflake.fact_name, i) for i in self.tables.keys()]

    metadata = []
    if columns[-1].es_column != GUID:
        columns.append(Column(
            name=GUID, json_type=STRING, es_column=GUID, es_index=self.snowflake.fact_name, nested_path=["."],
        ))

    for tname, table in zip(t, tables):
        if table_name != None and table_name != table:
            continue

        for col in columns:
            cname, ctype = untyped_column(col.es_column)
            if column_name != None and column_name != cname:
                continue

            metadata.append((table, relative_field(col.name, tname), col.jx_type, unwraplist(col.nested_path),))

    return self.format_metadata(metadata, query)


@extend(QueryTable)
def _window_op(self, query, window):
    # http://www2.sqlite.org/cvstrac/wiki?p=UnsupportedSqlAnalyticalFunctions
    if window.value == "rownum":
        return (
            "ROW_NUMBER()-1 OVER ("
            + " PARTITION BY "
            + sql_iso(sql_list(window.edges.values))
            + SQL_ORDERBY
            + sql_iso(sql_list(window.edges.sort))
            + ") AS "
            + quote_column(window.name)
        )

    range_min = text(coalesce(window.range.min, "UNBOUNDED"))
    range_max = text(coalesce(window.range.max, "UNBOUNDED"))

    return (
        sql_aggs[window.aggregate]
        + sql_iso(window.value.to_sql(schema))
        + " OVER ("
        + " PARTITION BY "
        + sql_iso(sql_list(window.edges.values))
        + SQL_ORDERBY
        + sql_iso(sql_list(window.edges.sort))
        + " ROWS BETWEEN "
        + range_min
        + " PRECEDING AND "
        + range_max
        + " FOLLOWING "
        + ") AS "
        + quote_column(window.name)
    )


@extend(QueryTable)
def _normalize_select(self, select) -> SelectOp:
    return normalize_one(Null, select, "list")


@extend(QueryTable)
def transaction(self):
    """
    PERFORM MULTIPLE ACTIONS IN A TRANSACTION
    """
    return Transaction(self)


class Transaction:
    def __init__(self, table):
        self.transaction = None
        self.table = table

    def __enter__(self):
        self.transaction = self.container.db.transaction()
        self.table.db = self.transaction  # REDIRECT SQL TO TRANSACTION
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.table.db = self.table.container.db
        self.transaction.__exit__(exc_type, exc_val, exc_tb)
        self.transaction = None

    def __getattr__(self, item):
        return getattr(self.table, item)
