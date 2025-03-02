# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import jx_expression, QueryOp, NULL
from mo_sqlite import SQLang
from jx_sqlite.format import format_flat
from mo_sqlite.models.facts import Facts
from jx_sqlite.utils import unique_name
from mo_dots import (
    listwrap,
    relative_field,
    startswith_field,
    list_to_data,
)
from mo_future import is_text, extend
from mo_json import STRUCT
from mo_logs import Log
from mo_sql import SQL_CREATE, SQL_AS, SQL_STAR
from mo_sqlite import (
    SQL_FROM,
    SQL_SELECT,
    SQL_WHERE,
    sql_count,
    SQL_DELETE,
    ConcatSQL,
    JoinSQL,
    SQL_COMMA,
)
from mo_sqlite import quote_column, sql_alias
from mo_threads import register_thread


@extend(Facts)
def get_column_name(self, column):
    return relative_field(column.name, self.snowflake.fact_name)


@extend(Facts)
@register_thread
def __len__(self):
    counter = self.container.db.query(ConcatSQL(
        SQL_SELECT, sql_count(SQL_STAR), SQL_FROM, quote_column(self.snowflake.fact_name)
    ))[0][0]
    return counter


@extend(Facts)
def __nonzero__(self):
    return bool(self.__len__())


@extend(Facts)
def delete(self, where):
    filter = jx_expression(where).partial_eval(SQLang).to_sql(self.schema)
    with self.container.db.transaction() as t:
        t.execute(ConcatSQL(SQL_DELETE, SQL_FROM, quote_column(self.snowflake.fact_name), SQL_WHERE, filter,))


@extend(Facts)
def vars(self):
    return set(self.schema.columns.keys())


@extend(Facts)
def map(self, map_):
    return self


@extend(Facts)
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


@extend(Facts)
def query(self, query=None):
    """
    :param query:  JSON Query Expression, SET `format="container"` TO MAKE NEW TABLE OF RESULT
    :return:
    """
    query = query or {}

    # SIMPLISITC INSERTION OF FACTS INTO QUERY
    frum = query.get("from", self.name)
    if is_text(frum):
        if not startswith_field(frum, self.name):
            Log.error("Expecting table, or some nested table")
        query.frum = self

    normalized_query = QueryOp.define(query)
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

    if query.format == "container":
        new_table = "temp_" + unique_name()
        create_table = SQL_CREATE + quote_column(new_table) + SQL_AS
        self.container.db.query(create_table + command)
        return Facts(new_table, container=self.container)

    result = self.container.db.query(command)

    return format_flat(result, normalized_query, index_to_columns)


@extend(Facts)
def get_table(self, table_name):
    if startswith_field(table_name, self.name):
        return Facts(table_name, self.container)
    Log.error("programmer error")


@extend(Facts)
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
