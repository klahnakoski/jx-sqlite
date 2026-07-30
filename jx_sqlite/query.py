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
from jx_sqlite.utils import unique_name, frames_one_document
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


def _per_document_aggregates(terms, schema):
    """
    AN AGGREGATE DECLARES A GROUPING; WHICH ONE DEPENDS ON WHAT IT SITS BESIDE AND HOW DEEP ITS
    VALUE REACHES.  AN ALL-AGGREGATE SELECT IS THE CLASSIC COLLAPSE OF THE WHOLE RESULT (ONE
    ROW).  A PLAIN TERM BESIDE IT KEEPS ONE ROW PER DOCUMENT, SO THE AGGREGATE HAS TO FRAME ONE
    DOCUMENT INSTEAD - WHICH ITS VALUE CAN ONLY MEAN IF IT LIVES IN A NESTED BRANCH, WHERE ONE
    DOCUMENT HAS MANY ROWS TO COLLAPSE (ToListOp).  THE QUERY IS THEN A SET OP WHOSE TERMS
    HAPPEN TO BE COLLECTION AGGREGATES - `{"select":["v",{"value":"a._b.b","aggregate":"sum"}]}`
    IS ONE SUM PER DOCUMENT BESIDE THAT DOCUMENT'S v.
    """
    aggregates = [t for t in terms if t.aggregate is not NULL]
    if not aggregates or len(aggregates) == len(terms):
        return False
    return all(frames_one_document(term, schema) for term in aggregates)


def _groupby_is_enough(query):
    """
    CAN THE OPTIMIZATION ANSWER THIS groupby?  IT EMITS ONE ROW PER GROUP THAT OCCURS, WHICH IS
    ENOUGH WHEN EVERY SELECT TERM COLLAPSES ITS GROUP.  A CUBE NEEDS A DOMAIN TO BE RECTANGULAR,
    AND A PLAIN TERM CAN NOT COLLAPSE ANYTHING - BOTH ARE THE GENERAL PATH'S BUSINESS
    """
    if query.format == "cube":
        return False
    return all(t.aggregate is not NULL for t in listwrap(query.select.terms))


def _edges_op_on_groupby(facts, query):
    """
    ANSWER A groupby ON THE EDGES PATH.  IT PADS COORDINATES NO DOCUMENT REACHED, WHICH A groupby
    WOULD NOT REPORT - THE DENSITY DIFFERENCE, SEE docs/TEST_TRIAGE.md CLUSTER 8
    """
    query.edges, query.groupby = query.groupby, query.edges
    try:
        return facts._edges_op(query, facts.schema)
    finally:
        query.edges, query.groupby = query.groupby, query.edges


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
    if not query:
        query = {}

    if not query.get("from"):
        query["from"] = self.name

    if is_text(query["from"]) and not startswith_field(query["from"], self.name):
        Log.error("Expecting table, or some nested table")
    normalized_query = QueryOp.wrap(query, self, SQLang)

    # A groupby IS AN EDGE WHOSE DOMAIN IS EVERY VALUE THAT OCCURS, AND _groupby_op IS THE
    # OPTIMIZATION OF THAT: A PLAIN GROUP BY, NO DOMAIN SUBQUERY, NO OUTER JOIN.  WHEN THE
    # OPTIMIZATION CAN NOT ANSWER - A CUBE NEEDS A DOMAIN TO BE RECTANGULAR, AND A PLAIN TERM BESIDE
    # AN AGGREGATE NEEDS THE DOCUMENT FRAMED (edges.py) - DEFER TO THE GENERAL PATH.
    # (`format != "cube"` WOULD READ FALSE WHEN NO FORMAT WAS NAMED: BOTH `Null == x` AND
    # `Null != x` ARE Null, WHICH IS FALSY, SO A DEFAULT-FORMAT groupby CAME BACK PADDED)
    if normalized_query.groupby and _groupby_is_enough(normalized_query):
        command, index_to_columns = self._groupby_op(normalized_query, self.schema)
    elif normalized_query.groupby:
        command, index_to_columns = _edges_op_on_groupby(self, normalized_query)
    elif normalized_query.edges or (
        any(t.aggregate is not NULL for t in listwrap(normalized_query.select.terms))
        and not _per_document_aggregates(listwrap(normalized_query.select.terms), normalized_query.frum.schema)
    ):
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
