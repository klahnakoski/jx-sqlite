# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions.variable import is_variable
from jx_sqlite.expressions._utils import SQLang
from mo_dots import startswith_field, tail_field
from mo_future import first
from mo_json.types import to_jx_type
from mo_logs import logger
from mo_sql import *
from mo_sql.utils import PARENT, UID
from mo_sqlite import quote_column
from mo_sqlite.expressions import SqlCoalesceOp, SqlVariable

LIST_COLUMN = SQL("c")  # THE ONE COLUMN OF THE RELATION
LIST_ALIAS = "__list__"  # THE CHILD TABLE, ALIASED SO IT CAN NOT SHADOW THE OUTER QUERY
SQL_DISTINCT = SQL(" DISTINCT ")


class ToListOp:
    """
    THE ONE-COLUMN RELATION BEHIND A COLLECTION.  TWO SOURCES, ONE SHAPE:

      * N SCALAR EXPRESSIONS  -> ONE ROW EACH (UNION ALL)
      * A MULTI-VALUED COLUMN -> THE CHILD ROWS BELONGING TO THIS DOCUMENT

    A COLLECTION OPERATOR IS THEN AN AGGREGATE OVER THAT RELATION: max/min/count/cardinality
    DIFFER ONLY IN WHICH AGGREGATE.  SQL AGGREGATES SKIP NULL ROWS, WHICH IS EXACTLY THE
    DECISIVE SEMANTIC, SO THE null GUARDS DISAPPEAR HERE INSTEAD OF BEING WRITTEN PER OP.

    NOT A REGISTERED LANGUAGE OP: IT IS RELATION-VALUED AND SqlScript CAN ONLY CARRY A SCALAR,
    SO IT NEVER STANDS ALONE - IT APPEARS ONLY UNDER aggregate()
    """

    __slots__ = ["terms"]

    def __init__(self, *terms):
        self.terms = terms

    def aggregate(self, agg, schema, distinct=False) -> SQL:
        """
        SCALAR: THE agg AGGREGATE APPLIED TO THIS RELATION
        """
        column = ConcatSQL(SQL_DISTINCT, LIST_COLUMN) if distinct else LIST_COLUMN
        return sql_iso(SQL_SELECT, sql_call(agg, column), SQL_FROM, sql_iso(self.to_sql(schema)))

    def to_sql(self, schema) -> SQL:
        """
        THE RELATION ITSELF - NOT A SqlScript, THIS IS ROWS, NOT A VALUE
        """
        return JoinSQL(SQL_UNION_ALL, [self._rows(t, schema) for t in self.terms])

    def _rows(self, term, schema) -> SQL:
        child_rows = _child_rows(term, schema)
        if child_rows is not None:
            return child_rows
        # SINGLE-VALUED FROM THIS ORIGIN: ONE ROW
        return ConcatSQL(SQL_SELECT, sql_iso(term.partial_eval(SQLang).to_sql(schema).expr), SQL_AS, LIST_COLUMN)


def sql_membership(value, collection, schema) -> SQL:
    """
    `value IN (<THE ROWS OF collection>)` - THE EXISTENTIAL COMPARISON.  RAW SQL, NOT SqlInOp:
    THE SUPERSET IS A *RELATION*, AND AN OP CAN ONLY HOLD EXPRESSIONS.  A COLLECTION WITH NO
    ROWS MAKES IN FALSE, WHICH IS THE ANSWER FOR A DOCUMENT THAT HAS NONE
    """
    return ConcatSQL(sql_iso(value), SQL_IN, sql_iso(ToListOp(collection).to_sql(schema)))


def _leaf_columns(term, schema):
    """
    THE COLUMNS term NAMES, OR None WHEN term IS NOT A NAME (OR NAMES NOTHING HERE)
    """
    if not is_variable(term):
        return None
    var_name = term.var
    if startswith_field(var_name, "row"):
        _, var_name = tail_field(var_name)

    return [c for _, c in schema.leaves(var_name)] or None


def is_multivalued(term, schema) -> bool:
    """
    TRUE WHEN THE DOCUMENT HAS A *COLLECTION* OF term: ITS COLUMNS LIVE IN ONE TABLE STRICTLY
    BELOW THE ORIGIN.  A NAME THAT SPANS TABLES (A MERGED SHAPE) IS NOT ONE COLLECTION, SO IT
    ANSWERS False - THE SAME LIMIT _child_rows REFUSES, REPORTED HERE INSTEAD OF RAISED
    """
    cols = _leaf_columns(term, schema)
    if not cols:
        return False
    tables = set(c.nested_path[0] for c in cols)
    if len(tables) > 1:
        return False
    table = first(tables)
    origin = schema.nested_path[0]
    return table != origin and startswith_field(table, origin)


def _child_rows(term, schema):
    """
    THE CHILD ROWS OF A MULTI-VALUED COLUMN, OR None WHEN term IS SINGLE-VALUED AT THIS ORIGIN
    (A PLAIN COLUMN IS A ONE-ROW COLLECTION, WHICH IS WHY count OF A SCALAR IS 1)
    """
    cols = _leaf_columns(term, schema)
    if not cols:
        return None
    var_name = term.var
    tables = set(c.nested_path[0] for c in cols)
    if len(tables) > 1:
        logger.error("{{name}} spans more than one table: {{tables}}", name=var_name, tables=tables)
    table = first(tables)
    origin = schema.nested_path[0]
    if table == origin or not startswith_field(table, origin):
        return None  # AT, OR ABOVE, THE ORIGIN: ONE VALUE PER DOCUMENT
    nested_path = cols[0].nested_path
    if nested_path[1] != origin:
        logger.error(
            "{{name}} is more than one table below {{origin}}; the join chain is not built yet",
            name=var_name,
            origin=origin,
        )

    values = [SqlVariable(LIST_ALIAS, c.es_column, jx_type=to_jx_type(c.es_type)) for c in cols]
    value = values[0] if len(values) == 1 else SqlCoalesceOp(*values)
    return ConcatSQL(
        SQL_SELECT,
        sql_iso(value),
        SQL_AS,
        LIST_COLUMN,
        SQL_FROM,
        quote_column(table),
        SQL_AS,
        quote_column(LIST_ALIAS),
        SQL_WHERE,
        SqlVariable(LIST_ALIAS, PARENT),
        SQL_EQ,
        SqlVariable(origin, UID),
    )
