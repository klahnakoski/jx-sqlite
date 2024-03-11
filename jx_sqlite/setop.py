# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from dataclasses import dataclass
from typing import List, Dict, Tuple

from jx_base import Column, is_op, FALSE
from jx_base.expressions import NULL, ZERO, SqlScript
from jx_base.expressions.sql_is_null_op import SqlIsNullOp
from jx_base.expressions.sql_order_by_op import OneOrder
from jx_base.utils import GUID
from jx_sqlite import Facts
from jx_sqlite.expressions.leaves_op import LeavesOp
from jx_sqlite.expressions.to_boolean_op import ToBooleanOp
from jx_sqlite.format import format_deep
from jx_sqlite.utils import (
    COLUMN,
    ColumnMapping,
    ORDER,
    _make_column_name,
    get_column,
    UID,
    PARENT,
    table_alias,
    untype_field,
)
from mo_dots import (
    Data,
    startswith_field,
    unwraplist,
    relative_field,
    is_missing,
    Null,
    tail_field,
    unliteral_field,
    list_to_data,
)
from mo_future import extend
from mo_json.types import OBJECT, jx_type_to_json_type, JX_ANY, STRING, INTEGER, JX_TEXT, JX_INTEGER
from mo_logs import Log
from mo_sql import SQL_DESC, SQL_ASC, NO_SQL
from mo_sqlite import (
    SQL_AND,
    SQL_FROM,
    SQL_LEFT_JOIN,
    SQL_NULL,
    SQL_ON,
    SQL_SELECT,
    SQL_UNION_ALL,
    SQL_WHERE,
    sql_iso,
    sql_list,
    ConcatSQL,
    SQL_ZERO,
    SQL_GT,
)
from mo_sqlite import quote_column, sql_alias
from mo_sqlite.expressions import SqlVariable, SqlOrderByOp, SqlEqOp, SqlAliasOp, SqlLimitOp, SqlGtOp
from mo_sqlite import SQLang
from mo_sqlite.expressions.sql_and_op import SqlAndOp
from mo_sqlite.expressions.sql_script import SqlScript
from mo_times import Date


@dataclass
class DocumentDetails(object):
    sub_table: str
    alias: str
    id_coord: int
    nested_path: List[str]
    index_to_column: Dict[int, ColumnMapping]
    children: List["DocumentDetails"]

    def __init__(self, sub_table: str):
        self.sub_table = sub_table
        self.alias = ""
        self.id_coord = -1
        self.nested_path = [sub_table]
        self.index_to_column = {}
        self.children = []


@extend(Facts)
def _set_op(self, query):
    index_to_column, command, primary_doc_details = to_sql(self, query)
    result = self.container.db.query(command)

    def _accumulate_nested(
        rows,  # row generator
        row,  # current row
        next_row,  # we got this row, but it belongs to the next document
        nested_doc_details: DocumentDetails,  # describes how the rows get mapped to nested docs
        parent_id: int,  # the id of the parent doc (for detecting when to step out of loop)
        parent_id_coord: int,  # the column of the parent_id, so we may get the value
    ) -> Tuple[Data, Data, List[Data]]:
        output = []
        id_coord = nested_doc_details.id_coord
        curr_nested_path, _ = untype_field(nested_doc_details.nested_path[0])

        index_to_column = tuple((c.push_list_name, c.pull) for _, c in nested_doc_details.index_to_column.items())
        while True:
            doc = Null
            for rel_field, pull in index_to_column:
                value = pull(row)
                if is_missing(value):
                    continue
                doc = doc or Data()
                doc[rel_field] = value

            for child_details in nested_doc_details.children:
                # EACH NESTED TABLE MUST BE ASSEMBLED INTO A LIST OF OBJECTS
                child_id = row[child_details.id_coord]
                if child_id is None:
                    continue

                next_row, row, nested_value = _accumulate_nested(
                    rows, row, next_row, child_details, row[id_coord], id_coord,
                )
                if not nested_value:
                    continue
                doc = doc or Data()
                rel_field = relative_field(untype_field(child_details.nested_path[0])[0], curr_nested_path)
                doc[rel_field] = unwraplist(nested_value)

            if doc or not parent_id:
                output.append(doc)

            if not next_row:
                try:
                    next_row = next(rows)
                except StopIteration:
                    return Null, Null, output
            if parent_id and parent_id != next_row[parent_id_coord]:
                return next_row, row, output
            row, next_row = next_row, None

    cols = tuple(i for i in index_to_column.values() if i.push_list_name != None)

    if result.data:
        all_rows = iter(result.data)
        first_row = next(all_rows)
        _, _, data = _accumulate_nested(all_rows, first_row, None, primary_doc_details, 0, 0)
    else:
        data = result.data

    # the above returns data relative to snowflake.fact_name.  Get the nested_path
    rel_path = untype_field(relative_field(query.frum.nested_path[0], query.frum.schema.snowflake.fact_name))[0]
    if rel_path != ".":
        data = list_to_data(data).get(rel_path)

    return format_deep(data, cols, query)


@extend(Facts)
def to_sql(self, query) -> Tuple[Dict[int, ColumnMapping], SqlScript, DocumentDetails]:
    # EACH SELECT VALUE BELONGS AT QUERY DEPTH, FIND LEAST DEEP FOR EACH (AND THE VARIABLES REQUIRED)

    # GET LIST OF SELECTED COLUMNS
    select_vars = set(
        rest if first == "row" else v
        for s in query.select.terms
        for v in s.value.vars()
        for first, rest in [tail_field(v)]
    )
    schema = query.frum.schema
    known_vars = schema.keys()
    active_paths = {schema.nested_path[0]: {
        Column(
            name=GUID,
            json_type=STRING,
            es_column=GUID,
            es_index=self.name,
            es_type=str,
            nested_path=[self.name],
            multi=1,
            cardinality=0,
            last_updated=Date.now(),
        ),
        Column(
            name=UID,
            json_type=INTEGER,
            es_column=UID,
            es_index=self.name,
            es_type=str,
            nested_path=[self.name],
            multi=1,
            cardinality=0,
            last_updated=Date.now(),
        ),
    }}
    for v in select_vars:
        for _, c in schema.leaves(v):
            active_paths.setdefault(c.nested_path[0], set()).add(c)

    # ANY VARS MENTIONED WITH NO COLUMNS?
    for v in select_vars:
        if not any(startswith_field(cname, v) for cname in known_vars):
            active_paths[schema.path].add(Column(
                name=v,
                json_type=OBJECT,
                es_column=".",
                es_index=schema.path,
                es_type="NULL",
                nested_path=[schema.path],
                multi=1,
                cardinality=0,
                last_updated=Date.now(),
            ))
    # EVERY COLUMN, AND THE COLUMN INDEX IT OCCUPIES
    index_to_column: Dict[int, ColumnMapping] = {}  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
    index_to_uid = {}  # FROM ARRAY PATH TO THE INDEX OF UID
    sql_selects = []  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE)
    # nest_to_alias = {query_path: table_alias(i) for i, query_path in enumerate(self.snowflake.query_paths)}
    # ADD SQL SELECT COLUMNS

    selects = query.select.partial_eval(SQLang)

    # EVERY SELECT STATEMENT THAT WILL BE REQUIRED, NO MATTER THE DEPTH
    # WE WILL CREATE THEM ACCORDING TO THE DEPTH REQUIRED
    for table_number, sub_table in enumerate(self.snowflake.query_paths):
        nested_doc_details = DocumentDetails(sub_table)
        sub_schema = self.snowflake.get_schema(list(reversed([
            t for t in self.snowflake.query_paths if startswith_field(sub_table, t)
        ])))
        if table_number == 0:
            # ROOT OF TREE
            primary_doc_details = nested_doc_details
        else:
            # INSERT INTO TREE
            def place(parent_doc_details: DocumentDetails):
                if startswith_field(sub_table, parent_doc_details.nested_path[0]):
                    for c in parent_doc_details.children:
                        if place(c):
                            return True
                    parent_doc_details.children.append(nested_doc_details)
                    nested_doc_details.nested_path = [sub_table, *parent_doc_details.nested_path]
                    return True

            place(primary_doc_details)

        nested_path = nested_doc_details.nested_path
        alias = nested_doc_details.alias = sub_table

        # WE ALWAYS ADD THE UID
        column_number = index_to_uid[sub_table] = nested_doc_details.id_coord = len(sql_selects)
        sql_select = SqlVariable(alias, UID, jx_type=JX_TEXT)
        sql_selects.append(sql_alias(sql_select, _make_column_name(column_number)))
        if table_number > 0:
            # UID FOR CHILD TABLE
            index_to_column[column_number] = ColumnMapping(
                sql=sql_select, type="number", nested_path=nested_path, column_alias=_make_column_name(column_number),
            )

            # ORDER FOR CHILD TABLE
            column_number = len(sql_selects)
            sql_select = SqlVariable(alias, ORDER, jx_type=JX_INTEGER)
            sql_selects.append(sql_alias(sql_select, _make_column_name(column_number)))
            index_to_column[column_number] = ColumnMapping(
                sql=sql_select, type="number", nested_path=nested_path, column_alias=_make_column_name(column_number),
            )

        # WE DO NOT NEED DATA FROM TABLES WE REQUEST NOTHING FROM
        if sub_table not in active_paths:
            continue

        sub_selects = selects.partial_eval(SQLang).to_sql(sub_schema).expr
        for i, term in enumerate(sub_selects.terms):
            name, value = term.name, term.value
            column_number = len(sql_selects)
            if is_op(value, LeavesOp):
                Log.error("expecting SelectOp to subsume the LeavesOp")

            sql = value
            column_alias = _make_column_name(column_number)
            sql_selects.append(SqlAliasOp(sql, column_alias))
            push_column_name, push_column_child = tail_field(name)
            index_to_column[column_number] = nested_doc_details.index_to_column[column_number] = ColumnMapping(
                push_list_name=name,
                push_column_child=push_column_child,
                push_column_name=unliteral_field(push_column_name),
                push_column_index=i,
                pull=get_column(column_number, json_type=value.jx_type),
                sql=sql,
                type=jx_type_to_json_type(sql.jx_type),
                column_alias=column_alias,
                nested_path=nested_path,
            )
    where_clause = ToBooleanOp(query.where).partial_eval(SQLang).to_sql(schema)
    # ORDERING
    sorts = []
    if query.sort:
        for sort in query.sort:
            sql = sort.value.partial_eval(SQLang).to_sql(schema)
            column_number = len(sql_selects)
            # SQL HAS ABS TABLE REFERENCE
            column_alias = _make_column_name(column_number)
            sql_selects.append(sql_alias(sql, column_alias))
            sorts.append(OneOrder(SqlIsNullOp(SqlVariable(None, column_alias)), NO_SQL))
            sorts.append(OneOrder(SqlVariable(None, column_alias), sort_to_sqlite_order[sort.sort]))
    for t in self.snowflake.query_paths:
        sorts.append(OneOrder(SqlVariable(None, f"{COLUMN}{index_to_uid[t]}", jx_type=JX_TEXT), NO_SQL))

    unsorted_sql = _make_sql_for_one_nest_in_set_op(
        self,
        self.snowflake.fact_name,
        sql_selects,
        where_clause,
        active_paths,
        index_to_column,
        index_to_uid,
        query.limit,
        schema,
    )

    ordered_sql = SqlOrderByOp(unsorted_sql, sorts)
    if query.limit is not NULL:
        ordered_sql = SqlLimitOp(ordered_sql, query.limit.to_sql(schema))
    return index_to_column, ordered_sql, primary_doc_details


@extend(Facts)
def _make_sql_for_one_nest_in_set_op(
    self,
    primary_nested_path,
    selects,  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE
    where_clause,
    active_columns,
    index_to_sql_select,  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
    nested_path_to_uid_index,  # COLUMNS USED FOR UID (REQUIRED)
    limit,
    schema,
):
    """
    FOR EACH NESTED LEVEL, WE MAKE A QUERY THAT PULLS THE VALUES/COLUMNS REQUIRED
    WE `UNION ALL` THEM WHEN DONE
    """

    parent_alias = "a"
    from_clause = []
    select_clause = []
    children_sql = []
    done = []

    # STATEMENT FOR EACH NESTED PATH
    tables = self.snowflake.query_paths
    for i, sub_table_name in enumerate(tables):
        if any(startswith_field(sub_table_name, d) for d in done):
            continue

        alias = sub_table_name  # was table_alias(i)

        if primary_nested_path == sub_table_name:
            select_clause = []
            # ADD SELECT CLAUSE HERE
            for select_index, s in enumerate(selects):
                column_mapping = index_to_sql_select.get(select_index)
                if not column_mapping:
                    select_clause.append(s)
                    continue

                if startswith_field(column_mapping.nested_path[0], sub_table_name):
                    select_clause.append(SqlAliasOp(column_mapping.sql, column_mapping.column_alias))
                else:
                    # DO NOT INCLUDE DEEP STUFF AT THIS LEVEL
                    select_clause.append(SqlAliasOp(NULL.to_sql(schema), column_mapping.column_alias))

            if sub_table_name == self.snowflake.fact_name:
                from_clause.append(ConcatSQL(
                    SQL_FROM,
                    SqlAliasOp(SqlVariable(self.snowflake.fact_name, None, jx_type=self.schema.jx_type), alias),
                ))
            else:
                from_clause.append(ConcatSQL(
                    SQL_LEFT_JOIN,
                    SqlAliasOp(SqlVariable(sub_table_name, None), alias),
                    SQL_ON,
                    SqlEqOp(SqlVariable(alias, PARENT), SqlVariable(parent_alias, UID)),
                ))
                where_clause = SqlAndOp(where_clause, SqlGtOp(SqlVariable(alias, ORDER), ZERO))
            parent_alias = alias

        elif startswith_field(primary_nested_path, sub_table_name):
            # PARENT TABLE
            # NO NEED TO INCLUDE COLUMNS, BUT WILL INCLUDE ID AND ORDER
            if sub_table_name == self.snowflake.fact_name:
                from_clause.append(ConcatSQL(SQL_FROM, sql_alias(quote_column(self.snowflake.fact_name), alias)))
            else:
                parent_alias = alias = table_alias(i)
                from_clause.append(ConcatSQL(
                    SQL_LEFT_JOIN,
                    sql_alias(quote_column(sub_table_name), alias),
                    SQL_ON,
                    SqlEqOp(SqlVariable(alias, PARENT), SqlVariable(parent_alias, UID)),
                ))
                where_clause = ConcatSQL(
                    sql_iso(where_clause), SQL_AND, SqlVariable(parent_alias, ORDER), SQL_GT, SQL_ZERO,
                )
            parent_alias = alias

        elif startswith_field(sub_table_name, primary_nested_path):
            # CHILD TABLE
            # GET FIRST ROW FOR EACH NESTED TABLE
            from_clause.append(ConcatSQL(
                SQL_LEFT_JOIN,
                sql_alias(SqlVariable(sub_table_name, None), alias),
                SQL_ON,
                SqlEqOp(SqlVariable(alias, PARENT), SqlVariable(parent_alias, UID)),
                SQL_AND,
                SqlEqOp(SqlVariable(alias, ORDER), ZERO),
            ))

            # IMMEDIATE CHILDREN ONLY
            done.append(sub_table_name)
            # NESTED TABLES WILL USE RECURSION
            children_sql.append(_make_sql_for_one_nest_in_set_op(
                self,
                sub_table_name,
                selects,  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE
                where_clause,
                active_columns,
                index_to_sql_select,  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
                None,
                None,
                schema=schema,
            ))
        else:
            # SIBLING PATHS ARE IGNORED
            continue

    sql = SqlScript(
        jx_type=JX_ANY,
        # TODO: IS THIS THE TYPE FOR THE SET OF COLUMNS?  (INCLUDE NESTING, SO WE MAY UNION TO GET FINAL TYPE)
        expr=SQL_UNION_ALL.join([
            ConcatSQL(SQL_SELECT, sql_list(select_clause), ConcatSQL(*from_clause), SQL_WHERE, where_clause),
            *children_sql,
        ]),
        frum=None,
        miss=FALSE,
        schema=schema,
    )

    return sql


sort_to_sqlite_order = {-1: SQL_DESC, 0: SQL_ASC, 1: SQL_ASC}


def test_dots(cols):
    for c in cols:
        if "\\" in c.push_column_name:
            return True
    return False
