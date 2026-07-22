# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from typing import List, Dict, Tuple

from jx_base import Column, is_op, FALSE
from jx_base.expressions import NULL, ZERO, SqlScript
from jx_base.expressions.sql_is_null_op import SqlIsNullOp
from jx_base.expressions.sql_order_by_op import OneOrder
from jx_base.utils import GUID
from jx_sqlite.builder import DocumentDetails, BranchBuilder
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
)
from mo_dots import (
    Data,
    startswith_field,
    unwraplist,
    relative_field,
    is_missing,
    listwrap,
    Null,
    tail_field,
    unliteral_field,
    list_to_data,
)
from mo_future import extend, first
from mo_json.types import OBJECT, jx_type_to_json_type, JX_ANY, STRING, INTEGER, JX_TEXT, JX_INTEGER
from mo_logs import Log
from mo_sql import SQL_DESC, SQL_ASC, NO_SQL
from mo_sql.utils import untype_field
from mo_sqlite import Facts
from mo_sqlite import (
    SQL_AND,
    SQL_FROM,
    SQL_LEFT_JOIN,
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
from mo_sqlite import SQLang
from mo_sqlite import quote_column, sql_alias
from mo_sqlite.expressions import SqlVariable, SqlOrderByOp, SqlEqOp, SqlAliasOp, SqlLimitOp, SqlGtOp
from mo_sqlite.expressions.sql_and_op import SqlAndOp
from mo_sqlite.expressions.sql_script import SqlScript
from mo_times import Date


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
                if rel_field == ".":
                    # WHOLE-VALUE SELECT; doc["."] = value WOULD WRAP A SCALAR IN Data
                    doc = value
                else:
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
                # THE CHILD'S ASSEMBLED VALUE LANDS AT ITS OWN PUSH NAME (AN EXPLICIT
                # DEEP-LEAF SELECT ACCUMULATES BARE VALUES AT THE TERM PATH); DEFAULT IS
                # THE CHILD TABLE'S RELATIVE PATH
                rel_field = child_details.push_list_name or relative_field(
                    untype_field(child_details.nested_path[0])[0], curr_nested_path
                )
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
        _, _, data = _accumulate_nested(all_rows, next(all_rows), None, primary_doc_details, 0, 0)
    else:
        data = result.data

    # the above returns data relative to snowflake.fact_name.  Get the nested_path
    rel_path = untype_field(relative_field(query.frum.nested_path[0], query.frum.schema.snowflake.fact_name))[0]
    if rel_path != ".":
        # NESTED ORIGIN: EACH PARENT ROW (GUARANTEED BY LEFT JOIN) YIELDS ITS CHILDREN,
        # OR ONE EMPTY DOC WHEN IT HAS NONE
        data = list_to_data([
            child
            for doc in data
            for child in (listwrap(doc[rel_path]) or [{}])
        ])

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
    builder = BranchBuilder()
    # ALIASES: SAME OBJECTS THE BUILDER OWNS (IN-PLACE MUTATION IS SHARED); THE REST OF to_sql
    # AND _make_sql_for_one_nest_in_set_op READ THESE DIRECTLY
    index_to_column: Dict[int, ColumnMapping] = builder.index_to_column
    index_to_uid = builder.index_to_uid
    sql_selects = builder.sql_selects

    selects = query.select.partial_eval(SQLang)

    # BRANCHES ALSO NEED TABLES THE where/sort REFERENCE (JOINED FOR FILTERING/ORDERING, NOT
    # SELECTED AS VALUES).  RESOLVE THOSE VARS TO THEIR TABLES THE SAME WAY select_vars ARE.
    referenced_paths = set(active_paths)
    for v in set(
        rest if first == "row" else v
        for source in (query.where.vars(), *(s.value.vars() for s in listwrap(query.sort)))
        for v in source
        for first, rest in [tail_field(v)]
    ):
        for _, c in schema.leaves(v):
            referenced_paths.add(c.nested_path[0])

    # THE BRANCHES OF THE RESULT HIERARCHY.  QUERY-DRIVEN: KEEP ONLY THE BRANCHES ON THE PATH TO
    # SOMETHING THE QUERY REFERENCES - EACH REFERENCED PATH AND ITS ANCESTORS (FOR THE JOIN
    # CHAIN).  UNRELATED/SIBLING TABLES CONTRIBUTE NOTHING, SO DROPPING THEM ONLY REMOVES EMPTY
    # UNION-ALL BRANCHES.  THE SORT KEYS AND THE UNION-ALL SQL ARE BOTH DRIVEN FROM THIS LIST.
    # docs/INTERSECTION_SURVEY.md §6
    branches = [
        t
        for t in self.snowflake.query_paths
        if any(startswith_field(a, t) for a in referenced_paths)
    ]

    # EVERY SELECT STATEMENT THAT WILL BE REQUIRED, NO MATTER THE DEPTH
    # WE WILL CREATE THEM ACCORDING TO THE DEPTH REQUIRED
    for table_number, sub_table in enumerate(branches):
        nested_doc_details = builder.add_branch(sub_table, table_number)
        nested_path = nested_doc_details.nested_path
        sub_schema = self.snowflake.get_schema(list(reversed([
            t for t in self.snowflake.query_paths if startswith_field(sub_table, t)
        ])))

        # WE DO NOT NEED DATA FROM TABLES WE REQUEST NOTHING FROM
        if sub_table not in active_paths:
            continue

        sub_selects = selects.partial_eval(SQLang).to_sql(sub_schema).expr

        # A BRANCH BELOW THE ORIGIN NORMALLY RE-ROOTS ITS COLUMNS TO BRANCH-RELATIVE NAMES;
        # select_op KEEPS THE ORIGIN-ROOTED NAME (docs/NAMES.md push_name=push_child=".")
        # ONLY FOR AN EXPLICIT DEEP LEAF - A SINGLE VALUE SELECTED FROM ABOVE.  READ THAT
        # DECISION BACK OFF THE RESOLVED TERM NAME: A DEEP LEAF STILL STARTS WITH THE
        # BRANCH'S ORIGIN-RELATIVE PATH.
        origin = query.frum.nested_path[0]
        branch_rel = (
            untype_field(relative_field(sub_table, origin))[0]
            if startswith_field(sub_table, origin) and sub_table != origin
            else None
        )
        deep_leaves = []  # COLUMN NUMBERS OF EXPLICIT DEEP LEAVES IN THIS BRANCH

        for i, term in enumerate(sub_selects.terms):
            name, value = term.name, term.value
            if is_op(value, LeavesOp):
                Log.error("expecting SelectOp to subsume the LeavesOp")

            push_column_name, push_column_child = tail_field(name)
            push_column_name = unliteral_field(push_column_name)
            column_number = builder.add_column(
                nested_doc_details,
                value,
                # LIST FORMAT SPLATS THE "." CONTAINER INTO THE DOC ROOT
                push_list_name=push_column_child if push_column_name == "." else name,
                push_column_name=push_column_name,
                push_column_child=push_column_child,
                push_column_index=i,
                nested_path=nested_path,
            )
            if branch_rel and startswith_field(unliteral_field(name), branch_rel) and unliteral_field(name) != branch_rel:
                deep_leaves.append(column_number)

        # ONE DEEP LEAF COLLAPSES TO BARE VALUES (push_list_name=".") ACCUMULATED AS A
        # MULTIVALUE AT ITS TERM PATH ON THE ORIGIN DOC; BARE VALUES CANNOT SHARE A DOC,
        # SO SEVERAL LEAVES KEEP THEIR (RE-ROOTED) SUB-DOC INSTEAD
        if len(deep_leaves) == 1:
            leaf = index_to_column[deep_leaves[0]]
            nested_doc_details.push_list_name = leaf.push_list_name
            leaf.push_list_name = "."
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
    for t in branches:
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
        branches,
    )

    ordered_sql = SqlOrderByOp(unsorted_sql, sorts)
    if query.limit is not NULL:
        ordered_sql = SqlLimitOp(ordered_sql, query.limit.to_sql(schema))
    return index_to_column, ordered_sql, builder.primary_doc_details


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
    branches,  # THE NESTED LEVELS OF THE RESULT HIERARCHY (ONE UNION-ALL BRANCH EACH)
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
    tables = branches
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
                branches=branches,
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
