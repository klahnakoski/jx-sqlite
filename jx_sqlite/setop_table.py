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

from jx_base import Column, jx_expression
from jx_base.language import is_op
from jx_python import jx
from jx_sqlite.expressions._utils import SQLang
from jx_sqlite.expressions.leaves_op import LeavesOp
from jx_sqlite.expressions.to_boolean_op import ToBooleanOp
from jx_sqlite.insert_table import InsertTable
from jx_sqlite.sqlite import (
    SQL_AND,
    SQL_FROM,
    SQL_IS_NULL,
    SQL_LEFT_JOIN,
    SQL_LIMIT,
    SQL_NULL,
    SQL_ON,
    SQL_ORDERBY,
    SQL_SELECT,
    SQL_UNION_ALL,
    SQL_WHERE,
    sql_iso,
    sql_list,
    ConcatSQL,
    SQL_STAR,
    SQL_EQ,
    SQL_ZERO, SQL_GT, SQL_DESC,
)
from jx_sqlite.sqlite import quote_column, quote_value, sql_alias
from jx_sqlite.utils import (
    COLUMN,
    ColumnMapping,
    ORDER,
    _make_column_name,
    get_column,
    UID,
    PARENT, table_alias,
)
from mo_dots import (
    Data,
    Null,
    concat_field,
    is_list,
    listwrap,
    startswith_field,
    unwraplist,
    exists, to_data, is_null, relative_field, is_not_null, dict_to_data, )
from mo_future import text
from mo_json.types import IS_NULL, FromJsonType, OBJECT
from mo_logs import Log
from mo_math import UNION
from mo_times import Date


class SetOpTable(InsertTable):
    def _set_op(self, query):
        # GET LIST OF SELECTED COLUMNS
        vars_ = UNION([
            v.var for select in listwrap(query.select) for v in select.value.vars()
        ])
        schema = query.frum.schema
        known_vars = schema.keys()

        active_columns = {".": set()}
        for v in vars_:
            for c in schema.leaves(v):
                nest = c.nested_path[0]
                active_columns.setdefault(nest, set()).add(c)

        # ANY VARS MENTIONED WITH NO COLUMNS?
        for v in vars_:
            if not any(startswith_field(cname, v) for cname in known_vars):
                active_columns["."].add(Column(
                    name=v,
                    jx_type=OBJECT,
                    es_column=".",
                    es_index=".",
                    es_type="NULL",
                    nested_path=["."],
                    multi=1,
                    cardinality=0,
                    last_updated=Date.now(),
                ))

        # EVERY COLUMN, AND THE INDEX IT TAKES UP
        index_to_column = {}  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
        index_to_uid = {}  # FROM ARRAY PATH TO THE INDEX OF UID
        sql_selects = []  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE)
        nest_to_alias = {
            nested_path: table_alias(i)
            for i, nested_path in enumerate(self.snowflake.query_paths)
        }

        sorts = []
        if query.sort:
            for select in query.sort:
                sql = select.value.partial_eval(SQLang).to_sql(schema)
                column_number = len(sql_selects)
                # SQL HAS ABS TABLE REFERENCE
                column_alias = _make_column_name(column_number)
                sql_selects.append(sql_alias(sql, column_alias))
                sorts.append(quote_column(column_alias) + SQL_IS_NULL)
                sorts.append(quote_column(column_alias))
                if select.sort == -1:
                    sorts.append(SQL_DESC)

        primary_doc_details = Data()
        # EVERY SELECT STATEMENT THAT WILL BE REQUIRED, NO MATTER THE DEPTH
        # WE WILL CREATE THEM ACCORDING TO THE DEPTH REQUIRED
        for step, sub_table in self.snowflake.tables:
            nested_doc_details = {
                "sub_table": sub_table,
                "children": [],
                "index_to_column": {},
                "nested_path": ['.']
            }

            if step != ".":
                # INSERT INTO TREE
                def place(parent_doc_details):
                    if step == parent_doc_details["nested_path"][0]:
                        return True
                    if startswith_field(step, parent_doc_details["nested_path"][0]):
                        for c in parent_doc_details["children"]:
                            if place(c):
                                return True
                        parent_doc_details["children"].append(nested_doc_details)
                        nested_doc_details['nested_path'] = [step]+parent_doc_details['nested_path']

                place(primary_doc_details)
            else:
                # ROOT OF TREE
                primary_doc_details = nested_doc_details

            nested_path = nested_doc_details['nested_path']
            alias = nested_doc_details["alias"] = nest_to_alias[step]

            # WE ALWAYS ADD THE UID
            column_number = index_to_uid[step] = nested_doc_details["id_coord"] = len(sql_selects)
            sql_select = quote_column(alias, UID)
            sql_selects.append(sql_alias(sql_select, _make_column_name(column_number)))
            if step != ".":
                # ID FOR CHILD TABLE (REPLACE UID)
                index_to_column[column_number] = ColumnMapping(
                    sql=sql_select,
                    type="number",
                    nested_path=nested_path,
                    column_alias=_make_column_name(column_number),
                )
                # ORDER FOR CHILD TABLE
                column_number = len(sql_selects)
                sql_select = quote_column(alias, ORDER)
                sql_selects.append(sql_alias(
                    sql_select, _make_column_name(column_number)
                ))
                index_to_column[column_number] = ColumnMapping(
                    sql=sql_select,
                    type="number",
                    nested_path=nested_path,
                    column_alias=_make_column_name(column_number),
                )

            # WE DO NOT NEED DATA FROM TABLES WE REQUEST NOTHING FROM
            if step not in active_columns:
                continue

            # ADD SQL SELECT COLUMNS
            selects = (
                jx_expression({"select": query.select})
                .partial_eval(SQLang)
                .to_sql(schema)
            )
            for i, (name, value) in enumerate(selects.frum):
                column_number = len(sql_selects)
                if is_op(value, LeavesOp):
                    Log.error("expecting SelectOp to subsume the LeavesOp")

                sql = value.partial_eval(SQLang).to_sql(schema)
                column_alias = _make_column_name(column_number)
                sql_selects.append(sql_alias(sql, column_alias))
                index_to_column[column_number] = nested_doc_details["index_to_column"][column_number] = ColumnMapping(
                    push_name=name,
                    push_child=".",
                    push_column_name=name.replace("\\.", "."),
                    push_column_index=i,
                    pull=get_column(column_number),
                    sql=sql,
                    type=FromJsonType(value.data_type),
                    column_alias=column_alias,
                    nested_path=nested_path,
                )

        where_clause = ToBooleanOp(query.where).partial_eval(SQLang).to_sql(schema)
        unsorted_sql = self._make_sql_for_one_nest_in_set_op(
            ".", sql_selects, where_clause, active_columns, index_to_column
        )

        for n, _ in self.snowflake.tables:
            sorts.append(quote_column(COLUMN + text(index_to_uid[n])))

        ordered_sql = ConcatSQL(
            SQL_SELECT,
            SQL_STAR,
            SQL_FROM,
            sql_iso(unsorted_sql),
            SQL_ORDERBY,
            sql_list(sorts),
            SQL_LIMIT,
            quote_value(query.limit),
        )
        result = self.db.query(ordered_sql)
        rows = result.data

        def _accumulate_nested(
            rownum, nested_doc_details, parent_doc_id, parent_id_coord
        ):
            """
            :param row: CURRENT ROW BEING EXTRACTED
            :param rownum: index into rows for row
            :param rows: all the rows
            :param nested_doc_details: {
                    "nested_path": wrap_nested_path(nested_path),
                    "index_to_column": map from column number to column details
                    "children": all possible direct decedents' nested_doc_details
                 }
            :param parent_doc_id: the id of the parent doc (for detecting when to step out of loop)
            :param parent_id_coord: the column number for the parent id (so we ca extract from each row)
            :return: the nested property (usually an array), rownum
            """
            previous_doc_id = -1
            doc = dict_to_data({})
            output = []
            id_coord = nested_doc_details["id_coord"]
            index_to_column = tuple(
                (i, c, concat_field(c.push_name, c.push_child))
                for i, c in nested_doc_details["index_to_column"].items()
            )

            while True:
                row = rows[rownum]
                doc_id = row[id_coord]

                if doc_id == None or (
                    parent_id_coord is not None
                    and row[parent_id_coord] != parent_doc_id
                ):
                    rownum -= 1  # NEXT DOCUMENT, BACKUP A BIT
                    return rownum, output

                if doc_id != previous_doc_id:
                    previous_doc_id = doc_id
                    doc = dict_to_data({})
                    curr_nested_path = nested_doc_details["nested_path"][0]
                    for i, c, rel_field in index_to_column:
                        value = row[i]
                        if rel_field == ".":
                            if exists(value):
                                doc = value
                        elif exists(value):
                            doc[rel_field] = value

                for child_details in nested_doc_details["children"]:
                    # EACH NESTED TABLE MUST BE ASSEMBLED INTO A LIST OF OBJECTS
                    child_id = row[child_details["id_coord"]]
                    if child_id is not None:
                        rownum, nested_value = _accumulate_nested(
                            rownum, child_details, doc_id, id_coord
                        )
                        if is_not_null(nested_value):
                            push_name = child_details["nested_path"][0]
                            if is_list(query.select) or is_op(
                                query.select.value, LeavesOp
                            ):
                                # ASSIGN INNER PROPERTIES
                                rel_field = relative_field(
                                    push_name, curr_nested_path
                                )
                            else:  # FACT IS EXPECTED TO BE A SINGLE VALUE, NOT AN OBJECT
                                rel_field = "."

                            if rel_field == ".":
                                doc = unwraplist(nested_value)
                            else:
                                doc[rel_field] = unwraplist(nested_value)

                output.append(doc)

                rownum += 1
                if rownum >= len(rows):
                    return rownum, output

        cols = tuple(i for i in index_to_column.values() if i.push_name != None)

        if rows:
            _, data = _accumulate_nested(0, primary_doc_details, None, None)
        else:
            data = result.data

        if query.format == "cube":
            if is_list(query.select) or is_op(query.select.value, LeavesOp):
                num_rows = len(data)
                temp_data = {c.push_column_name: [None] * num_rows for c in cols}
                for rownum, d in enumerate(data):
                    for c in cols:
                        temp_data[c.push_column_name][rownum] = d[c.push_name]
                return Data(
                    meta={"format": "cube"},
                    data=temp_data,
                    edges=[{
                        "name": "rownum",
                        "domain": {
                            "type": "rownum",
                            "min": 0,
                            "max": num_rows,
                            "interval": 1,
                        },
                    }],
                )
            else:
                key = query.select.name
                num_rows = len(data)
                if key == ".":
                    temp_data = data
                else:
                    temp_data = [d[key] for d in data]

                return Data(
                    meta={"format": "cube"},
                    data={key: temp_data},
                    edges=[{
                        "name": "rownum",
                        "domain": {
                            "type": "rownum",
                            "min": 0,
                            "max": num_rows,
                            "interval": 1,
                        },
                    }],
                )

        elif query.format == "table":
            if is_list(query.select) or is_op(query.select.value, LeavesOp):
                columns = jx.sort(cols, "push_column_index")
                temp_data = [
                    tuple(d[c.push_name] for c in columns)
                    for d in data
                ]

                return Data(
                    meta={"format": "table"},
                    header=tuple(c.push_column_name for c in columns),
                    data=temp_data,
                )
            else:
                key = query.select.name
                if key==".":
                    return Data(
                        meta={"format": "table"},
                        header=(key, ),
                        data=[(d, ) for d in data],
                    )
                else:
                    return Data(
                        meta={"format": "table"},
                        header=(key, ),
                        data=[(d[key], ) for d in data],
                    )

        else:
            if is_list(query.select) or is_op(query.select.value, LeavesOp):
                return Data(meta={"format": "list"}, data=data)
            else:
                values = to_data(data).get(query.select.name)
                return Data(meta={"format": "list"}, data=values)

    def _make_sql_for_one_nest_in_set_op(
        self,
        primary_nested_path,
        selects,  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE
        where_clause,
        active_columns,
        index_to_sql_select,  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
    ):
        """
        FOR EACH NESTED LEVEL, WE MAKE A QUERY THAT PULLS THE VALUES/COLUMNS REQUIRED
        WE `UNION ALL` THEM WHEN DONE
        :param primary_nested_path:
        :param selects:
        :param where_clause:
        :param active_columns:
        :param index_to_sql_select:
        :return: SQL FOR ONE NESTED LEVEL
        """

        parent_alias = "a"
        from_clause = []
        select_clause = []
        children_sql = []
        done = []

        # STATEMENT FOR EACH NESTED PATH
        tables = self.snowflake.tables
        for i, (nested_path, sub_table_name) in enumerate(tables):
            if any(startswith_field(nested_path, d) for d in done):
                continue

            alias = table_alias(i)

            if primary_nested_path == nested_path:
                select_clause = []
                # ADD SELECT CLAUSE HERE
                for select_index, s in enumerate(selects):
                    column_mapping = index_to_sql_select.get(select_index)
                    if not column_mapping:
                        select_clause.append(s)
                        continue

                    if startswith_field(column_mapping.nested_path[0], nested_path):
                        select_clause.append(sql_alias(
                            column_mapping.sql, column_mapping.column_alias
                        ))
                    else:
                        # DO NOT INCLUDE DEEP STUFF AT THIS LEVEL
                        select_clause.append(sql_alias(
                            SQL_NULL, column_mapping.column_alias
                        ))

                if nested_path == ".":
                    from_clause.append(SQL_FROM)
                    from_clause.append(sql_alias(
                        quote_column(self.snowflake.fact_name), alias
                    ))
                else:
                    from_clause.append(SQL_LEFT_JOIN)
                    from_clause.append(sql_alias(
                        quote_column(sub_table_name), alias
                    ))
                    from_clause.append(SQL_ON)
                    from_clause.append(quote_column(alias, PARENT))
                    from_clause.append(SQL_EQ)
                    from_clause.append(quote_column(parent_alias, UID))
                    where_clause = ConcatSQL(
                        sql_iso(where_clause),
                        SQL_AND,
                        quote_column(alias, ORDER),
                        SQL_GT,
                        SQL_ZERO
                    )
                parent_alias = alias

            elif startswith_field(primary_nested_path, nested_path):
                # PARENT TABLE
                # NO NEED TO INCLUDE COLUMNS, BUT WILL INCLUDE ID AND ORDER
                if nested_path == ".":
                    from_clause.append(SQL_FROM)
                    from_clause.append(sql_alias(
                        quote_column(self.snowflake.fact_name), alias
                    ))
                else:
                    parent_alias = alias = table_alias(i)
                    from_clause.append(SQL_LEFT_JOIN)
                    from_clause.append(sql_alias(
                        quote_column(sub_table_name), alias
                    ))
                    from_clause.append(SQL_ON)
                    from_clause.append(quote_column(alias, PARENT))
                    from_clause.append(SQL_EQ)
                    from_clause.append(quote_column(parent_alias, UID))
                    where_clause = ConcatSQL(
                        sql_iso(where_clause),
                        SQL_AND,
                        quote_column(parent_alias, ORDER),
                        SQL_GT,
                        SQL_ZERO
                    )
                parent_alias = alias

            elif startswith_field(nested_path, primary_nested_path):
                # CHILD TABLE
                # GET FIRST ROW FOR EACH NESTED TABLE
                from_clause.append(ConcatSQL(
                    SQL_LEFT_JOIN,
                    sql_alias(
                        quote_column(sub_table_name), alias
                    ),
                    SQL_ON,
                    quote_column(alias, PARENT),
                    SQL_EQ,
                    quote_column(parent_alias, UID),
                    SQL_AND,
                    quote_column(alias, ORDER),
                    SQL_EQ,
                    SQL_ZERO,
                ))

                # IMMEDIATE CHILDREN ONLY
                done.append(nested_path)
                # NESTED TABLES WILL USE RECURSION
                children_sql.append(self._make_sql_for_one_nest_in_set_op(
                    nested_path,
                    selects,  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE
                    where_clause,
                    active_columns,
                    index_to_sql_select,  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
                ))
            else:
                # SIBLING PATHS ARE IGNORED
                continue

        sql = SQL_UNION_ALL.join(
            [ConcatSQL(
                SQL_SELECT,
                sql_list(select_clause),
                ConcatSQL(*from_clause),
                SQL_WHERE,
                where_clause,
            )] + children_sql
        )

        return sql


def test_dots(cols):
    for c in cols:
        if "\\" in c.push_column_name:
            return True
    return False
