# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from dataclasses import is_dataclass
from typing import Dict, List
from uuid import uuid4

from jx_base import Column
from jx_base.expressions import jx_expression, TRUE, SqlScript
from jx_sqlite import Facts
from jx_sqlite.utils import (
    GUID,
    ORDER,
    PARENT,
    UID,
    typed_column,
)
from jx_sqlite.utils import untyped_column
from mo_dots import (
    Data,
    concat_field,
    listwrap,
    startswith_field,
    from_data,
    is_many,
    is_data,
    to_data,
    relative_field, exists,
)
from mo_future import text, first, extend
from mo_json import STRUCT, ARRAY, OBJECT, value_to_json_type, get_if_type, jx_type_to_json_type, base_type
from mo_logs import logger
from mo_sql.utils import json_type_to_sql_type_key
from mo_sqlite import (
    SQL_AND,
    SQL_FROM,
    SQL_INNER_JOIN,
    SQL_NULL,
    SQL_SELECT,
    SQL_TRUE,
    SQL_UNION_ALL,
    SQL_WHERE,
    sql_iso,
    sql_list,
    SQL_VALUES,
    SQL_INSERT,
    ConcatSQL,
    SQL_EQ,
    SQL_UPDATE,
    SQL_SET,
    SQL_ONE,
    SQL_DELETE,
    SQL_ON,
    SQL_COMMA,
)
from mo_sqlite import (
    json_type_to_sqlite_type,
    quote_column,
    quote_value,
    sql_alias,
)
from mo_times import Date


@extend(Facts)
def insert(self, docs):
    if not is_many(docs):
        logger.error("Expecting a list of documents")
    if not docs:
        return self
    doc_actions = self.flatten_many(docs)
    return self._insert(doc_actions)


@extend(Facts)
def update(self, command):
    """
    :param command:  EXPECTING dict WITH {"set": s, "clear": c, "where": w} FORMAT
    """
    command = to_data(command)
    clear_columns = set(listwrap(command["clear"]))

    # REJECT DEEP UPDATES
    touched_columns = command.set.keys() | clear_columns
    for c in self.schema.columns:
        if c.name in touched_columns and len(c.nested_path) > 1:
            logger.error("Deep update not supported")

    # ADD NEW COLUMNS
    where = jx_expression(command.where) if command.where else TRUE
    _vars = where.vars()
    _map = {v: c.es_column for v in _vars for _, c in self.schema.leaves(v) if c.json_type not in STRUCT}
    where_sql = where.map(_map).to_sql(self.schema)
    new_columns = set(command.set.keys()) - set(c.name for c in self.schema.columns)
    for new_column_name in new_columns:
        expr = jx_expression(command.set[new_column_name])
        json_type = jx_type_to_json_type(expr.jx_type)
        column = Column(
            name=new_column_name,
            json_type=json_type,
            es_index=self.name,
            es_type=json_type_to_sqlite_type[json_type],
            es_column=typed_column(new_column_name, json_type_to_sql_type_key[json_type]),
            nested_path=[self.name],
            multi=1,
            cardinality=1,
            last_updated=Date.now(),
        )
        self.snowflake._add_column(column)

    # UPDATE THE ARRAY VALUES
    for nested_column_name, expr in command.set.items():
        if value_to_json_type(expr) == "nested":
            nested_table_name = concat_field(self.name, nested_column_name)
            nested_table = nested_tables[nested_column_name]
            self_primary_key = sql_list(quote_column(c.es_column) for u in self.uid for c in self.columns[u])
            extra_key_name = UID + str(len(self.uid))
            extra_key = [e for e in nested_table.columns[extra_key_name]][0]

            sql_command = ConcatSQL(
                SQL_DELETE,
                SQL_FROM,
                quote_column(nested_table.name),
                SQL_WHERE,
                "EXISTS",
                sql_iso(
                    SQL_SELECT,
                    SQL_ONE,
                    SQL_FROM,
                    sql_alias(quote_column(nested_table.name), "n"),
                    SQL_INNER_JOIN,
                    sql_iso(
                        SQL_SELECT, self_primary_key, SQL_FROM, quote_column(abs_schema.fact), SQL_WHERE, where_sql,
                    ),
                    quote_column("t"),
                    SQL_ON,
                    SQL_AND.join(
                        ConcatSQL(quote_column("t", c.es_column), SQL_EQ, quote_column("n", c.es_column),)
                        for u in self.uid
                        for c in self.columns[u]
                    ),
                ),
            )
            self.container.db.execute(sql_command)

            # INSERT NEW RECORDS
            if not expr:
                continue

            doc_collection = {}
            for d in listwrap(expr):
                nested_table.flatten(d, Data(), doc_collection, path=nested_column_name)

            prefix = ConcatSQL(
                SQL_INSERT,
                quote_column(nested_table.name),
                sql_iso(sql_list(
                    [self_primary_key]
                    + [quote_column(extra_key)]
                    + [quote_column(c.es_column) for c in doc_collection["."].active_columns]
                )),
            )

            # BUILD THE PARENT TABLES
            parent = ConcatSQL(
                SQL_SELECT,
                self_primary_key,
                SQL_FROM,
                quote_column(abs_schema.fact),
                SQL_WHERE,
                jx_expression(command.where).to_sql(schema),
            )

            # BUILD THE RECORDS
            children = SQL_UNION_ALL.join(
                ConcatSQL(
                    SQL_SELECT,
                    sql_alias(quote_value(i), extra_key.es_column),
                    SQL_COMMA,
                    sql_list(
                        sql_alias(quote_value(row[c.name]), quote_column(c.es_column))
                        for c in doc_collection["."].active_columns
                    ),
                )
                for i, row in enumerate(doc_collection["."].rows)
            )

            sql_command = ConcatSQL(
                prefix,
                SQL_SELECT,
                sql_list(
                    [quote_column("p", c.es_column) for u in self.uid for c in self.columns[u]]
                    + [quote_column("c", extra_key)]
                    + [quote_column("c", c.es_column) for c in doc_collection["."].active_columns]
                ),
                SQL_FROM,
                sql_iso(parent),
                quote_column("p"),
                SQL_INNER_JOIN,
                sql_iso(children),
                quote_column("c"),
                SQL_ON,
                SQL_TRUE,
            )

            self.container.db.execute(sql_command)

            # THE CHILD COLUMNS COULD HAVE EXPANDED
            # ADD COLUMNS TO SELF
            for n, cs in nested_table.columns.items():
                for c in cs:
                    column = Column(
                        name=c.name,
                        json_type=c.json_type,
                        es_type=c.es_type,
                        es_index=c.es_index,
                        es_column=c.es_column,
                        nested_path=[nested_column_name] + c.nested_path,
                        last_updated=Date.now(),
                    )
                    if c.name not in self.columns:
                        self.columns[column.name] = {column}
                    elif c.json_type not in [c.json_type for c in self.columns[c.name]]:
                        self.columns[column.name].add(column)

    if "." in clear_columns:
        if not command.set:
            self.delete(where)
            return
        else:
            # PROBABLY A DELETE AND INSERT
            logger.error("do not know how to handle")

    command = ConcatSQL(
        SQL_UPDATE,
        quote_column(self.name),
        SQL_SET,
        sql_list([
            *(
                ConcatSQL(quote_column(c.es_column), SQL_EQ, jx_expression(v).to_sql(self.schema))
                for c in self.schema.columns
                if c.json_type != ARRAY and len(c.nested_path) == 1
                for v in [command.set[c.name]]
                if v != None
            ),
            *(
                ConcatSQL(quote_column(c.es_column), SQL_EQ, SQL_NULL)
                for c in self.schema.columns
                if (
                    c.name in clear_columns
                    and command.set[c.name] != None
                    and c.json_type != ARRAY
                    and len(c.nested_path) == 1
                )
            )
        ]),
        SQL_WHERE,
        where_sql,
    )

    with self.container.db.transaction() as t:
        t.execute(command)


@extend(Facts)
def upsert(self, doc, where):
    self.delete(where)
    self.insert([doc])


@extend(Facts)
def flatten_many(self, docs):
    """
    :param docs: THE JSON DOCUMENTS
    :return: TUPLE (success, command, doc_collection) WHERE
             success: BOOLEAN INDICATING PROPER PARSING
             command: SCHEMA CHANGES REQUIRED TO BE SUCCESSFUL NEXT TIME
             doc_collection: MAP FROM NESTED PATH TO INSERTION PARAMETERS:
             {"active_columns": list, "rows": list of objects}
    """

    facts_insertion = Insertion()
    doc_collection: Dict[str, Insertion] = {self.name: facts_insertion}
    doc_actions = {"delete":[], "insert": doc_collection}
    # KEEP TRACK OF WHAT TABLE WILL BE MADE (SHORTLY)
    required_changes = []
    snowflake = self.container.get_or_create_facts(self.name).snowflake

    def _flatten(doc, doc_path, nested_path, row, row_num, row_id, parent_id):
        """
        :param doc: the data we are pulling apart
        :param doc_path: path to this (sub)doc
        :param nested_path: list of paths, deepest first, pointing to table
        :param row: we will be filling this
        :param row_num: the number of siblings before this one
        :param row_id: the id we are giving this row
        :param parent_id: the parent id of this (sub)doc
        :return:
        """
        table_name = nested_path[0]
        insertion = doc_collection.setdefault(table_name, Insertion())
        known_columns = snowflake.get_schema(nested_path).columns

        if is_data(doc):
            items = [(k, v) for k, v in to_data(doc).leaves()]
        else:
            # PRIMITIVE VALUES
            items = [(".", doc)]

        for rel_name, v in items:
            abs_name = concat_field(doc_path, rel_name)
            json_type = value_to_json_type(v)
            if json_type is None:
                continue

            columns = known_columns + insertion.active_columns
            if json_type == ARRAY:
                curr_column = first(
                    cc for cc in columns if cc.json_type in STRUCT and untyped_column(cc.name)[0] == abs_name
                )
                if curr_column:
                    deeper_insertion = doc_collection.setdefault(
                        concat_field(curr_column.es_index, curr_column.es_column), Insertion()
                    )

            else:
                curr_column = first(cc for cc in columns if cc.json_type == json_type and cc.name == abs_name)

            if not curr_column:
                curr_column = Column(
                    name=abs_name,
                    json_type=json_type,
                    es_type=json_type_to_sqlite_type.get(json_type, json_type),
                    es_column=typed_column(
                        concat_field(relative_field(nested_path[0], table_name), rel_name),
                        json_type_to_sql_type_key.get(json_type),
                    ),
                    es_index=table_name,
                    cardinality=0,
                    multi=1,
                    nested_path=nested_path,
                    last_updated=Date.now(),
                )
                if json_type == ARRAY:
                    # NOTE: ADVANCE active_columns TO THIS NESTED LEVEL
                    # SCHEMA (AND DATABASE) WILL BE UPDATED LATER
                    new_query_path = concat_field(curr_column.es_index, curr_column.es_column)
                    deeper_insertion = doc_collection.setdefault(new_query_path, Insertion())
                    old_column_prefix, _ = untyped_column(curr_column.es_column)
                    for c in list(insertion.active_columns):
                        if c.nested_path[0] == nested_path[0] and startswith_field(c.es_column, old_column_prefix):
                            doc_collection[table_name].active_columns.remove(c)
                            doc_collection[new_query_path].active_columns.append(c)
                    insertion.query_paths.append(curr_column.es_column)
                    required_changes.append({"nest": curr_column})
                else:
                    required_changes.append({"add": curr_column})

                insertion.active_columns.append(curr_column)

            elif curr_column.json_type == ARRAY and json_type == OBJECT:
                # ALWAYS PROMOTE OBJECTS TO NESTED
                json_type = ARRAY
                v = [v]
            elif len(curr_column.nested_path) < len(nested_path):
                es_column = curr_column.es_column

                # PROMOTE COLUMN TO ARRAY OF VALUES
                parent_rows = doc_collection[table_name].rows
                for r in parent_rows:
                    if es_column in r:
                        deeper_es_column = typed_column(
                            concat_field(nested_path[0], rel_name), json_type_to_sql_type_key.get(json_type),
                        )

                        row1 = {
                            UID: self.container.next_uid(),
                            PARENT: r[UID],
                            ORDER: 0,
                            deeper_es_column: r[es_column],
                        }
                        insertion.rows.append(row1)
            elif len(curr_column.nested_path) > len(nested_path):
                insertion = doc_collection[curr_column.nested_path[0]]
                row = {
                    UID: self.container.next_uid(),
                    PARENT: row_id,
                    ORDER: row_num,
                }
                insertion.rows.append(row)

            # BE SURE TO NEST VALUES, IF NEEDED
            if json_type == ARRAY:
                for child_row_num, child_data in enumerate(v):
                    child_uid = self.container.next_uid()
                    child_row = {
                        UID: child_uid,
                        PARENT: row_id,
                        ORDER: child_row_num,
                    }
                    deeper_insertion.rows.append(child_row)

                    _flatten(
                        doc=child_data,
                        doc_path=abs_name,
                        nested_path=[concat_field(curr_column.es_index, curr_column.es_column), *nested_path],
                        row=child_row,
                        row_num=child_row_num,
                        row_id=child_uid,
                        parent_id=row_id,
                    )
            elif json_type == OBJECT:
                _flatten(
                    doc=v,
                    doc_path=abs_name,
                    nested_path=nested_path,
                    row=row,
                    row_num=row_num,
                    row_id=row_id,
                    parent_id=parent_id,
                )
            elif curr_column.json_type:
                if curr_column not in insertion.active_columns:
                    known_columns.remove(curr_column)
                    insertion.active_columns.append(curr_column)
                row[curr_column.es_column] = v

    guids = doc_actions['delete']
    for doc in docs:
        if is_dataclass(doc):
            doc = {k: v for k, v in doc.__dict__.items() if exists(v)}

        if is_data(doc):
            if UID in doc:
                logger.error("not allowed {uid} in as top level property", uid=UID)
            if GUID in doc:
                guid = doc[GUID]
                guids.append(guid)
            else:
                guid = str(uuid4())
        else:
            guid = str(uuid4())
        uid = self.container.next_uid()
        row = {GUID: guid, UID: uid}
        facts_insertion.rows.append(row)
        _flatten(
            doc=doc, doc_path=".", nested_path=[self.name], row=row, row_num=0, row_id=uid, parent_id=0,
        )
        if required_changes:
            snowflake.change_schema(required_changes)
            required_changes = []

    return doc_actions


@extend(Facts)
def _insert(self, doc_actions):
    with self.container.db.transaction() as t:

        if doc_actions['delete']:
            self.delete({"in": {GUID: doc_actions['delete']}})

        collection = doc_actions['insert']
        for nested_path, insertion in collection.items():
            column_names = [c.es_column for c in insertion.active_columns if c.json_type != ARRAY]
            rows = insertion.rows
            table_name = nested_path

            if table_name == self.name:
                # DO NOT REQUIRE PARENT OR ORDER COLUMNS
                meta_columns = [GUID, UID]
            else:
                meta_columns = [UID, PARENT, ORDER]

            all_columns = tuple(meta_columns + column_names)
            command = ConcatSQL(
                SQL_INSERT,
                quote_column(table_name),
                sql_iso(sql_list(map(quote_column, all_columns))),
                SQL_VALUES,
                sql_list(sql_iso(sql_list(quote_value(row.get(c)) for c in all_columns)) for row in from_data(rows)),
            )
            t.execute(command)
    return self


class Insertion:
    def __init__(self):
        self.active_columns = []
        self.rows: List[Dict] = []
        self.query_paths: List[str] = []  # CHILDREN ARRAYS
