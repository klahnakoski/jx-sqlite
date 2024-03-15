# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from copy import copy

import jx_base
from jx_base import Schema, Table, Container, Column, Snowflake
from jx_base.meta_columns import (
    META_COLUMNS_DESC,
    META_COLUMNS_NAME,
    SIMPLE_METADATA_COLUMNS,
)
from jx_python import jx
from jx_sqlite.utils import untyped_column, untype_field
from mo_dots import Data, Null, coalesce, is_data, is_list, startswith_field, unwraplist, wrap, list_to_data
from mo_future import first
from mo_json import STRUCT, IS_NULL
from mo_json.typed_encoder import detype
from mo_logs import Log, logger
from mo_sql.utils import sql_type_key_to_json_type, SQL_ARRAY_KEY
from mo_threads import Queue, Lock
from mo_times.dates import Date

DEBUG = False
singlton = None
COLUMN_LOAD_PERIOD = 10
COLUMN_EXTRACT_PERIOD = 2 * 60
ID = {"field": ["es_index", "es_column"], "version": "last_updated"}

CACHE_LOCKER = Lock()
CACHE = {}  # MAP FROM id(db) TO ColumnList MANAGING THAT DB


class ColumnList(Table, Container):
    """
    OPTIMIZED FOR fact column LOOKUP
    """

    def __new__(cls, db):
        with CACHE_LOCKER:
            output = CACHE.get(id(db))
            if not output:
                output = CACHE[id(db)] = object.__new__(cls)
        return output

    def __init__(self, db):
        with CACHE_LOCKER:
            if hasattr(self, "db"):
                return
            self.db = db
        Table.__init__(self, META_COLUMNS_NAME)
        self.data = {}  # MAP FROM fact_name TO (abs_column_name to COLUMNS)
        self.locker = Lock()
        self._schema = None
        self.dirty = False
        self.es_index = None
        self.last_load = Null
        self.todo = Queue("update columns to es")  # HOLD (action, column) PAIR, WHERE action in ['insert', 'update']
        self._snowflakes = {}  # MAP FROM fact_name TO LIST OF PATHS, STARTING WITH FACT AND BREADTH FIRST TO LEAVES
        self.primary_keys = {}  # MAP FROM table TO LIST OF PRIMARY KEY COLUMNS
        self.relations = None
        self._load_from_database()

    def rename_tables(self, name_map):
        output = object.__new__(ColumnList)
        output.db = None
        output.data = {
            name_map.get(table_name, table_name): {
                col_name: [
                    Column(
                        name=col.name,
                        es_column=col.es_column,
                        es_index=name_map.get(col.es_index, col.es_index),
                        es_type=col.es_type,
                        json_type=col.json_type,
                        nested_path=[name_map.get(n, n) for n in col.nested_path],
                        count=col.count,
                        cardinality=col.cardinality,
                        multi=col.multi,
                        partitions=col.partitions,
                        last_updated=col.last_updated,
                    )
                    for col in cols
                ]
                for col_name, cols in v.items()
            }
            for table_name, v in self.data.items()
        }
        output.locker = Lock()
        output._schema = None
        output.dirty = False
        output.es_index = None
        output.last_load = Null
        output.todo = None
        output._snowflakes = {
            name_map.get(table_name, table_name): [name_map.get(qp, qp) for qp in query_paths]
            for table_name, query_paths in self._snowflakes.items()
        }
        output.primary_keys = {}
        output.relations = None
        return output

    def _query(self, query):
        result = Data()
        curr = self.db.execute(query)
        result.meta.format = "table"
        result.header = [d[0] for d in curr.description] if curr.description else None
        result.data = curr.fetchall()
        return result

    def load_existing_table(self, table_name, known_tables=None):
        """
        IN THE EVENT SOMETHING ELSE MADE THE TABLE
        """
        if table_name in [vv for v in self._snowflakes.values() for vv in v]:
            # ALREADY LOADED
            return

        tables = known_tables or self.db.get_tables()
        if table_name not in tables.name:
            logger.error("expecting table to be in database")

        # CONSTRUCT NESTED PATH, MAKE PARENT TABLES TOO
        full_nested_path = []
        for table in tables:
            if startswith_field(table_name, table.name) and table_name != table.name:
                self.load_existing_table(table.name, known_tables)
                full_nested_path = [table.name, *full_nested_path]

        if not full_nested_path and table_name not in self._snowflakes:
            self._snowflakes[table_name] = []
        full_nested_path = [table_name, *full_nested_path]
        self._snowflakes[full_nested_path[-1]].append(table_name)

        # LOAD THE COLUMNS
        details = self.db.about(table_name)

        for cid, name, sql_type, notnull, dfft_value, pk in details:
            if name.startswith("__"):
                continue
            cname, sql_type_key = untyped_column(name)
            self.add(Column(
                name=cname,
                json_type=coalesce(
                    sql_type_key_to_json_type.get(sql_type_key),
                    sql_type_key_to_json_type.get(sql_type),
                    IS_NULL,
                ),
                nested_path=full_nested_path,
                es_type=sql_type,
                es_column=name,
                es_index=table.name,
                multi=1,
                last_updated=Date.now(),
            ))

        self.relations = [r for t in tables for r in self.db.get_relations(t.name)]


    def _load_from_database(self):
        # FIND ALL TABLES
        tables = self.db.get_tables()
        last_nested_path = []
        for table in tables:
            if table.name.startswith("__"):
                continue

            # FIND COMMON ARRAY PATH SUFFIX
            for i, p in enumerate(last_nested_path):
                if startswith_field(table.name, p):
                    last_nested_path = last_nested_path[i:]
                    break
            else:
                last_nested_path = []

            full_nested_path = [table.name] + last_nested_path
            self._snowflakes.setdefault(full_nested_path[-1], []).append(table.name)

            # LOAD THE COLUMNS
            details = self.db.about(table.name)

            for cid, name, sql_type, notnull, dfft_value, pk in details:
                if name.startswith("__"):
                    continue
                cname, sql_type_key = untyped_column(name)
                self.add(Column(
                    name=cname,
                    json_type=coalesce(
                        sql_type_key_to_json_type.get(sql_type_key),
                        sql_type_key_to_json_type.get(sql_type),
                        IS_NULL,
                    ),
                    nested_path=full_nested_path,
                    es_type=sql_type,
                    es_column=name,
                    es_index=table.name,
                    multi=1,
                    last_updated=Date.now(),
                ))
            last_nested_path=full_nested_path

        self.relations = [r for t in tables for r in self.db.get_relations(t.name)]

    def find(self, fact_table, abs_column_name=None):
        try:
            with self.locker:
                if fact_table.startswith("meta."):
                    self._update_meta()

                if not abs_column_name:
                    return [
                        cc
                        for table, cs in self.data.items()
                        if startswith_field(table, fact_table)
                        for c in cs.values()
                        for cc in c
                    ]
                else:
                    return self.data.get(fact_table, {}).get(abs_column_name, [])
        except Exception as cause:
            Log.error("not expected", cause=cause)

    def extend(self, columns):
        self.dirty = True
        with self.locker:
            for column in columns:
                self._add(column)

    def add(self, column):
        self.dirty = True
        with self.locker:
            canonical = self._add(column)
        if canonical == None:
            return column  # ALREADY ADDED
        self.todo.add(canonical)
        return canonical

    def remove(self, column):
        self.dirty = True
        with self.locker:
            self._remove(column)

    def remove_table(self, table_name):
        try:
            del self.data[table_name]
        except KeyError:
            pass

    def _add(self, column):
        """
        :param column: ANY COLUMN OBJECT
        :return: None IF column IS canonical ALREADY (NET-ZERO EFFECT)
        """
        columns_for_table = self.data.setdefault(column.es_index, {})
        existing_columns = columns_for_table.setdefault(column.es_column, [])

        for canonical in existing_columns:
            if canonical is column:
                return None
            if canonical.es_type == column.es_type:
                if column.last_updated > canonical.last_updated:
                    for key in Column.__slots__:
                        old_value = canonical[key]
                        new_value = column[key]
                        if new_value == None:
                            pass  # DO NOT BOTHER CLEARING OLD VALUES (LIKE cardinality AND paritiions)
                        elif new_value == old_value:
                            pass  # NO NEED TO UPDATE WHEN NO CHANGE MADE (COMMON CASE)
                        else:
                            canonical[key] = new_value
                return canonical
        existing_columns.append(column)
        return column

    def _remove(self, column):
        """
        :param column: ANY COLUMN OBJECT
        """
        columns_for_table = self.data.setdefault(column.es_index, {})
        existing_columns = columns_for_table.setdefault(column.es_column, [])

        for i, canonical in enumerate(existing_columns):
            if canonical is column:
                del existing_columns[i]
                return

    def _update_meta(self):
        if not self.dirty:
            return

        now = Date.now()
        for mc in META_COLUMNS_DESC.columns:
            count = 0
            values = set()
            objects = 0
            multi = 1
            for column in self._all_columns():
                value = column[mc.name]
                if value == None:
                    pass
                else:
                    count += 1
                    if is_list(value):
                        multi = max(multi, len(value))
                        try:
                            values |= set(value)
                        except Exception:
                            objects += len(value)
                    elif is_data(value):
                        objects += 1
                    else:
                        values.add(value)
            mc.count = count
            mc.cardinality = len(values) + objects
            mc.partitions = jx.sort(values)
            mc.multi = multi
            mc.last_updated = now

        META_COLUMNS_DESC.last_updated = now
        self.dirty = False

    def _all_columns(self):
        return [column for t, cs in self.data.items() for _, css in cs.items() for column in css]

    def __iter__(self):
        with self.locker:
            self._update_meta()
            return iter(self._all_columns())

    def update(self, command):
        self.dirty = True
        try:
            command = list_to_data(command)
            DEBUG and Log.note(
                "Update {{timestamp}}: {{command|json}}", command=command, timestamp=Date(command["set"].last_updated),
            )
            eq = command.where.eq
            if eq.es_index:
                if len(eq) == 1:
                    if unwraplist(command.clear) == ".":
                        d = self.data
                        i = eq.es_index
                        with self.locker:
                            cols = d[i]
                            del d[i]

                        for c in cols:
                            mark_as_deleted(c)
                            self.todo.add(c)
                        return

                    # FASTEST
                    all_columns = self.data.get(eq.es_index, {}).values()
                    with self.locker:
                        columns = [c for cs in all_columns for c in cs]
                elif eq.es_column and len(eq) == 2:
                    # FASTER
                    all_columns = self.data.get(eq.es_index, {}).values()
                    with self.locker:
                        columns = [c for cs in all_columns for c in cs if c.es_column == eq.es_column]

                else:
                    # SLOWER
                    all_columns = self.data.get(eq.es_index, {}).values()
                    with self.locker:
                        columns = [
                            c
                            for cs in all_columns
                            for c in cs
                            if all(c[k] == v for k, v in eq.items())  # THIS LINE IS VERY SLOW
                        ]
            else:
                columns = list(self)
                columns = jx.filter(columns, command.where)

            with self.locker:
                for col in columns:
                    DEBUG and Log.note(
                        "update column {table}.{column}", table=col.es_index, column=col.es_column,
                    )
                    for k in command["clear"]:
                        if k == ".":
                            mark_as_deleted(col)
                            self.todo.add(col)
                            lst = self.data[col.es_index]
                            cols = lst[col.name]
                            cols.remove(col)
                            if len(cols) == 0:
                                del lst[col.name]
                                if len(lst) == 0:
                                    del self.data[col.es_index]
                            break
                        else:
                            col[k] = None
                    else:
                        # DID NOT DELETE COLUMNM ("."), CONTINUE TO SET PROPERTIES
                        for k, v in command.set.items():
                            col[k] = v
                        self.todo.add(col)

        except Exception as e:
            Log.error("should not happen", cause=e)

    def query(self, query):
        # NOT EXPECTED TO BE RUN
        Log.error("not")
        with self.locker:
            self._update_meta()
            if not self._schema:
                self._schema = Schema(
                    [self.name],
                    Snowflake(None, [self.name], [c for cs in self.data[META_COLUMNS_NAME].values() for c in cs]),
                )
            snapshot = self._all_columns()

        from jx_python.containers.list import ListContainer

        query.frum = ListContainer(META_COLUMNS_NAME, snapshot, self._schema)
        return jx.run(query)

    def groupby(self, keys):
        with self.locker:
            self._update_meta()
            return jx.groupby(self.__iter__(), keys)

    def window(self, window):
        raise NotImplemented()

    @property
    def schema(self):
        if not self._schema:
            with self.locker:
                self._update_meta()
                self._schema = Schema(".", [c for cs in self.data[META_COLUMNS_NAME].values() for c in cs])
        return self._schema

    @property
    def namespace(self):
        return self

    def get_table(self, table_name):
        if table_name != META_COLUMNS_NAME:
            Log.error("this container has only the " + META_COLUMNS_NAME)
        return self

    def get_columns(self, table_name):
        if table_name != META_COLUMNS_NAME:
            Log.error("this container has only the " + META_COLUMNS_NAME)
        return self._all_columns()

    def get_nested_path(self, table_name):
        fixer = (lambda x: x) if SQL_ARRAY_KEY in table_name else (lambda x: untype_field(x)[0])
        clean_name = fixer(table_name)

        query_paths = first(qps for k, qps in self._snowflakes.items() for qp in qps if fixer(qp) == clean_name)
        if not query_paths:
            Log.error("not found", table_name=table_name)
        nested_path = []
        for query_path in query_paths:
            if startswith_field(clean_name, query_path):
                nested_path.append(query_path)
        return list(reversed(nested_path))

    def get_query_paths(self, fact_name):
        """
        RETURN LIST OF QUERY PATHS FOR GIVEN FACT
        :return:
        """
        return copy(self._snowflakes[fact_name])

    def denormalized(self):
        """
        THE INTERNAL STRUCTURE FOR THE COLUMN METADATA IS VERY DIFFERENT FROM
        THE DENORMALIZED PERSPECTIVE. THIS PROVIDES THAT PERSPECTIVE FOR QUERIES
        """
        with self.locker:
            self._update_meta()
            output = [
                {
                    "table": c.es_index,
                    "name": untyped_column(c.name)[0],
                    "cardinality": c.cardinality,
                    "es_column": c.es_column,
                    "es_index": c.es_index,
                    "last_updated": c.last_updated,
                    "count": c.count,
                    "nested_path": [untype_field(n)[0] for n in c.nested_path],
                    "es_type": c.es_type,
                    "type": c.json_type,
                }
                for tname, css in self.data.items()
                for cname, cs in css.items()
                for c in cs
                if c.json_type not in STRUCT  # and c.es_column != "_id"
            ]

        from jx_python.containers.list import ListContainer

        return ListContainer(
            self.name, data=output, schema=jx_base.Schema(META_COLUMNS_NAME, SIMPLE_METADATA_COLUMNS),
        )


def doc_to_column(doc):
    return Column(**wrap(detype(doc)))


def mark_as_deleted(col):
    col.count = 0
    col.cardinality = 0
    col.multi = 0
    col.partitions = None
    col.last_updated = Date.now()


class _FakeLock:
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
