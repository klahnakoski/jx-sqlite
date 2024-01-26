# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
from typing import List

import jx_base
from jx_sqlite.expressions._utils import SQL_ARRAY_KEY
from jx_sqlite.schema import Schema
from jx_sqlite.table import Table
from jx_sqlite.utils import (
    quoted_ORDER,
    quoted_PARENT,
    quoted_UID,
    GUID,
    untype_field,
)
from mo_dots import (
    concat_field,
    to_data,
    startswith_field,
    split_field,
    join_field,
    relative_field,
)
from mo_future import first
from mo_json import ARRAY, OBJECT, EXISTS
from mo_logs import Log, Except
from mo_sqlite import (
    SQL_FROM,
    SQL_SELECT,
    SQL_ZERO,
    sql_iso,
    sql_list,
    SQL_CREATE,
    SQL_AS,
    ConcatSQL,
    SQL_ALTER_TABLE,
    SQL_ADD_COLUMN,
    SQL_RENAME_COLUMN,
    SQL_RENAME_TO,
    SQL_TO,
    TextSQL,
    SQL_INSERT,
)
from mo_sqlite import quote_column


class Snowflake(jx_base.Snowflake):
    """
    MANAGE SINGLE HIERARCHY IN SQLITE DATABASE
    """

    def __init__(self, fact_name, namespace):
        if not namespace.columns._snowflakes.get(fact_name):
            Log.error("{{name}} does not exist", name=fact_name)

        self.fact_name = fact_name  # THE CENTRAL FACT TABLE
        self.namespace = namespace

    def __copy__(self):
        Log.error("con not copy")

    @property
    def container(self):
        return self.namespace.container

    def change_schema(self, required_changes):
        """
        ACCEPT A LIST OF CHANGES
        :param required_changes:
        :return: None
        """
        required_changes = to_data(required_changes)
        for required_change in required_changes:
            if required_change.add:
                self._add_column(required_change.add)
            elif required_change.nest:
                self._nest_column(required_change.nest)

    def _add_column(self, column):
        cname = column.name
        if column.json_type == ARRAY:
            # WE ARE ALSO NESTING
            self._nest_column(column, [cname] + column.nested_path)

        table = column.nested_path[0]

        try:
            with self.namespace.container.db.transaction() as t:
                t.execute(ConcatSQL(
                    SQL_ALTER_TABLE,
                    quote_column(table),
                    SQL_ADD_COLUMN,
                    quote_column(column.es_column),
                    quote_column(column.es_type),
                ))
            self.namespace.columns.add(column)
        except Exception as e:
            e = Except.wrap(e)
            if "duplicate column name" in e:
                # THIS HAPPENS WHEN MULTIPLE THREADS ARE ASKING FOR MORE COLUMNS TO STORE DATA
                # THIS SHOULD NOT BE A PROBLEM SINCE THE THREADS BOTH AGREE THE COLUMNS SHOULD EXIST
                # BUT, IT WOULD BE NICE TO MAKE LARGER TRANSACTIONS SO THIS NEVER HAPPENS
                # CONFIRM THE COLUMN EXISTS IN LOCAL DATA STRUCTURES
                for c in self.namespace.columns:
                    if c.es_column == column.es_column:
                        break
                else:
                    Log.error(
                        "Did not add column {{column}}", column=column.es_column, cause=e,
                    )
            else:
                Log.error("Did not add column {{column}}", column=column.es_column, cause=e)

    def _drop_column(self, column):
        # DROP COLUMN BY RENAMING IT, WITH __ PREFIX TO HIDE IT
        cname = column.name
        if column.json_type == "nested":
            # WE ARE ALSO NESTING
            self._nest_column(column, [cname] + column.nested_path)

        table = concat_field(self.fact_name, column.nested_path[0])

        with self.namespace.container.db.transaction() as t:
            t.execute(ConcatSQL(
                SQL_ALTER_TABLE,
                quote_column(table),
                SQL_RENAME_COLUMN,
                quote_column(column.es_column),
                SQL_TO,
                quote_column("__" + column.es_column),
            ))
        self.namespace.columns.remove(column)

    def _nest_column(self, column):
        destination_table = concat_field(column.es_index, column.es_column)
        existing_table = column.nested_path[0]
        if column.es_column.endswith(SQL_ARRAY_KEY):
            old_column_prefix = join_field(split_field(column.es_column)[:-1])
        else:
            raise Log.error("not expected")

        # FIND THE INNER COLUMNS WE WILL BE MOVING
        moving_columns = []
        for c in self.columns:
            if (
                destination_table != column.es_index
                and startswith_field(c.es_column, old_column_prefix)
                and c.es_column != GUID
            ):
                moving_columns.append(c)

        # TODO: IF THERE ARE CHILD TABLES, WE MUST UPDATE THEIR RELATIONS TOO?

        # LOAD THE COLUMNS
        parent_columns = [name for _, name, _, _, _, _ in self.namespace.container.db.about(existing_table)]
        data = self.namespace.container.db.about(destination_table)
        if not data:
            # DEFINE A NEW TABLE
            command = ConcatSQL(
                SQL_CREATE,
                quote_column(destination_table),
                sql_iso(sql_list([
                    ConcatSQL(quoted_UID, TextSQL("INTEGER")),
                    ConcatSQL(quoted_PARENT, TextSQL("INTEGER")),
                    ConcatSQL(quoted_ORDER, TextSQL("INTEGER")),
                    ConcatSQL(TextSQL("PRIMARY KEY "), sql_iso(quoted_UID)),
                    ConcatSQL(
                        TextSQL(" FOREIGN KEY "),
                        sql_iso(quoted_PARENT),
                        TextSQL(" REFERENCES "),
                        quote_column(existing_table),
                        sql_iso(quoted_UID),
                    ),
                ])),
            )
            with self.namespace.container.db.transaction() as t:
                t.execute(command)
                self.add_table([destination_table, *column.nested_path])

        # TEST IF THERE IS ANY DATA IN THE NEW NESTED ARRAY
        if not moving_columns:
            return

        def new_es_column(c):
            return concat_field(destination_table, relative_field(c.es_column, old_column_prefix))

        def new_nested_path(c):
            return [destination_table, *c.nested_path]

        with self.namespace.container.db.transaction() as t:
            # MAKE NEW COLUMNS
            for c in moving_columns:
                t.execute(ConcatSQL(
                    SQL_ALTER_TABLE,
                    quote_column(destination_table),
                    SQL_ADD_COLUMN,
                    quote_column(new_es_column(c)),
                    quote_column(column.es_type),
                ))

            # FILL THE NESTED TABLE WITH EXISTING DATA
            t.execute(ConcatSQL(
                SQL_INSERT,
                quote_column(destination_table),
                sql_iso(sql_list(
                    [quoted_UID, quoted_PARENT, quoted_ORDER]
                    + [quote_column(new_es_column(c)) for c in moving_columns]
                )),
                SQL_SELECT,
                sql_list([quoted_UID, quoted_UID, SQL_ZERO] + [quote_column(c.es_column) for c in moving_columns]),
                SQL_FROM,
                quote_column(existing_table),
            ))

            # DELETE OLD COLUMNS
            old_columns = [c.es_column for c in moving_columns]
            tmp_table = "tmp_" + existing_table

            t.execute(ConcatSQL(
                SQL_ALTER_TABLE, quote_column(existing_table), SQL_RENAME_TO, quote_column(tmp_table),
            ))
            t.execute(ConcatSQL(
                SQL_CREATE,
                quote_column(existing_table),
                SQL_AS,
                SQL_SELECT,
                sql_list([quote_column(c) for c in parent_columns if c not in old_columns]),
                SQL_FROM,
                quote_column(tmp_table),
            ))
            t.execute(ConcatSQL(TextSQL("DROP TABLE"), quote_column(tmp_table)))

        all_columns = self.namespace.columns
        for c in moving_columns:
            # NOTE: c HAS ALREADY BEEN MOVED TO active_columns
            all_columns.remove(c)
            c.es_column = new_es_column(c)
            c.nested_path = new_nested_path(c)
            all_columns.add(c)

    def add_table(self, nested_path):
        query_paths = self.namespace.columns._snowflakes[self.fact_name]
        if nested_path in query_paths:
            Log.error("table exists")
        query_paths.append(nested_path[0])
        return Table(nested_path, self)

    @property
    def tables(self):
        """
        :return:  LIST OF (nested_path, full_name) PAIRS
        """
        return self.query_paths

    def get_table(self, query_path):
        """
        RETURN TABLE FOR query_path (WITH SOME PATTERN MATCHING)
        """
        abs_path, _ = untype_field(query_path)

        best = first(np for np in self.query_paths if untype_field(np)[0] == abs_path)
        if not best:
            Log.error("Can not find table with path {{path|quote}}", path=query_path)
        nested_path = list(reversed(sorted(p for p in self.query_paths if startswith_field(best, p))))

        return Table(nested_path, self)

    def get_schema(self, nested_path):
        return Schema(nested_path, self)

    @property
    def schema(self):
        return Schema(["."], self)

    @property
    def columns(self):
        return self.namespace.columns.find(self.fact_name)

    @property
    def query_paths(self) -> List[str]:
        return self.namespace.columns.get_query_paths(self.fact_name)

    def values(self, name):
        """
        RETURN THE POSSIBLE COLUMNS THIS name REPRESENTS
        :param name:
        :return:
        """
        return self.namespace.columns.find(name)

    def leaves(self, prefix):
        return set(
            c
            for c in self.namespace.columns.find(self.fact_name)
            for k in [c.name, c.es_column]
            if startswith_field(k, prefix) and k != GUID or k == prefix
            if c.json_type not in [OBJECT, EXISTS]
        )
