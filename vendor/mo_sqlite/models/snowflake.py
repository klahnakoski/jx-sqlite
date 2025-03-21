# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#


import jx_base
from mo_dots import concat_field, startswith_field, relative_field
from mo_imports import export
from mo_json import ARRAY, OBJECT, EXISTS, INTEGER
from mo_logs import Except
from mo_sql.utils import SQL_ARRAY_KEY, untype_field
from mo_sqlite.models.schema import Schema
from mo_sqlite.utils import *
from mo_sqlite.utils import GUID
from mo_times import Date

Table = expect("Table")


class Snowflake(jx_base.Snowflake):
    """
    MANAGE SINGLE HIERARCHY IN SQLITE DATABASE
    """

    def __init__(self, fact_name, namespace):
        if not namespace.find_snowflake(fact_name):
            Log.error("{name} does not exist", name=fact_name)
        self.fact_name = fact_name  # THE CENTRAL FACT TABLE
        self.namespace = namespace

    @property
    def query_paths(self):
        return self.namespace.columns.get_query_paths(self.fact_name)

    @property
    def tables(self):
        Log.error("use Snowflake.query_paths")

    def __copy__(self):
        Log.error("con not copy")

    @property
    def container(self):
        return self.namespace.container

    def get_relations(self):
        """
        RETURN ALL RELATIONS WITHIN THIS SNOWFLAKE
        """
        return self.namespace.get_relations()

    def rename_tables(self, name_map):
        """
        :param name_map: IN {new_name: old_name} FORM
        :return: NEW Snowflake WITH RENAMED TABLES
        """
        return Snowflake(
            name_map.get(self.fact_name, self.fact_name),
            self.namespace.rename_tables(name_map)
        )

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
                        "Did not add column {column}", column=column.es_column, cause=e,
                    )
            else:
                Log.error("Did not add column {column}", column=column.es_column, cause=e)

    def _drop_column(self, column):
        # DROP COLUMN BY RENAMING IT, WITH __ PREFIX TO HIDE IT
        cname = column.name
        if column.json_type == ARRAY:
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
        new_nest = column.es_column
        existing_table = column.nested_path[0]
        destination_table = concat_field(column.es_index, new_nest)
        if new_nest.endswith(SQL_ARRAY_KEY):
            old_column_prefix, _ = untype_field(new_nest)
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
            now = Date.now()
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
            dest_nested_path = [destination_table, *column.nested_path]
            with self.namespace.container.db.transaction() as t:
                t.execute(command)
                self.add_table(dest_nested_path)

            self.namespace.columns.add(jx_base.Column(
                name=UID,
                es_column=UID,
                es_index=destination_table,
                es_type="INTEGER",
                json_type=INTEGER,
                nested_path=dest_nested_path,
                last_updated=now,
                multi=0,
            ))
            self.namespace.columns.add(jx_base.Column(
                name=PARENT,
                es_column=PARENT,
                es_index=destination_table,
                es_type="INTEGER",
                json_type=INTEGER,
                nested_path=[destination_table, *column.nested_path],
                last_updated=now,
                multi=0,
            ))
            self.namespace.columns.add(jx_base.Column(
                name=ORDER,
                es_column=ORDER,
                es_index=destination_table,
                es_type="INTEGER",
                json_type=INTEGER,
                nested_path=[destination_table, *column.nested_path],
                last_updated=now,
                multi=0,
            ))
            self.namespace.columns.relations.extend(self.namespace.container.db.get_relations(destination_table))
            self.namespace.columns.primary_keys[destination_table] = (UID,)

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
        query_paths = self.namespace.find_snowflake(self.fact_name)
        if nested_path in query_paths:
            Log.error("table exists")
        query_paths.append(nested_path[0])
        return Table(nested_path, self)

    @property
    def tables(self):
        """
        :return:  LIST OF (nested_path, full_name) PAIRS
        """
        return [(path, concat_field(self.fact_name, path)) for path in self.query_paths]

    def get_table(self, nested_path):
        """
        RETURN TABLE FOR ABSOLUTE nested_path (WITH SOME PATTERN MATCHING)
        """
        abs_path, _ = untype_field(nested_path[0])

        best = first(p for p in self.query_paths if untype_field(p[0])[0] == abs_path)
        if not best:
            matching_table = first(t for t in self.namespace.get_tables() if untype_field(t)[0] == abs_path)
            if matching_table:
                # EXPAND THIS SNOWFLAKE TO INCLUDE THE REQUESTED PATH
                best = [matching_table]
                self.query_paths.insert(0, matching_table)
            else:
                Log.error("Can not find table with name {{table|quote}}", table=best)

        return Table(best, self)

    def get_schema(self, nested_path):
        if nested_path not in self.query_paths:
            for i, q in enumerate(self.query_paths):
                if startswith_field(nested_path[0], q[0]):
                    self.query_paths = self.query_paths[:i] + [nested_path] + self.query_paths[i:]
                    break
        return Schema(nested_path, self)

    @property
    def schema(self):
        """
        RETURN THE FACT TABLE SCHEMA
        """
        return Schema([self.fact_name], self)

    @property
    def columns(self):
        return self.namespace.columns.find(self.fact_name)

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


export("mo_sqlite.models.container", Snowflake)
