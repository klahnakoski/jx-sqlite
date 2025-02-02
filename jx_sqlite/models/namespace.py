# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#


from copy import copy

import jx_base
from jx_base import Facts
from jx_sqlite.meta_columns import ColumnList
from jx_sqlite.models.schema import Schema
from jx_sqlite.models.snowflake import Snowflake
from mo_imports import export


class Namespace(jx_base.Namespace):
    """
    MANAGE SQLITE DATABASE
    """

    def __init__(self, container):
        self.container = container
        self.columns = ColumnList(container.db)

    def __copy__(self):
        output = object.__new__(Namespace)
        output.db = None
        output.columns = copy(self.columns)
        return output

    def rename_tables(self, name_map):
        output = object.__new__(Namespace)
        output.db = None
        output.columns = self.columns.rename_tables(name_map)
        return output

    def find_snowflake(self, fact_name):
        """
        RETURN ALL PATHS IF EXISTS
        """
        return self.columns._snowflakes.get(fact_name)

    def get_facts(self, fact_name):
        snowflake = Snowflake(fact_name, self)
        return Facts(self, snowflake)

    def get_schema(self, fact_name):
        # TODO: HOW TO REDUCE RELATIONS TO JUST THIS TREE? (AVOID CYCLES)
        return Schema([fact_name], Snowflake(fact_name, self))

    def get_snowflake(self, fact_name):
        return Snowflake(fact_name, self)

    def get_relations(self):
        return self.columns.relations[:]

    def get_columns(self, table_name):
        return self.columns.find_columns(table_name)

    def get_tables(self):
        return list(sorted(self.columns.data.keys()))

    def add_column_to_schema(self, column):
        self.columns.add(column)


export("jx_sqlite.models.container", Namespace)
