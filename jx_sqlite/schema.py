# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
from typing import Set, Tuple

from jx_base import Column, enlist

from jx_base.queries import get_property_name
from jx_sqlite.utils import GUID, untyped_column, untype_field
from mo_dots import concat_field, relative_field, set_default, startswith_field, split_field, tail_field, listwrap
from mo_future import first
from mo_json import EXISTS, OBJECT, STRUCT
from mo_logs import logger


class Schema(object):
    """
    A Schema MAPS ALL COLUMNS IN SNOWFLAKE FROM THE PERSPECTIVE OF A SINGLE TABLE (a nested_path)
    """

    def __init__(self, nested_path, snowflake):
        if not isinstance(nested_path, (list, tuple)):
            logger.error("expecting list")
        if nested_path[-1] == ".":
            logger.error(
                "Expecting absolute nested path so we can track the tables, and deal with"
                " abiguity in the event the names are not typed"
            )
        self.path = nested_path[0]
        self.nested_path = nested_path
        self.snowflake = snowflake

    def __getitem__(self, item):
        output = self.snowflake.namespace.columns.find(self.path, item)
        return output

    def get_table(self, relative_path):
        abs_path = concat_field(self.nested_path[0], relative_path)
        return self.snowflake.get_table(abs_path)

    def get_column_name(self, column):
        """
        RETURN THE COLUMN NAME, FROM THE PERSPECTIVE OF THIS SCHEMA
        :param column:
        :return: NAME OF column
        """
        relative_name = relative_field(column.name, self.nested_path[0])
        return get_property_name(relative_name)

    @property
    def namespace(self):
        return self.snowflake.namespace

    @property
    def container(self):
        return self.snowflake.container

    def keys(self):
        """
        :return: ALL DYNAMIC TYPED COLUMN NAMES
        """
        return set(c.name for c in self.columns)

    @property
    def columns(self):
        return self.snowflake.namespace.columns.find(self.snowflake.fact_name)

    def column(self, prefix):
        full_name = untyped_column(concat_field(self.nested_path, prefix))
        return set(
            c
            for c in self.snowflake.namespace.columns.find(self.snowflake.fact_name)
            for k, t in [untyped_column(c.name)]
            if k == full_name and k != GUID
            if c.json_type not in [OBJECT, EXISTS]
        )

    def leaves(self, prefix) -> Set[Tuple[str, Column]]:
        """
        :param prefix:
        :return: set of (relative_name, column) pairs
        """
        if prefix == GUID and len(self.nested_path) == 1:
            return {(".", first(c for c in self.columns if c.name == GUID))}

        candidates = [
            c
            for c in self.columns
            if c.json_type not in [OBJECT, EXISTS] and c.name != GUID
        ]

        search_path = [
            *self.nested_path,
            *(p for p in self.snowflake.query_paths if p.startswith(self.nested_path[0] + "."))
        ]

        for np in search_path:
            rel_path, _ = untype_field(relative_field(np, self.snowflake.fact_name))
            if startswith_field(prefix, rel_path):
                prefix = relative_field(prefix, rel_path)

            full_name = concat_field(np, prefix)
            output = set(
                pair
                for c in candidates
                if startswith_field(c.nested_path[0], np)
                for pair in [first(
                    (untype_field(relative_field(k, full_name))[0], c)
                    for k in [
                        concat_field(c.nested_path[0], relative_field(c.name, rel_path)),
                        concat_field(c.es_index, c.es_column),
                        # concat_field(c.nested_path[0], c.name),  # if the column name includes nested path
                    ]
                    if startswith_field(k, full_name)
                )]
                if pair is not None
            )
            if output:
                return output
        return set()

    def map_to_sql(self, var=""):
        """
        RETURN A MAP FROM THE RELATIVE AND ABSOLUTE NAME SPACE TO COLUMNS
        """
        origin = self.nested_path[0]
        if startswith_field(var, origin) and origin != var:
            var = relative_field(var, origin)
        fact_dict = {}
        origin_dict = {}
        for k, cs in self.namespace.items():
            for c in cs:
                if c.json_type in STRUCT:
                    continue

                if startswith_field(get_property_name(k), var):
                    origin_dict.setdefault(c.names[origin], []).append(c)

                    if origin != c.nested_path[0]:
                        fact_dict.setdefault(c.name, []).append(c)
                elif origin == var:
                    origin_dict.setdefault(concat_field(var, c.names[origin]), []).append(c)

                    if origin != c.nested_path[0]:
                        fact_dict.setdefault(concat_field(var, c.name), []).append(c)

        return set_default(origin_dict, fact_dict)
