# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from mo_dots import startswith_field, last

from jx_base.utils import GUID
from mo_imports import export

from jx_base import enlist
from jx_base.models.facts import Facts as _Facts
from mo_json import to_jx_type
from mo_sqlite.expressions import SqlSelectOp, SqlAliasOp, SqlVariable
from mo_sqlite.sql_script import SqlStep, SqlTree


class Facts(_Facts):
    @property
    def nested_path(self):
        return self.container.get_table(self.name).nested_path

    def add(self, documents):
        self.insert(enlist(documents))

    def partial_eval(self, lang):
        sql_steps = {}

        for step in self.snowflake.query_paths:
            columns = [
                SqlVariable(c.es_column, jx_type=to_jx_type(c.json_type))
                for c in self.snowflake.columns
                if c.es_index == step
            ]
            aliases = [
                SqlAliasOp(SqlVariable(c.es_column, jx_type=to_jx_type(c.json_type)), c.name)
                for c in self.snowflake.columns
                if c.es_index == step
            ]
            parent = last(v for k, v in sql_steps.items() if startswith_field(k, step))
            sql_steps[step] = SqlStep(
                parent, SqlSelectOp(SqlVariable(step), *columns), aliases, uids=(SqlVariable(GUID),), order=(),
            )
        return SqlTree(list(sql_steps.values()))


export("mo_sqlite.models.container", Facts)
