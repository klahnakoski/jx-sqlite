# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
import jx_base
from jx_sqlite.models.facts import Facts
from jx_sqlite.models.schema import Schema
from jx_sqlite.models.snowflake import Snowflake
from mo_imports import export
from mo_logs import Log


class Table(jx_base.Table):
    def __init__(self, nested_path, container):
        if not isinstance(nested_path, list):
            Log.error("Expecting list of paths")
        self.nested_path = nested_path
        self.container = container

    @property
    def name(self):
        """
        :return: THE TABLE NAME RELATIVE TO THE FACT TABLE
        """
        return self.schema.nested_path[0]

    def query(self, query):
        return Facts(self.nested_path[0], self.container).query(query)

    def map(self, mapping):
        return self

    @property
    def schema(self):
        return Schema(self.nested_path, Snowflake(self.nested_path[-1], self.container.namespace))


export("jx_sqlite.models.snowflake", Table)
export("jx_sqlite.models.container", Table)
