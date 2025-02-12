# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import CountOp as _CountOp, SqlScript
from jx_sqlite.expressions._utils import check
from mo_sqlite.expressions.sql_script import SqlScript
from mo_json import JX_INTEGER
from mo_sqlite import sql_call


class CountOp(_CountOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        return SqlScript(
            jx_type=JX_INTEGER, expr=sql_call("MAX", self.frum.to_sql(schema).expr), frum=self, schema=schema,
        )
