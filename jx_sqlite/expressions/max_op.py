# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import MaxOp as _MaxOp, SqlScript
from mo_sqlite import check
from mo_sqlite.expressions.sql_script import SqlScript
from mo_json import JX_NUMBER
from mo_sqlite import sql_call


class MaxOp(_MaxOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        expr = sql_call("MAX", self.frum.to_sql(schema))
        return SqlScript(jx_type=JX_NUMBER, expr=expr, frum=self, schema=schema)
