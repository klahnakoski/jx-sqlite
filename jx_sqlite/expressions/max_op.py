# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import MaxOp as _MaxOp, MissingOp
from jx_sqlite.expressions._utils import SQLang, check
from jx_sqlite.expressions.sql_script import SqlScript
from mo_sqlite.sqlite import sql_call
from mo_json import JX_NUMBER


class MaxOp(_MaxOp):
    @check
    def to_sql(self, schema):
        expr = sql_call("MAX", self.frum.to_sql(schema))
        return SqlScript(
            jx_type=JX_NUMBER, expr=expr, frum=self, schema=schema
        )
