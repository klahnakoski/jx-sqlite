# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import AbsOp as _AbsOp, TRUE, IsNumberOp
from jx_sqlite.expressions._utils import SQLang, check
from mo_imports import expect
from mo_json import JX_IS_NULL, JX_NUMBER
from mo_sqlite import SQL_NULL, sql_call

SqlScript = expect("SqlScript")


class AbsOp(_AbsOp):
    @check
    def to_sql(self, schema):
        expr = IsNumberOp(self.term).partial_eval(SQLang).to_sql(schema)
        if not expr:
            return SqlScript(
                expr=SQL_NULL, jx_type=JX_IS_NULL, frum=self, miss=TRUE, schema=schema,
            )

        return SqlScript(
            expr=sql_call("ABS", expr), jx_type=JX_NUMBER, frum=self, schema=schema,
        )
