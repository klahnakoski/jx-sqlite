# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import (SqlScript,
    NotRightOp as NotRightOp_,
    LengthOp,
    MaxOp,
    SubOp,
    ZERO,
)
from mo_sqlite import check
from jx_sqlite.expressions._utils import OrOp, SQLang
from mo_sqlite.expressions.sql_script import SqlScript
from mo_json import JX_TEXT
from mo_sqlite import SQL_ONE
from mo_sqlite import sql_call


class NotRightOp(NotRightOp_):
    @check
    def to_sql(self, schema) -> SqlScript:
        v = self.value.to_sql(schema)
        if self.length == ZERO:
            return v

        r = self.length.to_sql(schema)
        end = MaxOp(ZERO, SubOp(LengthOp(self.value), MaxOp(ZERO, self.length))).partial_eval(SQLang).to_sql(schema)
        sql = sql_call("SUBSTR", v.expr, SQL_ONE, end)
        return SqlScript(jx_type=JX_TEXT, expr=sql, frum=self, miss=OrOp(r.miss, v.miss), schema=schema,)
