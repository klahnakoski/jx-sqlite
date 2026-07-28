# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import (
    ModOp as _ModOp,
    MissingOp,
    OrOp,
    ToNumberOp,
)
from mo_sqlite import SQLang
from jx_sqlite.expressions._utils import check
from jx_sqlite.expressions._utils import SqlScript
from mo_json import JX_NUMBER
from mo_sql import SQL
from mo_sqlite import sql_iso, ConcatSQL


class ModOp(_ModOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        lhs = ToNumberOp(self.lhs).partial_eval(SQLang).to_sql(schema)
        rhs = ToNumberOp(self.rhs).partial_eval(SQLang).to_sql(schema)

        # SQLite's % follows the DIVIDEND sign (-7 % 3 = -1); jx mod follows the DIVISOR sign
        # (-7 mod 3 = 2, like Python).  ((x % y) + y) % y normalizes the sign.
        remainder = ConcatSQL(sql_iso(lhs), SQL(" % "), sql_iso(rhs))
        shifted = ConcatSQL(sql_iso(remainder), SQL(" + "), sql_iso(rhs))
        expr = ConcatSQL(sql_iso(shifted), SQL(" % "), sql_iso(rhs))

        return SqlScript(
            jx_type=JX_NUMBER,
            expr=expr,
            frum=self,
            miss=OrOp(MissingOp(self.lhs), MissingOp(self.rhs)),
            schema=schema,
        )
