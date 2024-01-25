# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import (
    FindOp as _FindOp,
    BasicEqOp,
    ZERO, BasicBooleanOp,
)
from jx_base.expressions._utils import simplified
from jx_sqlite.expressions._utils import SQLang, check, with_var
from jx_sqlite.expressions.and_op import AndOp
from jx_sqlite.expressions.not_left_op import NotLeftOp
from jx_sqlite.expressions.or_op import OrOp
from jx_sqlite.expressions.sql_instr_op import SqlInstrOp
from jx_sqlite.expressions.sql_script import SqlScript
from mo_sql import SQL_NULL
from mo_sqlite import (
    SQL_CASE,
    SQL_ELSE,
    SQL_END,
    SQL_THEN,
    SQL_WHEN,
    SQL_ZERO,
    ConcatSQL,
    SQL_ONE,
    SQL_PLUS,
    SQL_SUB,
)
from mo_sqlite import sql_call, quote_column
from mo_json import JX_INTEGER


class FindOp(_FindOp):
    @simplified
    def partial_eval(self, lang):
        return FindOp(
            self.value.partial_eval(SQLang),
            self.find.partial_eval(SQLang),
            self.start.partial_eval(SQLang)
        )

    @check
    def to_sql(self, schema):
        value = self.value.partial_eval(SQLang).to_sql(schema)
        find = self.find.partial_eval(SQLang).to_sql(schema)
        start = self.start.partial_eval(SQLang).to_sql(schema)

        if start.sql != SQL_ZERO.sql:
            value = NotLeftOp(self.value, self.start).to_sql(schema)

        index = sql_call("INSTR", value, find)
        i = quote_column("i")
        sql = with_var(
            i,
            index,
            ConcatSQL(
                SQL_CASE,
                SQL_WHEN,
                i,
                SQL_THEN,
                i,
                SQL_SUB,
                SQL_ONE,
                SQL_PLUS,
                start,
                SQL_ELSE,
                SQL_NULL,
                SQL_END,
            ),
        )
        return SqlScript(jx_type=JX_INTEGER, expr=sql, frum=self, schema=schema)

    def missing(self, lang):
        not_found = BasicEqOp(
            SqlInstrOp(NotLeftOp(self.value, self.start), self.find),
            ZERO,
        )

        output = OrOp(self.value.missing(lang), self.find.missing(lang), not_found).partial_eval(self.lang)
        return output
