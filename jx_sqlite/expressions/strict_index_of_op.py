# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import StrictIndexOfOp as _StrictIndexOfOp, FALSE, SqlScript
from jx_sqlite.expressions._utils import check
from jx_sqlite.expressions._utils import SqlScript
from jx_sqlite.expressions.literal import Literal
from mo_json.types import JX_NUMBER, JX_INTEGER
from mo_sql import (
    SQL_CASE,
    SQL_ELSE,
    SQL_END,
    SQL_THEN,
    SQL_WHEN,
    SQL_ONE,
    ConcatSQL,
    SQL_NEG,
    SQL_PLUS,
    SQL_NEG_ONE,
    SQL_ZERO,
)
from mo_sqlite import sql_call


class StrictIndexOfOp(_StrictIndexOfOp):
    data_type = JX_NUMBER

    @check
    def to_sql(self, schema) -> SqlScript:
        value = self.value.to_sql(schema)
        find = self.find.to_sql(schema)
        start = self.start

        if isinstance(start, Literal) and start.value == 0:
            expr = ConcatSQL(sql_call("INSTR", value, find), SQL_NEG, SQL_ONE)

            return SqlScript(expr=expr, miss=FALSE, frum=self)
        else:
            start_index = start.to_sql(schema)
            found = sql_call(
                "INSTR",
                sql_call(
                    "SUBSTR",
                    value,
                    ConcatSQL(start_index, SQL_PLUS, SQL_ONE)
                ),
                find
            )
            return SqlScript(
                JX_INTEGER,
                ConcatSQL(
                    SQL_CASE,
                    SQL_WHEN,
                    found,
                    SQL_THEN,
                    found,
                    SQL_PLUS,
                    start_index,
                    SQL_NEG_ONE,
                    SQL_ELSE,
                    SQL_NEG_ONE,
                    SQL_END,
                ),
                self,
                FALSE,
                schema,
            )
