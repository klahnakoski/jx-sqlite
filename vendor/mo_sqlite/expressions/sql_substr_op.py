# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import NULL, SqlSubstrOp as _SqlSubstrOp, is_literal
from mo_sqlite.expressions._utils import SQL
from mo_sql import SQL_OP, SQL_CP
from mo_sqlite.expressions._utils import SQLang


class SqlSubstrOp(_SqlSubstrOp, SQL):
    def __iter__(self):
        yield from SQL("SUBSTR")
        yield from SQL_OP
        yield from self.value
        yield from SQL(",")
        yield from self.start
        if self.length is not NULL:
            yield from SQL(",")
            yield from self.length
        yield from SQL_CP

    def partial_eval(self, lang):
        value = self.value.partial_eval(SQLang)
        start = self.start.partial_eval(SQLang)
        length = self.length.partial_eval(SQLang)
        if is_literal(start) and start.value == 1:
            if length is NULL:
                return value
        return SqlSubstrOp(value, start, length)
