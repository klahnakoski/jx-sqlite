# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from jx_base.expressions.expression import Expression
from jx_base.expressions.literal import Literal
from jx_base.expressions.literal import is_literal
from jx_base.expressions.variable import is_variable
from mo_logs import Log


class RowsOp(Expression):
    has_simple_form = True

    def __init__(self, *term):
        Expression.__init__(self, *term)
        self.var, self.offset = term
        if is_variable(self.var):
            if is_variable(self.var) and not any(
                self.var.var.startswith(p) for p in ["row.", "rows.", "rownum"]
            ):  # VARIABLES ARE INTERPRETED LITERALLY
                self.var = Literal(self.var.var)
            else:
                Log.error("can not handle")
        else:
            Log.error("can not handle")

    def __call__(self, row, rownum, rows):
        v = self.var(row, rownum, rows)
        o = self.offset(row, rownum, rows)
        return rows[rownum+o][v]

    def __data__(self):
        if is_literal(self.var) and is_literal(self.offset):
            return {"rows": {self.var.json, self.offset.value}}
        else:
            return {"rows": [self.var.__data__(), self.offset.__data__()]}

    def vars(self):
        return self.var.vars() | self.offset.vars() | {"rows", "rownum"}

    def map(self, map_):
        return RowsOp(self.var.map(map_), self.offset.map(map_))
