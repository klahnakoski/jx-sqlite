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
from mo_logs import Log
from mo_math import is_integer


class OffsetOp(Expression):
    """
    OFFSET INDEX INTO A TUPLE
    """

    def __init__(self, *var):
        Expression.__init__(self, None)
        if not is_integer(var):
            Log.error("Expecting an integer")
        self.var = var

    def __call__(self, row, rownum=None, rows=None):
        try:
            return row[self.var]
        except Exception:
            return None

    def __data__(self):
        return {"offset": self.var}

    def vars(self):
        return {}

    def __hash__(self):
        return self.var.__hash__()

    def __eq__(self, other):
        return self.var == other

    def __str__(self):
        return str(self.var)
