# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from jx_base.expressions.to_integer_op import ToIntegerOp

from jx_base.expressions.expression import Expression
from jx_base.expressions.false_op import FALSE
from jx_base.expressions.literal import ZERO
from jx_base.expressions.max_op import MaxOp
from jx_base.expressions.to_text_op import ToTextOp
from jx_base.language import is_op
from mo_json import JX_INTEGER


class StrictIndexOfOp(Expression):
    """
    PLACEHOLDER FOR STRICT value.indexOf(find, start) (CAN NOT DEAL WITH NULLS)
    RETURN -1 IF NOT FOUND
    """

    _jx_type = JX_INTEGER

    def __init__(self, value, find, start):
        Expression.__init__(self, value, find, start)
        self.value = value
        self.find = find
        self.start = start

    def __call__(self, row, rownum=None, rows=None):
        value = self.value(row)
        find = self.find(row)
        start = self.start(row)
        return value.find(find, start)

    def __data__(self):
        return {"strict.indexOf": [self.value.__data__(), self.find.__data__(), self.start.__data__()]}

    def vars(self):
        return self.value.vars() | self.find.vars() | self.start.vars()

    def missing(self, lang):
        return FALSE

    def invert(self, lang):
        return FALSE

    def partial_eval(self, lang):
        start = ToIntegerOp(MaxOp(ZERO, self.start)).partial_eval(lang)
        return self.lang.StrictIndexOfOp(
            ToTextOp(self.value).partial_eval(lang), ToTextOp(self.find).partial_eval(lang), start,
        )

    def __eq__(self, other):
        if not is_op(other, StrictIndexOfOp):
            return False
        return self.value == self.value and self.find == other.find and self.start == other.start
