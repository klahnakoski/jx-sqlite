# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from jx_base.expressions._utils import jx_expression
from jx_base.expressions.add_op import AddOp
from jx_base.expressions.case_op import CaseOp
from jx_base.expressions.expression import Expression
from jx_base.expressions.find_op import FindOp
from jx_base.expressions.is_number_op import IsNumberOp
from jx_base.expressions.length_op import LengthOp
from jx_base.expressions.literal import Literal, ZERO, is_literal
from jx_base.expressions.max_op import MaxOp
from jx_base.expressions.min_op import MinOp
from jx_base.expressions.null_op import NULL
from jx_base.expressions.strict_substring_op import StrictSubstringOp
from jx_base.expressions.variable import Variable, is_variable
from jx_base.expressions.when_op import WhenOp
from mo_dots import is_data, is_sequence, to_data, coalesce
from mo_json.types import JX_TEXT
from mo_logs import Log


class BetweenOp(Expression):
    _jx_type = JX_TEXT

    def __init__(self, value, prefix, suffix, start=NULL):
        Expression.__init__(self, value, prefix, suffix, start)
        self.value = value
        self.prefix = coalesce(prefix, NULL)
        self.suffix = coalesce(suffix, NULL)
        self.start = coalesce(start, NULL)
        if is_literal(self.prefix) and is_literal(self.suffix):
            pass
        else:
            Log.error("Expecting literal prefix and suffix only")

    @classmethod
    def define(cls, expr):
        expr = to_data(expr)
        term = expr.between
        if is_sequence(term):
            return BetweenOp(
                value=jx_expression(term[0]),
                prefix=jx_expression(term[1]),
                suffix=jx_expression(term[2]),
                start=jx_expression(expr.start),
            )
        elif is_data(term):
            var, vals = term.items()[0]
            if is_sequence(vals) and len(vals) == 2:
                return BetweenOp(
                    value=Variable(var),
                    prefix=Literal(vals[0]),
                    suffix=Literal(vals[1]),
                    start=jx_expression(expr.start),
                )
            else:
                Log.error("`between` parameters are expected to be in {var: [prefix, suffix]} form")
        else:
            Log.error("`between` parameters are expected to be in {var: [prefix, suffix]} form")

    def vars(self):
        return self.value.vars() | self.prefix.vars() | self.suffix.vars() | self.start.vars()

    def map(self, map_):
        return BetweenOp(
            self.value.map(map_), self.prefix.map(map_), self.suffix.map(map_), start=self.start.map(map_),
        )

    def __data__(self):
        if is_variable(self.value) and is_literal(self.prefix) and is_literal(self.suffix):
            output = to_data({"between": {self.value.var: [self.prefix.value, self.suffix.value]}})
        else:
            output = to_data({"between": [self.value.__data__(), self.prefix.__data__(), self.suffix.__data__()]})
        if self.start is not NULL:
            output.start = self.start.__data__()
        return output

    def partial_eval(self, lang):
        value = self.value.partial_eval(lang)

        start_index = CaseOp(
            WhenOp(self.prefix.missing(lang), then=ZERO),
            WhenOp(IsNumberOp(self.prefix), then=MaxOp(ZERO, self.prefix)),
            FindOp(value, self.prefix, self.start),
        ).partial_eval(lang)

        len_prefix = CaseOp(
            WhenOp(self.prefix.missing(lang), then=ZERO),
            WhenOp(IsNumberOp(self.prefix), then=ZERO),
            LengthOp(self.prefix),
        ).partial_eval(lang)

        end_index = CaseOp(
            WhenOp(start_index.missing(lang), then=NULL),
            WhenOp(self.suffix.missing(lang), then=LengthOp(value)),
            WhenOp(IsNumberOp(self.suffix), then=MinOp(self.suffix, LengthOp(value))),
            FindOp(value, self.suffix, AddOp(start_index, len_prefix)),
        ).partial_eval(lang)

        start_index = AddOp(start_index, len_prefix).partial_eval(lang)
        substring = StrictSubstringOp(value, start_index, end_index).partial_eval(lang)

        between = WhenOp(end_index.missing(lang), **{"else": substring}).partial_eval(lang)

        return between
