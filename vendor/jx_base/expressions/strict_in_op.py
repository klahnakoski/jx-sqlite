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
from jx_base.expressions.false_op import FALSE
from jx_base.expressions.literal import Literal, is_literal
from jx_base.language import is_op
from mo_dots import is_many
from mo_imports import expect
from mo_json.types import JX_BOOLEAN
from mo_logs import Log

EqOp, MissingOp, NestedOp, NotOp, NULL, Variable, is_variable = expect(
    "EqOp", "MissingOp", "NestedOp", "NotOp", "NULL", "Variable", "is_variable"
)


class StrictInOp(Expression):
    has_simple_form = True
    _jx_type = JX_BOOLEAN

    def __new__(cls, value, superset):
        if is_variable(value) and is_op(superset, Literal) and not is_many(superset.value):
            return EqOp(value, Literal(superset.value))
        return object.__new__(cls)

    def __init__(self, value, superset):
        Expression.__init__(self, value, superset)
        self.value = value
        self.superset = superset
        if self.value is None:
            Log.error("Should not happpen")

    def __data__(self):
        if is_variable(self.value) and is_literal(self.superset):
            return {"strict.in": {self.value.var: self.superset.value}}
        else:
            return {"strict.in": [self.value.__data__(), self.superset.__data__()]}

    def __eq__(self, other):
        if is_op(other, StrictInOp):
            return self.value == other.value and self.superset == other.superset
        return False

    def vars(self):
        return self.value.vars()

    def map(self, map_):
        return StrictInOp(self.value.map(map_), self.superset.map(map_))

    def partial_eval(self, lang):
        value = self.value.partial_eval(lang)
        superset = self.superset.partial_eval(lang)
        if superset is NULL:
            return FALSE
        elif value is NULL:
            return NULL
        elif is_literal(value) and is_literal(superset):
            return Literal(value() in superset())
        elif is_op(value, NestedOp):
            return (
                NestedOp(value.nested_path, None, lang.AndOp(StrictInOp(value.select, superset), value.where),)
                .exists()
                .partial_eval(lang)
            )
        else:
            return lang.StrictInOp(value, superset)

    def __call__(self, row, rownum=None, rows=None):
        value = self.value(row)
        superset = self.superset(row)
        if value == None:
            return None
        if superset == None:
            return False
        return value in superset

    def missing(self, lang):
        return FALSE

    def invert(self, lang):
        this = self.partial_eval(lang)
        if is_op(this, StrictInOp):
            inv = NotOp(this)
            inv.simplified = True
            return inv
        else:
            return this.invert(lang)

    def __rcontains__(self, superset):
        if (
            is_variable(self.value)
            and is_op(superset, MissingOp)
            and is_variable(superset.value)
            and superset.value.var == self.value.var
        ):
            return True
        return self.__eq__(superset)
