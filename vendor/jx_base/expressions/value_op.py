# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from jx_base.expressions.expression import Expression, _jx_expression
from jx_base.language import is_op
from jx_base.models.container import Container
from mo_dots import to_data


class ValueOp(Expression):
    has_simple_form = True

    def __init__(self, value):
        """
        IDENTITY FUNCTION
        """
        Expression.__init__(self, value)
        self.value = value
        self._jx_type = value.jx_type

    @classmethod
    def define(cls, expr):
        return _jx_expression(to_data(expr["value"]), cls.lang)

    def apply(self, container: Container):
        return container.query(self.value)

    def __data__(self):
        return {"value": self.value.__data__()}

    def vars(self):
        return self.value.vars()

    def map(self, map):
        return ValueOp(self.value.map(map))

    def missing(self, lang):
        return self.value.missing()

    def exists(self):
        return self.value.exists()

    def invert(self, lang):
        return self.value.invert()

    def partial_eval(self, lang):
        return self.value.partial_eval(lang)

    @property
    def jx_type(self):
        return self._jx_type

    def __eq__(self, other):
        if is_op(other, ValueOp):
            return self.value == other.value
        return self.value == other
