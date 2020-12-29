# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

from jx_base.expressions._utils import simplified
from jx_base.expressions.expression import jx_expression, Expression, _jx_expression
from jx_base.expressions.null_op import NULL
from jx_base.language import is_op
from jx_base.utils import is_variable_name
from mo_dots import to_data, listwrap
from mo_future import is_text
from mo_logs import Log
from mo_math import UNION


class SelectOp(Expression):
    has_simple_form = True

    def __init__(self, terms):
        """
        :param terms: list OF {"name":name, "value":value} DESCRIPTORS
        """
        if not isinstance(terms, list) or not all(
            isinstance(term, dict) for term in terms
        ):
            Log.error("expecting list of dicts")
        Expression.__init__(self, None)
        self.terms = terms

    @classmethod
    def define(cls, expr):
        term = listwrap(to_data(expr).select)
        terms = []
        for t in term:
            if is_text(t):
                if not is_variable_name(t):
                    Log.error(
                        "expecting {{value}} a simple dot-delimited path name", value=t
                    )
                terms.append({"name": t, "value": _jx_expression(t, cls.lang)})
            elif t.name == None:
                if t.value == None:
                    Log.error(
                        "expecting select parameters to have name and value properties"
                    )
                elif is_text(t.value):
                    if not is_variable_name(t):
                        Log.error(
                            "expecting {{value}} a simple dot-delimited path name",
                            value=t.value,
                        )
                    else:
                        terms.append({
                            "name": t.value,
                            "value": _jx_expression(t.value, cls.lang),
                        })
                else:
                    Log.error("expecting a name property")
            else:
                terms.append({"name": t.name, "value": jx_expression(t.value)})
        return SelectOp(terms)

    @simplified
    def partial_eval(self, lang):
        new_terms = []
        diff = False
        for name, expr in self:
            new_expr = expr.partial_eval(lang)
            if new_expr is expr:
                new_terms.append({"name": name, "value": expr})
                continue
            diff = True

            if expr is NULL:
                continue
            elif is_op(expr, SelectOp):
                for t_name, t_value in expr.terms:
                    new_terms.append({
                        "name": concat_field(name, t_name),
                        "value": t_value,
                    })
            else:
                new_terms.append({"name": name, "value": new_expr})
                diff = True
        if diff:
            return SelectOp(new_terms)
        else:
            return self

    def __iter__(self):
        """
        :return:  return iterator of (name, value) tuples
        """
        for term in self.terms:
            yield term["name"], term["value"]

    def __data__(self):
        return {"select": [
            {"name": name, "value": value.__data__()} for name, value in self
        ]}

    def vars(self):
        return UNION(value for _, value in self)

    def map(self, map_):
        return SelectOp([
            {"name": name, "value": value.map(map_)} for name, value in self
        ])
