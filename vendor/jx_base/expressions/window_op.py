# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from jx_base.expressions.expression import jx_expression, Expression


class WindowOp(Expression):
    def __init__(self, frum, query):
        Expression.__init__(self, frum, query)
        self.frum = frum
        self.query = query

    @classmethod
    def define(cls, expr):
        raw_frum, *raw_windows = expr["window"]
        frum = jx_expression(raw_frum, cls.lang)
        for raw_window in raw_windows:
            name, value = raw_window['name'], raw_window['value']
            query = {k: v for k, v in raw_window.items() if k not in ['name', 'value']}
            query['select'] = {'name': name, 'value': value}
            frum = WindowOp(frum, jx_expression({"from": frum, **query}))
        return frum
