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

from jx_base.expressions import ToNumberOp as NumberOp_, Variable
from jx_base.language import is_op
from jx_sqlite.expressions._utils import SQLang, check
from mo_imports import export
from mo_json import T_NUMBER, Log


class ToNumberOp(NumberOp_):
    @check
    def to_sql(self, schema):
        value = self.term.partial_eval(SQLang).to_sql(schema)

        if is_op(value.frum, Variable) and value.frum.type == T_NUMBER:
            return value
        else:
            Log.error("not supported")
            #  acc.append("CAST(" + v + " as FLOAT)")


export("jx_sqlite.expressions._utils", ToNumberOp)
