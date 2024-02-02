# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import NeOp as _NeOp
from jx_sqlite.expressions._utils import check, SQLang
from jx_sqlite.expressions.eq_op import EqOp
from jx_sqlite.expressions.not_op import NotOp


class NeOp(_NeOp):
    @check
    def to_sql(self, schema):
        return NotOp("not", EqOp(self.lhs, self.rhs).partial_eval(SQLang)).partial_eval(SQLang).to_sql(schema)
