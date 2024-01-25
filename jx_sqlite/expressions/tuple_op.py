# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import TupleOp as _TupleOp, SelectOp
from jx_base.expressions.select_op import SelectOne
from jx_sqlite.expressions._utils import SQLang, check
from mo_dots import Null


class TupleOp(_TupleOp):
    @check
    def to_sql(self, schema):
        output = SelectOp(Null, *(SelectOne(str(i), term) for i, term in enumerate(self.terms))).partial_eval(SQLang).to_sql(schema)
        output.frum = self
        return output
