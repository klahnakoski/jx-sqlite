# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import ToBooleanOp as ToBooleanOp_
from jx_sqlite.expressions._utils import SQLang, check
from mo_json import JX_BOOLEAN


class ToBooleanOp(ToBooleanOp_):
    @check
    def to_sql(self, schema):
        term = self.term.partial_eval(SQLang)
        if term.jx_type == JX_BOOLEAN:
            return term.to_sql(schema)
        else:
            return term.exists().to_sql(schema)
