# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from jx_base.expressions import LimitOp as LimitOp_, SqlScript
from mo_sqlite import SQLang, SqlScript

from mo_sql import SQL_LIMIT, ConcatSQL


class LimitOp(LimitOp_):
    def to_sql(self, schema) -> SqlScript:
        frum = self.frum.partial_eval(SQLang).to_sql(schema)
        amount = self.amount.partial_eval(SQLang).to_sql(schema)
        return SqlScript(data_type=frum.type, expr=ConcatSQL(frum, SQL_LIMIT, amount), frum=self, schema=schema)

    def apply(self, container):
        amount = self.amount.partial_eval(SQLang)
        return LimitOp(self.frum.apply(container), amount)
