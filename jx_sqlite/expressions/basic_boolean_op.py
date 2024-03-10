# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import BasicBooleanOp as _BasicBooleanOp, SqlScript, SqlScript
from mo_sqlite import SQLang
from mo_sqlite import check


class BasicBooleanOp(_BasicBooleanOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        return self.partial_eval(SQLang).to_sql(schema)
