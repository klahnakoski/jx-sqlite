# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import ExistsOp as ExistsOp_, FALSE, SqlScript
from mo_sqlite import SQLang
from mo_sqlite import check
from jx_sqlite.expressions._utils import SqlScript
from mo_json import JX_BOOLEAN


class ExistsOp(ExistsOp_):
    @check
    def to_sql(self, schema) -> SqlScript:
        sql = self.expr.partial_eval(SQLang).to_sql(schema)
        return SqlScript(jx_type=JX_BOOLEAN, expr=sql, frum=self, miss=FALSE, schema=schema)
