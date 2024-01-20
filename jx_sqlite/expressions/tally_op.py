# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from jx_base.expressions import TallyOp as _TallyOp, ZERO, AndOp, FALSE, ONE, WhenOp, NotOp
from jx_sqlite.expressions._utils import multiop_to_sql, SQLang, SqlScript
from mo_json import JX_INTEGER
from mo_sql import SQL_PLUS, sql_call, sql_iso


class TallyOp(_TallyOp):

    def to_sql(self, schema):
        if len(self.terms) == 0:
            return ZERO.to_sql(schema)

        expr = [
            NotOp(t.missing(SQLang)).to_sql(schema)
            for t in self.terms
        ]

        return SqlScript(
            jx_type=JX_INTEGER,
            expr=SQL_PLUS.join(sql_iso(e.expr) for e in expr),
            frum=self,
            miss=FALSE,
            schema=schema
        )