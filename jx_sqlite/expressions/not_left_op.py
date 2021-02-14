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

from jx_base.expressions import NotLeftOp as NotLeftOp_, GteOp, LengthOp
from jx_sqlite.expressions._utils import check, SQLang, SQLScript, OrOp
from jx_sqlite.sqlite import sql_call, SQL_ZERO, ConcatSQL, SQL_ONE, SQL_PLUS
from mo_json import T_STRING


class NotLeftOp(NotLeftOp_):
    @check
    def to_sql(self, schema):
        v = self.value.to_sql(schema)
        start = ConcatSQL(
            sql_call("MAX", SQL_ZERO, self.length.to_sql(schema)), SQL_PLUS, SQL_ONE
        )

        expr = sql_call("SUBSTR", v, start)
        return SQLScript(
            data_type=T_STRING,
            expr=expr,
            frum=self,
            missing=OrOp([
                self.value.missing(SQLang),
                self.length.missing(SQLang),
                GteOp([self.length, LengthOp(self.value)]),
            ]),
            schema=schema,
        )
