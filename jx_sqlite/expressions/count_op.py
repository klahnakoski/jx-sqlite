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

from jx_base.expressions import CountOp as CountOp_, NotOp
from jx_sqlite.expressions import AndOp
from jx_sqlite.expressions._utils import SQLang, check
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import JoinSQL, SQL_SUM, sql_iso
from mo_json import T_INTEGER


class CountOp(CountOp_):
    @check
    def to_sql(self, schema):
        acc = []
        miss = []
        for term in self.terms:
            m = NotOp(term.missing(SQLang)).partial_eval(SQLang)
            miss.append(m)
            acc.append(sql_iso(m.to_sql(schema).expr))
        return SQLScript(
            data_type=T_INTEGER,
            expr=JoinSQL(SQL_SUM, acc),
            frum=self,
            miss=AndOp(miss),
            schema=schema
        )
