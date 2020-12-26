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

from jx_base.expressions import BasicEqOp as BasicEqOp_, FALSE
from jx_sqlite.expressions._utils import check, SQLang
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import sql_iso, SQL_EQ
from mo_json.types import T_BOOLEAN
from mo_sql import ConcatSQL


class BasicEqOp(BasicEqOp_):
    @check
    def to_sql(self, schema):
        return SQLScript(
            data_type=T_BOOLEAN,
            expr=ConcatSQL(
                sql_iso(self.rhs.partial_eval(SQLang).to_sql(schema)),
                SQL_EQ,
                sql_iso(self.lhs.partial_eval(SQLang).to_sql(schema)),
            ),
            frum=self,
            miss=FALSE,
        )
