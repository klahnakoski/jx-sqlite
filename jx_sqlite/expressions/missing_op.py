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

from jx_base.expressions import MissingOp as MissingOp_, FALSE, TRUE, Variable, SelectOp
from jx_base.language import is_op
from jx_sqlite.expressions._utils import SQLang, check
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import (
    sql_iso,
    ConcatSQL,
    SQL_IS_NULL,
)
from mo_json.types import T_BOOLEAN


class MissingOp(MissingOp_):
    @check
    def to_sql(self, schema):
        sql = self.expr.partial_eval(SQLang).to_sql(schema)

        if is_op(sql.miss, MissingOp):
            return SQLScript(
                miss=FALSE,
                data_type=T_BOOLEAN,
                expr=ConcatSQL(sql.expr, SQL_IS_NULL),
                frum=self,
            )

        expr = sql.miss.to_sql(schema)
        return SQLScript(
            miss=FALSE,
            data_type=T_BOOLEAN,
            expr=expr,
            frum=sql.miss,
        )
