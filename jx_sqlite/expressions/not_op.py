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

from jx_base.expressions import NotOp as NotOp_, MissingOp
from jx_base.language import is_op
from jx_sqlite.expressions._utils import check, SQLang
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.expressions.to_boolean_op import ToBooleanOp
from jx_sqlite.sqlite import sql_iso, SQL_NOT, ConcatSQL, SQL_IS_NOT_NULL
from mo_json.types import T_BOOLEAN



class NotOp(NotOp_):
    @check
    def to_sql(self, schema):
        term = NotOp(ToBooleanOp(self.term)).partial_eval(SQLang)
        if is_op(term, NotOp):
            if is_op(term.term, MissingOp):
                return SQLScript(
                    data_type=T_BOOLEAN,
                    expr=ConcatSQL(term.term.expr.to_sql(schema), SQL_IS_NOT_NULL),
                    miss=self.term.missing(SQLang),
                    frum=self,
                )
            else:
                return SQLScript(
                    data_type=T_BOOLEAN,
                    expr=ConcatSQL(SQL_NOT, sql_iso(term.term.to_sql(schema))),
                    miss=self.term.missing(SQLang),
                    frum=self,
                )
        else:
            return term.to_sql(schema)
