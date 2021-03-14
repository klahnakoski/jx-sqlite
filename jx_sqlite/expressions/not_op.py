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

from jx_base.expressions import NotOp as NotOp_, MissingOp, FALSE
from jx_base.language import is_op
from jx_sqlite.expressions._utils import check, SQLang
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import sql_iso, SQL_NOT, ConcatSQL, SQL_IS_NOT_NULL
from mo_json.types import T_BOOLEAN


class NotOp(NotOp_):
    @check
    def to_sql(self, schema):
        term = self.partial_eval(SQLang)
        if is_op(term, NotOp):
            is_expr = term.term.to_sql(schema).frum.partial_eval(SQLang)
            if is_op(is_expr, MissingOp):
                exists = is_expr.expr
                return SQLScript(
                    data_type=T_BOOLEAN,
                    expr=ConcatSQL(exists.to_sql(schema), SQL_IS_NOT_NULL),
                    miss=FALSE,
                    frum=exists,
                    schema=schema
                )
            else:
                return SQLScript(
                    data_type=T_BOOLEAN,
                    miss=FALSE,
                    expr=ConcatSQL(SQL_NOT, sql_iso(is_expr.to_sql(schema))),
                    frum=is_expr,
                    schema=schema
                )
        else:
            return term.to_sql(schema)

