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

from jx_base.expressions import ToStringOp as StringOp_
from jx_sqlite.expressions._utils import check
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import (
    SQL_CASE,
    SQL_ELSE,
    SQL_END,
    SQL_THEN,
    SQL_WHEN,
    sql_iso,
    ConcatSQL,
)
from jx_sqlite.sqlite import quote_value, sql_call
from mo_json import T_STRING, T_BOOLEAN, T_NUMBER_TYPES


class ToStringOp(StringOp_):
    @check
    def to_sql(self, schema):
        expr = self.term.to_sql(schema)
        if expr.type == T_STRING:
            return expr
        elif expr.type == T_BOOLEAN:
            return SQLScript(
                data_type=T_STRING,
                expr=ConcatSQL(
                    SQL_CASE,
                    SQL_WHEN,
                    sql_iso(expr.expr),
                    SQL_THEN,
                    quote_value("true"),
                    SQL_ELSE,
                    quote_value("false"),
                    SQL_END,
                ),
                frum=self,
                schema=schema,
            )
        elif expr.type in T_NUMBER_TYPES:
            return SQLScript(
                data_type=T_STRING,
                expr=sql_call(
                    "RTRIM",
                    sql_call(
                        "RTRIM",
                        ConcatSQL("CAST", sql_iso(expr.expr, " as TEXT")),
                        quote_value("0"),
                    ),
                    quote_value("."),
                ),
                frum=self,
                schema=schema,
            )
        else:
            return SQLScript(
                data_type=T_STRING,
                expr=ConcatSQL("CAST", sql_iso(expr.expr, " as TEXT")),
                frum=self,
                schema=schema,
            )
