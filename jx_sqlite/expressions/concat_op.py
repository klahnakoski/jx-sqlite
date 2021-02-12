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

from jx_base.expressions import ConcatOp as ConcatOp_, TrueOp, ZERO, is_literal, ToStringOp, AddOp, AndOp, MissingOp, \
    ONE
from jx_sqlite.expressions._utils import SQLang, check
from jx_sqlite.expressions.length_op import LengthOp
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import quote_value, sql_call
from mo_dots import coalesce
from mo_json import STRING, T_STRING
from jx_sqlite.sqlite import (
    SQL,
    SQL_CASE,
    SQL_ELSE,
    SQL_EMPTY_STRING,
    SQL_END,
    SQL_NULL,
    SQL_THEN,
    SQL_WHEN,
    sql_iso,
    sql_list,
    sql_concat_text,
    ConcatSQL,
    SQL_PLUS,
    SQL_ONE,
    SQL_ZERO,
)


class ConcatOp(ConcatOp_):
    @check
    def to_sql(self, schema):
        default = self.default.to_sql(schema)
        if len(self.terms) == 0:
            return default
        len_sep = LengthOp(self.separator).partial_eval(SQLang)
        no_sep = is_literal(len_sep) and len_sep.value == 0
        sep = self.separator.partial_eval(SQLang).to_sql(schema).expr

        acc = []
        for t in self.terms:
            t = ToStringOp(t).partial_eval(SQLang)
            missing = t.missing(SQLang).partial_eval(SQLang)

            term = t.to_sql(schema).expr

            if no_sep:
                sep_term = term
            else:
                sep_term = sql_iso(sql_concat_text([sep, term]))

            if isinstance(missing, TrueOp):
                acc.append(SQL_EMPTY_STRING)
            elif missing:
                acc.append(ConcatSQL(
                    SQL_CASE,
                    SQL_WHEN,
                    missing.to_sql(schema).expr,
                    SQL_THEN,
                    SQL_EMPTY_STRING,
                    SQL_ELSE,
                    sep_term,
                    SQL_END
                ))
            else:
                acc.append(sep_term)

        if no_sep:
            sql = sql_concat_text(acc)
        else:
            sql = sql_call(
                "SUBSTR",
                sql_concat_text(acc),
                AddOp([ONE, LengthOp(self.separator)]).partial_eval(SQLang).to_sql(schema).expr
            )

        return SQLScript(
            data_type=T_STRING,
            expr=sql,
            frum=self,
            miss=AndOp(MissingOp(t) for t in self.terms),
            schema=schema,
        )
