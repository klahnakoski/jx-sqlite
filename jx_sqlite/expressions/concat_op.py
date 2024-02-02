# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions.null_op import NULL

from jx_base.expressions.literal import Literal

from jx_base.expressions import (
    ConcatOp as ConcatOp_,
    TrueOp,
    is_literal,
    ToTextOp,
    AddOp,
    AndOp,
    MissingOp,
    ONE,
)
from jx_sqlite.expressions._utils import SQLang, check
from jx_sqlite.expressions.length_op import LengthOp
from jx_sqlite.expressions.sql_script import SqlScript
from mo_sqlite import (
    SQL_CASE,
    SQL_ELSE,
    SQL_EMPTY_STRING,
    SQL_END,
    SQL_THEN,
    SQL_WHEN,
    sql_iso,
    sql_concat_text,
    ConcatSQL,
)
from mo_sqlite import sql_call
from mo_json import JX_TEXT


class ConcatOp(ConcatOp_):
    @check
    def to_sql(self, schema):
        if len(self.terms) == 0:
            return NULL.to_sql(schema)
        len_sep = LengthOp(self.separator).partial_eval(SQLang)
        no_sep = len_sep is NULL
        if no_sep:
            sep = None
        else:
            sep = self.separator.partial_eval(SQLang).to_sql(schema).expr

        acc = []
        for t in self.terms:
            t = ToTextOp(t).partial_eval(SQLang)
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
                    SQL_END,
                ))
            else:
                acc.append(sep_term)

        if no_sep:
            sql = sql_concat_text(acc)
        else:
            sql = sql_call(
                "SUBSTR",
                sql_concat_text(acc),
                AddOp(ONE, LengthOp(self.separator), nulls=False).partial_eval(SQLang).to_sql(schema).expr,
            )

        return SqlScript(
            jx_type=JX_TEXT,
            expr=sql,
            frum=self,
            miss=AndOp(*(MissingOp(t) for t in self.terms), nulls=False),
            schema=schema,
        )
