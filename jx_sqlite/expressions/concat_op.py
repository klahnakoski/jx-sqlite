# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base import TRUE, FALSE
from jx_base.expressions import SqlScript, ConcatOp as ConcatOp_, ToTextOp, AddOp, AndOp, MissingOp, ONE
from jx_base.expressions.null_op import NULL
from jx_sqlite.expressions.length_op import LengthOp
from mo_json import JX_TEXT
from mo_sqlite import SQL_EMPTY_STRING
from mo_sqlite import SQLang
from jx_sqlite.expressions._utils import check
from mo_sqlite.expressions import SqlCoalesceOp, SqlConcatOp, SqlSubstrOp, SqlCaseOp, SqlWhenOp, SqlScript


class ConcatOp(ConcatOp_):
    @check
    def to_sql(self, schema) -> SqlScript:
        if len(self.terms) == 0:
            return NULL
        len_sep = LengthOp(self.separator).partial_eval(SQLang)
        no_sep = len_sep is NULL
        if no_sep:
            sep = None
        else:
            sep = self.separator.partial_eval(SQLang).to_sql(schema).expr

        acc = []
        for t in self.terms:
            t = ToTextOp(t).partial_eval(SQLang)

            term = t.to_sql(schema).expr
            # TODO - use this
            missing = term.missing(SQLang).partial_eval(SQLang).to_sql(schema).expr

            if no_sep:
                sep_term = term
            else:
                sep_term = SqlConcatOp(sep, term)

            if missing is TRUE:
                pass
            elif missing is FALSE:
                acc.append(sep_term)
            else:
                acc.append(SqlCaseOp(SqlWhenOp(missing, SQL_EMPTY_STRING), _else=sep_term))

        if no_sep:
            sql = SqlConcatOp(*acc)
        else:
            sql = SqlSubstrOp(
                SqlConcatOp(*acc),
                AddOp(ONE, LengthOp(self.separator), nulls=False).partial_eval(SQLang).to_sql(schema).expr,
            )
        sql = sql.partial_eval(SQLang)

        return SqlScript(
            jx_type=JX_TEXT,
            expr=sql,
            frum=self,
            miss=AndOp(*(MissingOp(t) for t in self.terms), nulls=False),
            schema=schema,
        )
