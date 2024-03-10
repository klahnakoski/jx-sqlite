# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions.sql_and_op import SqlAndOp

from jx_base import FALSE, TRUE, NULL, is_op
from jx_base.expressions import SqlOrOp as _SqlOrOp
from mo_sql import NO_SQL, SQL_OR, SQL_OP, SQL_CP
from mo_sqlite.expressions._utils import SQL


class SqlOrOp(_SqlOrOp, SQL):
    def __iter__(self):
        op = NO_SQL
        for t in self.terms:
            yield from op
            op = SQL_OR
            yield from SQL_OP
            yield from t
            yield from SQL_CP

    def partial_eval(self, lang):
        terms = []
        ands = []
        for t in self.terms:
            simple = t.partial_eval(lang)
            if simple is FALSE or simple is NULL:
                continue
            elif simple is TRUE:
                return TRUE
            elif is_op(simple, SqlOrOp):
                terms.extend([tt for tt in simple.terms if tt not in terms])
            elif is_op(simple, SqlAndOp):
                ands.append(simple)
            elif simple not in terms:
                terms.append(simple)

        if ands:  # REMOVE TERMS THAT ARE MORE RESTRICTIVE THAN OTHERS
            for a in ands:
                for tt in a.terms:
                    if tt in terms:
                        break
                else:
                    terms.append(a)

        if len(terms) == 0:
            return FALSE
        if len(terms) == 1:
            return terms[0]
        return lang.SqlOrOp(*terms)
