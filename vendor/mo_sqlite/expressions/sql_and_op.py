# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base import TRUE, FALSE, is_op
from jx_base.expressions import SqlAndOp as _SqlAndOp
from mo_json import JX_BOOLEAN
from mo_sql import NO_SQL, SQL_LP, SQL_RP, SQL_AND, SQL
from mo_sqlite.expressions.sql_not_op import NotOp as SqlNotOp
from mo_sqlite.expressions.sql_or_op import SqlOrOp
from mo_sqlite.expressions.sql_script import SqlScript, SQLang


class SqlAndOp(_SqlAndOp, SQL):
    def __iter__(self):
        op = NO_SQL
        for t in self.terms:
            yield from op
            op = SQL_AND
            yield from SQL_LP
            yield from t
            yield from SQL_RP

    def to_sql(self, schema):
        return SqlScript(
            jx_type = JX_BOOLEAN,
            expr = SqlAndOp(*(t.to_sql(schema) for t in self.terms)).partial_eval(SQLang),
            frum = self,
            miss = FALSE,
            schema=schema
        )

    def partial_eval(self, lang):
        or_terms = [[]]  # LIST OF TUPLES FOR or-ing and and-ing
        for i, t in enumerate(self.terms):
            simple = t.partial_eval(lang)
            if simple is TRUE:
                continue
            elif simple is FALSE:
                return FALSE
            elif is_op(simple, SqlAndOp):
                for and_terms in or_terms:
                    for tt in simple.terms:
                        if tt in and_terms:
                            continue
                        if SqlNotOp(tt).partial_eval(lang) in and_terms:
                            or_terms.remove(and_terms)
                            break
                        and_terms.append(tt)
                continue
            elif is_op(simple, SqlOrOp):
                or_terms = [
                    and_terms + ([o] if o not in and_terms else [])
                    for o in simple.terms
                    for and_terms in or_terms
                    if SqlNotOp(o).partial_eval(lang) not in and_terms
                ]
                continue
            for and_terms in list(or_terms):
                inv = lang.NotOp(simple).partial_eval(lang)
                if inv in and_terms:
                    or_terms.remove(and_terms)
                elif simple not in and_terms:
                    and_terms.append(simple)
        if len(or_terms) == 0:
            return FALSE
        elif len(or_terms) == 1:
            and_terms = or_terms[0]
            if len(and_terms) == 0:
                return TRUE
            elif len(and_terms) == 1:
                return and_terms[0]
            else:
                return lang.SqlAndOp(*and_terms)

        return SqlOrOp(
            *(lang.AndOp(*and_terms) if len(and_terms) > 1 else and_terms[0] for and_terms in or_terms)
        ).partial_eval(lang)

