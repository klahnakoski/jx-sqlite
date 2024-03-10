# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import CoalesceOp as _CoalesceOp, is_literal, NULL
from mo_sqlite.expressions._utils import SQL
from mo_sql import sql_call


class CoalesceOp(_CoalesceOp, SQL):
    def partial_eval(self, lang):
        terms = []
        for t in self.terms:
            simple = t.partial_eval(lang)
            if simple is NULL:
                pass
            elif is_literal(simple):
                terms.append(simple)
                break
            else:
                terms.append(simple)

        if len(terms) == 0:
            return NULL
        elif len(terms) == 1:
            return terms[0]
        else:
            return lang.CoalesceOp(*terms)

    def __iter__(self):
        yield from sql_call("COALESCE", *self.terms)
