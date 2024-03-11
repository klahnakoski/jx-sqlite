# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base import NULL, TRUE, FALSE, is_op
from jx_base.expressions import CaseOp as _CaseOp, ZERO, is_literal
from jx_base.expressions import WhenOp as _WhenOp
from mo_imports import export
from mo_json import JX_BOOLEAN
from mo_sql import (
    SQL_CASE,
    SQL_ELSE,
    SQL_END,
    SQL_THEN,
    SQL_WHEN,
    SQL,
)
from mo_sqlite.expressions.sql_and_op import SqlAndOp
from mo_sqlite.expressions.sql_not_op import NotOp as SqlNotOp
from mo_sqlite.expressions.sql_or_op import SqlOrOp


class WhenOp(_WhenOp, SQL):
    def __iter__(self):
        yield from SQL_WHEN
        yield from self.when
        yield from SQL_THEN
        yield from self.then


class CaseOp(_CaseOp, SQL):
    def __init__(self, *whens, _else=NULL):
        super().__init__(*whens, _else)

    def __iter__(self):
        yield from SQL_CASE
        for w in self.whens:
            yield from w
        if self.els_ is not NULL:
            yield from SQL_ELSE
            yield from self.els_
        yield from SQL_END

    def partial_eval(self, lang):
        if self.jx_type is JX_BOOLEAN:
            nots = []
            ors = []
            for w in self.whens:
                ors.append(SqlAndOp(*nots, w.when, w.then))
                nots.append(SqlNotOp(w.when))
            ors.append(SqlAndOp(*nots, self.els_))
            return SqlOrOp(*ors).partial_eval(lang)

        whens = []
        _else = self._else.partial_eval(lang)
        for w in self.whens:
            when = w.when.partial_eval(lang)
            if is_literal(when):
                if when is ZERO or when is FALSE or when.missing(lang) is TRUE:
                    pass
                else:
                    _else = w.then.partial_eval(lang)
                    break
            else:
                then = w.then.partial_eval(lang)
                if is_op(then, CaseOp):
                    for ww in then.whens:
                        whens.append(lang.WhenOp(SqlAndOp(when, ww.when).partial_eval(lang), then=ww.then))
                        if then.els_ is not NULL:
                            whens.append(lang.WhenOp(when, then.els_))
                elif is_op(then, WhenOp):
                    whens.append(lang.WhenOp(SqlAndOp(when, then.when).partial_eval(lang), then=then.then))
                    whens.append(lang.WhenOp(when, then.els_))
                else:
                    whens.append(lang.WhenOp(when, then=then))

        if len(whens) == 0:
            return _else
        return lang.CaseOp(*whens, _else=_else)


export("mo_sqlite.expressions.sql_script", "SqlCaseOp", CaseOp)
export("mo_sqlite.expressions.sql_script", "SqlWhenOp", WhenOp)
