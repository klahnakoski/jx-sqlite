# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import Expression
from jx_base.expressions.select_op import SelectOp as _SelectOp
from mo_sql import sql_list
from mo_sqlite.expressions.variable import Variable
from mo_sqlite.expressions.sql_alias_op import SqlAliasOp
from mo_sqlite.utils import sql_alias, SQL_SELECT, sql_iso, SQL_FROM, SQL


class SelectOp(_SelectOp, SQL):

    def __init__(self, frum, *terms, **kwargs):
        terms = [SqlAliasOp(t.var, t) if isinstance(t, Variable) else t for t in terms]+ [SqlAliasOp(k, v) for k, v in kwargs.items()]
        Expression.__init__(self, frum, *(t.value for t in terms))
        self.frum = frum
        self.terms = terms

    def __iter__(self):
        yield from SQL_SELECT
        yield from sql_list(self.terms)
        yield from SQL_FROM
        yield from sql_iso(self.frum)
