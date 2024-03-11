# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import SqlOrderByOp as _SqlOrderByOp
from jx_base.expressions.sql_order_by_op import OneOrder
from mo_future import extend
from mo_sql import SQL_SELECT, sql_iso, SQL_FROM, SQL_STAR, SQL_ORDERBY, NO_SQL, SQL_COMMA, SQL


class SqlOrderByOp(_SqlOrderByOp, SQL):
    def __iter__(self):
        yield from SQL_SELECT
        yield from SQL_STAR
        yield from SQL_FROM
        yield from sql_iso(self.frum)
        yield from SQL_ORDERBY
        sep = NO_SQL
        for o in self.order:
            yield from sep
            yield from o
            sep = SQL_COMMA


@extend(OneOrder)
def __iter__(self):
    yield from self.expr
    if self.direction:
        yield from self.direction
