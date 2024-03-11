# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from jx_base.expressions import SqlLtOp as _SqlLtOp
from mo_sql import SQL_CP, SQL_OP, SQL_LT, SQL


class SqlLtOp(_SqlLtOp, SQL):
    def __iter__(self):
        yield from SQL_OP
        yield from self.lhs
        yield from SQL_CP
        yield from SQL_LT
        yield from SQL_OP
        yield from self.rhs
        yield from SQL_CP
