# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from jx_base.expressions import SqlEqOp as _SqlEqOp
from mo_sql import SQL_EQ, SQL


class SqlEqOp(_SqlEqOp, SQL):
    def __iter__(self):
        yield from self.lhs
        yield from SQL_EQ
        yield from self.rhs

