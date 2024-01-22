# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from jx_base.expressions import SqlEqOp as SqlEqOp_
from mo_sqlite import SQL_EQ, SQL


class SqlEqOp(SqlEqOp_, SQL):
    def __iter__(self):
        yield from self.lhs.sql
        yield from SQL_EQ
        yield from self.rhs.sql
