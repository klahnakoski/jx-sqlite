# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from jx_base.expressions import SqlGtOp as _SqlGtOp
from mo_sql import SQL_GT, SQL


class SqlGtOp(_SqlGtOp, SQL):
    def __iter__(self):
        yield from self.lhs
        yield from SQL_GT
        yield from self.rhs
