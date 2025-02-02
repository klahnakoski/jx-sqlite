# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from jx_base.expressions import SqlLteOp as _SqlLteOp
from mo_sql import SQL_LE, SQL


class SqlLteOp(_SqlLteOp, SQL):
    def __iter__(self):
        yield from self.lhs
        yield from SQL_LE
        yield from self.rhs
