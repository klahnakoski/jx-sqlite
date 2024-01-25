# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from jx_base.expressions import SqlInnerJoinOp as _SqlInnerJoinOp
from mo_sqlite.utils import SQL, SQL_INNER_JOIN, SQL_ON


class SqlInnerJoinOp(_SqlInnerJoinOp, SQL):
    def __iter__(self):
        yield from self.frum
        for j in self.joins:
            yield from SQL_INNER_JOIN
            yield from j.join
            yield from SQL_ON
            yield from j.on
